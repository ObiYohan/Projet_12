"""Extraction for the AFP Factuel Bluesky RSS feed.

Mirrors notebooks/bluesky_afp_extraction.ipynb's collection logic as
importable, orchestration-friendly functions - needed so this source can run
as an Airflow task instead of only interactively in a notebook.

The feed always returns the same last ~30 posts, so extract() is
incremental/append-only: it skips posts (by guid) already present in
output_path, and skips downloading an image that's already on disk. Running
it repeatedly in a short window (e.g. the DAG firing twice in a row) must not
re-fetch or re-download anything already collected.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin
from xml.etree import ElementTree

import pandas as pd
import requests
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests

logger = logging.getLogger(__name__)

DEFAULT_RSS_URL = "https://bsky.app/profile/did:plc:4ks5wkubjfcbgxvphqkd3wxm/rss"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Projet12-FactCheckCollector/1.0)"}
REQUEST_DELAY_SECONDS = 1.0
DEFAULT_MIN_ARTICLE_TEXT_LENGTH = 200

# u.afp.com / factuel.afp.com sit behind an Akamai WAF that blocks plain `requests`
# calls by TLS fingerprint (not just headers) - curl_cffi impersonates a real Chrome
# TLS handshake, which gets through where `requests` gets a 403.
BROWSER_IMPERSONATION = "chrome124"

# Resolved from this file's location (not the current working directory), so it stays
# correct whether this runs via `uv run`, a notebook, or an Airflow container with a
# different working directory.
DEFAULT_IMAGES_DIR = Path(__file__).resolve().parents[2] / "data" / "bluesky_afp" / "images"


def parse_rss_feed(feed_url: str) -> pd.DataFrame:
    """Fetch and parse a Bluesky RSS feed into one row per post."""
    response = requests.get(feed_url, headers=HTTP_HEADERS, timeout=30)
    response.raise_for_status()

    root = ElementTree.fromstring(response.content)
    rows = []
    for item in root.iter("item"):
        post_text = (item.findtext("description") or "").strip()
        urls_in_text = re.findall(r"https?://\S+", post_text)

        rows.append(
            {
                "post_url": item.findtext("link"),
                "post_text": post_text,
                "pub_date": item.findtext("pubDate"),
                "guid": item.findtext("guid"),
                # The article link is usually the last URL mentioned in the post text.
                "article_url": urls_in_text[-1] if urls_in_text else None,
            }
        )

    df = pd.DataFrame(rows)
    df["pub_date"] = pd.to_datetime(df["pub_date"], utc=True, errors="coerce")
    return df


def fetch_article(url, min_length: int = DEFAULT_MIN_ARTICLE_TEXT_LENGTH) -> tuple[int | None, str | None, str | None]:
    """Fetch an article page. Returns (status_code, article_text, image_url).

    status_code is None on a network-level failure (timeout, DNS failure, WAF block
    before any response, ...). article_text is None if the page isn't a usable 200,
    or its extracted text is too short. image_url is None if no og:image/twitter:image
    meta tag is found.
    """
    try:
        response = curl_requests.get(
            url, headers=HTTP_HEADERS, timeout=15, allow_redirects=True, impersonate=BROWSER_IMPERSONATION
        )
    except curl_requests.errors.RequestsError:
        return None, None, None

    if response.status_code != 200:
        return response.status_code, None, None

    soup = BeautifulSoup(response.text, "html.parser")

    image_url = None
    for meta_name in ("og:image", "twitter:image", "twitter:image:src"):
        tag = soup.find("meta", attrs={"property": meta_name}) or soup.find("meta", attrs={"name": meta_name})
        if tag and tag.get("content"):
            image_url = urljoin(response.url, tag["content"])
            break

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    article = soup.find("article") or soup.find("main") or soup.find(attrs={"role": "main"})
    paragraphs = (article or soup).find_all("p")

    text = "\n".join(p.get_text(" ", strip=True) for p in paragraphs)
    text = re.sub(r"\n{2,}", "\n", text).strip()

    return response.status_code, (text if len(text) >= min_length else None), image_url


def download_image(image_url: str, dest_path: Path) -> bool:
    """Download a single image to dest_path. Returns True on success."""
    try:
        response = curl_requests.get(image_url, headers=HTTP_HEADERS, timeout=15, impersonate=BROWSER_IMPERSONATION)
        response.raise_for_status()
        dest_path.write_bytes(response.content)
        return True
    except curl_requests.errors.RequestsError:
        return False


def _post_id_from_guid(guid: str) -> str:
    """Extract a short, filename-safe id from an AT-URI guid (.../app.bsky.feed.post/<id>)."""
    return guid.rsplit("/", 1)[-1]


def extract(
    output_path: Path,
    *,
    feed_url: str = DEFAULT_RSS_URL,
    images_dir: Path = DEFAULT_IMAGES_DIR,
    min_length: int = DEFAULT_MIN_ARTICLE_TEXT_LENGTH,
) -> Path:
    """Fetch new posts from the feed, scrape their linked article, and append to output_path.

    Posts already present in output_path (by guid) are skipped entirely - no re-fetch,
    no re-download - since the feed always returns the same last ~30 posts and this may
    run repeatedly in a short window (e.g. an hourly DAG schedule).
    """
    images_dir.mkdir(parents=True, exist_ok=True)

    df_feed = parse_rss_feed(feed_url)
    logger.info("Feed has %d post(s)", len(df_feed))

    if output_path.exists():
        df_existing = pd.read_csv(output_path, dtype=str)
        known_guids = set(df_existing["guid"].dropna())
    else:
        df_existing = pd.DataFrame()
        known_guids = set()

    df_new = df_feed[~df_feed["guid"].isin(known_guids)].reset_index(drop=True)
    logger.info("%d new post(s) to process (%d already known)", len(df_new), len(known_guids))

    if df_new.empty:
        logger.info("Nothing new: leaving %s untouched", output_path)
        return output_path

    statuses, texts, image_urls, local_paths = [], [], [], []
    for _, row in df_new.iterrows():
        article_url = row["article_url"]

        if pd.isna(article_url):
            statuses.append(None)
            texts.append(None)
            image_urls.append(None)
            local_paths.append(None)
            continue

        status, text, image_url = fetch_article(article_url, min_length=min_length)
        statuses.append(status)
        texts.append(text)
        image_urls.append(image_url)

        local_path = None
        if image_url:
            extension = Path(image_url.split("?")[0]).suffix or ".jpg"
            dest_path = images_dir / f"{_post_id_from_guid(row['guid'])}{extension}"
            if dest_path.exists() or download_image(image_url, dest_path):
                local_path = str(dest_path)
        local_paths.append(local_path)

        time.sleep(REQUEST_DELAY_SECONDS)

    df_new["http_status"] = statuses
    df_new["article_text"] = texts
    df_new["image_url"] = image_urls
    df_new["local_image_path"] = local_paths

    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(output_path, index=False)
    logger.info("Wrote %d row(s) (%d new) to %s", len(df_combined), len(df_new), output_path)
    return output_path
