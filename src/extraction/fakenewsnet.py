"""Extraction for FakeNewsNet CSV datasets (data/FakeNewsNet/*.csv).

Mirrors notebooks/fakenewsnet_extraction.ipynb's collection logic as
importable, orchestration-friendly functions - needed so this source can run
as an Airflow task instead of only interactively in a notebook.

FakeNewsNet ships one file per label (id, news_url, title, tweet_ids), so
`extract()` handles a single (csv_filename, label) pair; extraction.EXTRACTORS
registers one entry per file via functools.partial.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Projet12-FactCheckCollector/1.0)"}
REQUEST_DELAY_SECONDS = 1.0
DEFAULT_MAX_ROWS = 200

# Statuses that mean "this link no longer exists", beyond an outright connection failure.
DEAD_LINK_STATUSES = {404, 410}

# Resolved from this file's location (not the current working directory), so it stays
# correct whether this runs via `uv run`, a notebook, or an Airflow container with a
# different working directory.
DEFAULT_RAW_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "FakeNewsNet"

ORIGINAL_COLUMNS = ["id", "news_url", "title", "tweet_ids"]


def normalize_url(url) -> str | None:
    """Add a scheme if missing; return None for empty/missing input."""
    if pd.isna(url):
        return None
    candidate = str(url).strip()
    if not candidate:
        return None
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://", candidate):
        candidate = f"https://{candidate}"
    return candidate


def fetch_article(url, min_length: int = 200) -> tuple[int | None, str | None]:
    """Fetch a news article page. Returns (status_code, article_text).

    status_code is None on a network-level failure (timeout, DNS failure, ...) -
    as dead as a 404 in practice, but not an HTTP status per se. article_text is
    None whenever the page isn't a usable 200, or its extracted text is too short.
    """
    normalized_url = normalize_url(url)
    if normalized_url is None:
        return None, None

    try:
        response = requests.get(normalized_url, headers=HTTP_HEADERS, timeout=15, allow_redirects=True)
    except requests.RequestException:
        return None, None

    if response.status_code != 200:
        return response.status_code, None

    soup = BeautifulSoup(response.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    article = soup.find("article") or soup.find("main") or soup.find(attrs={"role": "main"})
    paragraphs = (article or soup).find_all("p")

    text = "\n".join(p.get_text(" ", strip=True) for p in paragraphs)
    text = re.sub(r"\n{2,}", "\n", text).strip()

    return response.status_code, (text if len(text) >= min_length else None)


def extract(
    output_path: Path,
    *,
    csv_filename: str,
    label: str,
    max_rows: int | None = DEFAULT_MAX_ROWS,
    raw_data_dir: Path = DEFAULT_RAW_DATA_DIR,
    clean_source: bool = True,
) -> Path:
    """Fetch article text for one FakeNewsNet file, filter dead links, write the raw CSV.

    Only a random sample of up to max_rows is checked per run (checking the full file
    at 1 req/s would take hours). If clean_source is True, rows confirmed dead this run
    are removed from the source CSV (raw_data_dir / csv_filename) so future runs don't
    re-check them - rows outside this run's sample are left untouched, not discarded.
    """
    full_df = pd.read_csv(raw_data_dir / csv_filename)
    logger.info("Read %d row(s) from %s", len(full_df), csv_filename)

    if max_rows is not None and len(full_df) > max_rows:
        sample_df = full_df.sample(n=max_rows)
    else:
        sample_df = full_df

    sample_df = sample_df.copy()
    sample_df["label"] = label

    statuses, texts = [], []
    for url in sample_df["news_url"]:
        status, text = fetch_article(url)
        statuses.append(status)
        texts.append(text)
        time.sleep(REQUEST_DELAY_SECONDS)

    sample_df["http_status"] = statuses
    sample_df["article_text"] = texts

    is_dead_link = sample_df["http_status"].isna() | sample_df["http_status"].isin(DEAD_LINK_STATUSES)
    dead_index = sample_df.index[is_dead_link]
    alive_df = sample_df[~is_dead_link].reset_index(drop=True)
    logger.info("%s: checked %d, dropped %d dead link(s), kept %d", csv_filename, len(sample_df), len(dead_index), len(alive_df))

    if clean_source:
        # Drop only the rows confirmed dead this run; rows never sampled stay untouched.
        remaining_df = full_df.drop(index=dead_index)
        source_path = raw_data_dir / csv_filename
        remaining_df[ORIGINAL_COLUMNS].to_csv(source_path, index=False)
        logger.info("Cleaned %s: %d/%d row(s) kept", source_path, len(remaining_df), len(full_df))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    alive_df.to_csv(output_path, index=False)
    logger.info("Wrote %d row(s) to %s", len(alive_df), output_path)
    return output_path
