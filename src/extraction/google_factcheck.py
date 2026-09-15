"""Extraction for the Google Fact Check Tools API (Claims Search).

Mirrors notebooks/google_factcheck_extraction.ipynb's collection logic as
importable, orchestration-friendly functions - needed so this source can run
as an Airflow task instead of only interactively in a notebook.
"""

from __future__ import annotations

import logging
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

CLAIMS_SEARCH_URL = "https://factchecktools.googleapis.com/v1alpha1/claims:search"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Projet12-FactCheckCollector/1.0)"}
REQUEST_DELAY_SECONDS = 1.0

DEFAULT_SITES = [
    "politifact.com", "snopes.com", "factcheck.org", "fullfact.org",
    "leadstories.com", "factcheck.afp.com", "reuters.com", "apnews.com",
    "africacheck.org", "boomlive.in", "altnews.in", "correctiv.org",
    "maldita.es", "newtral.es", "rappler.com", "verafiles.org",
]
DEFAULT_MAX_AGE_DAYS = 180
DEFAULT_PER_SITE_MAX_RESULTS = 100


def _api_key() -> str:
    load_dotenv()
    api_key = os.environ.get("GOOGLE_FACTCHECK_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GOOGLE_FACTCHECK_API_KEY. Set it in a .env file at the project root.")
    return api_key


def search_claims(
    api_key: str,
    query: str | None = None,
    language_code: str | None = None,
    page_size: int = 20,
    page_token: str | None = None,
    max_age_days: int | None = None,
    review_publisher_site_filter: str | None = None,
) -> dict:
    """Call the Google Fact Check Tools claims:search endpoint once.

    Either query or review_publisher_site_filter must be provided (the API requires one).
    """
    if not query and not review_publisher_site_filter:
        raise ValueError("Provide at least one of query or review_publisher_site_filter")

    params = {"key": api_key, "pageSize": page_size}
    if query:
        params["query"] = query
    if language_code:
        params["languageCode"] = language_code
    if page_token:
        params["pageToken"] = page_token
    if max_age_days:
        params["maxAgeDays"] = max_age_days
    if review_publisher_site_filter:
        params["reviewPublisherSiteFilter"] = review_publisher_site_filter

    response = requests.get(CLAIMS_SEARCH_URL, params=params, timeout=30)
    try:
        response.raise_for_status()
    except requests.HTTPError as error:
        raise requests.HTTPError(f"{error}\nResponse body: {response.text}") from error
    return response.json()


def collect_claims(
    api_key: str,
    review_publisher_site_filter: str,
    max_results: int = DEFAULT_PER_SITE_MAX_RESULTS,
    page_size: int = 20,
    max_age_days: int | None = DEFAULT_MAX_AGE_DAYS,
) -> list[dict]:
    """Collect raw claim records for a single publisher site, following pagination."""
    all_claims: list[dict] = []
    page_token = None

    while True:
        payload = search_claims(
            api_key,
            page_size=page_size,
            page_token=page_token,
            max_age_days=max_age_days,
            review_publisher_site_filter=review_publisher_site_filter,
        )
        claims = payload.get("claims", [])
        all_claims.extend(claims)

        page_token = payload.get("nextPageToken")
        time.sleep(REQUEST_DELAY_SECONDS)

        if not page_token or len(all_claims) >= max_results or not claims:
            break

    logger.info("Collected %d claim(s) from %s", len(all_claims), review_publisher_site_filter)
    return all_claims


def claims_to_dataframe(raw_claims: list[dict]) -> pd.DataFrame:
    """Flatten raw claims:search results into one row per claim review."""
    rows = []
    for claim in raw_claims:
        for review in claim.get("claimReview", []):
            publisher = review.get("publisher", {})
            rows.append(
                {
                    "claim_text": claim.get("text"),
                    "claimant": claim.get("claimant"),
                    "claim_date": claim.get("claimDate"),
                    "review_publisher_name": publisher.get("name"),
                    "review_publisher_site": publisher.get("site"),
                    "review_url": review.get("url"),
                    "review_title": review.get("title"),
                    "review_date": review.get("reviewDate"),
                    "textual_rating": review.get("textualRating"),
                    "language_code": review.get("languageCode"),
                }
            )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["review_url"]).reset_index(drop=True)
    return df


def fetch_article_text(page_url: str, min_length: int = 200) -> str | None:
    """Fetch a review article page and extract its main body text.

    Returns None on a fetch failure or if the extracted text looks too short
    to be real article content (e.g. a paywall or JS-only page).
    """
    try:
        response = requests.get(page_url, headers=HTTP_HEADERS, timeout=15)
        response.raise_for_status()
    except requests.RequestException:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    article = soup.find("article") or soup.find("main") or soup.find(attrs={"role": "main"})
    paragraphs = (article or soup).find_all("p")

    text = "\n".join(p.get_text(" ", strip=True) for p in paragraphs)
    text = re.sub(r"\n{2,}", "\n", text).strip()

    return text if len(text) >= min_length else None


def extract(
    output_path: Path,
    *,
    sites: list[str] | None = None,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    per_site_max_results: int = DEFAULT_PER_SITE_MAX_RESULTS,
    fetch_article_bodies: bool = True,
) -> Path:
    """Run the full extraction (collect + article text) and write the raw CSV. Returns output_path."""
    sites = sites or DEFAULT_SITES
    api_key = _api_key()

    logger.info("Extracting from %d publisher site(s)", len(sites))
    raw_claims: list[dict] = []
    for site in sites:
        raw_claims.extend(
            collect_claims(api_key, site, max_results=per_site_max_results, max_age_days=max_age_days)
        )
    logger.info("Collected %d raw claim(s) across %d publisher(s)", len(raw_claims), len(sites))

    df = claims_to_dataframe(raw_claims)
    logger.info("Flattened into %d unique claim review(s)", len(df))

    if fetch_article_bodies and not df.empty:
        logger.info("Fetching article body text for %d review(s)", len(df))
        article_texts = []
        for review_url in df["review_url"]:
            article_texts.append(fetch_article_text(review_url))
            time.sleep(REQUEST_DELAY_SECONDS)
        df["article_text"] = article_texts
    else:
        df["article_text"] = None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Wrote %d row(s) to %s", len(df), output_path)
    return output_path
