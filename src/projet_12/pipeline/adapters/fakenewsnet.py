"""Adapter for FakeNewsNet extractions (data/FakeNewsNet/*.csv).

Included mainly to demonstrate that a second, structurally different source
(one file per label, no article body or rating text) plugs into the same
pipeline: only this module changes, cleaning/schema/export are untouched.
"""

from __future__ import annotations

import pandas as pd


def adapt(df_raw: pd.DataFrame, *, label: str) -> pd.DataFrame:
    """Map a raw FakeNewsNet CSV (id, news_url, title, tweet_ids) to the canonical schema.

    FakeNewsNet ships one file per label (e.g. gossipcop_fake.csv / gossipcop_real.csv):
    the label isn't a column, so the caller passes it in based on which file was read.
    """
    if label not in ("fake", "true"):
        raise ValueError(f"fakenewsnet adapter: label must be 'fake' or 'true', got {label!r}")

    missing = [column for column in ("news_url", "title") if column not in df_raw.columns]
    if missing:
        raise ValueError(f"fakenewsnet adapter: missing expected raw column(s) {missing}")

    df = pd.DataFrame(
        {
            "claim_text": df_raw["title"],
            "article_title": df_raw["title"],
            "article_text": None,
            "article_url": df_raw["news_url"],
            "claimant": None,
            "claim_date": None,
            "published_at": None,
            "publisher_name": None,
            "publisher_site": None,
            "language_code": "en",
            "textual_rating_raw": label,
        }
    )
    df["label"] = label
    df["source"] = "fakenewsnet"
    df["image_url"] = None
    return df
