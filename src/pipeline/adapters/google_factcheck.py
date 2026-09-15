"""Adapter for the Google Fact Check Tools extraction (notebooks/google_factcheck_extraction.ipynb).

Maps the raw claims:search output columns to the canonical intermediate
names. Label normalization is intentionally left to the common cleaning
step (pipeline.cleaning.map_textual_rating_to_label), even though
the extraction notebook also computes one for quick exploration: the
pipeline is the single source of truth for that mapping.
"""

from __future__ import annotations

import pandas as pd

_RAW_TO_CANONICAL = {
    "claim_text": "claim_text",
    "claimant": "claimant",
    "claim_date": "claim_date",
    "review_title": "article_title",
    "article_text": "article_text",
    "review_url": "article_url",
    "review_date": "published_at",
    "review_publisher_name": "publisher_name",
    "review_publisher_site": "publisher_site",
    "textual_rating": "textual_rating_raw",
    "language_code": "language_code",
}


def adapt(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Map a raw google_factcheck extraction dataframe to the canonical intermediate schema."""
    missing = [column for column in _RAW_TO_CANONICAL if column not in df_raw.columns]
    if missing:
        raise ValueError(f"google_factcheck adapter: missing expected raw column(s) {missing}")

    df = df_raw.rename(columns=_RAW_TO_CANONICAL)[list(_RAW_TO_CANONICAL.values())].copy()
    df["source"] = "google_factcheck"
    df["image_url"] = None
    return df
