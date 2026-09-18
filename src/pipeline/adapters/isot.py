"""Adapter for the ISOT Fake News extraction (extraction.isot).

Only this module changes to support this source: cleaning, schema, and
export are shared with every other source.
"""

from __future__ import annotations

import pandas as pd


def adapt(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Map a raw ISOT extraction (title, text, subject, date, label) to the canonical schema.

    Unlike FakeNewsNet's per-file sources, extraction.isot already merges both
    label files and carries a `label` column per row - no kwarg needed here.
    """
    missing = [column for column in ("title", "text", "label") if column not in df_raw.columns]
    if missing:
        raise ValueError(f"isot adapter: missing expected raw column(s) {missing}")

    unexpected_labels = sorted(set(df_raw["label"].dropna().unique()) - {"fake", "true"})
    if unexpected_labels:
        raise ValueError(f"isot adapter: unexpected label value(s) {unexpected_labels}")

    df = pd.DataFrame(
        {
            "claim_text": df_raw["title"],
            "article_title": df_raw["title"],
            "article_text": df_raw["text"],
            "article_url": None,
            "claimant": None,
            "claim_date": df_raw.get("date"),
            "published_at": df_raw.get("date"),
            "publisher_name": None,
            "publisher_site": None,
            "language_code": "en",
            "textual_rating_raw": df_raw["label"],
            "label": df_raw["label"],
        }
    )
    df["source"] = "isot"
    df["image_url"] = None
    return df
