"""Adapter for the AFP Factuel Bluesky RSS extraction (extraction.bluesky_afp).

Only this module changes to support this source: cleaning, schema, and
export are shared with every other source.
"""

from __future__ import annotations

import pandas as pd


def adapt(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Map a raw bluesky_afp extraction to the canonical schema.

    AFP posts don't carry an explicit machine-readable verdict field the way
    Google Fact Check's textual_rating does, so label is left unset (None)
    here - textual_rating_raw is also None, so every row is dropped by the
    common label-mapping step in pipeline.run.clean() until a verdict-detection
    heuristic (e.g. from the post's emoji) is added.
    """
    missing = [column for column in ("post_text", "guid") if column not in df_raw.columns]
    if missing:
        raise ValueError(f"bluesky_afp adapter: missing expected raw column(s) {missing}")

    df = pd.DataFrame(
        {
            "claim_text": df_raw["post_text"],
            "article_title": None,
            "article_text": df_raw.get("article_text"),
            "article_url": df_raw.get("article_url"),
            "claimant": None,
            "claim_date": df_raw.get("pub_date"),
            "published_at": df_raw.get("pub_date"),
            "publisher_name": "AFP Factuel",
            "publisher_site": "factuel.afp.com",
            "language_code": "fr",
            "textual_rating_raw": None,
            "image_url": df_raw.get("image_url"),
        }
    )
    df["label"] = None
    df["source"] = "bluesky_afp"
    return df
