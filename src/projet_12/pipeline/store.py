"""SQLite storage for the canonical schema: DDL + load ("chargement" step of the ETL).

Physical model matches docs/schema_conceptuel.md's ER diagram: a claim_record
table referencing small source/publisher dimension tables by natural key
(their name), so the same source/publisher is never duplicated across loads.
record_id is a deterministic hash of (source, article_url), so reloading the
same extraction replaces the same rows instead of duplicating them.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from . import schema

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("data/projet_12.db")

_DDL = """
CREATE TABLE IF NOT EXISTS source (
    name TEXT PRIMARY KEY,
    first_loaded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS publisher (
    name TEXT PRIMARY KEY,
    site TEXT
);

CREATE TABLE IF NOT EXISTS claim_record (
    record_id TEXT PRIMARY KEY,
    source TEXT NOT NULL REFERENCES source(name),
    publisher_name TEXT REFERENCES publisher(name),
    claim_text TEXT NOT NULL,
    claimant TEXT,
    claim_date TEXT,
    article_title TEXT,
    article_text TEXT,
    article_text_length INTEGER,
    article_url TEXT,
    image_url TEXT,
    has_valid_image INTEGER,
    published_at TEXT,
    textual_rating_raw TEXT,
    label TEXT NOT NULL CHECK (label IN ('fake', 'true')),
    language_code TEXT,
    ingested_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_claim_record_source ON claim_record(source);
CREATE INDEX IF NOT EXISTS idx_claim_record_label ON claim_record(label);
CREATE INDEX IF NOT EXISTS idx_claim_record_publisher ON claim_record(publisher_name);
"""

# Insert order for claim_record - kept in sync with schema.CANONICAL_SCHEMA by hand,
# since record_id/source/publisher_name carry PK/FK constraints schema.py doesn't model.
_CLAIM_RECORD_COLUMNS = [
    "record_id", "source", "publisher_name", "claim_text", "claimant", "claim_date",
    "article_title", "article_text", "article_text_length", "article_url", "image_url",
    "has_valid_image", "published_at", "textual_rating_raw", "label", "language_code",
    "ingested_at",
]


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_db(db_path: Path = DEFAULT_DB_PATH) -> Path:
    """Create the database file and its tables if they don't already exist."""
    connection = get_connection(db_path)
    try:
        connection.executescript(_DDL)
        connection.commit()
    finally:
        connection.close()
    logger.info("Database ready at %s", db_path)
    return db_path


def _sql_value(value):
    """Convert a pandas cell to something sqlite3 accepts: NaN/NaT -> None, bool -> int."""
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def load_dataframe(df: pd.DataFrame, db_path: Path = DEFAULT_DB_PATH) -> dict:
    """Chargement: upsert a transformed (canonical-schema) dataframe into SQLite.

    Creates the database/tables if needed, then upserts source and publisher
    dimension rows before loading claim_record rows. Returns a summary dict.
    """
    missing = [column for column in _CLAIM_RECORD_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"load_dataframe: dataframe is missing canonical column(s) {missing}")

    issues = schema.validate_schema(df)
    for issue in issues:
        logger.warning("Schema validation issue before load: %s", issue)

    init_db(db_path)
    connection = get_connection(db_path)
    try:
        now = pd.Timestamp.now("UTC").isoformat()

        sources = sorted(df["source"].dropna().unique())
        connection.executemany(
            "INSERT OR IGNORE INTO source (name, first_loaded_at) VALUES (?, ?)",
            [(name, now) for name in sources],
        )
        logger.info("Upserted %d source(s)", len(sources))

        publishers = (
            df.loc[df["publisher_name"].notna(), ["publisher_name", "publisher_site"]]
            .drop_duplicates(subset=["publisher_name"])
        )
        connection.executemany(
            "INSERT OR IGNORE INTO publisher (name, site) VALUES (?, ?)",
            list(publishers.itertuples(index=False, name=None)),
        )
        logger.info("Upserted %d publisher(s)", len(publishers))

        rows = [
            tuple(_sql_value(row[column]) for column in _CLAIM_RECORD_COLUMNS)
            for _, row in df.iterrows()
        ]
        placeholders = ", ".join("?" * len(_CLAIM_RECORD_COLUMNS))
        connection.executemany(
            f"INSERT OR REPLACE INTO claim_record ({', '.join(_CLAIM_RECORD_COLUMNS)}) "
            f"VALUES ({placeholders})",
            rows,
        )
        connection.commit()
        logger.info("Loaded %d claim record(s) into %s", len(rows), db_path)
    finally:
        connection.close()

    return {"sources": len(sources), "publishers": len(publishers), "claim_records": len(rows)}
