"""CLI: load a transformed (canonical-schema) CSV into the SQLite database."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from .logging_config import configure_logging
from .store import DEFAULT_DB_PATH, load_dataframe

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Load a transformed CSV into the SQLite database.")
    parser.add_argument("--input", required=True, type=Path, help="Path to a transformed (canonical-schema) CSV")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="Path to the SQLite database file")
    parser.add_argument("--log-dir", type=Path, default=Path("logs"), help="Directory to write pipeline run logs")
    args = parser.parse_args(argv)

    configure_logging(args.log_dir)
    # dtype=str for label: a column that's entirely "true" gets auto-inferred as boolean
    # otherwise (pandas treats "true"/"false" as boolean literals), corrupting it. Date
    # columns are already ISO 8601 UTC strings (pipeline.cleaning.normalize_datetime)
    # and need no reparsing here.
    df = pd.read_csv(args.input, dtype={"label": str})

    summary = load_dataframe(df, args.db_path)
    logger.info("Load summary: %s", summary)


if __name__ == "__main__":
    main()
