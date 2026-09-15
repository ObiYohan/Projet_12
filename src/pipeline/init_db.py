"""CLI: create (or verify) the SQLite database and its tables from the canonical schema."""

from __future__ import annotations

import argparse
from pathlib import Path

from .store import DEFAULT_DB_PATH, init_db


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Create the SQLite database and its tables.")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="Path to the SQLite database file")
    args = parser.parse_args(argv)

    db_path = init_db(args.db_path)
    print(f"Database ready at {db_path}")


if __name__ == "__main__":
    main()
