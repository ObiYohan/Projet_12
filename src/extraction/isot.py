"""Extraction for the ISOT Fake News dataset (data/ISOT fake news/{True,Fake}.csv).

Unlike the other sources, everything needed (title, full text, subject, date)
is already in the CSVs: no network calls, no article-body fetching, no
images. extract() reads both label files, tags each with its label, and
merges them into a single output - unlike FakeNewsNet's per-file sources,
there's no per-file processing difference here (no dead-link filtering, no
article fetching) that would justify keeping them as separate registered
sources, so this is one merge step producing one raw CSV.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Resolved from this file's location (not the current working directory), so it stays
# correct whether this runs via `uv run`, a notebook, or an Airflow container with a
# different working directory.
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "ISOT fake news"
DEFAULT_FILES = (("Fake.csv", "fake"), ("True.csv", "true"))


def extract(
    output_path: Path,
    *,
    files: tuple[tuple[str, str], ...] | None = None,
    max_rows_per_file: int | None = None,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> Path:
    """Read every ISOT label file, tag each with its label, and write one merged CSV.

    max_rows_per_file, if set, keeps a random sample of each file instead of the
    full ~20k+ rows.
    """
    files = files or DEFAULT_FILES

    frames = []
    for csv_filename, label in files:
        df = pd.read_csv(data_dir / csv_filename)
        logger.info("Read %d row(s) from %s", len(df), csv_filename)

        if max_rows_per_file is not None and len(df) > max_rows_per_file:
            df = df.sample(n=max_rows_per_file).reset_index(drop=True)
            logger.info("Sampled down to %d row(s)", len(df))

        df["label"] = label
        frames.append(df)

    df_combined = pd.concat(frames, ignore_index=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(output_path, index=False)
    logger.info("Wrote %d row(s) total to %s", len(df_combined), output_path)
    return output_path
