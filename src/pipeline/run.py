"""Transformation pipeline: read -> adapt -> clean -> enrich -> validate -> export.

Usage (CLI):
    python -m pipeline.run --source google_factcheck \\
        --input data/google_factcheck/google_factcheck_multimodal.csv \\
        --output data/processed/google_factcheck.csv

Usage (library):
    from pipeline import run_pipeline
    run_pipeline("google_factcheck", input_path, output_path, log_dir)

Adding a new source means writing one adapter (see pipeline.adapters)
and registering it in ADAPTERS - this module and the cleaning/schema/export
steps stay the same regardless of the source's raw shape.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from . import cleaning, schema
from .adapters import ADAPTERS
from .export import export_dataset
from .logging_config import configure_logging

logger = logging.getLogger(__name__)


def read_raw(input_path: Path) -> pd.DataFrame:
    """Lecture: load the raw extraction CSV."""
    logger.info("Reading raw extraction from %s", input_path)
    df = pd.read_csv(input_path)
    logger.info("Read %d raw row(s), %d column(s)", len(df), df.shape[1])
    return df


def adapt_source(df_raw: pd.DataFrame, source: str, **adapter_kwargs) -> pd.DataFrame:
    """Lecture: map raw columns to the canonical intermediate schema via the source's adapter."""
    if source not in ADAPTERS:
        raise ValueError(f"Unknown source '{source}'. Available: {sorted(ADAPTERS)}")
    logger.info("Adapting source '%s' to the canonical schema", source)
    df = ADAPTERS[source](df_raw, **adapter_kwargs)
    logger.info("Adapted %d row(s)", len(df))
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Traitement: text cleanup, URL/date normalization, label mapping, de-duplication."""
    text_columns = ["claim_text", "article_title", "article_text", "claimant", "publisher_name"]
    for column in text_columns:
        if column in df.columns:
            df[column] = df[column].apply(cleaning.nettoie_texte)
    logger.info("Cleaned free-text columns: %s", [c for c in text_columns if c in df.columns])

    for column in ("article_url", "image_url"):
        if column in df.columns:
            df[column] = df[column].apply(cleaning.normalize_url)

    if "image_url" in df.columns:
        df["has_valid_image"] = df["image_url"].apply(cleaning.valide_image)
        logger.info("Validated image_url: %d/%d look like valid image URLs", df["has_valid_image"].sum(), len(df))

    for column in ("claim_date", "published_at"):
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)

    if "label" not in df.columns:
        df["label"] = df["textual_rating_raw"].apply(cleaning.map_textual_rating_to_label)
        logger.info("Mapped textual_rating_raw -> label")

    before = len(df)
    df = df[df["claim_text"].notna()].reset_index(drop=True)
    logger.info("Dropped %d row(s) with no usable claim_text", before - len(df))

    before = len(df)
    subset = ["article_url"] if "article_url" in df.columns else None
    df = df.drop_duplicates(subset=subset, keep="first").reset_index(drop=True)
    logger.info("Dropped %d duplicate row(s)", before - len(df))

    before = len(df)
    df = df[df["label"].notna()].reset_index(drop=True)
    logger.info("Dropped %d row(s) with an unrecognized/ambiguous rating", before - len(df))

    return df


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Traitement: generate derived columns (record_id, article_text_length, ingested_at)."""
    df["record_id"] = [
        cleaning.make_record_id(row.source, getattr(row, "article_url", None), fallback=str(i))
        for i, row in enumerate(df.itertuples())
    ]
    df["article_text_length"] = (
        df["article_text"].fillna("").str.len() if "article_text" in df.columns else 0
    )
    df["ingested_at"] = pd.Timestamp.now("UTC")
    logger.info("Generated derived columns: record_id, article_text_length, ingested_at")
    return df


def run_pipeline(
    source: str,
    input_path: Path,
    output_path: Path,
    log_dir: Path = Path("logs"),
    **adapter_kwargs,
) -> Path:
    """Run the full read -> adapt -> clean -> enrich -> validate -> export pipeline. Returns output_path."""
    log_path = configure_logging(log_dir)
    logger.info("=== Starting transformation pipeline (source=%s) ===", source)
    logger.info("Run log: %s", log_path)

    df_raw = read_raw(input_path)
    df = adapt_source(df_raw, source, **adapter_kwargs)
    df = clean(df)
    df = enrich(df)

    for column in schema.column_order():
        if column not in df.columns:
            df[column] = None
    df = df[schema.column_order()]

    issues = schema.validate_schema(df)
    if issues:
        for issue in issues:
            logger.warning("Schema validation issue: %s", issue)
    else:
        logger.info("Schema validation passed: all required fields present and non-null")

    export_dataset(df, output_path, source=source, input_path=input_path, issues=issues)
    logger.info("=== Pipeline complete: %d row(s) -> %s ===", len(df), output_path)
    return output_path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Transform a raw extraction into the canonical dataset schema.")
    parser.add_argument("--source", required=True, choices=sorted(ADAPTERS), help="Name of the source adapter to use")
    parser.add_argument("--input", required=True, type=Path, help="Path to the raw extraction CSV")
    parser.add_argument("--output", required=True, type=Path, help="Path to write the transformed CSV")
    parser.add_argument("--log-dir", type=Path, default=Path("logs"), help="Directory to write pipeline run logs")
    parser.add_argument("--label", help="Label override for adapters that need one (e.g. fakenewsnet: fake/true)")
    args = parser.parse_args(argv)

    adapter_kwargs = {"label": args.label} if args.label else {}
    run_pipeline(args.source, args.input, args.output, args.log_dir, **adapter_kwargs)


if __name__ == "__main__":
    main()
