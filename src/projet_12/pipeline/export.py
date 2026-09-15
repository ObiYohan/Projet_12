"""Export step: writes the transformed dataset plus a JSON run manifest.

The manifest captures what a log line alone doesn't make easy to query later:
row count, label distribution, and any schema issues found, tied to the
input/output paths and a timestamp - enough to audit a given run after the fact.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def export_dataset(
    df: pd.DataFrame,
    output_path: Path,
    *,
    source: str,
    input_path: Path,
    issues: list[str],
) -> Path:
    """Write df to output_path (CSV) and a sibling `<output>.manifest.json`. Returns the manifest path."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Exported %d rows to %s", len(df), output_path)

    manifest = {
        "source": source,
        "input_path": str(input_path),
        "output_path": str(output_path),
        "row_count": len(df),
        "generated_at": pd.Timestamp.now("UTC").isoformat(),
        "label_distribution": df["label"].value_counts().to_dict() if "label" in df.columns else {},
        "schema_issues": issues,
    }
    manifest_path = output_path.with_suffix(".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    logger.info("Wrote run manifest to %s", manifest_path)

    return manifest_path
