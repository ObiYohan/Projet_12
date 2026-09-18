"""Shared setup and per-source task-chain builder for this project's Airflow DAGs.

Each DAG file explicitly lists which source names it owns (see
factcheck_etl_dag.py and bluesky_afp_etl_dag.py) - sources aren't
auto-distributed across DAGs, since two DAGs racing to extract/transform/load
the same source on different schedules would corrupt each other's output.

Environment notes:
- Airflow 3.3.1+ supports Python 3.14 (this project's `requires-python`), so
  Airflow can be installed in the same `uv` project via the `airflow` extra:
  `uv sync --extra airflow`.
- The sys.path tweak below makes `pipeline`/`extraction` importable without
  a separate `pip install -e .` step, which matters if Airflow's worker
  processes don't otherwise see this project as an installed package.
- Airflow adds each DAG file's own directory to sys.path when parsing it, so
  sibling modules in this dags/ folder (like this one) import normally.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import pandas as pd
from airflow.sdk import task

from extraction import EXTRACTORS
from pipeline.adapters import ADAPTERS
from pipeline.metrics import RunMetrics, append_run_record, merge_phase_metrics
from pipeline.run import run_pipeline
from pipeline.store import DEFAULT_DB_PATH, load_dataframe

RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
LOG_DIR = REPO_ROOT / "logs"
DB_PATH = DEFAULT_DB_PATH if DEFAULT_DB_PATH.is_absolute() else REPO_ROOT / DEFAULT_DB_PATH


def build_source_chain(source: str, adapter_kwargs: dict | None = None) -> None:
    """Wire up an extract -> transform -> load task chain for one source.

    Must be called from within a @dag-decorated function's body (the @task
    calls inside need an active DAG context).
    """
    if source not in EXTRACTORS or source not in ADAPTERS:
        raise ValueError(f"Source '{source}' needs an entry in both EXTRACTORS and ADAPTERS")

    raw_path = RAW_DIR / f"{source}.csv"
    processed_path = PROCESSED_DIR / f"{source}.csv"
    adapter_kwargs = adapter_kwargs or {}

    @task(task_id=f"extract_{source}")
    def extract_task(source: str = source, raw_path: Path = raw_path) -> dict:
        """Extraction: run the source's extractor, write the raw CSV.

        Runs metrics.RunMetrics locally (this task may execute on a different
        worker than transform_task/load_task) and passes its snapshot onward via
        XCom - load_task merges every phase's snapshot into one final record.
        """
        phase_metrics = RunMetrics(source)
        with phase_metrics.step("extract"):
            EXTRACTORS[source](raw_path)
        return {"raw_path": str(raw_path), "metrics": phase_metrics.to_dict()}

    @task(task_id=f"transform_{source}")
    def transform_task(
        extract_result: dict,
        source: str = source,
        processed_path: Path = processed_path,
        adapter_kwargs: dict = adapter_kwargs,
    ) -> dict:
        """Transformation: run the shared pipeline (clean/normalize/map/enrich)."""
        raw_path = Path(extract_result["raw_path"])
        _, transform_metrics = run_pipeline(
            source, raw_path, processed_path, log_dir=LOG_DIR, record_metrics=False, **adapter_kwargs
        )
        return {
            "processed_path": str(processed_path),
            "extract_metrics": extract_result["metrics"],
            "transform_metrics": transform_metrics,
        }

    @task(task_id=f"load_{source}")
    def load_task(transform_result: dict, source: str = source) -> dict:
        """Chargement: upsert the transformed dataset into the SQLite database.

        Last task in the chain: merges the extraction, transformation, and this
        phase's own metrics into the single combined record written to
        data/metrics/etl_runs.jsonl.
        """
        # dtype=str for label: a column that's entirely "true" (single-label sources
        # like fakenewsnet's per-file CSVs) gets auto-inferred as boolean otherwise,
        # corrupting it - see pipeline.store's CHECK constraint on label. Date columns
        # are already ISO 8601 UTC strings (pipeline.cleaning.normalize_datetime) and
        # need no reparsing here.
        phase_metrics = RunMetrics(source)
        df = pd.read_csv(transform_result["processed_path"], dtype={"label": str})
        with phase_metrics.step("load"):
            summary = load_dataframe(df, DB_PATH)
        phase_metrics.set_count("rows_loaded", summary.get("claim_records", 0))

        final_record = merge_phase_metrics(
            source,
            transform_result["extract_metrics"],
            transform_result["transform_metrics"],
            phase_metrics.to_dict(),
        )
        append_run_record(final_record)

        return summary

    raw = extract_task()
    processed = transform_task(raw)
    load_task(processed)
