"""ETL metrics: precision (valid rows), speed (per-step timing), cost (memory).

A full pipeline run has three phases - extraction, transformation, chargement -
that Airflow runs as separate tasks, possibly on different workers. A live
RunMetrics object can't be shared across that process boundary, only plain
JSON-serializable data (an XCom). So each task builds its own RunMetrics,
snapshots it with to_dict(), and passes that dict along; the last task in the
chain (chargement) merges every phase's dict with merge_phase_metrics() and
writes the single combined record with append_run_record().

Standalone use (CLI/notebook, no Airflow) stays one call: RunMetrics.record()
does the to_dict() + merge_phase_metrics() + append_run_record() for you.

Everything is appended as one JSON line per run to data/metrics/etl_runs.jsonl -
a time series across many runs, built for monitoring/trend analysis. Kept
separate from the run manifest (pipeline.export), which is a per-output
artifact for auditing one dataset, not a history. Feeds
notebooks/kpi_exploration.ipynb and streamlit_dashboard.py.
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_METRICS_PATH = Path("data/metrics/etl_runs.jsonl")


def _peak_memory_mb() -> float | None:
    """Best-effort peak resident memory for this process, in MB. None if unavailable."""
    try:
        import psutil

        return round(psutil.Process().memory_info().rss / (1024 * 1024), 2)
    except ImportError:
        return None


class RunMetrics:
    """Collects step timings and counts for one phase (extraction, transformation, or
    chargement) running in a single process. Snapshot with to_dict() to pass across a
    process boundary (e.g. an Airflow XCom); call record() only for a self-contained,
    single-phase run (CLI/notebook).
    """

    def __init__(self, source: str):
        self.source = source
        self._started_at = time.time()
        self.step_durations: dict[str, float] = {}
        self.counts: dict[str, float] = {}

    @contextmanager
    def step(self, name: str):
        """Time one named step (e.g. extract; or read/adapt/clean/enrich/export; or load)."""
        start = time.perf_counter()
        try:
            yield
        finally:
            self.step_durations[name] = round(time.perf_counter() - start, 4)

    def set_count(self, name: str, value: float) -> None:
        self.counts[name] = value

    def to_dict(self) -> dict:
        """Plain, JSON-serializable snapshot of this phase - safe to put in an XCom."""
        return {
            "elapsed_seconds": round(time.time() - self._started_at, 4),
            "step_durations": dict(self.step_durations),
            "counts": dict(self.counts),
            "peak_memory_mb": _peak_memory_mb(),
        }

    def record(self, metrics_path: Path = DEFAULT_METRICS_PATH) -> Path:
        """Self-contained record: this phase IS the whole run (standalone CLI/notebook use)."""
        record = merge_phase_metrics(self.source, self.to_dict())
        return append_run_record(record, metrics_path)


def merge_phase_metrics(source: str, *phase_snapshots: dict) -> dict:
    """Combine phase snapshots (RunMetrics.to_dict()) from extraction, transformation,
    and/or chargement - measured independently, possibly in separate processes - into
    one run record ready for append_run_record().

    peak_memory_mb is the max across phases (each Airflow task is scheduled/sized
    independently, so the worst single-task footprint is the meaningful cost signal,
    not a sum that no single process ever actually used).
    """
    step_durations: dict[str, float] = {}
    counts: dict[str, float] = {}
    peak_memory_mb = 0.0
    total_duration = 0.0

    for snapshot in phase_snapshots:
        step_durations.update(snapshot.get("step_durations", {}))
        for key, value in snapshot.get("counts", {}).items():
            counts[key] = counts.get(key, 0) + value
        peak_memory_mb = max(peak_memory_mb, snapshot.get("peak_memory_mb") or 0)
        total_duration += snapshot.get("elapsed_seconds", 0)

    rows_read = counts.get("rows_read", 0)
    rows_exported = counts.get("rows_exported", 0)
    pct_valid = round(100 * rows_exported / rows_read, 2) if rows_read else None

    return {
        "source": source,
        "run_timestamp": pd.Timestamp.now("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_seconds": round(total_duration, 4),
        "step_durations": step_durations,
        "rows_read": rows_read,
        "rows_exported": rows_exported,
        "rows_loaded": counts.get("rows_loaded", 0),
        "pct_valid": pct_valid,
        "rows_dropped_missing_text": counts.get("rows_dropped_missing_text", 0),
        "rows_dropped_duplicate": counts.get("rows_dropped_duplicate", 0),
        "rows_dropped_label": counts.get("rows_dropped_label", 0),
        "peak_memory_mb": peak_memory_mb or None,
    }


def append_run_record(record: dict, metrics_path: Path = DEFAULT_METRICS_PATH) -> Path:
    """Append one fully-assembled run record as a JSON line. Returns metrics_path."""
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with metrics_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    logger.info("Recorded run metrics to %s", metrics_path)
    return metrics_path


def load_metrics(metrics_path: Path = DEFAULT_METRICS_PATH) -> pd.DataFrame:
    """Load every recorded run as a dataframe, one row per run. Empty dataframe if none yet."""
    if not metrics_path.exists():
        return pd.DataFrame(
            columns=[
                "source", "run_timestamp", "duration_seconds", "step_durations",
                "rows_read", "rows_exported", "rows_loaded", "pct_valid",
                "rows_dropped_missing_text", "rows_dropped_duplicate",
                "rows_dropped_label", "peak_memory_mb",
            ]
        )
    df = pd.read_json(metrics_path, lines=True)
    df["run_timestamp"] = pd.to_datetime(df["run_timestamp"], utc=True)
    return df
