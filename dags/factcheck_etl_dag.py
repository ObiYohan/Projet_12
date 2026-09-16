"""Airflow DAG: extraction -> transformation -> chargement, one chain per registered source.

Modularity: a new source needs (1) an extractor registered in
`extraction.EXTRACTORS` and (2) an adapter registered in
`pipeline.adapters.ADAPTERS`, both keyed by the same source name.
This DAG builds an extract -> transform -> load task chain for every source
name present in *both* registries - no changes needed here to add a source.

Environment notes:
- Airflow 3.3.1+ supports Python 3.14 (this project's `requires-python`), so
  Airflow can be installed in the same `uv` project via the `airflow` extra:
  `uv sync --extra airflow`.
- The sys.path tweak below makes `pipeline`/`extraction` importable without
  a separate `pip install -e .` step, which matters if Airflow's worker
  processes don't otherwise see this project as an installed package.
- GOOGLE_FACTCHECK_API_KEY must be set wherever the extraction task runs -
  an Airflow Variable/Connection in production, or a .env file (picked up by
  python-dotenv) for local development.
- Point Airflow's dags_folder at this directory (or symlink/copy this file
  into it).
"""

from __future__ import annotations

import datetime
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import pandas as pd
from airflow.sdk import dag, task

from extraction import EXTRACTORS
from pipeline.adapters import ADAPTERS
from pipeline.run import run_pipeline
from pipeline.store import DEFAULT_DB_PATH, load_dataframe

RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
LOG_DIR = REPO_ROOT / "logs"
DB_PATH = DEFAULT_DB_PATH if DEFAULT_DB_PATH.is_absolute() else REPO_ROOT / DEFAULT_DB_PATH

# Only sources with BOTH an extractor and an adapter get a task chain.
SOURCES = sorted(set(EXTRACTORS) & set(ADAPTERS))

# Extra keyword arguments some adapters need at transform time (e.g. fakenewsnet's
# per-file `label`). Add an entry here when such a source is registered.
ADAPTER_KWARGS: dict[str, dict] = {}

default_args = {
    "owner": "projet_12",
    "retries": 1,
}


@dag(
    dag_id="factcheck_etl",
    description="Extraction -> transformation -> chargement des sources de fact-checking",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["projet_12", "etl"],
)
def factcheck_etl():
    for source in SOURCES:
        raw_path = RAW_DIR / f"{source}.csv"
        processed_path = PROCESSED_DIR / f"{source}.csv"
        adapter_kwargs = ADAPTER_KWARGS.get(source, {})

        @task(task_id=f"extract_{source}")
        def extract_task(source: str = source, raw_path: Path = raw_path) -> str:
            """Extraction: run the source's extractor, write the raw CSV."""
            EXTRACTORS[source](raw_path)
            return str(raw_path)

        @task(task_id=f"transform_{source}")
        def transform_task(
            raw_path_str: str,
            source: str = source,
            processed_path: Path = processed_path,
            adapter_kwargs: dict = adapter_kwargs,
        ) -> str:
            """Transformation: run the shared pipeline (clean/normalize/map/enrich)."""
            run_pipeline(source, Path(raw_path_str), processed_path, log_dir=LOG_DIR, **adapter_kwargs)
            return str(processed_path)

        @task(task_id=f"load_{source}")
        def load_task(processed_path_str: str) -> dict:
            """Chargement: upsert the transformed dataset into the SQLite database."""
            # dtype=str for label: a column that's entirely "true" (single-label sources
            # like fakenewsnet's per-file CSVs) gets auto-inferred as boolean otherwise,
            # corrupting it - see pipeline.store's CHECK constraint on label.
            df = pd.read_csv(processed_path_str, dtype={"label": str})
            for column in ("claim_date", "published_at", "ingested_at"):
                if column in df.columns:
                    df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)
            return load_dataframe(df, DB_PATH)

        raw = extract_task()
        processed = transform_task(raw)
        load_task(processed)


factcheck_etl()
