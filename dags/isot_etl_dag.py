"""Airflow DAG: extraction -> transformation -> chargement for the ISOT Fake News dataset.

Kept as its own DAG (separate from factcheck_etl_dag.py) even though it's also a
batch/on-demand source: it's a different data domain (general political news,
not fact-check claims) with no network dependency at all - everything needed is
already in data/ISOT fake news/{True,Fake}.csv, so extraction here is a pure
local read, unlike every other registered source.

extraction.isot.extract() merges both label files itself, so this is a single
extract -> transform -> load chain (one task per step), not one per file.
"""

from __future__ import annotations

import datetime

from _common import build_source_chain
from airflow.sdk import dag

SOURCE = "isot"

default_args = {
    "owner": "projet_12",
    "retries": 1,
}


@dag(
    dag_id="isot_etl",
    description="Extraction -> transformation -> chargement du dataset ISOT Fake News",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["projet_12", "etl", "batch"],
)
def isot_etl():
    build_source_chain(SOURCE)


isot_etl()
