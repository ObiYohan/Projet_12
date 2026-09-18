"""Airflow DAG: extraction -> transformation -> chargement for the AFP Factuel Bluesky RSS feed.

Separate from factcheck_etl_dag.py (batch sources) because this one needs a much
tighter schedule: the RSS feed only ever exposes the last ~30 posts, so it must be
polled regularly to avoid missing older posts scrolling out of the window between
DAG runs. `hourly` is a reasonable default given AFP publishes roughly daily -
adjust `schedule` below if that assumption changes.

Deduplication: extraction.bluesky_afp.extract() is itself incremental (skips posts
already present in the raw CSV by guid, skips images already on disk), so running
this DAG repeatedly in a short window is safe and won't re-fetch or re-download
anything, regardless of how often it fires.
"""

from __future__ import annotations

import datetime

from _common import build_source_chain
from airflow.sdk import dag

SOURCE = "bluesky_afp"

default_args = {
    "owner": "projet_12",
    "retries": 1,
}


@dag(
    dag_id="bluesky_afp_etl",
    description="Extraction -> transformation -> chargement du flux RSS Bluesky AFP Factuel",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["projet_12", "etl", "rss"],
)
def bluesky_afp_etl():
    build_source_chain(SOURCE)


bluesky_afp_etl()
