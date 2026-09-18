"""Airflow DAG: extraction -> transformation -> chargement for the batch fact-check sources.

These sources (Google Fact Check, FakeNewsNet) are pulled once/on demand to grow
a training dataset - unlike the Bluesky AFP RSS feed (see bluesky_afp_etl_dag.py),
which needs frequent polling for new posts and has its own schedule. Kept as
separate DAGs so neither blocks or races the other.

Modularity: a new batch source needs (1) an extractor registered in
`extraction.EXTRACTORS`, (2) an adapter registered in `pipeline.adapters.ADAPTERS`,
and (3) its name added to SOURCES below - the extract -> transform -> load chain
itself is shared (see dags/_common.py).
"""

from __future__ import annotations

import datetime

from _common import build_source_chain
from airflow.sdk import dag

from extraction import EXTRACTORS
from pipeline.adapters import ADAPTERS

SOURCES = sorted(
    {
        "google_factcheck",
        "fakenewsnet_gossipcop_fake",
        "fakenewsnet_gossipcop_true",
        "fakenewsnet_politifact_fake",
        "fakenewsnet_politifact_true",
    }
    & set(EXTRACTORS)
    & set(ADAPTERS)
)

default_args = {
    "owner": "projet_12",
    "retries": 1,
}


@dag(
    dag_id="factcheck_etl",
    description="Extraction -> transformation -> chargement des sources batch de fact-checking",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["projet_12", "etl", "batch"],
)
def factcheck_etl():
    for source in SOURCES:
        build_source_chain(source)


factcheck_etl()
