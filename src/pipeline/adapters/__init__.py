"""Registry of source adapters.

To support a new extraction, write an `adapt(df_raw, **kwargs) -> pd.DataFrame`
function that maps its raw columns to the canonical intermediate names (see
pipeline.schema), then register it here. Nothing else in the
pipeline needs to change.
"""

from functools import partial

from . import bluesky_afp, fakenewsnet, google_factcheck, isot

ADAPTERS = {
    "google_factcheck": google_factcheck.adapt,
    "bluesky_afp": bluesky_afp.adapt,
    "fakenewsnet": fakenewsnet.adapt,
    # One entry per (file, label) pair, matching extraction.EXTRACTORS - the label is
    # bound here instead of passed at transform time, so the DAG needs no per-source
    # special-casing to know which label goes with which source name.
    "fakenewsnet_gossipcop_fake": partial(fakenewsnet.adapt, label="fake"),
    "fakenewsnet_gossipcop_true": partial(fakenewsnet.adapt, label="true"),
    "fakenewsnet_politifact_fake": partial(fakenewsnet.adapt, label="fake"),
    "fakenewsnet_politifact_true": partial(fakenewsnet.adapt, label="true"),
    # isot.adapt reads the label from the raw dataframe's own "label" column
    # (set by the merged extraction), not a kwarg - so no partial binding needed.
    "isot": isot.adapt,
}

__all__ = ["ADAPTERS"]
