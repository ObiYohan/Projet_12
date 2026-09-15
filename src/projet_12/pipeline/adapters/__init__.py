"""Registry of source adapters.

To support a new extraction, write an `adapt(df_raw, **kwargs) -> pd.DataFrame`
function that maps its raw columns to the canonical intermediate names (see
projet_12.pipeline.schema), then register it here. Nothing else in the
pipeline needs to change.
"""

from . import fakenewsnet, google_factcheck

ADAPTERS = {
    "google_factcheck": google_factcheck.adapt,
    "fakenewsnet": fakenewsnet.adapt,
}

__all__ = ["ADAPTERS"]
