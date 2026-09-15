"""Registry of source extractors.

Mirrors projet_12.pipeline.adapters: to support a new extraction, write an
`extract(output_path, **kwargs) -> Path` function that writes a raw CSV, then
register it here.
"""

from . import google_factcheck

EXTRACTORS = {
    "google_factcheck": google_factcheck.extract,
}

__all__ = ["EXTRACTORS"]
