"""Registry of source extractors.

Mirrors pipeline.adapters: to support a new extraction, write an
`extract(output_path, **kwargs) -> Path` function that writes a raw CSV, then
register it here.
"""

from functools import partial

from . import bluesky_afp, fakenewsnet, google_factcheck, isot

EXTRACTORS = {
    "google_factcheck": google_factcheck.extract,
    "bluesky_afp": bluesky_afp.extract,
    # FakeNewsNet ships one file per label: register each (file, label) pair as its
    # own source so it plugs into the DAG's per-source extract -> transform -> load
    # chain generation without any special-casing there.
    "fakenewsnet_gossipcop_fake": partial(fakenewsnet.extract, csv_filename="gossipcop_fake.csv", label="fake"),
    "fakenewsnet_gossipcop_true": partial(fakenewsnet.extract, csv_filename="gossipcop_real.csv", label="true"),
    "fakenewsnet_politifact_fake": partial(fakenewsnet.extract, csv_filename="politifact_fake.csv", label="fake"),
    "fakenewsnet_politifact_true": partial(fakenewsnet.extract, csv_filename="politifact_real.csv", label="true"),
    # Unlike FakeNewsNet, ISOT's extract() merges both label files itself into one
    # output - no per-file registration needed.
    "isot": isot.extract,
}

__all__ = ["EXTRACTORS"]
