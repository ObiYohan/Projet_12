"""Logging setup shared by every pipeline run.

Every run writes to both the console and a timestamped file under the given
log directory, so past runs stay auditable without re-running the pipeline.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path


def configure_logging(log_dir: Path, level: int = logging.INFO) -> Path:
    """Reset root logging handlers and attach console + timestamped file handlers.

    Returns the path of the log file created for this run.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"pipeline_{time.strftime('%Y%m%dT%H%M%S')}.log"

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    return log_path
