"""Reusable, source-agnostic cleaning/normalization functions.

These operate on single values (not whole dataframes) so they can be unit
tested and reused independently of any particular extraction's shape.
"""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlparse

import pandas as pd

_WHITESPACE_RE = re.compile(r"\s+")
_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://")
_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg")

# Ratings that are inherently ambiguous for a binary fake/true task: excluded rather than guessed
AMBIGUOUS_RATING_PATTERNS = [
    "half true", "half false", "mixture", "mixed", "unproven", "unverified",
    "opinion", "satire", "outdated", "missing context", "out of context",
    "needs context", "unclear", "research in progress", "altered context",
    "mi-vrai", "mi-faux", "à vérifier", "sans fondement suffisant",
]
FAKE_RATING_PATTERNS = [
    "false", "fake", "pants on fire", "misleading", "incorrect", "not true",
    "no evidence", "no such report", "distort", "manipulated", "hoax", "scam",
    "alter", "miscaption",
    "faux", "trompeur", "mensonge", "inexact", "erroné", "erronee", "falso",
]
TRUE_RATING_PATTERNS = [
    "true", "correct", "accurate", "vrai", "exact", "verified", "confirmed",
]


def nettoie_texte(text) -> str | None:
    """Collapse whitespace and strip a free-text field. None/blank input -> None."""
    if pd.isna(text):
        return None
    cleaned = _WHITESPACE_RE.sub(" ", str(text)).strip()
    return cleaned or None


def normalize_url(url) -> str | None:
    """Normalize a URL: strip, add a scheme if missing, reject anything unparsable."""
    if pd.isna(url):
        return None
    candidate = str(url).strip()
    if not candidate:
        return None
    if not _SCHEME_RE.match(candidate):
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    return candidate if parsed.scheme and parsed.netloc else None


def valide_image(url) -> bool:
    """Syntactic check that a URL plausibly points to an image file.

    Deliberately offline (no HTTP request): reachability is the extraction
    step's concern, and checking it here would make this pipeline's output
    depend on network state, breaking reproducibility.
    """
    normalized = normalize_url(url)
    if not normalized:
        return False
    return urlparse(normalized).path.lower().endswith(_IMAGE_EXTENSIONS)


def map_textual_rating_to_label(rating) -> str | None:
    """Map a free-text fact-check rating to a clean binary label, or None if ambiguous/unrecognized."""
    if pd.isna(rating):
        return None
    normalized = str(rating).strip().lower()
    if not normalized:
        return None

    if any(re.search(pattern, normalized) for pattern in AMBIGUOUS_RATING_PATTERNS):
        return None
    if any(re.search(pattern, normalized) for pattern in FAKE_RATING_PATTERNS):
        return "fake"
    if any(re.search(pattern, normalized) for pattern in TRUE_RATING_PATTERNS):
        return "true"
    return None


ISO8601_UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def normalize_datetime(series: pd.Series) -> pd.Series:
    """Parse a date-like column and format it as a consistent ISO 8601 UTC string.

    Every source uses a different raw date shape (AFP's RSS pubDate, Google's
    already-ISO8601 dates, ISOT's mixed "December 31, 2017" / "19-Feb-18") -
    this gives every source's output the same YYYY-MM-DDTHH:MM:SSZ shape, so
    dates are comparable/sortable as plain strings regardless of origin.
    Unparsable values become NaN (not the literal string "NaT").
    """
    parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    return parsed.dt.strftime(ISO8601_UTC_FORMAT)


def make_record_id(source: str, url: str | None, content: tuple) -> str:
    """Deterministic short id from the source name and article URL, or the row's content if no URL.

    Never derived from the row's position: for URL-less sources (ISOT) a positional
    id shifts whenever the input order or the set of dropped rows changes, and the
    load's INSERT OR REPLACE would then overwrite a *different* article in the database.
    """
    if not pd.isna(url) and url:
        basis = f"{source}:{url}"
    else:
        basis = f"{source}:" + "\x1f".join("" if pd.isna(part) else str(part) for part in content)
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]
