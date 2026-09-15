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


def make_record_id(source: str, url: str | None, fallback: str) -> str:
    """Deterministic short id from the source name and article URL (or a fallback if no URL)."""
    basis = f"{source}:{url or fallback}"
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]
