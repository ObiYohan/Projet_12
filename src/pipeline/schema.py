"""Canonical output schema shared by every source adapter.

Adapters translate a source's raw columns into this intermediate shape; the
common cleaning/enrichment steps then only ever need to know about these
field names, which is what lets a new extraction be plugged in by writing a
small adapter instead of touching the rest of the pipeline.

Field roles (used by docs/schema_conceptuel.md and by anyone consuming the
final dataset) are one of:
- "identifier": stable row identifier, not a modeling feature
- "classification_target": the label a model is trained to predict
- "nlp_text": free text usable as model input (tokenization, embeddings, ...)
- "multimodal_image": image reference, usable as a vision-model input
- "metadata": contextual field usable as a feature or for filtering/analysis
- "provenance": traceability/audit field, not intended as a model feature
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FieldSpec:
    name: str
    dtype: str
    role: str
    required: bool
    description: str


CANONICAL_SCHEMA: list[FieldSpec] = [
    FieldSpec("record_id", "string", "identifier", True, "Stable row id, hashed from source + article_url (or claim_text + article_text if no URL)"),
    FieldSpec("source", "string", "provenance", True, "Name of the extraction/adapter this row came from"),
    FieldSpec("claim_text", "string", "nlp_text", True, "The claim being fact-checked"),
    FieldSpec("claimant", "string", "metadata", False, "Who or what made the claim"),
    FieldSpec("claim_date", "string (ISO 8601 UTC)", "metadata", False, "Date the claim was made"),
    FieldSpec("article_title", "string", "nlp_text", False, "Title of the review/source article"),
    FieldSpec("article_text", "string", "nlp_text", False, "Full body text of the review/source article"),
    FieldSpec("article_text_length", "Int64", "metadata", False, "Character length of article_text (0 if missing)"),
    FieldSpec("article_url", "string", "provenance", False, "URL of the review/source article"),
    FieldSpec("image_url", "string", "multimodal_image", False, "URL of an image associated with the claim, if any"),
    FieldSpec("has_valid_image", "boolean", "multimodal_image", False, "Whether image_url is a syntactically valid image URL"),
    FieldSpec("publisher_name", "string", "metadata", False, "Name of the fact-checking/publishing organization"),
    FieldSpec("publisher_site", "string", "metadata", False, "Domain of the publisher"),
    FieldSpec("published_at", "string (ISO 8601 UTC)", "metadata", False, "Date the review/article was published"),
    FieldSpec("textual_rating_raw", "string", "provenance", False, "Original free-text rating before normalization"),
    FieldSpec("label", "string", "classification_target", True, "Normalized binary label: 'fake' or 'true'"),
    FieldSpec("language_code", "string", "metadata", False, "BCP-47 language code of the article/review"),
    FieldSpec("ingested_at", "string (ISO 8601 UTC)", "provenance", True, "Timestamp this row was processed by the pipeline"),
]

COLUMN_ORDER: list[str] = [field.name for field in CANONICAL_SCHEMA]
REQUIRED_FIELDS: list[str] = [field.name for field in CANONICAL_SCHEMA if field.required]


def column_order() -> list[str]:
    """Column order the exported dataset should follow."""
    return list(COLUMN_ORDER)


def validate_schema(df) -> list[str]:
    """Check a transformed dataframe against the canonical schema.

    Returns a list of human-readable issues; an empty list means the frame is valid.
    Missing *optional* columns are not an issue (a source may simply not have them).
    """
    issues: list[str] = []

    for field in CANONICAL_SCHEMA:
        if field.name not in df.columns:
            if field.required:
                issues.append(f"Missing required column: {field.name}")
            continue

        if field.required:
            missing_count = df[field.name].isna().sum()
            if missing_count:
                issues.append(f"Column '{field.name}' is required but has {missing_count} missing value(s)")

    if "label" in df.columns:
        unexpected_labels = sorted(set(df["label"].dropna().unique()) - {"fake", "true"})
        if unexpected_labels:
            issues.append(f"Column 'label' has unexpected values: {unexpected_labels}")

    return issues
