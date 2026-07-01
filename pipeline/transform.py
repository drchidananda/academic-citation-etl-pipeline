"""
Transform stage: turn raw, deeply-nested OpenAlex JSON into two tidy,
analysis-ready tables:

  works_df   - one row per paper (grain: work_id)
  authors_df - one row per (work_id, author) pair, i.e. an exploded
               authorship bridge table

Implemented with pandas since the per-run record volume (hundreds to low
thousands of works) is well within pandas' comfort zone; swapping the same
functions to Polars would only mean changing `pd.DataFrame` construction.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from pipeline.logging_utils import get_logger

logger = get_logger(__name__)

REQUIRED_FIELDS = ("id", "title")


def _clean_work_id(openalex_id: str | None) -> str | None:
    if not openalex_id:
        return None
    # OpenAlex ids are full URLs like https://openalex.org/W123; keep just "W123"
    return openalex_id.rsplit("/", 1)[-1]


def _top_concepts(concepts: list[dict[str, Any]] | None, n: int = 3) -> str:
    if not concepts:
        return ""
    top = sorted(concepts, key=lambda c: c.get("score", 0), reverse=True)[:n]
    return ", ".join(c.get("display_name", "") for c in top if c.get("display_name"))


def _primary_source(primary_location: dict[str, Any] | None) -> str | None:
    if not primary_location:
        return None
    source = primary_location.get("source") or {}
    return source.get("display_name")


def clean_works(raw_works: list[dict[str, Any]]) -> pd.DataFrame:
    """Flatten + clean the work-level fields. Drops records missing a
    required field, de-duplicates by work_id (keeping the most-cited
    version if OpenAlex ever returns the same id twice across pages), and
    normalizes types so DuckDB gets a stable schema."""
    rows = []
    dropped_missing = 0

    for w in raw_works:
        work_id = _clean_work_id(w.get("id"))
        title = w.get("title")
        if not work_id or not title:
            dropped_missing += 1
            continue

        open_access = w.get("open_access") or {}
        rows.append(
            {
                "work_id": work_id,
                "title": title.strip(),
                "publication_year": w.get("publication_year"),
                "publication_date": w.get("publication_date"),
                "work_type": w.get("type"),
                "cited_by_count": w.get("cited_by_count") or 0,
                "language": w.get("language"),
                "is_retracted": bool(w.get("is_retracted", False)),
                "is_open_access": bool(open_access.get("is_oa", False)),
                "primary_source": _primary_source(w.get("primary_location")),
                "top_concepts": _top_concepts(w.get("concepts")),
                "author_count": len(w.get("authorships") or []),
            }
        )

    if dropped_missing:
        logger.warning("Dropped %d raw records missing id/title", dropped_missing)

    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("clean_works produced an empty DataFrame")
        return df

    before = len(df)
    df = df.drop_duplicates(subset="work_id", keep="first")
    if len(df) != before:
        logger.info("Dropped %d duplicate work_id rows", before - len(df))

    df["publication_year"] = pd.to_numeric(df["publication_year"], errors="coerce").astype("Int64")
    df["cited_by_count"] = pd.to_numeric(df["cited_by_count"], errors="coerce").fillna(0).astype("int64")
    df["publication_date"] = pd.to_datetime(df["publication_date"], errors="coerce").dt.date
    df["extracted_at"] = datetime.now(timezone.utc).isoformat()

    return df.reset_index(drop=True)


def explode_authors(raw_works: list[dict[str, Any]]) -> pd.DataFrame:
    """Build the work<->author bridge table. One row per authorship, with
    the author's first listed institution and country for quick grouping."""
    rows = []
    for w in raw_works:
        work_id = _clean_work_id(w.get("id"))
        if not work_id:
            continue
        for authorship in w.get("authorships") or []:
            author = authorship.get("author") or {}
            institutions = authorship.get("institutions") or []
            first_institution = institutions[0] if institutions else {}
            rows.append(
                {
                    "work_id": work_id,
                    "author_id": _clean_work_id(author.get("id")),
                    "author_name": author.get("display_name"),
                    "author_position": authorship.get("author_position"),
                    "institution_name": first_institution.get("display_name"),
                    "country_code": first_institution.get("country_code"),
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("explode_authors produced an empty DataFrame")
        return df

    before = len(df)
    df = df.drop_duplicates(subset=["work_id", "author_id"], keep="first")
    if len(df) != before:
        logger.info("Dropped %d duplicate (work_id, author_id) rows", before - len(df))

    return df.reset_index(drop=True)


def clean(raw_works: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convenience entry point used by the flow: raw JSON in, two clean
    DataFrames out."""
    works_df = clean_works(raw_works)
    authors_df = explode_authors(raw_works)
    # Keep authors only for works that survived cleaning
    if not works_df.empty and not authors_df.empty:
        authors_df = authors_df[authors_df["work_id"].isin(works_df["work_id"])].reset_index(drop=True)
    logger.info("Transformed into %d works and %d authorships", len(works_df), len(authors_df))
    return works_df, authors_df
