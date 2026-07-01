"""
Load stage: write cleaned DataFrames into DuckDB.

DuckDB is used as the analytical store: it's a single embedded file
(db/openalex.duckdb), needs no server, and still gives full SQL for the
dashboard/query-interface layer - a good fit for a portfolio-scale
pipeline that still needs to *feel* like a real warehouse.

Loads are idempotent: works/authors are upserted on primary key so
re-running the pipeline (or backfilling) never creates duplicates, and
pipeline_runs is an append-only audit log of every run for monitoring.
"""
from __future__ import annotations

import duckdb
import pandas as pd

from pipeline import config
from pipeline.logging_utils import RunSummary, get_logger

logger = get_logger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS works (
    work_id           VARCHAR PRIMARY KEY,
    title             VARCHAR,
    publication_year  INTEGER,
    publication_date  DATE,
    work_type         VARCHAR,
    cited_by_count    BIGINT,
    language          VARCHAR,
    is_retracted      BOOLEAN,
    is_open_access    BOOLEAN,
    primary_source    VARCHAR,
    top_concepts      VARCHAR,
    author_count      INTEGER,
    extracted_at      VARCHAR
);

CREATE TABLE IF NOT EXISTS authors (
    work_id           VARCHAR,
    author_id         VARCHAR,
    author_name       VARCHAR,
    author_position   VARCHAR,
    institution_name  VARCHAR,
    country_code      VARCHAR
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              VARCHAR PRIMARY KEY,
    started_at          VARCHAR,
    finished_at         VARCHAR,
    duration_seconds    DOUBLE,
    status              VARCHAR,
    stage               VARCHAR,
    rows_extracted      INTEGER,
    rows_loaded_works    INTEGER,
    rows_loaded_authors  INTEGER,
    error_message       VARCHAR
);
"""


def get_connection(db_path: str = config.DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute(_SCHEMA_SQL)
    return con


def load_works(con: duckdb.DuckDBPyConnection, works_df: pd.DataFrame) -> int:
    if works_df.empty:
        return 0
    con.register("works_df_view", works_df)
    con.execute(
        """
        INSERT OR REPLACE INTO works
        SELECT
            work_id, title, publication_year, publication_date, work_type,
            cited_by_count, language, is_retracted, is_open_access,
            primary_source, top_concepts, author_count, extracted_at
        FROM works_df_view
        """
    )
    con.unregister("works_df_view")
    return len(works_df)


def load_authors(con: duckdb.DuckDBPyConnection, authors_df: pd.DataFrame, work_ids: list[str]) -> int:
    if authors_df.empty:
        return 0
    # Delete-then-insert per touched work_id keeps this idempotent without
    # needing a composite primary key / ON CONFLICT clause in DuckDB.
    con.register("authors_df_view", authors_df)
    con.register("work_ids_view", pd.DataFrame({"work_id": work_ids}))
    con.execute("DELETE FROM authors WHERE work_id IN (SELECT work_id FROM work_ids_view)")
    con.execute(
        """
        INSERT INTO authors
        SELECT work_id, author_id, author_name, author_position, institution_name, country_code
        FROM authors_df_view
        """
    )
    con.unregister("authors_df_view")
    con.unregister("work_ids_view")
    return len(authors_df)


def load_pipeline_run(con: duckdb.DuckDBPyConnection, summary: RunSummary) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO pipeline_runs
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            summary.run_id,
            summary.started_at,
            summary.finished_at,
            summary.duration_seconds,
            summary.status,
            summary.stage,
            summary.rows_extracted,
            summary.rows_loaded_works,
            summary.rows_loaded_authors,
            summary.error_message,
        ],
    )


def load_to_duckdb(
    works_df: pd.DataFrame,
    authors_df: pd.DataFrame,
    db_path: str = config.DUCKDB_PATH,
) -> tuple[int, int]:
    con = get_connection(db_path)
    try:
        n_works = load_works(con, works_df)
        work_ids = works_df["work_id"].tolist() if not works_df.empty else []
        n_authors = load_authors(con, authors_df, work_ids)
        logger.info("Loaded %d works and %d authorships into %s", n_works, n_authors, db_path)
        return n_works, n_authors
    finally:
        con.close()
