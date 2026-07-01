"""
Smoke tests for the ETL pipeline. These don't hit the real OpenAlex API -
extract's pagination is tested with a mocked HTTP session, and the rest of
the pipeline (transform -> load -> dashboard) is exercised against
tests/fixtures/sample_openalex_response.json, a small synthetic-but-schema-
accurate batch of works (including a couple of deliberately dirty records
to prove the cleaning logic works: a null title, a null id, and an exact
duplicate).

Run: pytest tests/ -v
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from pipeline import extract, load, transform

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sample_openalex_response.json"


@pytest.fixture
def raw_works():
    return json.loads(FIXTURE_PATH.read_text())


def test_fixture_has_dirty_records(raw_works):
    assert any(w.get("title") is None for w in raw_works)
    assert any(w.get("id") is None for w in raw_works)


def test_transform_drops_bad_records_and_dedupes(raw_works):
    works_df, authors_df = transform.clean(raw_works)

    # null-title and null-id records must be dropped
    assert works_df["title"].isna().sum() == 0
    assert works_df["work_id"].isna().sum() == 0

    # the deliberately duplicated record must be collapsed to one row
    assert works_df["work_id"].is_unique

    # every author row must point at a work_id that survived cleaning
    assert set(authors_df["work_id"]).issubset(set(works_df["work_id"]))

    # basic type sanity
    assert works_df["cited_by_count"].dtype.kind in "iu"


def test_load_is_idempotent(tmp_path, raw_works):
    db_path = str(tmp_path / "test.duckdb")
    works_df, authors_df = transform.clean(raw_works)

    n_works_1, n_authors_1 = load.load_to_duckdb(works_df, authors_df, db_path=db_path)
    # loading the exact same batch again must not create duplicates
    n_works_2, n_authors_2 = load.load_to_duckdb(works_df, authors_df, db_path=db_path)

    con = duckdb.connect(db_path)
    total_works = con.execute("SELECT COUNT(*) FROM works").fetchone()[0]
    total_authors = con.execute("SELECT COUNT(*) FROM authors").fetchone()[0]
    con.close()

    assert total_works == len(works_df)
    assert total_authors == n_authors_1 == n_authors_2
    assert n_works_1 == n_works_2 == len(works_df)


def test_extract_pagination_with_mocked_http(raw_works):
    """Verifies fetch_works() walks cursor pagination correctly and stops
    at max_records, without making a real network call."""
    page_1 = raw_works[:30]
    page_2 = raw_works[30:]

    resp_1 = MagicMock(status_code=200)
    resp_1.json.return_value = {"results": page_1, "meta": {"next_cursor": "abc123"}}
    resp_2 = MagicMock(status_code=200)
    resp_2.json.return_value = {"results": page_2, "meta": {"next_cursor": None}}

    mock_session = MagicMock()
    mock_session.get.side_effect = [resp_1, resp_2]

    with patch("pipeline.extract._build_session", return_value=mock_session):
        works = extract.fetch_works(max_records=1000, session=mock_session)

    assert len(works) == len(raw_works)
    assert mock_session.get.call_count == 2


def test_extract_raises_on_http_error():
    resp = MagicMock(status_code=500, text="server exploded")
    mock_session = MagicMock()
    mock_session.get.return_value = resp

    with pytest.raises(extract.OpenAlexExtractionError):
        extract.fetch_works(max_records=10, session=mock_session)
