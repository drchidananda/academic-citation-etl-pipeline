"""
Extract stage: pull academic-work records from the OpenAlex API.

OpenAlex (https://openalex.org) is a free, no-API-key-required catalog of
scholarly works, authors, institutions and concepts - a good stand-in for
"real-world academic citation data" without any auth friction.

This module is deliberately network-call-free at import time and returns
plain Python data structures, so it's easy to unit test by monkeypatching
`requests.Session.get`.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline import config
from pipeline.logging_utils import get_logger

logger = get_logger(__name__)


class OpenAlexExtractionError(RuntimeError):
    """Raised when the OpenAlex API can't be reached or returns bad data."""


def _build_session() -> requests.Session:
    """A requests Session with HTTP-level retries (connection errors, 429s,
    5xx) baked in, on top of the task-level retries Prefect adds later.
    Belt-and-suspenders: this handles transient network blips within a
    single task attempt; Prefect's retries handle a whole attempt failing."""
    session = requests.Session()
    retry = Retry(
        total=config.HTTP_MAX_RETRIES,
        backoff_factor=config.HTTP_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_works(
    search_query: str = config.OPENALEX_SEARCH_QUERY,
    from_year: int = config.OPENALEX_FROM_YEAR,
    to_year: int = config.OPENALEX_TO_YEAR,
    max_records: int = config.MAX_RECORDS,
    per_page: int = config.PER_PAGE,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Cursor-paginate through OpenAlex /works until `max_records` is hit or
    the API runs out of results. Cursor pagination (rather than page=N) is
    what OpenAlex recommends for walking deep into a result set reliably.
    """
    session = session or _build_session()
    works: list[dict[str, Any]] = []
    cursor = "*"
    page_num = 0

    while cursor and len(works) < max_records:
        params = {
            "search": search_query,
            "filter": f"publication_year:{from_year}-{to_year}",
            "select": config.OPENALEX_SELECT_FIELDS,
            "per-page": min(per_page, 200),
            "cursor": cursor,
            "mailto": config.OPENALEX_MAILTO,
        }
        page_num += 1
        logger.info("Fetching OpenAlex works page %d (cursor=%s)", page_num, cursor[:12])

        try:
            resp = session.get(config.OPENALEX_BASE_URL, params=params, timeout=config.HTTP_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            raise OpenAlexExtractionError(f"Network error calling OpenAlex: {exc}") from exc

        if resp.status_code != 200:
            raise OpenAlexExtractionError(
                f"OpenAlex returned HTTP {resp.status_code}: {resp.text[:300]}"
            )

        payload = resp.json()
        results = payload.get("results", [])
        works.extend(results)

        cursor = payload.get("meta", {}).get("next_cursor")
        if not results:
            break
        # be a good API citizen even though OpenAlex's polite-pool limits are generous
        time.sleep(0.1)

    works = works[:max_records]
    logger.info("Extracted %d raw work records from OpenAlex", len(works))
    return works


def save_raw(works: list[dict[str, Any]], run_id: str) -> Path:
    """Persist the raw API payload to disk. Keeping the untouched raw
    extract on disk (rather than piping bytes straight into transform) makes
    the pipeline replayable/debuggable and matches the classic ETL pattern
    of separating landing-zone data from cleaned data."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = config.RAW_DIR / f"works_{ts}_{run_id}.json"
    path.write_text(json.dumps(works, indent=None))
    logger.info("Saved raw extract (%d records) to %s", len(works), path)
    return path


def load_raw(path: str | Path) -> list[dict[str, Any]]:
    return json.loads(Path(path).read_text())


if __name__ == "__main__":
    # Manual smoke test: `python -m pipeline.extract`
    data = fetch_works(max_records=50)
    out_path = save_raw(data, run_id="manual")
    print(f"Wrote {len(data)} records to {out_path}")
