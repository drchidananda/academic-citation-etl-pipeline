"""
Central configuration for the OpenAlex ETL pipeline.

Everything here can be overridden with environment variables so the same
code runs unmodified on a laptop, in CI (GitHub Actions), or under a
Prefect deployment.
"""
from __future__ import annotations

import os
from pathlib import Path

# ---- Paths -----------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
LOG_DIR = PROJECT_ROOT / "logs"
DB_DIR = PROJECT_ROOT / "db"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"

for _d in (RAW_DIR, PROCESSED_DIR, LOG_DIR, DB_DIR):
    _d.mkdir(parents=True, exist_ok=True)

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str(DB_DIR / "openalex.duckdb"))
LOG_FILE = os.environ.get("LOG_FILE", str(LOG_DIR / "pipeline.log"))
RUN_HISTORY_FILE = os.environ.get("RUN_HISTORY_FILE", str(LOG_DIR / "run_history.jsonl"))
DASHBOARD_HTML = os.environ.get("DASHBOARD_HTML", str(DASHBOARD_DIR / "dashboard.html"))

# ---- Extraction settings ----------------------------------------------------
OPENALEX_BASE_URL = os.environ.get("OPENALEX_BASE_URL", "https://api.openalex.org/works")
# "Polite pool" - OpenAlex gives faster/more reliable rate limits if you pass a contact email.
OPENALEX_MAILTO = os.environ.get("OPENALEX_MAILTO", "chidapaper@gmail.com")

# What we're pulling: a search term + a publication-year window. Change these
# via env vars to repoint the whole pipeline at a different research topic.
OPENALEX_SEARCH_QUERY = os.environ.get("OPENALEX_SEARCH_QUERY", "applied artificial intelligence")
OPENALEX_FROM_YEAR = int(os.environ.get("OPENALEX_FROM_YEAR", "2018"))
OPENALEX_TO_YEAR = int(os.environ.get("OPENALEX_TO_YEAR", "2025"))

# Fields fetched from the API. Keeping this narrow keeps payloads small and
# extraction fast; extend it if downstream transforms need more.
OPENALEX_SELECT_FIELDS = os.environ.get(
    "OPENALEX_SELECT_FIELDS",
    "id,title,publication_year,publication_date,type,cited_by_count,"
    "language,authorships,primary_location,concepts,is_retracted,open_access",
)

PER_PAGE = int(os.environ.get("OPENALEX_PER_PAGE", "100"))
MAX_RECORDS = int(os.environ.get("OPENALEX_MAX_RECORDS", "500"))

# ---- Reliability settings ----------------------------------------------------
HTTP_TIMEOUT_SECONDS = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "30"))
HTTP_MAX_RETRIES = int(os.environ.get("HTTP_MAX_RETRIES", "4"))
HTTP_BACKOFF_FACTOR = float(os.environ.get("HTTP_BACKOFF_FACTOR", "1.5"))

# Dashboard: how many top-cited works / top concepts to show.
DASHBOARD_TOP_N = int(os.environ.get("DASHBOARD_TOP_N", "15"))
