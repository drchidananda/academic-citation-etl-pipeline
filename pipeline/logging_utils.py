"""
Shared logging + pipeline-health-monitoring helpers.

Two complementary channels are written on every run:
  1. A rotating text log (logs/pipeline.log) - human-readable, for debugging.
  2. A JSON-lines run history (logs/run_history.jsonl) - one compact record
     per pipeline run, consumed by the dashboard to show pipeline health
     over time (success rate, duration trend, row counts).
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipeline import config


def get_logger(name: str = "openalex_etl") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:  # already configured (e.g. re-imported in Prefect worker)
        return logger

    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    Path(config.LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        config.LOG_FILE, maxBytes=2_000_000, backupCount=5
    )
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


@dataclass
class RunSummary:
    """One record of pipeline health, written to run_history.jsonl."""

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    status: str = "running"  # running | success | failed
    stage: Optional[str] = None  # which stage it failed at, if any
    rows_extracted: int = 0
    rows_loaded_works: int = 0
    rows_loaded_authors: int = 0
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


def append_run_history(summary: RunSummary) -> None:
    Path(config.RUN_HISTORY_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(config.RUN_HISTORY_FILE, "a") as f:
        f.write(json.dumps(summary.to_dict()) + "\n")


def read_run_history(limit: int = 50) -> list[dict]:
    path = Path(config.RUN_HISTORY_FILE)
    if not path.exists():
        return []
    lines = path.read_text().strip().splitlines()
    records = [json.loads(line) for line in lines if line.strip()]
    return records[-limit:]


@contextmanager
def timed_run():
    """Context manager that produces a RunSummary and always records it,
    even if the pipeline raises - this is what lets the dashboard show
    failed runs, not just successful ones."""
    summary = RunSummary()
    start = time.monotonic()
    try:
        yield summary
        summary.status = "success"
    except Exception as exc:  # noqa: BLE001 - we want to capture *any* failure
        summary.status = "failed"
        summary.error_message = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        summary.duration_seconds = round(time.monotonic() - start, 2)
        summary.finished_at = datetime.now(timezone.utc).isoformat()
        append_run_history(summary)
