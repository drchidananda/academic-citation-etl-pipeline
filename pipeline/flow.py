"""
Prefect flow: orchestrates extract -> transform -> load -> dashboard as a
DAG, with per-task retries and centralized failure handling.

Run once (e.g. from GitHub Actions or cron):
    python -m pipeline.flow

Run as a long-lived, self-scheduling process (e.g. on a always-on box):
    python -m pipeline.flow --serve --cron "0 6 * * *"
"""
from __future__ import annotations

import argparse
import sys

from prefect import flow, task

from pipeline import config, extract, load, transform
from pipeline.logging_utils import RunSummary, append_run_history, get_logger, timed_run

logger = get_logger(__name__)


# --------------------------------------------------------------------------
# Tasks - each is retried independently. Retries + backoff live here because
# this is exactly the kind of transient failure (API hiccup, DB file lock)
# that's worth retrying automatically rather than failing the whole run.
# --------------------------------------------------------------------------


@task(
    name="extract-openalex-works",
    retries=3,
    retry_delay_seconds=[10, 30, 60],
    timeout_seconds=300,
    log_prints=True,
)
def extract_task(run_id: str) -> str:
    works = extract.fetch_works()
    if not works:
        raise ValueError("OpenAlex returned zero works - treating as a failed extract")
    raw_path = extract.save_raw(works, run_id=run_id)
    return str(raw_path)


@task(name="transform-works", retries=2, retry_delay_seconds=15, log_prints=True)
def transform_task(raw_path: str):
    raw_works = extract.load_raw(raw_path)
    works_df, authors_df = transform.clean(raw_works)
    if works_df.empty:
        raise ValueError("Transform produced zero clean rows - failing the run")
    return works_df, authors_df


@task(name="load-to-duckdb", retries=3, retry_delay_seconds=10, log_prints=True)
def load_task(dataframes) -> tuple[int, int]:
    works_df, authors_df = dataframes
    return load.load_to_duckdb(works_df, authors_df)


@task(name="record-pipeline-health", retries=1, log_prints=True)
def record_run_task(summary_dict: dict) -> None:
    """Writes the run's own health metrics into DuckDB's pipeline_runs
    table so the dashboard can chart success rate / duration over time."""
    from pipeline.logging_utils import RunSummary as _RS

    con = load.get_connection()
    try:
        load.load_pipeline_run(con, _RS(**summary_dict))
    finally:
        con.close()


@task(name="build-dashboard", retries=1, log_prints=True)
def dashboard_task() -> str:
    from dashboard.generate_dashboard import build_dashboard

    return build_dashboard()


# --------------------------------------------------------------------------
# Flow (the DAG itself)
# --------------------------------------------------------------------------


@flow(name="openalex-etl-pipeline", log_prints=True)
def etl_flow() -> dict:
    holder: dict = {}
    try:
        with timed_run() as summary:
            holder["summary"] = summary
            try:
                # `summary.stage` is set *before* each task runs, so if a
                # task exhausts its retries and raises, the recorded stage
                # is where the run actually broke - not the last stage
                # that succeeded.
                summary.stage = "extract"
                raw_path = extract_task(summary.run_id)

                summary.stage = "transform"
                dataframes = transform_task(raw_path)
                works_df, _ = dataframes
                summary.rows_extracted = len(works_df)

                summary.stage = "load"
                n_works, n_authors = load_task(dataframes)
                summary.rows_loaded_works = n_works
                summary.rows_loaded_authors = n_authors

                summary.stage = "dashboard"
                dashboard_task()
            except Exception:
                # timed_run() finalizes + records the failure to
                # logs/run_history.jsonl before this propagates further;
                # re-raise so Prefect marks the flow run Failed.
                logger.exception("ETL flow failed at stage=%s", summary.stage)
                raise
    finally:
        # Runs on success *and* failure, so a failed run shows up in the
        # DuckDB pipeline_runs table too - not just in the JSONL log. This
        # is what lets the dashboard's health panel surface red runs.
        summary = holder.get("summary")
        if summary is not None:
            record_run_task(summary.to_dict())
            logger.info("Pipeline run %s finished with status=%s", summary.run_id, summary.status)

    return summary.to_dict()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run or serve the OpenAlex ETL pipeline")
    parser.add_argument("--serve", action="store_true", help="Run as a long-lived scheduled deployment")
    parser.add_argument("--cron", default="0 6 * * *", help="Cron schedule when using --serve")
    args = parser.parse_args()

    if args.serve:
        logger.info("Serving flow with cron schedule: %s", args.cron)
        etl_flow.serve(name="openalex-etl-scheduled", cron=args.cron)
    else:
        try:
            etl_flow()
        except Exception:
            # Non-zero exit so CI (GitHub Actions) marks the run as failed.
            sys.exit(1)


if __name__ == "__main__":
    main()
