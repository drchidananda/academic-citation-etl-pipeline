"""
Generates a static, self-contained HTML dashboard from the DuckDB
warehouse: this is the "summary dashboard / query interface" deliverable.

Design choice: rather than stand up a live web server (Streamlit/Flask)
for a scheduled batch job, this renders a plain HTML file after every
pipeline run. It opens in any browser, needs no server, and is trivial to
publish as a GitHub Pages artifact or attach to a CI run.

Run standalone:  python -m dashboard.generate_dashboard
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from pipeline import config
from pipeline.logging_utils import get_logger, read_run_history

logger = get_logger(__name__)


def _query(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()


def _df_to_html_table(df: pd.DataFrame, max_rows: int = 25) -> str:
    if df.empty:
        return "<p class='empty'>No data yet.</p>"
    return df.head(max_rows).to_html(index=False, border=0, classes="data-table", escape=True)


def build_dashboard(db_path: str = config.DUCKDB_PATH, out_path: str = config.DASHBOARD_HTML) -> str:
    con = duckdb.connect(db_path)
    try:
        totals = _query(
            con,
            """
            SELECT
                (SELECT COUNT(*) FROM works) AS total_works,
                (SELECT COUNT(DISTINCT author_id) FROM authors WHERE author_id IS NOT NULL) AS total_authors,
                (SELECT COALESCE(SUM(cited_by_count), 0) FROM works) AS total_citations,
                (SELECT COALESCE(ROUND(AVG(cited_by_count), 1), 0) FROM works) AS avg_citations
            """,
        ).iloc[0]

        by_year = _query(
            con,
            """
            SELECT publication_year, COUNT(*) AS works
            FROM works
            WHERE publication_year IS NOT NULL
            GROUP BY publication_year
            ORDER BY publication_year
            """,
        )

        top_cited = _query(
            con,
            f"""
            SELECT title, publication_year, cited_by_count, primary_source, top_concepts
            FROM works
            ORDER BY cited_by_count DESC
            LIMIT {config.DASHBOARD_TOP_N}
            """,
        )

        top_concepts = _query(
            con,
            f"""
            WITH split_concepts AS (
                SELECT TRIM(unnest(str_split(top_concepts, ','))) AS concept
                FROM works
                WHERE top_concepts IS NOT NULL AND top_concepts != ''
            )
            SELECT concept, COUNT(*) AS mentions
            FROM split_concepts
            WHERE concept != ''
            GROUP BY concept
            ORDER BY mentions DESC
            LIMIT {config.DASHBOARD_TOP_N}
            """,
        )

        top_sources = _query(
            con,
            f"""
            SELECT primary_source, COUNT(*) AS works
            FROM works
            WHERE primary_source IS NOT NULL
            GROUP BY primary_source
            ORDER BY works DESC
            LIMIT {config.DASHBOARD_TOP_N}
            """,
        )

        recent_runs = _query(
            con,
            """
            SELECT run_id, started_at, status, stage, duration_seconds,
                   rows_extracted, rows_loaded_works, rows_loaded_authors, error_message
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 20
            """,
        )
    finally:
        con.close()

    # Fall back to the JSONL run history if pipeline_runs is empty (e.g. a
    # dry run of the dashboard alone, before any full pipeline run).
    if recent_runs.empty:
        history = read_run_history(limit=20)
        recent_runs = pd.DataFrame(history[::-1]) if history else recent_runs

    n_runs = len(recent_runs)
    n_success = int((recent_runs["status"] == "success").sum()) if n_runs else 0
    success_rate = f"{(n_success / n_runs * 100):.0f}%" if n_runs else "n/a"

    year_chart_labels = json.dumps(by_year["publication_year"].astype(str).tolist()) if not by_year.empty else "[]"
    year_chart_values = json.dumps(by_year["works"].tolist()) if not by_year.empty else "[]"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>OpenAlex ETL Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
  :root {{ color-scheme: light; }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    margin: 0; padding: 32px; background: #f5f6f8; color: #1a1a1a;
  }}
  h1 {{ font-size: 22px; margin-bottom: 4px; }}
  .subtitle {{ color: #666; margin-bottom: 28px; font-size: 14px; }}
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 16px; margin-bottom: 32px; }}
  .card {{ background: white; border-radius: 10px; padding: 18px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
  .card .value {{ font-size: 26px; font-weight: 700; }}
  .card .label {{ font-size: 12px; color: #777; text-transform: uppercase; letter-spacing: .03em; margin-top: 4px; }}
  .grid {{ display: grid; grid-template-columns: 2fr 1fr; gap: 20px; margin-bottom: 24px; }}
  .panel {{ background: white; border-radius: 10px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); overflow-x: auto; }}
  .panel h2 {{ font-size: 15px; margin: 0 0 14px 0; }}
  table.data-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  table.data-table th {{ text-align: left; padding: 8px 10px; border-bottom: 2px solid #eee; color: #555; }}
  table.data-table td {{ padding: 8px 10px; border-bottom: 1px solid #f0f0f0; }}
  table.data-table tr:hover {{ background: #fafafa; }}
  .status-success {{ color: #1a7f37; font-weight: 600; }}
  .status-failed {{ color: #c62828; font-weight: 600; }}
  .status-running {{ color: #9a6700; font-weight: 600; }}
  .empty {{ color: #999; font-size: 13px; }}
  footer {{ margin-top: 24px; font-size: 12px; color: #999; }}
</style>
</head>
<body>
  <h1>OpenAlex ETL Pipeline Dashboard</h1>
  <div class="subtitle">Source: OpenAlex works API &middot; Query: "{config.OPENALEX_SEARCH_QUERY}" &middot; Store: DuckDB ({db_path})</div>

  <div class="cards">
    <div class="card"><div class="value">{int(totals['total_works']):,}</div><div class="label">Works loaded</div></div>
    <div class="card"><div class="value">{int(totals['total_authors']):,}</div><div class="label">Distinct authors</div></div>
    <div class="card"><div class="value">{int(totals['total_citations']):,}</div><div class="label">Total citations</div></div>
    <div class="card"><div class="value">{totals['avg_citations']}</div><div class="label">Avg. citations / work</div></div>
    <div class="card"><div class="value">{success_rate}</div><div class="label">Pipeline success rate (last {n_runs})</div></div>
  </div>

  <div class="grid">
    <div class="panel">
      <h2>Works published per year</h2>
      <canvas id="yearChart" height="90"></canvas>
    </div>
    <div class="panel">
      <h2>Top concepts</h2>
      {_df_to_html_table(top_concepts)}
    </div>
  </div>

  <div class="grid">
    <div class="panel">
      <h2>Top-cited works</h2>
      {_df_to_html_table(top_cited)}
    </div>
    <div class="panel">
      <h2>Top publication venues</h2>
      {_df_to_html_table(top_sources)}
    </div>
  </div>

  <div class="panel">
    <h2>Pipeline run history (health/monitoring)</h2>
    {_df_to_html_table(recent_runs, max_rows=20)}
  </div>

  <footer>Generated by dashboard/generate_dashboard.py</footer>

<script>
new Chart(document.getElementById('yearChart'), {{
  type: 'bar',
  data: {{
    labels: {year_chart_labels},
    datasets: [{{ label: 'Works', data: {year_chart_values}, backgroundColor: '#3b6fd6' }}]
  }},
  options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
}});
</script>
</body>
</html>
"""

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(html)
    logger.info("Dashboard written to %s", out_path)
    return out_path


if __name__ == "__main__":
    path = build_dashboard()
    print(f"Dashboard written to {path}")
