"""
Renders a static PNG of the pipeline's task DAG (docs/dag.png) using
matplotlib, so the repo has a visual of the orchestration graph without
requiring a running Prefect server/UI to view it.

Run: python scripts/generate_dag_diagram.py
"""
from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

NODES = [
    # (x, y, label, sublabel)
    (0.0, 0.0, "extract_task", "OpenAlex API -> raw JSON\nretries=3 (10s/30s/60s)"),
    (2.6, 0.0, "transform_task", "pandas clean/flatten\nretries=2 (15s)"),
    (5.2, 0.0, "load_task", "upsert -> DuckDB\nretries=3 (10s)"),
    (7.8, 0.0, "dashboard_task", "render dashboard.html\nretries=1"),
    (5.2, -1.6, "record_run_task", "write run health\n-> pipeline_runs table"),
]

EDGES = [(0, 1), (1, 2), (2, 3), (2, 4), (3, 4)]

FAIL_NOTE = "Any task exception -> flow marked Failed; timed_run() always records\na RunSummary (status=failed, stage=<where it broke>) to logs + DuckDB."


def main() -> None:
    fig, ax = plt.subplots(figsize=(11, 4.2))
    ax.set_xlim(-1, 9.5)
    ax.set_ylim(-2.6, 1.3)
    ax.axis("off")

    box_w, box_h = 1.9, 0.85
    centers = []
    for x, y, label, sub in NODES:
        centers.append((x, y))
        box = FancyBboxPatch(
            (x - box_w / 2, y - box_h / 2), box_w, box_h,
            boxstyle="round,pad=0.06,rounding_size=0.08",
            linewidth=1.5, edgecolor="#3b6fd6", facecolor="#eaf1fd",
        )
        ax.add_patch(box)
        ax.text(x, y + 0.14, label, ha="center", va="center", fontsize=10.5, fontweight="bold", color="#1a1a1a")
        ax.text(x, y - 0.2, sub, ha="center", va="center", fontsize=7.5, color="#444")

    for a, b in EDGES:
        xa, ya = centers[a]
        xb, yb = centers[b]
        arrow = FancyArrowPatch(
            (xa + box_w / 2 if ya == yb else xa, ya - (box_h / 2 if ya != yb and yb < ya else 0)),
            (xb - box_w / 2 if ya == yb else xb, yb + (box_h / 2 if ya != yb and yb < ya else 0)),
            arrowstyle="-|>", mutation_scale=14, linewidth=1.4, color="#555",
        )
        ax.add_patch(arrow)

    ax.text(4.4, -2.35, FAIL_NOTE, ha="center", va="center", fontsize=8, color="#c62828")
    ax.set_title("openalex-etl-pipeline  —  Prefect flow DAG", fontsize=13, fontweight="bold", pad=14)

    out_dir = Path(__file__).resolve().parent.parent / "docs"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "dag.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
