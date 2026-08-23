"""Chart the engine benchmark from spark/bench/results.jsonl.

    python scripts/plot_benchmark.py

Writes docs/images/benchmark_engines.png. Log-log, because both axes span
orders of magnitude and the shape of each curve is the point: Spark is a flat
high line dominated by start-up, DuckDB a low one that barely rises.
"""

from __future__ import annotations

import json
import pathlib
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = pathlib.Path(__file__).resolve().parents[1]
RESULTS = ROOT / "spark" / "bench" / "results.jsonl"
OUT = ROOT / "docs" / "images" / "benchmark_engines.png"

COLORS = {"duckdb": "#00879B", "polars": "#C2622D", "spark": "#7A5EA8"}
LABELS = {"duckdb": "DuckDB", "polars": "Polars", "spark": "PySpark"}
INK, MUTED, GRID, SURFACE = "#1F2328", "#6B7280", "#E6E8EB", "#FFFFFF"


def main() -> None:
    runs = [
        json.loads(line[len("RESULT ") :])
        for line in RESULTS.read_text().splitlines()
        if line.startswith("RESULT")
    ]
    rows_for = {r["files"]: r["input_rows"] for r in runs}
    agg: dict[tuple[int, str], list[float]] = {}
    for r in runs:
        agg.setdefault((r["files"], r["engine"]), []).append(r["seconds"])

    scales = sorted(rows_for)
    fig, ax = plt.subplots(figsize=(9, 5.4), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    fig.subplots_adjust(left=0.10, right=0.82, top=0.80, bottom=0.13)

    for engine in ("duckdb", "polars", "spark"):
        xs = [rows_for[s] for s in scales]
        ys = [statistics.median(agg[(s, engine)]) for s in scales]
        ax.plot(xs, ys, marker="o", markersize=8, linewidth=2,
                color=COLORS[engine], label=LABELS[engine], zorder=3)
        # Direct label at the right end so identity is never colour alone.
        ax.annotate(LABELS[engine], (xs[-1], ys[-1]), xytext=(12, 0),
                    textcoords="offset points", va="center",
                    fontsize=11, color=COLORS[engine], fontweight="bold")

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks([rows_for[s] for s in scales])
    ax.set_xticklabels([f"{rows_for[s]:,}\n({s} file{'s' if s > 1 else ''})" for s in scales])
    ax.set_xlabel("input rows", fontsize=10.5, color=MUTED)
    ax.set_ylabel("seconds (median of 3, log scale)", fontsize=10.5, color=MUTED)
    ax.grid(True, which="major", color=GRID, linewidth=1, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=10)

    fig.text(0.035, 0.945, "Bronze to silver: same transform, three engines",
             fontsize=14.5, fontweight="bold", color=INK, va="center")
    fig.text(0.035, 0.888,
             "One machine. All three produce identical output row counts at every scale.",
             fontsize=10, color=MUTED, va="center")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT, facecolor=SURFACE)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
