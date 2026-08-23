"""Draw the pipeline architecture diagram used in the README.

    python scripts/plot_architecture.py

Writes docs/images/architecture.png. Same palette as plot_benchmark.py so the
two images read as one document rather than two different tools' defaults.
"""

from __future__ import annotations

import pathlib

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "images" / "architecture.png"

INK, MUTED, GRID, SURFACE = "#1F2328", "#6B7280", "#E6E8EB", "#FFFFFF"
TEAL, PURPLE, ORANGE = "#00879B", "#7A5EA8", "#C2622D"
NEUTRAL = "#F3F4F6"

HEADER_H = 0.36


def box(ax: plt.Axes, x: float, y: float, w: float, h: float, label: str, sub: str, color: str) -> tuple[float, float]:
    """Draw one labelled box; returns its (center_x, center_y)."""
    ax.add_patch(
        FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.02,rounding_size=0.09",
            linewidth=1.4, edgecolor=color, facecolor=SURFACE, zorder=3,
        )
    )
    ax.add_patch(
        FancyBboxPatch(
            (x, y + h - HEADER_H), w, HEADER_H,
            boxstyle="round,pad=0,rounding_size=0.09",
            linewidth=0, facecolor=color, zorder=3,
        )
    )
    ax.text(x + w / 2, y + h - HEADER_H / 2, label, ha="center", va="center",
             fontsize=10.5, fontweight="bold", color="white", zorder=4)
    ax.text(x + w / 2, y + (h - HEADER_H) / 2, sub, ha="center", va="center",
             fontsize=9, color=INK, zorder=4, linespacing=1.7)
    return x + w / 2, y + h / 2


def arrow(ax: plt.Axes, p1: tuple[float, float], p2: tuple[float, float],
          color: str = MUTED, style: str = "-", lw: float = 1.6) -> None:
    ax.add_patch(
        FancyArrowPatch(
            p1, p2, arrowstyle="-|>", mutation_scale=14, linewidth=lw,
            color=color, linestyle=style, zorder=2, shrinkA=3, shrinkB=3,
        )
    )


def main() -> None:
    width, height = 13.8, 5.6
    fig, ax = plt.subplots(figsize=(width, height), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    ax.set_xlim(0, width)
    ax.set_ylim(0, height)
    ax.axis("off")

    fig.text(0.025, 0.955, "Bronze, silver, gold: one path, two places it runs",
              fontsize=15, fontweight="bold", color=INK)
    fig.text(0.025, 0.895,
             "Solid arrows move data. Dashed arrows are orchestration and lineage, not data.",
             fontsize=9.5, color=MUTED)

    row_a_y, row_h = 3.5, 1.3
    row_b_y = 1.9

    gx, gy = box(ax, 0.3, row_a_y, 1.6, row_h, "GDELT feed", "15-min batch", MUTED)
    ix, iy = box(ax, 2.25, row_a_y, 1.9, row_h, "Ingest", "Python\nMD5 + checkpoint", TEAL)
    brx, bry = box(ax, 4.5, row_a_y, 1.6, row_h, "Bronze", "S3 / MinIO", NEUTRAL)
    b2sx, b2sy = box(ax, 6.45, row_a_y, 1.9, row_h, "Bronze to\nsilver", "PySpark\nclean + dedup", PURPLE)
    sx, sy = box(ax, 8.7, row_a_y, 1.9, row_h, "Silver", "Iceberg\ntyped, MERGE'd", NEUTRAL)

    s2gx, s2gy = box(ax, 8.7, row_b_y, 1.9, row_h, "Silver to\ngold", "dbt\nstar schema", ORANGE)
    goldx, goldy = box(ax, 10.95, row_b_y, 2.35, row_h, "Gold", "DuckDB / BigQuery /\nAthena", NEUTRAL)

    # medallion flow, left to right
    arrow(ax, (1.9, gy), (2.25, iy))
    arrow(ax, (4.15, iy), (4.5, bry))
    arrow(ax, (6.1, bry), (6.45, b2sy))
    arrow(ax, (8.35, b2sy), (8.7, sy))
    # silver -> dbt -> gold
    arrow(ax, (sx, row_a_y), (s2gx, row_b_y + row_h))
    arrow(ax, (8.7 + 1.9, s2gy), (10.95, goldy))

    # -- orchestration ---------------------------------------------------------
    orch_y = 0.55
    orch_h = 0.7
    mqx, mqy = box(ax, 0.3, orch_y, 1.6, orch_h, "Marquez", "OpenLineage", TEAL)
    afx, afy = box(ax, 2.25, orch_y, 1.9, orch_h, "Airflow", "3 DAGs", MUTED)

    arrow(ax, (1.9, mqy), (2.25, afy), color=TEAL, style="--", lw=1.3)
    arrow(ax, (afx, orch_y + orch_h), (ix, row_a_y), color=MUTED, style="--", lw=1.3)
    arrow(ax, (afx + 0.5, orch_y + orch_h), (b2sx - 0.3, row_a_y), color=MUTED, style="--", lw=1.3)
    arrow(ax, (afx + 1.0, orch_y + orch_h), (s2gx - 0.3, row_b_y + 0.15), color=MUTED, style="--", lw=1.3)

    fig.text(0.025, 0.035,
             "Local: MinIO, REST catalog, DuckDB.   AWS: S3, Glue catalog, Athena or BigQuery.   "
             "Same code, environment-driven config.",
             fontsize=9.5, color=MUTED)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT, facecolor=SURFACE, bbox_inches="tight")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
