"""Regenerate the README chart from the gold star schema.

    python scripts/plot_top_event_types.py

Reads gold.duckdb (built by `make dbt-build`) and writes
docs/images/gold_top_event_types.png.
"""

from __future__ import annotations

import pathlib

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ACCENT = "#00879B"
INK = "#1F2328"
MUTED = "#6B7280"
GRID = "#E6E8EB"
SURFACE = "#FFFFFF"

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "images" / "gold_top_event_types.png"

QUERY = """
select c.root_description as event_type, count(*) as events
from main_marts.fact_events f
join main_marts.dim_cameo_event c on f.cameo_key = c.cameo_key
where c.root_description is not null
group by 1 order by events desc limit 10
"""


def main() -> None:
    con = duckdb.connect(str(ROOT / "gold.duckdb"), read_only=True)
    rows = con.execute(QUERY).fetchall()
    total = con.execute("select count(*) from main_marts.fact_events").fetchone()[0]
    days = con.execute("select count(*) from main_marts.dim_date").fetchone()[0]
    con.close()

    labels = [r[0] for r in rows][::-1]
    values = [r[1] for r in rows][::-1]

    fig, ax = plt.subplots(figsize=(9, 5.2), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    # Explicit margins leave room for the header text block above the plot.
    fig.subplots_adjust(left=0.30, right=0.965, top=0.82, bottom=0.05)

    ax.barh(labels, values, height=0.62, color=ACCENT, zorder=3)

    # Recessive axes: no frame, no x ruler; the value labels carry magnitude.
    ax.set_axisbelow(True)
    for side in ("top", "right", "bottom", "left"):
        ax.spines[side].set_visible(False)
    ax.tick_params(axis="both", length=0, labelsize=10.5)
    for lbl in ax.get_yticklabels():
        lbl.set_color(INK)
    ax.set_xticks([])

    # Direct value labels: magnitude is never color-alone.
    span = max(values)
    for y, v in enumerate(values):
        ax.text(v + span * 0.012, y, f"{v:,}", va="center", ha="left",
                fontsize=10.5, color=INK)
    ax.set_xlim(0, span * 1.13)

    # Header drawn in figure coordinates so title and subtitle never collide.
    fig.text(0.035, 0.945, "Events by CAMEO event type",
             fontsize=14.5, fontweight="bold", color=INK, va="center")
    fig.text(0.035, 0.892,
             f"Top 10 of {total:,} events across {days} days, from the gold star schema",
             fontsize=10, color=MUTED, va="center")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT, facecolor=SURFACE)
    print(f"wrote {OUT} ({total:,} events, {len(rows)} bars)")


if __name__ == "__main__":
    main()
