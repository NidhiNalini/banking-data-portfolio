
# Credit Risk Dashboard 
#
# Reads from the segmentation database built in Week 6.
# Produces five charts that answer real banking business questions.
# Output: data/credit_risk_dashboard.png
#
# Design principle: every chart earns its place by answering
# a specific question. No chart exists just to look impressive.

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
from pathlib import Path

# ── Configuration ────────────────────────────────────────────
DB_PATH  = "../02-customer-segmentation/data/segmentation.db"
OUT_PATH = "data/credit_risk_dashboard.png"

# Colour palette — consistent with banking brand aesthetics
COLOURS = {
    "HIGH_VALUE":   "#00c9a7",
    "NEW_CUSTOMER": "#4a9eff",
    "AT_RISK":      "#f5a623",
    "DORMANT":      "#e05c5c",
}

DARK_BG  = "#0c0e0f"
SURFACE  = "#131618"
INK2     = "#9da3b4"
INK3     = "#565d72"


# ── Data Loading ─────────────────────────────────────────────
def load_data() -> dict:
    """Load all data needed for the dashboard in one place."""
    conn = sqlite3.connect(DB_PATH)

    data = {
        "segments": pd.read_sql("""
            SELECT segment, COUNT(*) AS count,
                   ROUND(COUNT(*) * 100.0 /
                   SUM(COUNT(*)) OVER (), 1) AS pct
            FROM rfm_scores
            GROUP BY segment
            ORDER BY count DESC
        """, conn),

        "spend": pd.read_sql("""
            SELECT segment,
                   ROUND(AVG(monetary_avg), 2) AS avg_spend,
                   ROUND(AVG(frequency), 1)    AS avg_freq
            FROM rfm_scores
            GROUP BY segment
            ORDER BY avg_spend DESC
        """, conn),

        "scatter": pd.read_sql("""
            SELECT account_id, recency_days, frequency,
                   monetary_avg, segment
            FROM rfm_scores
        """, conn),

        "province": pd.read_sql("""
            SELECT c.province, r.segment, COUNT(*) AS count
            FROM rfm_scores r
            JOIN customers c
              ON r.account_id = c.account_id
            GROUP BY c.province, r.segment
            ORDER BY c.province, count DESC
        """, conn),

        "summary": pd.read_sql("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN segment='HIGH_VALUE' THEN 1 ELSE 0 END) AS hv,
                   SUM(CASE WHEN segment='AT_RISK'    THEN 1 ELSE 0 END) AS ar,
                   SUM(CASE WHEN segment='DORMANT'    THEN 1 ELSE 0 END) AS do,
                   ROUND(AVG(monetary_avg), 2) AS avg_spend
            FROM rfm_scores
        """, conn),
    }

    conn.close()
    return data


# ── Styling Helper ──────────────────────────────────────────
def apply_style(ax, title: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, color="#e8eaf0", fontsize=11,
                 fontweight="bold", pad=12, loc="left")
    ax.tick_params(colors=INK2, labelsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(INK3)
    ax.spines["bottom"].set_color(INK3)


# ── Charts ──────────────────────────────────────────────────
def chart_segment_distribution(ax, segments: pd.DataFrame) -> None:
    colours = [COLOURS.get(s, "#888") for s in segments["segment"]]
    bars = ax.barh(segments["segment"], segments["count"],
                   color=colours, height=0.6)

    for bar, pct in zip(bars, segments["pct"]):
        ax.text(bar.get_width() + 0.3,
                bar.get_y() + bar.get_height() / 2,
                f"{pct}%", va="center", color=INK2, fontsize=9)

    ax.set_xlabel("Number of customers", color=INK2, fontsize=9)
    apply_style(ax, "Customer Risk Distribution")


def chart_avg_spend(ax, spend: pd.DataFrame) -> None:
    colours = [COLOURS.get(s, "#888") for s in spend["segment"]]
    bars = ax.bar(spend["segment"], spend["avg_spend"],
                  color=colours, width=0.6)

    for bar in bars:
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 2,
                f"${bar.get_height():.0f}",
                ha="center", color=INK2, fontsize=9)

    ax.set_ylabel("Avg transaction (CAD)", color=INK2, fontsize=9)
    ax.tick_params(axis="x", rotation=15)
    apply_style(ax, "Average Spend by Segment")


def chart_scatter(ax, scatter: pd.DataFrame) -> None:
    for segment, group in scatter.groupby("segment"):
        ax.scatter(group["recency_days"], group["frequency"],
                   c=COLOURS.get(segment, "#888"),
                   label=segment, alpha=0.75, s=55)

    ax.set_xlabel("Days since last transaction", color=INK2, fontsize=9)
    ax.set_ylabel("Transaction count", color=INK2, fontsize=9)
    ax.legend(fontsize=8, framealpha=0.1,
              labelcolor=INK2, facecolor=SURFACE)
    apply_style(ax, "Recency vs Frequency")


def chart_province_heatmap(ax, province: pd.DataFrame) -> None:
    pivot = province.pivot_table(
        index="province",
        columns="segment",
        values="count",
        fill_value=0
    )

    bottom = np.zeros(len(pivot))
    for seg in ["HIGH_VALUE", "NEW_CUSTOMER", "AT_RISK", "DORMANT"]:
        if seg in pivot.columns:
            ax.bar(pivot.index, pivot[seg],
                   bottom=bottom,
                   color=COLOURS[seg], label=seg, width=0.6)
            bottom += pivot[seg].values

    ax.set_ylabel("Customers", color=INK2, fontsize=9)
    ax.legend(fontsize=8, framealpha=0.1,
              labelcolor=INK2, facecolor=SURFACE)
    apply_style(ax, "Portfolio Health by Province")


def chart_rfm_distribution(ax) -> None:
    conn = sqlite3.connect(DB_PATH)
    rfm_dist = pd.read_sql("SELECT rfm_total FROM rfm_scores", conn)
    conn.close()

    ax.hist(rfm_dist["rfm_total"],
            bins=range(3, 11),
            color="#d4a843",
            alpha=0.8,
            edgecolor=DARK_BG)

    mean_score = rfm_dist["rfm_total"].mean()
    ax.axvline(mean_score,
               linestyle="--",
               color="#e8eaf0",
               label=f"Mean: {mean_score:.1f}")

    ax.legend(fontsize=8, framealpha=0.1,
              labelcolor=INK2, facecolor=SURFACE)
    ax.set_xlabel("RFM Total Score", color=INK2, fontsize=9)
    ax.set_ylabel("Number of customers", color=INK2, fontsize=9)
    apply_style(ax, "RFM Score Distribution")


# ── Summary Printout ────────────────────────────────────────
def print_dashboard_summary(summary: pd.DataFrame) -> None:
    row = summary.iloc[0]

    print("\n" + "=" * 50)
    print("PORTFOLIO HEALTH SUMMARY")
    print("=" * 50)
    print(f"Total customers      : {row['total']}")
    print(f"High‑value customers : {row['hv']}")
    print(f"At‑risk customers    : {row['ar']}")
    print(f"Dormant customers    : {row['do']}")
    print(f"Avg transaction spend: ${row['avg_spend']:.2f}")

    at_risk_pct = (row["ar"] / row["total"]) * 100
    print(f"AT_RISK as % of portfolio: {at_risk_pct:.1f}%")

    if at_risk_pct > 40:
        print("⚠ WARNING: AT_RISK exceeds 40% threshold")


# ── Main ────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Loading data from segmentation database...")
    data = load_data()

    print_dashboard_summary(data["summary"])

    fig = plt.figure(figsize=(18, 12), facecolor=DARK_BG)
    fig.suptitle("Customer Credit Risk Dashboard",
                 color="#e8eaf0", fontsize=16, fontweight="bold")

    gs = gridspec.GridSpec(2, 3, hspace=0.45, wspace=0.35)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[0, 2])
    ax4 = fig.add_subplot(gs[1, 0])
    ax5 = fig.add_subplot(gs[1, 1:3])

    chart_segment_distribution(ax1, data["segments"])
    chart_avg_spend(ax2, data["spend"])
    chart_scatter(ax3, data["scatter"])
    chart_province_heatmap(ax4, data["province"])
    chart_rfm_distribution(ax5)

    Path("data").mkdir(exist_ok=True)
    plt.savefig(OUT_PATH, dpi=150, bbox_inches="tight",
                facecolor=DARK_BG)
    plt.show()