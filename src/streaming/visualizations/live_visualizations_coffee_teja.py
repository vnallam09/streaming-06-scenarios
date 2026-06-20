"""src/streaming/visualizations/live_visualizations_coffee_teja.py.

Live visualization for the COFFEE SHOP scenario (Phase 5).

This is my copy of live_visualizations_case.py with a DIFFERENT live chart.

The case example drew a line chart of sale total by message (one point per
message). This version draws a live BAR chart of cumulative revenue per
store. Each consumed order adds its total to that store's bar, so the chart
shows which stores are pulling ahead as orders stream in.

Because the chart tracks running totals per store instead of a point series,
init_live_chart returns a {store_id: revenue} dict instead of x/y lists.

Author: Teja (adapted from Denise Case's live_visualizations_case.py)
Date: 2026-06
"""

# === DECLARE IMPORTS ===

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

# === DECLARE EXPORTS ===

__all__ = [
    "close_live_chart",
    "init_live_chart",
    "save_live_chart",
    "update_live_chart",
]

# Coffee-brown bars.
_BAR_COLOR = "#6f4e37"


# === DEFINE LIVE CHART HELPERS ===


def init_live_chart() -> tuple[Any, Any, dict[str, float]]:
    """Create and show an empty live bar chart.

    Returns:
        A tuple of (figure, axis, store_totals) where store_totals is an
        empty dict mapping store_id to cumulative revenue.
    """
    plt.ion()
    figure, axis = plt.subplots()

    # Running revenue per store, updated as each order is consumed.
    store_totals: dict[str, float] = {}

    axis.set_title("Cumulative Coffee Revenue by Store (live)")
    axis.set_xlabel("Store")
    axis.set_ylabel("Revenue ($)")

    figure.show()
    figure.canvas.draw()
    figure.canvas.flush_events()

    return figure, axis, store_totals


def update_live_chart(
    *,
    figure: Any,
    axis: Any,
    store_totals: dict[str, float],
    message: dict[str, Any],
) -> None:
    """Update the live bar chart with one consumed coffee order.

    All arguments after the asterisk (*) must be passed as keyword arguments.

    Arguments:
        figure: Matplotlib figure.
        axis: Matplotlib axis.
        store_totals: Running revenue per store_id (updated in place).
        message: One enriched Kafka message dictionary.
    """
    # Add this order's total to its store's running revenue.
    store_id = str(message["store_id"])
    store_totals[store_id] = store_totals.get(store_id, 0.0) + float(message["total"])

    # Redraw the bars from the updated totals.
    axis.clear()
    stores = sorted(store_totals)
    revenues = [store_totals[store] for store in stores]
    bars = axis.bar(stores, revenues, color=_BAR_COLOR)

    # Label each bar with its current revenue so the chart is readable live.
    axis.bar_label(bars, fmt="$%.0f", padding=2)

    axis.set_title("Cumulative Coffee Revenue by Store (live)")
    axis.set_xlabel("Store")
    axis.set_ylabel("Revenue ($)")
    axis.grid(True, axis="y")

    figure.canvas.draw()
    figure.canvas.flush_events()
    plt.pause(0.05)


def save_live_chart(
    *,
    figure: Any,
    chart_path: Path,
) -> None:
    """Save the final live chart to an image file.

    All arguments after the asterisk (*) must be passed as keyword arguments.

    Arguments:
        figure: Matplotlib figure.
        chart_path: Output image path.
    """
    chart_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart_path, bbox_inches="tight")


def close_live_chart() -> None:
    """Turn off interactive chart mode."""
    plt.ioff()
