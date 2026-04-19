"""
bakery_analyst.reporting.charts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Generate matplotlib charts for flagged shop/product combinations and return
them as base64-encoded PNG strings suitable for embedding in markdown as
``![](data:image/png;base64,...)``.  Nothing is written to disk.
"""

from __future__ import annotations

import base64
import io
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from bakery_analyst.db.connection import db_session
from bakery_analyst.models.domain_models import AnalysisRow


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fig_to_base64(fig) -> str:
    """Render a matplotlib figure to a base64-encoded PNG string, then close it."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("ascii")
    plt.close(fig)
    return encoded


def _date_window(target_date: str, window_days: int) -> tuple[str, str]:
    """Return (start_date_str, end_date_str) for a backwards window ending on target_date."""
    end = date.fromisoformat(target_date)
    start = end - timedelta(days=window_days - 1)
    return start.isoformat(), end.isoformat()


def _no_data_chart(title: str, figsize: tuple[float, float] = (9, 4)) -> str:
    """Return a base64 PNG showing a 'No data available' placeholder."""
    fig, ax = plt.subplots(figsize=figsize)
    ax.text(
        0.5, 0.5, "No data available",
        ha="center", va="center",
        fontsize=14, color="grey",
        transform=ax.transAxes,
    )
    ax.set_title(title)
    ax.axis("off")
    plt.tight_layout()
    return _fig_to_base64(fig)


# ---------------------------------------------------------------------------
# Chart 1 — forecast_vs_actual
# ---------------------------------------------------------------------------

def forecast_vs_actual(
    shop_id: str,
    product_code: str,
    target_date: str,
    mae: float | None = None,
    window_days: int = 28,
) -> str:
    """Return base64-encoded PNG of forecast vs actual trend chart.

    Plots actual units sold alongside the point forecast and a Q50–Q90
    prediction band for the given shop/product over the last *window_days*
    days ending on *target_date*.  Stockout dates are highlighted with red
    triangle markers on the x-axis.

    Parameters
    ----------
    shop_id:
        Identifier of the shop to chart.
    product_code:
        Identifier of the product to chart.
    target_date:
        ISO-8601 date string (``YYYY-MM-DD``) that is the last day of the
        window.
    mae:
        Optional mean absolute error to display in the chart title.
    window_days:
        Number of days in the look-back window (default 28).

    Returns
    -------
    str
        Base64-encoded PNG string.
    """
    start_date, end_date = _date_window(target_date, window_days)
    mae_label = f"{mae:.1f}" if mae is not None else "N/A"
    title = (
        f"Forecast vs Actual — {shop_id} / {product_code} "
        f"({window_days}d)  |  MAE={mae_label}"
    )

    with db_session() as conn:
        sales_rows = conn.execute(
            """
            SELECT date, units_sold, stockout_flag
            FROM sales_history
            WHERE shop_id = ? AND product_code = ?
              AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            (shop_id, product_code, start_date, end_date),
        ).fetchall()

        forecast_rows = conn.execute(
            """
            SELECT date, pred_point, pred_q50, pred_q90
            FROM forecast_history
            WHERE shop_id = ? AND product_code = ?
              AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            (shop_id, product_code, start_date, end_date),
        ).fetchall()

    if not sales_rows and not forecast_rows:
        return _no_data_chart(title)

    # Build lookup dicts keyed by date string
    sales_by_date: dict[str, dict] = {
        r["date"]: {"units_sold": r["units_sold"], "stockout_flag": r["stockout_flag"]}
        for r in sales_rows
    }
    forecast_by_date: dict[str, dict] = {
        r["date"]: {
            "pred_point": r["pred_point"],
            "pred_q50": r["pred_q50"],
            "pred_q90": r["pred_q90"],
        }
        for r in forecast_rows
    }

    all_dates_str = sorted(set(sales_by_date) | set(forecast_by_date))
    if not all_dates_str:
        return _no_data_chart(title)

    all_dates = [date.fromisoformat(d) for d in all_dates_str]

    actual = [sales_by_date.get(d, {}).get("units_sold") for d in all_dates_str]
    forecast = [forecast_by_date.get(d, {}).get("pred_point") for d in all_dates_str]
    q50 = [forecast_by_date.get(d, {}).get("pred_q50") for d in all_dates_str]
    q90 = [forecast_by_date.get(d, {}).get("pred_q90") for d in all_dates_str]
    stockout_dates = [
        date.fromisoformat(d)
        for d in all_dates_str
        if sales_by_date.get(d, {}).get("stockout_flag") == 1
    ]

    fig, ax = plt.subplots(figsize=(9, 4))

    # Shaded band Q50–Q90
    band_dates, band_q50, band_q90 = zip(
        *[
            (all_dates[i], q50[i], q90[i])
            for i in range(len(all_dates))
            if q50[i] is not None and q90[i] is not None
        ]
    ) if any(v is not None for v in q50) else ([], [], [])

    if band_dates:
        ax.fill_between(
            band_dates,
            band_q50,
            band_q90,
            alpha=0.15,
            color="orange",
            label="Q50–Q90 range",
        )

    # Actual sales line
    actual_pairs = [(all_dates[i], actual[i]) for i in range(len(all_dates)) if actual[i] is not None]
    if actual_pairs:
        ax_dates, ax_vals = zip(*actual_pairs)
        ax.plot(ax_dates, ax_vals, color="steelblue", linewidth=1.8, label="Actual sales")

    # Forecast line
    forecast_pairs = [(all_dates[i], forecast[i]) for i in range(len(all_dates)) if forecast[i] is not None]
    if forecast_pairs:
        fc_dates, fc_vals = zip(*forecast_pairs)
        ax.plot(fc_dates, fc_vals, color="orange", linestyle="--", linewidth=1.6, label="Forecast")

    # Stockout markers
    if stockout_dates:
        ax.scatter(
            stockout_dates,
            [0] * len(stockout_dates),
            marker="^",
            color="red",
            zorder=5,
            label="Stockout",
            s=60,
        )

    # X-axis tick management — show every 7th date to avoid crowding
    tick_dates = all_dates[::7]
    ax.set_xticks(tick_dates)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.set_title(title)
    ax.set_ylabel("Units")
    ax.legend(loc="lower left")
    plt.tight_layout()

    return _fig_to_base64(fig)


# ---------------------------------------------------------------------------
# Chart 2 — waste_stockout_bars
# ---------------------------------------------------------------------------

def waste_stockout_bars(rows: list[AnalysisRow]) -> str:
    """Return base64-encoded PNG of waste and stockout rate bar chart.

    Renders a grouped bar chart with one group per shop/product combination.
    Bars belonging to a flagged combination (any flag set) receive a thin
    black border to visually distinguish them.

    Parameters
    ----------
    rows:
        All :class:`~bakery_analyst.models.domain_models.AnalysisRow` objects
        for the report run (not just flagged ones, so full context is shown).

    Returns
    -------
    str
        Base64-encoded PNG string.
    """
    if not rows:
        return _no_data_chart("Waste & Stockout Rates by Shop/Product (28d)", figsize=(10, 5))

    labels = [f"{r.shop_id}\n{r.product_code}" for r in rows]
    waste_vals = [(r.waste_rate * 100 if r.waste_rate is not None else 0.0) for r in rows]
    stockout_vals = [(r.stockout_rate * 100 if r.stockout_rate is not None else 0.0) for r in rows]

    def _is_flagged(row: AnalysisRow) -> bool:
        return any([
            row.high_waste_flag,
            row.frequent_stockout_flag,
            row.high_variability_flag,
            row.persistent_overforecast_flag,
            row.incomplete_prediction_flag,
        ])

    n = len(rows)
    x = list(range(n))
    bar_width = 0.35

    fig, ax = plt.subplots(figsize=(10, 5))

    for i, (row, wv, sv) in enumerate(zip(rows, waste_vals, stockout_vals)):
        flagged = _is_flagged(row)
        edge_color = "black" if flagged else "none"
        lw = 1.2 if flagged else 0.0

        ax.bar(
            i - bar_width / 2,
            wv,
            width=bar_width,
            color="steelblue",
            edgecolor=edge_color,
            linewidth=lw,
            label="Waste rate (%)" if i == 0 else "_nolegend_",
        )
        ax.bar(
            i + bar_width / 2,
            sv,
            width=bar_width,
            color="tomato",
            edgecolor=edge_color,
            linewidth=lw,
            label="Stockout rate (%)" if i == 0 else "_nolegend_",
        )

    # Reference lines
    ax.axhline(20, color="steelblue", linestyle="--", linewidth=0.9, label="Waste threshold (20%)")
    ax.axhline(15, color="tomato", linestyle="--", linewidth=0.9, label="Stockout threshold (15%)")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Rate (%)")
    ax.set_ylim(0, 100)
    ax.set_title("Waste & Stockout Rates by Shop/Product (28d)")
    ax.legend(loc="upper right")
    plt.tight_layout()

    return _fig_to_base64(fig)


# ---------------------------------------------------------------------------
# Chart 3 — temp_vs_sales_scatter
# ---------------------------------------------------------------------------

def temp_vs_sales_scatter(
    shop_id: str,
    product_code: str,
    target_date: str,
    temp_sales_correlation: float,
    window_days: int = 28,
) -> str | None:
    """Return base64-encoded PNG of temperature vs sales scatter, or None.

    Only generates the chart when ``|temp_sales_correlation| > 0.35``.  The
    regression line is computed with plain Python arithmetic (no scipy
    dependency).

    Parameters
    ----------
    shop_id:
        Identifier of the shop to chart.
    product_code:
        Identifier of the product to chart.
    target_date:
        ISO-8601 date string (``YYYY-MM-DD``) that is the last day of the
        window.
    temp_sales_correlation:
        Pre-computed Pearson r value between temperature and units sold.
    window_days:
        Number of days in the look-back window (default 28).

    Returns
    -------
    str | None
        Base64-encoded PNG string, or ``None`` if ``|r| <= 0.35`` or there
        is insufficient data to render a meaningful chart.
    """
    if abs(temp_sales_correlation) <= 0.35:
        return None

    start_date, end_date = _date_window(target_date, window_days)
    title = f"Temperature vs Sales — {shop_id} / {product_code} ({window_days}d)"

    with db_session() as conn:
        weather_rows = conn.execute(
            """
            SELECT date, temp
            FROM weather_daily
            WHERE date BETWEEN ? AND ?
            ORDER BY date
            """,
            (start_date, end_date),
        ).fetchall()

        sales_rows = conn.execute(
            """
            SELECT date, units_sold
            FROM sales_history
            WHERE shop_id = ? AND product_code = ?
              AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            (shop_id, product_code, start_date, end_date),
        ).fetchall()

    if not weather_rows or not sales_rows:
        return None

    temp_by_date: dict[str, float] = {r["date"]: r["temp"] for r in weather_rows}
    sales_by_date: dict[str, float] = {r["date"]: r["units_sold"] for r in sales_rows}

    common_dates = sorted(set(temp_by_date) & set(sales_by_date))
    if len(common_dates) < 3:
        return None

    temps = [temp_by_date[d] for d in common_dates]
    sales = [sales_by_date[d] for d in common_dates]

    n = len(temps)
    mean_t = sum(temps) / n
    mean_s = sum(sales) / n

    cov_ts = sum((temps[i] - mean_t) * (sales[i] - mean_s) for i in range(n)) / n
    var_t = sum((t - mean_t) ** 2 for t in temps) / n

    if var_t == 0:
        return None

    slope = cov_ts / var_t
    intercept = mean_s - slope * mean_t

    t_min, t_max = min(temps), max(temps)
    reg_x = [t_min, t_max]
    reg_y = [slope * t + intercept for t in reg_x]

    fig, ax = plt.subplots(figsize=(6, 5))

    ax.scatter(temps, sales, color="grey", alpha=0.6, zorder=2)
    ax.plot(reg_x, reg_y, color="red", linestyle="--", linewidth=1.6, zorder=3)

    ax.annotate(
        f"r = {temp_sales_correlation:.2f}",
        xy=(0.05, 0.92),
        xycoords="axes fraction",
        fontsize=11,
        color="darkred",
    )

    ax.set_xlabel("Temperature (°C)")
    ax.set_ylabel("Units sold")
    ax.set_title(title)
    plt.tight_layout()

    return _fig_to_base64(fig)


# ---------------------------------------------------------------------------
# ChartBundle + main entry point
# ---------------------------------------------------------------------------

@dataclass
class ChartBundle:
    """All charts generated for a report run."""

    forecast_vs_actual: str          # always present (base64 PNG)
    waste_stockout_bars: str | None  # present if >= 2 flagged rows
    temp_scatter: str | None         # present if |r| > 0.35 on primary row

    primary_shop_id: str
    primary_product_code: str


def generate_report_charts(
    rows: list[AnalysisRow],
    target_date: str,
) -> ChartBundle | None:
    """Generate all charts for the report.

    Returns ``None`` if no rows carry any flag.  Otherwise, the *primary*
    combination — defined as the one with the most flags set, with ties broken
    by the highest ``stockout_rate`` — drives :func:`forecast_vs_actual` and
    optionally :func:`temp_vs_sales_scatter`.  :func:`waste_stockout_bars` is
    generated whenever there are at least two flagged rows.

    Parameters
    ----------
    rows:
        All :class:`~bakery_analyst.models.domain_models.AnalysisRow` objects
        for the report run.
    target_date:
        ISO-8601 date string (``YYYY-MM-DD``) representing the analysis date.

    Returns
    -------
    ChartBundle | None
        A populated :class:`ChartBundle`, or ``None`` if nothing is flagged.
    """

    def _flag_count(row: AnalysisRow) -> int:
        return sum([
            bool(row.high_waste_flag),
            bool(row.frequent_stockout_flag),
            bool(row.high_variability_flag),
            bool(row.persistent_overforecast_flag),
            bool(row.incomplete_prediction_flag),
        ])

    flagged_rows = [r for r in rows if _flag_count(r) > 0]
    if not flagged_rows:
        return None

    # Sort: most flags first, then highest stockout_rate as tiebreaker
    flagged_rows.sort(
        key=lambda r: (_flag_count(r), r.stockout_rate if r.stockout_rate is not None else 0.0),
        reverse=True,
    )
    primary = flagged_rows[0]

    fva_chart = forecast_vs_actual(
        shop_id=primary.shop_id,
        product_code=primary.product_code,
        target_date=target_date,
        mae=primary.mae,
    )

    wsb_chart: str | None = None
    if len(flagged_rows) >= 2:
        wsb_chart = waste_stockout_bars(rows)

    scatter_chart: str | None = None
    r = primary.temp_sales_correlation
    if r is not None and abs(r) > 0.35:
        scatter_chart = temp_vs_sales_scatter(
            shop_id=primary.shop_id,
            product_code=primary.product_code,
            target_date=target_date,
            temp_sales_correlation=r,
        )

    return ChartBundle(
        forecast_vs_actual=fva_chart,
        waste_stockout_bars=wsb_chart,
        temp_scatter=scatter_chart,
        primary_shop_id=primary.shop_id,
        primary_product_code=primary.product_code,
    )
