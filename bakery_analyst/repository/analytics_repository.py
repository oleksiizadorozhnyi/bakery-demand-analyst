"""Data-access layer for historical analytics queries.

All metric computation is done in SQL except temperature–sales correlation,
which requires a statistical function not available in SQLite. That calculation
is explicitly isolated in :func:`compute_temp_sales_correlation` with a clear
comment explaining the deviation.
"""

from __future__ import annotations

import math
import sqlite3
from typing import Any

from bakery_analyst.db.connection import db_session


# ---------------------------------------------------------------------------
# Bias metrics
# ---------------------------------------------------------------------------

BIAS_SQL = """
SELECT
    AVG(f.pred_point - s.units_sold)          AS mean_signed_error,
    AVG(ABS(f.pred_point - s.units_sold))     AS mae,
    AVG(CASE WHEN f.pred_point > s.units_sold THEN 1.0 ELSE 0.0 END)
                                               AS overforecast_ratio
FROM forecast_history f
JOIN sales_history s
    ON  f.shop_id      = s.shop_id
    AND f.product_code = s.product_code
    AND f.date         = s.date
WHERE f.shop_id      = :shop_id
  AND f.product_code = :product_code
  AND f.date >= :window_start
  AND f.date <= :window_end
"""

RECENT_BIAS_SQL = """
SELECT
    AVG(f.pred_point - s.units_sold) AS recent_mean_signed_error
FROM forecast_history f
JOIN sales_history s
    ON  f.shop_id      = s.shop_id
    AND f.product_code = s.product_code
    AND f.date         = s.date
WHERE f.shop_id      = :shop_id
  AND f.product_code = :product_code
  AND f.date >= :window_start
  AND f.date <= :window_end
"""


# ---------------------------------------------------------------------------
# Waste metrics
# ---------------------------------------------------------------------------

WASTE_SQL = """
SELECT
    CASE
        WHEN SUM(ordered_units) > 0
        THEN CAST(SUM(waste_units) AS REAL) / SUM(ordered_units)
        ELSE NULL
    END          AS waste_rate,
    AVG(CAST(waste_units AS REAL)) AS avg_daily_waste_units
FROM sales_history
WHERE shop_id      = :shop_id
  AND product_code = :product_code
  AND date >= :window_start
  AND date <= :window_end
"""


# ---------------------------------------------------------------------------
# Stockout / reliability
# ---------------------------------------------------------------------------

STOCKOUT_SQL = """
SELECT
    AVG(CAST(stockout_flag AS REAL)) AS stockout_rate
FROM sales_history
WHERE shop_id      = :shop_id
  AND product_code = :product_code
  AND date >= :window_start
  AND date <= :window_end
"""

# Stockout severity proxy — observed data only.
#
# On days where the shop ran out (stockout_flag = 1) the forecast was higher
# than what was actually ordered.  The average difference (pred_point minus
# ordered_units) across those days is a proxy for the typical daily shortfall
# as seen by the forecasting model.
#
# IMPORTANT: this is NOT true lost demand.  We do not observe how many
# customers were turned away after the stock-out occurred.  The forecasting
# model may itself be biased, so this figure should always be presented as an
# *estimate* rather than a fact.
STOCKOUT_SEVERITY_SQL = """
SELECT
    AVG(f.pred_point - s.ordered_units) AS stockout_severity_proxy
FROM sales_history s
JOIN forecast_history f
    ON  s.shop_id      = f.shop_id
    AND s.product_code = f.product_code
    AND s.date         = f.date
WHERE s.shop_id      = :shop_id
  AND s.product_code = :product_code
  AND s.date >= :window_start
  AND s.date <= :window_end
  AND s.stockout_flag = 1
"""


# ---------------------------------------------------------------------------
# Demand variability
# ---------------------------------------------------------------------------

VARIABILITY_SQL = """
SELECT
    AVG(units_sold)  AS avg_units,
    -- SQLite has no STDDEV; compute via variance identity: E[X²] - E[X]²
    SQRT(
        MAX(0.0,
            AVG(units_sold * units_sold) - AVG(units_sold) * AVG(units_sold)
        )
    )                AS stddev_units
FROM sales_history
WHERE shop_id      = :shop_id
  AND product_code = :product_code
  AND date >= :window_start
  AND date <= :window_end
"""


# ---------------------------------------------------------------------------
# Window coverage
# ---------------------------------------------------------------------------

# Number of calendar days in the main window that have at least one
# sales_history record.  Used as a confidence indicator in the report —
# a window with only 12 of 28 days populated warrants more cautious language
# than one with 27 of 28.
WINDOW_COVERAGE_SQL = """
SELECT COUNT(DISTINCT date) AS window_coverage_count
FROM sales_history
WHERE shop_id      = :shop_id
  AND product_code = :product_code
  AND date >= :window_start
  AND date <= :window_end
"""


# ---------------------------------------------------------------------------
# Recency — days since last event
# ---------------------------------------------------------------------------

# Searches all history up to (and including) window_end so that events
# outside the 28-day analysis window are still visible.  A shop that last
# stocked out 35 days ago shows 35, not NULL.
DAYS_SINCE_LAST_STOCKOUT_SQL = """
SELECT
    CAST(julianday(:window_end) - julianday(MAX(date)) AS INTEGER)
        AS days_since_last_stockout
FROM sales_history
WHERE shop_id      = :shop_id
  AND product_code = :product_code
  AND stockout_flag = 1
  AND date <= :window_end
"""

DAYS_SINCE_LAST_WASTE_SQL = """
SELECT
    CAST(julianday(:window_end) - julianday(MAX(date)) AS INTEGER)
        AS days_since_last_waste
FROM sales_history
WHERE shop_id      = :shop_id
  AND product_code = :product_code
  AND waste_units > 0
  AND date <= :window_end
"""


# ---------------------------------------------------------------------------
# Temperature + sales data for correlation (fetched into Python)
# ---------------------------------------------------------------------------

TEMP_SALES_SQL = """
SELECT
    w.temp,
    s.units_sold
FROM sales_history s
JOIN weather_daily w ON s.date = w.date
WHERE s.shop_id      = :shop_id
  AND s.product_code = :product_code
  AND s.date >= :window_start
  AND s.date <= :window_end
ORDER BY s.date
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _params(shop_id: str, product_code: str, window_start: str, window_end: str) -> dict:
    return {
        "shop_id": shop_id,
        "product_code": product_code,
        "window_start": window_start,
        "window_end": window_end,
    }


def _row_to_dict(row: sqlite3.Row, keys: tuple[str, ...]) -> dict[str, Any]:
    return {k: row[k] for k in keys}


# ---------------------------------------------------------------------------
# Repository functions
# ---------------------------------------------------------------------------

def fetch_bias_metrics(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return mean_signed_error, mae, overforecast_ratio over the given window."""
    with db_session() as conn:
        row = conn.execute(
            BIAS_SQL, _params(shop_id, product_code, window_start, window_end)
        ).fetchone()
    return _row_to_dict(row, ("mean_signed_error", "mae", "overforecast_ratio"))


def fetch_recent_bias(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return mean_signed_error over a shorter recent window."""
    with db_session() as conn:
        row = conn.execute(
            RECENT_BIAS_SQL, _params(shop_id, product_code, window_start, window_end)
        ).fetchone()
    return _row_to_dict(row, ("recent_mean_signed_error",))


def fetch_waste_metrics(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return waste_rate and avg_daily_waste_units.

    waste_rate          = sum(waste_units) / sum(ordered_units) — relative.
    avg_daily_waste_units = AVG(waste_units) per day — absolute operational number.
    """
    with db_session() as conn:
        row = conn.execute(
            WASTE_SQL, _params(shop_id, product_code, window_start, window_end)
        ).fetchone()
    return _row_to_dict(row, ("waste_rate", "avg_daily_waste_units"))


def fetch_stockout_metrics(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return stockout_rate = fraction of days with a stockout."""
    with db_session() as conn:
        row = conn.execute(
            STOCKOUT_SQL, _params(shop_id, product_code, window_start, window_end)
        ).fetchone()
    return _row_to_dict(row, ("stockout_rate",))


def fetch_stockout_severity(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return the stockout severity proxy: avg(pred_point - ordered_units) on stockout days.

    Returns None when no stockout days exist in the window.

    See STOCKOUT_SEVERITY_SQL for the full caveat on what this metric is and
    is NOT.  Always present this value as an *estimated gap*, never as true
    lost demand.
    """
    with db_session() as conn:
        row = conn.execute(
            STOCKOUT_SEVERITY_SQL,
            _params(shop_id, product_code, window_start, window_end),
        ).fetchone()
    return _row_to_dict(row, ("stockout_severity_proxy",))


def fetch_window_coverage(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, int | None]:
    """Return the number of days with sales data in the main analysis window.

    Used as a confidence / trust signal.  A value close to the window length
    (default 28) indicates the metrics are based on a full dataset; lower
    values warrant more cautious language in the report.
    """
    with db_session() as conn:
        row = conn.execute(
            WINDOW_COVERAGE_SQL,
            _params(shop_id, product_code, window_start, window_end),
        ).fetchone()
    count = row["window_coverage_count"]
    return {"window_coverage_count": int(count) if count is not None else None}


def fetch_recency_metrics(
    shop_id: str, product_code: str, window_end: str
) -> dict[str, int | None]:
    """Return days since the last stockout and last waste event.

    Searches the full history up to *window_end* so that events outside the
    28-day analysis window are still surfaced.  Returns None for each metric
    if no qualifying event exists in the historical record.
    """
    params = {"shop_id": shop_id, "product_code": product_code, "window_end": window_end}
    with db_session() as conn:
        so_row = conn.execute(DAYS_SINCE_LAST_STOCKOUT_SQL, params).fetchone()
        w_row = conn.execute(DAYS_SINCE_LAST_WASTE_SQL, params).fetchone()

    so_days = so_row["days_since_last_stockout"] if so_row else None
    w_days = w_row["days_since_last_waste"] if w_row else None
    return {
        "days_since_last_stockout": int(so_days) if so_days is not None else None,
        "days_since_last_waste": int(w_days) if w_days is not None else None,
    }


def fetch_variability_metrics(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return stddev_units_sold and coefficient_of_variation.

    SQLite lacks a native STDDEV function; the variance identity
    ``Var(X) = E[X²] - E[X]²`` is used inside the SQL query instead.

    Note: units_sold is censored at ordered_units on stockout days, so CV
    will understate true demand variability when the stockout rate is high.
    CV is retained as an internal flag trigger but should not be foregrounded
    in client-facing narrative.
    """
    with db_session() as conn:
        row = conn.execute(
            VARIABILITY_SQL, _params(shop_id, product_code, window_start, window_end)
        ).fetchone()

    avg = row["avg_units"]
    std = row["stddev_units"]
    cv = (std / avg) if (avg and avg > 0 and std is not None) else None
    return {"stddev_units_sold": std, "coefficient_of_variation": cv}


def compute_temp_sales_correlation(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> float | None:
    """Compute Pearson r between temperature and units_sold.

    SQLite has no CORR() function, so raw (temp, units_sold) pairs are fetched
    into Python and the correlation is computed using the standard formula.
    This is the only metric not computed entirely in SQL; the deviation is
    intentional and documented here.

    Returns None if fewer than 3 data points exist or if either series has
    zero variance (correlation would be undefined).
    """
    with db_session() as conn:
        rows = conn.execute(
            TEMP_SALES_SQL, _params(shop_id, product_code, window_start, window_end)
        ).fetchall()

    if len(rows) < 3:
        return None

    temps = [r["temp"] for r in rows]
    sales = [r["units_sold"] for r in rows]
    return _pearson_r(temps, sales)


def _pearson_r(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return None
    return cov / (sx * sy)
