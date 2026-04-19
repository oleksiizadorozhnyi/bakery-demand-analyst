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
    END AS waste_rate
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
# Repository functions
# ---------------------------------------------------------------------------

def _params(shop_id: str, product_code: str, window_start: str, window_end: str) -> dict:
    return {
        "shop_id": shop_id,
        "product_code": product_code,
        "window_start": window_start,
        "window_end": window_end,
    }


def fetch_bias_metrics(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return mean_signed_error, mae, overforecast_ratio over the given window."""
    with db_session() as conn:
        row = conn.execute(BIAS_SQL, _params(shop_id, product_code, window_start, window_end)).fetchone()
    return _row_to_dict(row, ("mean_signed_error", "mae", "overforecast_ratio"))


def fetch_recent_bias(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return mean_signed_error over a shorter recent window."""
    with db_session() as conn:
        row = conn.execute(RECENT_BIAS_SQL, _params(shop_id, product_code, window_start, window_end)).fetchone()
    return _row_to_dict(row, ("recent_mean_signed_error",))


def fetch_waste_metrics(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return waste_rate = sum(waste_units) / sum(ordered_units)."""
    with db_session() as conn:
        row = conn.execute(WASTE_SQL, _params(shop_id, product_code, window_start, window_end)).fetchone()
    return _row_to_dict(row, ("waste_rate",))


def fetch_stockout_metrics(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return stockout_rate = fraction of days with a stockout."""
    with db_session() as conn:
        row = conn.execute(STOCKOUT_SQL, _params(shop_id, product_code, window_start, window_end)).fetchone()
    return _row_to_dict(row, ("stockout_rate",))


def fetch_variability_metrics(
    shop_id: str, product_code: str, window_start: str, window_end: str
) -> dict[str, float | None]:
    """Return stddev_units_sold and coefficient_of_variation.

    SQLite lacks a native STDDEV function; the variance identity
    ``Var(X) = E[X²] - E[X]²`` is used inside the SQL query instead.
    """
    with db_session() as conn:
        row = conn.execute(VARIABILITY_SQL, _params(shop_id, product_code, window_start, window_end)).fetchone()

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


def _row_to_dict(row: sqlite3.Row, keys: tuple[str, ...]) -> dict[str, float | None]:
    return {k: row[k] for k in keys}
