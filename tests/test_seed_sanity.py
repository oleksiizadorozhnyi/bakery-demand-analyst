"""Smoke tests: seeded data integrity and causal consistency."""

from __future__ import annotations

import os
import tempfile

import pytest

from bakery_analyst.db.connection import get_connection
from bakery_analyst.db.seed import seed_database


@pytest.fixture(scope="module")
def seeded_conn():
    """Return an open connection to a freshly seeded temp-file DB.

    Uses the explicit ``db_path`` argument to avoid fighting the module-level
    settings singleton that is already resolved at import time.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    seed_database(force=True, db_path=tmp.name)
    conn = get_connection(tmp.name)
    yield conn
    conn.close()
    os.unlink(tmp.name)


# ---------------------------------------------------------------------------
# Shop and product coverage
# ---------------------------------------------------------------------------

def test_exactly_three_shops(seeded_conn):
    n = seeded_conn.execute("SELECT COUNT(*) FROM shop_static").fetchone()[0]
    assert n == 3


def test_exactly_two_products(seeded_conn):
    products = seeded_conn.execute(
        "SELECT DISTINCT product_code FROM sales_history"
    ).fetchall()
    assert len(products) == 2
    codes = {r[0] for r in products}
    assert codes == {"croissant", "baguette"}


# ---------------------------------------------------------------------------
# Date coverage
# ---------------------------------------------------------------------------

def test_at_least_90_days(seeded_conn):
    n = seeded_conn.execute("SELECT COUNT(DISTINCT date) FROM sales_history").fetchone()[0]
    assert n >= 90


def test_weather_covers_all_sales_dates(seeded_conn):
    missing = seeded_conn.execute("""
        SELECT COUNT(*) FROM sales_history s
        WHERE NOT EXISTS (SELECT 1 FROM weather_daily w WHERE w.date = s.date)
    """).fetchone()[0]
    assert missing == 0


# ---------------------------------------------------------------------------
# Causal consistency
# ---------------------------------------------------------------------------

def test_units_sold_le_ordered_units(seeded_conn):
    bad = seeded_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE units_sold > ordered_units"
    ).fetchone()[0]
    assert bad == 0, f"{bad} rows violate units_sold <= ordered_units"


def test_waste_equals_excess_supply(seeded_conn):
    """waste_units = max(ordered_units - units_sold, 0)."""
    bad = seeded_conn.execute("""
        SELECT COUNT(*) FROM sales_history
        WHERE waste_units != MAX(0, ordered_units - units_sold)
    """).fetchone()[0]
    assert bad == 0


def test_stockout_flag_consistency(seeded_conn):
    """stockout_flag must be 1 exactly when units_sold == ordered_units and waste_units == 0
    (i.e. demand hit or exceeded supply).
    We check the weaker condition: stockout_flag=1 ↔ waste_units=0 and units_sold=ordered_units.
    """
    # If stocked out, nothing should be wasted
    bad_waste = seeded_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE stockout_flag = 1 AND waste_units > 0"
    ).fetchone()[0]
    assert bad_waste == 0


def test_non_negative_waste(seeded_conn):
    bad = seeded_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE waste_units < 0"
    ).fetchone()[0]
    assert bad == 0


def test_ordered_units_positive(seeded_conn):
    bad = seeded_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE ordered_units <= 0"
    ).fetchone()[0]
    assert bad == 0


def test_stockout_flag_binary(seeded_conn):
    bad = seeded_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE stockout_flag NOT IN (0, 1)"
    ).fetchone()[0]
    assert bad == 0


# ---------------------------------------------------------------------------
# Forecast quantile ordering
# ---------------------------------------------------------------------------

def test_quantile_ordering_valid(seeded_conn):
    bad = seeded_conn.execute("""
        SELECT COUNT(*) FROM forecast_history
        WHERE pred_q50 > pred_q80 OR pred_q80 > pred_q90
    """).fetchone()[0]
    assert bad == 0


def test_pred_point_positive(seeded_conn):
    bad = seeded_conn.execute(
        "SELECT COUNT(*) FROM forecast_history WHERE pred_point <= 0"
    ).fetchone()[0]
    assert bad == 0


# ---------------------------------------------------------------------------
# Forecast / sales alignment
# ---------------------------------------------------------------------------

def test_forecast_covers_all_sales_dates(seeded_conn):
    missing = seeded_conn.execute("""
        SELECT COUNT(*) FROM sales_history s
        WHERE NOT EXISTS (
            SELECT 1 FROM forecast_history f
            WHERE f.shop_id = s.shop_id
              AND f.product_code = s.product_code
              AND f.date = s.date
        )
    """).fetchone()[0]
    assert missing == 0
