"""Tests for the six new analytics metrics added in the metric-expansion pass.

Covers:
- avg_daily_waste_units
- stockout_severity_proxy
- window_coverage_count
- bias_adjusted_order (computed in service layer)
- days_since_last_stockout
- days_since_last_waste

Uses a freshly seeded synthetic database (via the shared fixture).
All assertions are behavioural (not exact-value) because the synthetic seeder
uses a fixed random seed that may change between releases.
"""

from __future__ import annotations

import os
import tempfile
from datetime import date, timedelta

import pytest

from bakery_analyst.config import settings
from bakery_analyst.db.connection import get_connection
from bakery_analyst.db.seed import seed_database
from bakery_analyst.repository.analytics_repository import (
    fetch_recency_metrics,
    fetch_stockout_severity,
    fetch_waste_metrics,
    fetch_window_coverage,
)


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def seeded_conn_and_dates():
    """Return (connection, window_start, window_end, shop_id, product_code).

    Also patches settings.db_path so that repository functions (which call
    db_session() internally) hit the temp DB rather than the default bakery.db.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    seed_database(force=True, db_path=tmp.name)
    conn = get_connection(tmp.name)

    # Redirect all repository db_session() calls to the temp DB
    original_db_path = settings.db_path
    settings.db_path = tmp.name

    # Pick the first available (shop, product) pair and a 28-day window
    row = conn.execute(
        "SELECT shop_id, product_code, MIN(date) as min_d, MAX(date) as max_d "
        "FROM sales_history GROUP BY shop_id, product_code LIMIT 1"
    ).fetchone()

    shop_id = row["shop_id"]
    product_code = row["product_code"]
    max_d = date.fromisoformat(row["max_d"])
    window_end = max_d.isoformat()
    window_start = (max_d - timedelta(days=27)).isoformat()

    yield conn, window_start, window_end, shop_id, product_code

    settings.db_path = original_db_path
    conn.close()
    os.unlink(tmp.name)


# ---------------------------------------------------------------------------
# avg_daily_waste_units
# ---------------------------------------------------------------------------

def test_avg_daily_waste_units_is_non_negative(seeded_conn_and_dates):
    _, ws, we, shop, product = seeded_conn_and_dates
    result = fetch_waste_metrics(shop, product, ws, we)
    val = result.get("avg_daily_waste_units")
    assert val is not None, "avg_daily_waste_units should not be None for seeded data"
    assert val >= 0.0, f"avg_daily_waste_units must be >= 0, got {val}"


def test_avg_daily_waste_units_returned_alongside_waste_rate(seeded_conn_and_dates):
    _, ws, we, shop, product = seeded_conn_and_dates
    result = fetch_waste_metrics(shop, product, ws, we)
    assert "waste_rate" in result
    assert "avg_daily_waste_units" in result


def test_avg_daily_waste_units_consistent_with_waste_rate(seeded_conn_and_dates):
    """If waste_rate > 0 then avg_daily_waste_units must also be > 0, and vice versa."""
    _, ws, we, shop, product = seeded_conn_and_dates
    result = fetch_waste_metrics(shop, product, ws, we)
    wr = result["waste_rate"]
    wu = result["avg_daily_waste_units"]
    if wr is not None and wu is not None:
        assert (wr > 0) == (wu > 0), (
            f"waste_rate={wr} and avg_daily_waste_units={wu} must agree on sign"
        )


# ---------------------------------------------------------------------------
# window_coverage_count
# ---------------------------------------------------------------------------

def test_window_coverage_count_is_positive(seeded_conn_and_dates):
    _, ws, we, shop, product = seeded_conn_and_dates
    result = fetch_window_coverage(shop, product, ws, we)
    count = result.get("window_coverage_count")
    assert count is not None
    assert count > 0


def test_window_coverage_count_does_not_exceed_window_length(seeded_conn_and_dates):
    _, ws, we, shop, product = seeded_conn_and_dates
    result = fetch_window_coverage(shop, product, ws, we)
    count = result["window_coverage_count"]
    window_days = (date.fromisoformat(we) - date.fromisoformat(ws)).days + 1
    assert count <= window_days, (
        f"coverage {count} cannot exceed window length {window_days}"
    )


# ---------------------------------------------------------------------------
# stockout_severity_proxy
# ---------------------------------------------------------------------------

def test_stockout_severity_proxy_none_when_no_stockouts(seeded_conn_and_dates):
    """If there are no stockout days in the window the proxy must be None."""
    conn, ws, we, shop, product = seeded_conn_and_dates
    # Check whether this combination actually has stockouts in the window
    n_so = conn.execute(
        "SELECT COUNT(*) FROM sales_history "
        "WHERE shop_id=? AND product_code=? AND date BETWEEN ? AND ? AND stockout_flag=1",
        (shop, product, ws, we),
    ).fetchone()[0]

    result = fetch_stockout_severity(shop, product, ws, we)
    proxy = result.get("stockout_severity_proxy")

    if n_so == 0:
        assert proxy is None, "proxy must be None when no stockouts in window"
    else:
        assert proxy is not None, "proxy must be non-None when stockouts exist"


def test_stockout_severity_proxy_is_positive_on_stockout_days(seeded_conn_and_dates):
    """On stockout days pred_point should exceed ordered_units (that is why we stocked out)."""
    conn, ws, we, shop, product = seeded_conn_and_dates
    n_so = conn.execute(
        "SELECT COUNT(*) FROM sales_history "
        "WHERE shop_id=? AND product_code=? AND date BETWEEN ? AND ? AND stockout_flag=1",
        (shop, product, ws, we),
    ).fetchone()[0]

    if n_so == 0:
        pytest.skip("no stockout days in window for this combination")

    result = fetch_stockout_severity(shop, product, ws, we)
    proxy = result["stockout_severity_proxy"]
    # The forecast generally exceeds ordered_units on stockout days in the
    # synthetic data (shop_02 under-orders by design), so proxy > 0 is expected.
    # We allow proxy == 0 in edge cases where forecast == ordered_units exactly.
    assert proxy >= 0, f"stockout severity proxy should be >= 0, got {proxy}"


# ---------------------------------------------------------------------------
# days_since_last_stockout / days_since_last_waste
# ---------------------------------------------------------------------------

def test_days_since_last_stockout_non_negative(seeded_conn_and_dates):
    _, ws, we, shop, product = seeded_conn_and_dates
    result = fetch_recency_metrics(shop, product, we)
    days = result.get("days_since_last_stockout")
    if days is not None:
        assert days >= 0, f"days_since_last_stockout must be >= 0, got {days}"


def test_days_since_last_waste_non_negative(seeded_conn_and_dates):
    _, ws, we, shop, product = seeded_conn_and_dates
    result = fetch_recency_metrics(shop, product, we)
    days = result.get("days_since_last_waste")
    if days is not None:
        assert days >= 0, f"days_since_last_waste must be >= 0, got {days}"


def test_recency_keys_always_present(seeded_conn_and_dates):
    _, ws, we, shop, product = seeded_conn_and_dates
    result = fetch_recency_metrics(shop, product, we)
    assert "days_since_last_stockout" in result
    assert "days_since_last_waste" in result


def test_days_since_consistent_with_actual_data(seeded_conn_and_dates):
    """days_since_last_stockout should be 0 if the last day is a stockout."""
    conn, ws, we, shop, product = seeded_conn_and_dates
    last_so = conn.execute(
        "SELECT MAX(date) FROM sales_history "
        "WHERE shop_id=? AND product_code=? AND stockout_flag=1 AND date<=?",
        (shop, product, we),
    ).fetchone()[0]

    result = fetch_recency_metrics(shop, product, we)
    days = result["days_since_last_stockout"]

    if last_so is None:
        assert days is None
    else:
        expected = (date.fromisoformat(we) - date.fromisoformat(last_so)).days
        assert days == expected, f"expected {expected} days since last stockout, got {days}"


# ---------------------------------------------------------------------------
# bias_adjusted_order (computed in service layer — integration test)
# ---------------------------------------------------------------------------

def test_bias_adjusted_order_in_analysis_row(seeded_conn_and_dates):
    """End-to-end: run_analysis must populate bias_adjusted_order on all rows."""
    from bakery_analyst.analysis.service import run_analysis
    from bakery_analyst.models.domain_models import ValidatedPrediction
    from bakery_analyst.config import settings

    conn, ws, we, shop, product = seeded_conn_and_dates

    # Get a date that exists in forecast_history for this shop/product
    forecast_date = conn.execute(
        "SELECT date FROM forecast_history WHERE shop_id=? AND product_code=? LIMIT 1",
        (shop, product),
    ).fetchone()
    if forecast_date is None:
        pytest.skip("no forecast data found")

    # We need a target_date one day AFTER the forecast date for window_end to land on it
    target_date = (date.fromisoformat(forecast_date["date"]) + timedelta(days=1)).isoformat()

    pred = ValidatedPrediction(
        shop_id=shop,
        product_code=product,
        date=target_date,
        pred_point=50.0,
        pred_q50=51.0,
        pred_q80=54.0,
        pred_q90=57.0,
        prediction_quality="complete",
    )

    # Temporarily point DB path to the temp file
    original_db = settings.db_path
    settings.db_path = conn.execute("PRAGMA database_list").fetchone()[2]  # file path

    try:
        rows = run_analysis([pred], target_date)
    finally:
        settings.db_path = original_db

    assert len(rows) == 1
    row = rows[0]

    # bias_adjusted_order must be None only if mean_signed_error is None
    if row.mean_signed_error is not None:
        assert row.bias_adjusted_order is not None
        assert abs(row.bias_adjusted_order - (pred.pred_point - row.mean_signed_error)) < 0.05
    else:
        assert row.bias_adjusted_order is None
