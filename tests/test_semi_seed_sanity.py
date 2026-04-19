"""Smoke tests for the semi-synthetic seeder.

These tests do NOT require a Kaggle download or live network access.
A fake bakery CSV fixture (100 days, both products) is generated
programmatically, and ``load_weather`` is patched to return deterministic
synthetic weather data.
"""

from __future__ import annotations

import os
import random
import sqlite3
import tempfile
from datetime import date, timedelta
from typing import Generator
from unittest.mock import patch

import pytest

from bakery_analyst.db.connection import get_connection
from bakery_analyst.db.loaders.weather_loader import WeatherRow
from bakery_analyst.db.seed_semi import seed_database

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_NUM_DAYS: int = 100
_START_DATE: date = date(2022, 1, 1)
_FIXED_TEMP: float = 15.0
_FIXED_RAIN: float = 1.0
_FIXED_WIND: float = 8.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fake_weather(start: date, end: date) -> dict[date, WeatherRow]:
    """Return a dict of WeatherRow with fixed values for every date in [start, end]."""
    result: dict[date, WeatherRow] = {}
    current = start
    while current <= end:
        result[current] = WeatherRow(
            date=current,
            temp=_FIXED_TEMP,
            rain_mm=_FIXED_RAIN,
            wind=_FIXED_WIND,
        )
        current += timedelta(days=1)
    return result


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fake_bakery_csv(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Write a minimal but valid bakery CSV and return its path.

    Generates 100 days of transaction-level data starting from 2022-01-01.
    Each day contains multiple ticket rows for both products so that the
    aggregation logic inside ``bakery_loader`` is exercised:

    - 3 baguette tickets whose quantities sum to a target in [30, 50]
    - 2 croissant tickets whose quantities sum to a target in [15, 30]

    The RNG is seeded with 7 for full reproducibility.
    """
    rng = random.Random(7)

    tmp_dir = tmp_path_factory.mktemp("csv")
    csv_path = tmp_dir / "fake_bakery.csv"

    rows: list[str] = ["date,time,ticket_number,article,Quantity,unit_price"]
    ticket_number = 1

    for day_offset in range(_NUM_DAYS):
        day = _START_DATE + timedelta(days=day_offset)
        date_str = day.isoformat()

        # --- baguette: 3 tickets summing to a target in [30, 50] ---
        baguette_target = rng.randint(30, 50)
        b1 = rng.randint(5, baguette_target - 10)
        b2 = rng.randint(1, baguette_target - b1 - 1)
        b3 = baguette_target - b1 - b2
        for qty in (b1, b2, b3):
            rows.append(
                f"{date_str},08:{rng.randint(0,59):02d},{ticket_number},"
                f"BAGUETTE,{max(1, qty)},1.10"
            )
            ticket_number += 1

        # --- croissant: 2 tickets summing to a target in [15, 30] ---
        croissant_target = rng.randint(15, 30)
        c1 = rng.randint(1, croissant_target - 1)
        c2 = croissant_target - c1
        for qty in (c1, c2):
            rows.append(
                f"{date_str},09:{rng.randint(0,59):02d},{ticket_number},"
                f"CROISSANT,{max(1, qty)},1.30"
            )
            ticket_number += 1

    csv_path.write_text("\n".join(rows), encoding="utf-8")
    return str(csv_path)


@pytest.fixture(scope="module")
def seeded_semi_conn(
    fake_bakery_csv: str,
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[sqlite3.Connection, None, None]:
    """Seed a temp DB using fake CSV + mocked weather, return an open connection.

    Steps performed:
    1. Patches ``bakery_analyst.db.seed_semi.load_weather`` to return
       deterministic WeatherRow objects (no network).
    2. Patches ``settings.bakery_csv_path`` to point at the fake CSV.
    3. Calls ``seed_database(force=True, db_path=tmp_db_path)``.
    4. Opens and yields the SQLite connection.
    5. Closes the connection and deletes the temp DB on teardown.
    """
    tmp_dir = tmp_path_factory.mktemp("db")
    db_path = str(tmp_dir / "semi_test.db")

    def _fake_load_weather(
        start_date: date,
        end_date: date,
        rng,  # noqa: ANN001
        cache_path=None,  # noqa: ANN001
    ) -> dict[date, WeatherRow]:
        return _make_fake_weather(start_date, end_date)

    with (
        patch(
            "bakery_analyst.db.seed_semi.load_weather",
            side_effect=_fake_load_weather,
        ),
        patch(
            "bakery_analyst.config.settings.bakery_csv_path",
            new=fake_bakery_csv,
        ),
    ):
        seed_database(force=True, db_path=db_path)

    conn = get_connection(db_path)
    yield conn
    conn.close()
    try:
        os.unlink(db_path)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Shop and product coverage
# ---------------------------------------------------------------------------


def test_exactly_three_shops(seeded_semi_conn: sqlite3.Connection) -> None:
    """There must be exactly 3 rows in shop_static."""
    n = seeded_semi_conn.execute("SELECT COUNT(*) FROM shop_static").fetchone()[0]
    assert n == 3


def test_exactly_two_products(seeded_semi_conn: sqlite3.Connection) -> None:
    """sales_history must contain exactly baguette and croissant."""
    products = seeded_semi_conn.execute(
        "SELECT DISTINCT product_code FROM sales_history"
    ).fetchall()
    assert len(products) == 2
    codes = {r[0] for r in products}
    assert codes == {"baguette", "croissant"}


# ---------------------------------------------------------------------------
# Date coverage
# ---------------------------------------------------------------------------


def test_at_least_90_days(seeded_semi_conn: sqlite3.Connection) -> None:
    """At least 90 distinct dates must be present in sales_history."""
    n = seeded_semi_conn.execute(
        "SELECT COUNT(DISTINCT date) FROM sales_history"
    ).fetchone()[0]
    assert n >= 90


def test_weather_covers_all_sales_dates(seeded_semi_conn: sqlite3.Connection) -> None:
    """Every date in sales_history must have a matching row in weather_daily."""
    missing = seeded_semi_conn.execute("""
        SELECT COUNT(*) FROM sales_history s
        WHERE NOT EXISTS (
            SELECT 1 FROM weather_daily w WHERE w.date = s.date
        )
    """).fetchone()[0]
    assert missing == 0


# ---------------------------------------------------------------------------
# Causal consistency
# ---------------------------------------------------------------------------


def test_units_sold_le_ordered_units(seeded_semi_conn: sqlite3.Connection) -> None:
    """units_sold must never exceed ordered_units."""
    bad = seeded_semi_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE units_sold > ordered_units"
    ).fetchone()[0]
    assert bad == 0, f"{bad} rows violate units_sold <= ordered_units"


def test_waste_equals_excess_supply(seeded_semi_conn: sqlite3.Connection) -> None:
    """waste_units = max(ordered_units - units_sold, 0)."""
    bad = seeded_semi_conn.execute("""
        SELECT COUNT(*) FROM sales_history
        WHERE waste_units != MAX(0, ordered_units - units_sold)
    """).fetchone()[0]
    assert bad == 0


def test_stockout_flag_consistency(seeded_semi_conn: sqlite3.Connection) -> None:
    """stockout_flag=1 must imply waste_units=0 (nothing wasted when demand exceeded supply)."""
    bad_waste = seeded_semi_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE stockout_flag = 1 AND waste_units > 0"
    ).fetchone()[0]
    assert bad_waste == 0


def test_non_negative_waste(seeded_semi_conn: sqlite3.Connection) -> None:
    """waste_units must be >= 0 for every row."""
    bad = seeded_semi_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE waste_units < 0"
    ).fetchone()[0]
    assert bad == 0


def test_ordered_units_positive(seeded_semi_conn: sqlite3.Connection) -> None:
    """ordered_units must be > 0 for every row."""
    bad = seeded_semi_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE ordered_units <= 0"
    ).fetchone()[0]
    assert bad == 0


def test_stockout_flag_binary(seeded_semi_conn: sqlite3.Connection) -> None:
    """stockout_flag must be 0 or 1 only."""
    bad = seeded_semi_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE stockout_flag NOT IN (0, 1)"
    ).fetchone()[0]
    assert bad == 0


# ---------------------------------------------------------------------------
# Forecast quantile ordering
# ---------------------------------------------------------------------------


def test_quantile_ordering_valid(seeded_semi_conn: sqlite3.Connection) -> None:
    """pred_q50 <= pred_q80 <= pred_q90 must hold for every forecast row."""
    bad = seeded_semi_conn.execute("""
        SELECT COUNT(*) FROM forecast_history
        WHERE pred_q50 > pred_q80 OR pred_q80 > pred_q90
    """).fetchone()[0]
    assert bad == 0


def test_pred_point_positive(seeded_semi_conn: sqlite3.Connection) -> None:
    """pred_point must be > 0 for every forecast row."""
    bad = seeded_semi_conn.execute(
        "SELECT COUNT(*) FROM forecast_history WHERE pred_point <= 0"
    ).fetchone()[0]
    assert bad == 0


# ---------------------------------------------------------------------------
# Forecast / sales alignment
# ---------------------------------------------------------------------------


def test_forecast_covers_all_sales_dates(seeded_semi_conn: sqlite3.Connection) -> None:
    """Every (shop_id, product_code, date) in sales_history must have a forecast row."""
    missing = seeded_semi_conn.execute("""
        SELECT COUNT(*) FROM sales_history s
        WHERE NOT EXISTS (
            SELECT 1 FROM forecast_history f
            WHERE f.shop_id = s.shop_id
              AND f.product_code = s.product_code
              AND f.date = s.date
        )
    """).fetchone()[0]
    assert missing == 0


# ---------------------------------------------------------------------------
# Semi-synthetic specific checks
# ---------------------------------------------------------------------------


def test_at_least_one_nonzero_units_sold(seeded_semi_conn: sqlite3.Connection) -> None:
    """At least one row in sales_history must have units_sold > 0 (real data flowed through)."""
    n = seeded_semi_conn.execute(
        "SELECT COUNT(*) FROM sales_history WHERE units_sold > 0"
    ).fetchone()[0]
    assert n > 0, "Expected at least one row with units_sold > 0"


def test_sales_date_count_matches_window(seeded_semi_conn: sqlite3.Connection) -> None:
    """The distinct date count in sales_history must equal the seeder window size (seed_days).

    seed_database selects a contiguous window and writes exactly one record per
    (shop, product, date).  The number of distinct dates therefore equals the
    window length.
    """
    from bakery_analyst.config import settings

    distinct_dates: int = seeded_semi_conn.execute(
        "SELECT COUNT(DISTINCT date) FROM sales_history"
    ).fetchone()[0]
    assert distinct_dates == settings.seed_days, (
        f"Expected {settings.seed_days} distinct dates but found {distinct_dates}"
    )


def test_weather_temp_is_mock_value(seeded_semi_conn: sqlite3.Connection) -> None:
    """All weather_daily.temp values must equal 15.0, confirming mock weather was used."""
    bad = seeded_semi_conn.execute(
        "SELECT COUNT(*) FROM weather_daily WHERE temp != 15.0"
    ).fetchone()[0]
    assert bad == 0, (
        f"{bad} rows in weather_daily have temp != 15.0; mock weather may not have been applied"
    )
