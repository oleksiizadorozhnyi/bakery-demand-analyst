"""Synthetic data seeder for the bakery demand analytics project.

Generates causally consistent sales, forecast, weather, and shop-static rows
using Paris-like weather patterns and a structured demand model.
"""

from __future__ import annotations

import math
import sqlite3
from datetime import date, timedelta
from typing import Any

import numpy as np

from bakery_analyst.config import settings
from bakery_analyst.db.connection import db_session
from bakery_analyst.db.schema import apply_schema

# ---------------------------------------------------------------------------
# Static shop definitions
# ---------------------------------------------------------------------------

SHOPS: list[dict[str, Any]] = [
    {"shop_id": "shop_01", "city_center": 1, "seating_capacity": 50, "has_oven": 1},
    {"shop_id": "shop_02", "city_center": 0, "seating_capacity": 30, "has_oven": 0},
    {"shop_id": "shop_03", "city_center": 1, "seating_capacity": 20, "has_oven": 1},
]

PRODUCTS: list[str] = ["baguette", "croissant"]

# French Bakery Daily Sales baseline (single-shop aggregate)
PRODUCT_BASE: dict[str, float] = {
    "croissant": 22.0,
    "baguette": 38.0,
}

# Shop × product multipliers
SHOP_MULTIPLIERS: dict[str, dict[str, float]] = {
    "shop_01": {"croissant": 1.8, "baguette": 1.5},
    "shop_02": {"croissant": 1.0, "baguette": 1.6},
    "shop_03": {"croissant": 0.9, "baguette": 0.7},
}

# Day-of-week multipliers (Monday=0 … Sunday=6)
WEEKDAY_FACTORS_CITY: list[float] = [1.10, 1.00, 1.05, 1.10, 1.30, 0.95, 0.75]
WEEKDAY_FACTORS_SUBURBAN: list[float] = [0.80, 0.75, 0.80, 0.85, 1.10, 1.40, 1.30]

# Oven boost — croissant only
OVEN_FACTOR_CROISSANT: float = 1.12

# Ordering bias per shop
ORDER_BIAS: dict[str, float] = {
    "shop_01": 1.05,
    "shop_02": 0.92,
    "shop_03": 1.10,
}


# ---------------------------------------------------------------------------
# Helper: date range
# ---------------------------------------------------------------------------

def _date_range(n_days: int) -> list[date]:
    """Return *n_days* consecutive dates ending yesterday."""
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=n_days - 1)
    return [start + timedelta(days=i) for i in range(n_days)]


# ---------------------------------------------------------------------------
# Weather generation
# ---------------------------------------------------------------------------

def _generate_weather(
    dates: list[date],
    rng: np.random.Generator,
) -> dict[str, dict[str, float]]:
    """Generate Paris-like daily weather for each date.

    Returns a mapping ``{date_str: {"temp": float, "rain_mm": float, "wind": float}}``.
    """
    weather: dict[str, dict[str, float]] = {}
    for d in dates:
        doy = d.timetuple().tm_yday
        # Sinusoidal temperature: 8°C in mid-winter, 24°C in mid-summer
        temp_base = 16.0 + 8.0 * math.sin(2 * math.pi * (doy - 80) / 365)
        temp = float(temp_base + rng.normal(0.0, 2.5))

        # Rainy-day probability peaks in winter
        rain_prob = 0.40 + 0.15 * math.cos(2 * math.pi * (doy - 15) / 365)
        is_rainy = rng.random() < rain_prob
        rain_mm = float(rng.exponential(5.0)) if is_rainy else 0.0

        wind = float(rng.gamma(shape=2.0, scale=4.0))

        weather[d.isoformat()] = {"temp": temp, "rain_mm": rain_mm, "wind": wind}
    return weather


# ---------------------------------------------------------------------------
# Demand model helpers
# ---------------------------------------------------------------------------

def _weekday_factor(d: date, city_center: int) -> float:
    """Return the day-of-week demand multiplier for a given shop type."""
    dow = d.weekday()  # Monday=0
    if city_center:
        return WEEKDAY_FACTORS_CITY[dow]
    return WEEKDAY_FACTORS_SUBURBAN[dow]


def _oven_factor(product: str, has_oven: int) -> float:
    """Return the oven multiplier (only applies to croissant)."""
    if product == "croissant" and has_oven:
        return OVEN_FACTOR_CROISSANT
    return 1.0


def _weather_factors(product: str, rain_mm: float, temp: float) -> float:
    """Return the combined weather demand factor."""
    rain_factor = 1.0 - float(np.clip(rain_mm * 0.012, 0.0, 0.12))
    temp_factor = 1.0 + float(np.clip((12.0 - temp) * 0.004, -0.05, 0.04))
    if product == "croissant":
        return rain_factor
    # baguette gets both effects
    return rain_factor * temp_factor


def _baseline_demand(
    product: str,
    shop: dict[str, Any],
    d: date,
    weather: dict[str, float],
) -> float:
    """Compute the continuous baseline demand (pre-Poisson) for one row."""
    base = PRODUCT_BASE[product]
    shop_mult = SHOP_MULTIPLIERS[shop["shop_id"]][product]
    dow_mult = _weekday_factor(d, shop["city_center"])
    oven_mult = _oven_factor(product, shop["has_oven"])
    w_mult = _weather_factors(product, weather["rain_mm"], weather["temp"])
    return base * shop_mult * dow_mult * oven_mult * w_mult


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------

def _build_sales_row(
    shop: dict[str, Any],
    product: str,
    d: date,
    weather: dict[str, float],
    rng: np.random.Generator,
) -> tuple[Any, ...]:
    """Build a single sales_history row tuple."""
    baseline = _baseline_demand(product, shop, d, weather)
    lam = max(1.0, baseline)
    actual_demand = int(rng.poisson(lam))

    bias = ORDER_BIAS[shop["shop_id"]]
    noise = float(rng.normal(0.0, baseline * 0.08))
    ordered_units = max(1, int(baseline * bias + noise))

    units_sold = min(actual_demand, ordered_units)
    stockout_flag = 1 if actual_demand > ordered_units else 0
    waste_units = max(ordered_units - units_sold, 0)

    return (
        shop["shop_id"],
        product,
        d.isoformat(),
        units_sold,
        waste_units,
        stockout_flag,
        ordered_units,
    )


def _build_forecast_row(
    shop: dict[str, Any],
    product: str,
    d: date,
    weather: dict[str, float],
    rng: np.random.Generator,
) -> tuple[Any, ...]:
    """Build a single forecast_history row tuple.

    Forecasts are computed from baseline (no actual_demand visibility).
    """
    baseline = _baseline_demand(product, shop, d, weather)
    forecast_noise = float(rng.normal(0.0, baseline * 0.12))
    pred_point = max(1.0, baseline + forecast_noise)

    spread = max(2.0, pred_point * 0.10)
    delta_q50 = float(rng.uniform(0.5, spread * 0.3))
    delta_q80 = float(rng.uniform(spread * 0.5, spread * 1.2))
    delta_q90 = float(rng.uniform(spread * 0.3, spread * 0.7))

    pred_q50 = pred_point + delta_q50
    pred_q80 = pred_q50 + delta_q80
    pred_q90 = pred_q80 + delta_q90

    return (
        shop["shop_id"],
        product,
        d.isoformat(),
        pred_point,
        pred_q50,
        pred_q80,
        pred_q90,
    )


# ---------------------------------------------------------------------------
# Public seeder
# ---------------------------------------------------------------------------

def seed_database(force: bool = False, db_path: str | None = None) -> None:
    """Populate SQLite with synthetic bakery data.

    Parameters
    ----------
    force:
        When *True*, delete all existing rows from all four tables before
        inserting fresh data.  When *False* (default), ``INSERT OR IGNORE``
        leaves pre-existing rows untouched (idempotent).
    db_path:
        Override the database path from settings (useful in tests).
    """
    rng = np.random.default_rng(settings.seed_random_state)
    dates = _date_range(settings.seed_days)
    weather_map = _generate_weather(dates, rng)

    with db_session(db_path) as conn:
        apply_schema(conn)

        if force:
            _clear_tables(conn)

        _insert_shops(conn)
        _insert_weather(conn, weather_map)
        _insert_sales(conn, dates, weather_map, rng)
        _insert_forecasts(conn, dates, weather_map, rng)

    n_rows = settings.seed_days * len(SHOPS) * len(PRODUCTS)
    date_start = dates[0].isoformat()
    date_end = dates[-1].isoformat()
    print(
        f"Seeded {settings.seed_days} days × {len(SHOPS)} shops × "
        f"{len(PRODUCTS)} products = {n_rows} rows.\n"
        f"Date range: {date_start} → {date_end}"
    )


# ---------------------------------------------------------------------------
# Private insert helpers
# ---------------------------------------------------------------------------

def _clear_tables(conn: sqlite3.Connection) -> None:
    """Delete all rows from every seeded table."""
    for table in ("forecast_history", "sales_history", "weather_daily", "shop_static"):
        conn.execute(f"DELETE FROM {table}")


def _insert_shops(conn: sqlite3.Connection) -> None:
    """Upsert static shop records."""
    conn.executemany(
        """
        INSERT OR IGNORE INTO shop_static
            (shop_id, city_center, seating_capacity, has_oven)
        VALUES (?, ?, ?, ?)
        """,
        [
            (s["shop_id"], s["city_center"], s["seating_capacity"], s["has_oven"])
            for s in SHOPS
        ],
    )


def _insert_weather(
    conn: sqlite3.Connection,
    weather_map: dict[str, dict[str, float]],
) -> None:
    """Insert weather rows (one per date)."""
    rows = [
        (date_str, w["temp"], w["rain_mm"], w["wind"])
        for date_str, w in weather_map.items()
    ]
    conn.executemany(
        """
        INSERT OR IGNORE INTO weather_daily (date, temp, rain_mm, wind)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )


def _insert_sales(
    conn: sqlite3.Connection,
    dates: list[date],
    weather_map: dict[str, dict[str, float]],
    rng: np.random.Generator,
) -> None:
    """Build and insert all sales_history rows."""
    rows: list[tuple[Any, ...]] = []
    for d in dates:
        weather = weather_map[d.isoformat()]
        for shop in SHOPS:
            for product in PRODUCTS:
                rows.append(_build_sales_row(shop, product, d, weather, rng))
    conn.executemany(
        """
        INSERT OR IGNORE INTO sales_history
            (shop_id, product_code, date, units_sold, waste_units,
             stockout_flag, ordered_units)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _insert_forecasts(
    conn: sqlite3.Connection,
    dates: list[date],
    weather_map: dict[str, dict[str, float]],
    rng: np.random.Generator,
) -> None:
    """Build and insert all forecast_history rows."""
    rows: list[tuple[Any, ...]] = []
    for d in dates:
        weather = weather_map[d.isoformat()]
        for shop in SHOPS:
            for product in PRODUCTS:
                rows.append(_build_forecast_row(shop, product, d, weather, rng))
    conn.executemany(
        """
        INSERT OR IGNORE INTO forecast_history
            (shop_id, product_code, date, pred_point, pred_q50, pred_q80, pred_q90)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
