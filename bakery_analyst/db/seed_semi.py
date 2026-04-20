"""Semi-synthetic data seeder for the bakery demand analytics project.

Demand baseline and weather come from real data:
- French Bakery Daily Sales CSV  → daily units per product (real demand signal)
- Open-Meteo Archive API          → real Paris weather for the same dates

Everything else is synthesised on top of that real baseline using the same
causal model as the fully-synthetic seeder (seed.py):
- 3 shops via shop_static
- per-shop demand (real_baseline × shop/weekday/oven/weather multipliers)
- ordered_units, units_sold, stockout_flag, waste_units
- forecast_history with proportional-uncertainty quantiles

The DB schema is identical to the synthetic mode — only the demand signal
driving the model differs.
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any

import numpy as np

from bakery_analyst.config import settings
from bakery_analyst.db.connection import db_session
from bakery_analyst.db.loaders.bakery_loader import load_bakery_baseline
from bakery_analyst.db.loaders.weather_loader import WeatherRow, load_weather
from bakery_analyst.db.schema import apply_schema

# ---------------------------------------------------------------------------
# Shop / product constants (shared with seed.py)
# ---------------------------------------------------------------------------

SHOPS: list[dict[str, Any]] = [
    {"shop_id": "shop_01", "city_center": 1, "seating_capacity": 50, "has_oven": 1},
    {"shop_id": "shop_02", "city_center": 0, "seating_capacity": 30, "has_oven": 0},
    {"shop_id": "shop_03", "city_center": 1, "seating_capacity": 20, "has_oven": 1},
]

PRODUCTS: list[str] = ["baguette", "croissant"]

# How much each shop scales the single-shop real baseline
SHOP_MULTIPLIERS: dict[str, dict[str, float]] = {
    "shop_01": {"croissant": 1.8, "baguette": 1.5},
    "shop_02": {"croissant": 1.0, "baguette": 1.6},
    "shop_03": {"croissant": 0.9, "baguette": 0.7},
}

WEEKDAY_FACTORS_CITY: list[float]     = [1.10, 1.00, 1.05, 1.10, 1.30, 0.95, 0.75]
WEEKDAY_FACTORS_SUBURBAN: list[float] = [0.80, 0.75, 0.80, 0.85, 1.10, 1.40, 1.30]

OVEN_FACTOR_CROISSANT: float = 1.12

ORDER_BIAS: dict[str, float] = {
    "shop_01": 1.05,   # slight over-ordering
    "shop_02": 0.92,   # under-ordering → more stockouts
    "shop_03": 1.10,   # most aggressive over-ordering
}


# ---------------------------------------------------------------------------
# Demand model (identical structure to seed.py, but base comes from real data)
# ---------------------------------------------------------------------------

def _weekday_factor(d: date, city_center: int) -> float:
    dow = d.weekday()
    return WEEKDAY_FACTORS_CITY[dow] if city_center else WEEKDAY_FACTORS_SUBURBAN[dow]


def _oven_factor(product: str, has_oven: int) -> float:
    return OVEN_FACTOR_CROISSANT if (product == "croissant" and has_oven) else 1.0


def _weather_factor(product: str, rain_mm: float, temp: float) -> float:
    rain_f = 1.0 - float(np.clip(rain_mm * 0.012, 0.0, 0.12))
    temp_f = 1.0 + float(np.clip((12.0 - temp) * 0.004, -0.05, 0.04))
    return rain_f if product == "croissant" else rain_f * temp_f


def _per_shop_baseline(
    real_units: int,
    product: str,
    shop: dict[str, Any],
    d: date,
    weather: WeatherRow,
) -> float:
    """Scale the real single-shop daily units to a per-shop continuous demand.

    The real baseline already encodes day-of-week and seasonal demand variation
    from the actual bakery. We apply shop-specific, oven, and weather factors
    on top of it. We deliberately do NOT re-apply a weekday multiplier here
    because it is already baked into the real data; we only apply it to spread
    demand across the three shops meaningfully.

    However, the real data represents a single generic bakery. To introduce
    realistic shop-type differentiation beyond what the raw data contains, a
    mild weekday re-weighting is applied as a relative adjustment around 1.0
    (centred), not as an absolute boost.
    """
    shop_mult  = SHOP_MULTIPLIERS[shop["shop_id"]][product]
    oven_mult  = _oven_factor(product, shop["has_oven"])
    w_mult     = _weather_factor(product, weather.rain_mm, weather.temp)

    # Mild weekday re-weighting: normalise so the weekly mean ≈ 1.0
    raw_dow = _weekday_factor(d, shop["city_center"])
    factors = WEEKDAY_FACTORS_CITY if shop["city_center"] else WEEKDAY_FACTORS_SUBURBAN
    dow_relative = raw_dow / (sum(factors) / 7)   # centre around 1.0

    return max(1.0, real_units * shop_mult * dow_relative * oven_mult * w_mult)


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------

def _build_sales_row(
    shop: dict[str, Any],
    product: str,
    d: date,
    real_units: int,
    weather: WeatherRow,
    rng: np.random.Generator,
) -> tuple[Any, ...]:
    """Build one sales_history row from the real baseline + synthetic supply model."""
    baseline = _per_shop_baseline(real_units, product, shop, d, weather)

    actual_demand = int(rng.poisson(max(1.0, baseline)))

    bias  = ORDER_BIAS[shop["shop_id"]]
    noise = float(rng.normal(0.0, baseline * 0.08))
    ordered_units = max(1, int(baseline * bias + noise))

    units_sold    = min(actual_demand, ordered_units)
    stockout_flag = 1 if actual_demand > ordered_units else 0
    waste_units   = max(ordered_units - units_sold, 0)

    return (shop["shop_id"], product, d.isoformat(),
            units_sold, waste_units, stockout_flag, ordered_units)


def _build_forecast_row(
    shop: dict[str, Any],
    product: str,
    d: date,
    real_units: int,
    weather: WeatherRow,
    rng: np.random.Generator,
) -> tuple[Any, ...]:
    """Build one forecast_history row.

    The forecast is anchored to the per-shop baseline (not actual_demand),
    so it cannot observe stockouts — same intentional flaw as the synthetic mode.
    """
    baseline = _per_shop_baseline(real_units, product, shop, d, weather)
    noise    = float(rng.normal(0.0, baseline * 0.12))
    pred_point = max(1.0, baseline + noise)

    spread     = max(2.0, pred_point * 0.10)
    delta_q50  = float(rng.uniform(0.5, spread * 0.3))
    delta_q80  = float(rng.uniform(spread * 0.5, spread * 1.2))
    delta_q90  = float(rng.uniform(spread * 0.3, spread * 0.7))

    pred_q50 = pred_point + delta_q50
    pred_q80 = pred_q50   + delta_q80
    pred_q90 = pred_q80   + delta_q90

    return (shop["shop_id"], product, d.isoformat(),
            pred_point, pred_q50, pred_q80, pred_q90)


# ---------------------------------------------------------------------------
# Public seeder
# ---------------------------------------------------------------------------

def seed_database(force: bool = False, db_path: str | None = None) -> None:
    """Populate SQLite using real French Bakery demand + real Paris weather.

    Parameters
    ----------
    force:
        When *True*, all existing rows are deleted before insertion.
    db_path:
        Override ``settings.db_path`` (useful in tests).
    """
    rng = np.random.default_rng(settings.seed_random_state)

    # ------------------------------------------------------------------
    # 1. Load real bakery baseline
    # ------------------------------------------------------------------
    print(f"\n[semi_synthetic] Loading bakery baseline from {settings.bakery_csv_path} …")
    baseline, start_date, end_date = load_bakery_baseline(
        settings.bakery_csv_path, rng, window_size=settings.seed_days
    )
    dates = sorted(baseline.keys())

    # ------------------------------------------------------------------
    # 2. Load real Paris weather for the selected window
    # ------------------------------------------------------------------
    print(f"[semi_synthetic] Loading Paris weather ({start_date} → {end_date}) …")
    weather_map = load_weather(
        start_date, end_date, rng,
        cache_path=settings.weather_cache_path,
    )

    # ------------------------------------------------------------------
    # 3. Write to DB
    # ------------------------------------------------------------------
    n_shops    = len(SHOPS)
    n_products = len(PRODUCTS)
    n_days     = len(dates)

    with db_session(db_path) as conn:
        apply_schema(conn)

        if force:
            for tbl in ("forecast_history", "sales_history", "weather_daily", "shop_static"):
                conn.execute(f"DELETE FROM {tbl}")  # noqa: S608

        conn.executemany(
            "INSERT OR IGNORE INTO shop_static"
            " (shop_id, city_center, seating_capacity, has_oven)"
            " VALUES (:shop_id, :city_center, :seating_capacity, :has_oven)",
            SHOPS,
        )

        # weather_daily — from real Open-Meteo data
        conn.executemany(
            "INSERT OR IGNORE INTO weather_daily (date, temp, rain_mm, wind)"
            " VALUES (?, ?, ?, ?)",
            [
                (w.date.isoformat(), round(w.temp, 1),
                 round(w.rain_mm, 1), round(w.wind, 1))
                for w in weather_map.values()
            ],
        )

        sales_batch:    list[tuple] = []
        forecast_batch: list[tuple] = []

        for d in dates:
            w = weather_map.get(d)
            if w is None:
                # Synthetic fallback for this date (already warned by loader)
                doy = d.timetuple().tm_yday
                temp_base = 16.0 + 8.0 * math.sin(2 * math.pi * (doy - 80) / 365)
                w = WeatherRow(
                    date=d,
                    temp=float(temp_base + rng.normal(0.0, 2.5)),
                    rain_mm=float(rng.exponential(5.0)) if rng.random() < 0.4 else 0.0,
                    wind=float(rng.gamma(shape=2.0, scale=4.0)),
                )

            for shop in SHOPS:
                for product in PRODUCTS:
                    real_units = baseline.get(d, {}).get(product, 0)
                    sales_batch.append(
                        _build_sales_row(shop, product, d, real_units, w, rng)
                    )
                    forecast_batch.append(
                        _build_forecast_row(shop, product, d, real_units, w, rng)
                    )

        conn.executemany(
            "INSERT OR IGNORE INTO sales_history"
            " (shop_id, product_code, date, units_sold, waste_units, stockout_flag, ordered_units)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            sales_batch,
        )
        conn.executemany(
            "INSERT OR IGNORE INTO forecast_history"
            " (shop_id, product_code, date, pred_point, pred_q50, pred_q80, pred_q90)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            forecast_batch,
        )

    total = n_days * n_shops * n_products
    print(
        f"\n[semi_synthetic] Seeded {n_days} days × {n_shops} shops × {n_products} products"
        f" = {total} rows."
    )
    print(f"[semi_synthetic] Date range: {dates[0].isoformat()} → {dates[-1].isoformat()}")
    print("[semi_synthetic] Demand baseline: REAL (French Bakery Daily Sales)")
    print("[semi_synthetic] Weather:         REAL (Open-Meteo Paris archive)")
    print("[semi_synthetic] Shop/supply/forecast: SYNTHETIC extensions")
