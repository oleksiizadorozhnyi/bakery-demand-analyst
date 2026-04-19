"""DDL for all four analytical tables.

Design notes
------------
- ``waste_units`` is used instead of ``planned_waste``: it is computed directly
  as ``max(ordered_units - units_sold, 0)`` and is therefore causally linked to
  the supply decision, not a separate estimate.
- ``ordered_units`` is required to make waste and stockout explainable: without
  it, you cannot distinguish "low waste because demand was low" from "low waste
  because the shop ordered accurately".
- Quantile ordering is enforced by DB CHECK constraints on forecast_history.
"""

import sqlite3

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS shop_static (
    shop_id          TEXT    PRIMARY KEY,
    city_center      INTEGER NOT NULL CHECK (city_center IN (0, 1)),
    seating_capacity INTEGER NOT NULL CHECK (seating_capacity > 0),
    has_oven         INTEGER NOT NULL CHECK (has_oven IN (0, 1))
);

CREATE TABLE IF NOT EXISTS weather_daily (
    date    TEXT PRIMARY KEY,
    temp    REAL NOT NULL,
    rain_mm REAL NOT NULL CHECK (rain_mm >= 0),
    wind    REAL NOT NULL CHECK (wind >= 0)
);

CREATE TABLE IF NOT EXISTS sales_history (
    shop_id       TEXT    NOT NULL REFERENCES shop_static (shop_id),
    product_code  TEXT    NOT NULL,
    date          TEXT    NOT NULL,
    units_sold    INTEGER NOT NULL CHECK (units_sold >= 0),
    waste_units   INTEGER NOT NULL CHECK (waste_units >= 0),
    stockout_flag INTEGER NOT NULL CHECK (stockout_flag IN (0, 1)),
    ordered_units INTEGER NOT NULL CHECK (ordered_units >= 0),
    PRIMARY KEY (shop_id, product_code, date)
);

CREATE TABLE IF NOT EXISTS forecast_history (
    shop_id      TEXT NOT NULL REFERENCES shop_static (shop_id),
    product_code TEXT NOT NULL,
    date         TEXT NOT NULL,
    pred_point   REAL NOT NULL CHECK (pred_point >= 0),
    pred_q50     REAL NOT NULL CHECK (pred_q50 >= 0),
    pred_q80     REAL NOT NULL CHECK (pred_q80 >= pred_q50),
    pred_q90     REAL NOT NULL CHECK (pred_q90 >= pred_q80),
    PRIMARY KEY (shop_id, product_code, date)
);

CREATE INDEX IF NOT EXISTS idx_sales_date
    ON sales_history (date);
CREATE INDEX IF NOT EXISTS idx_sales_shop_product_date
    ON sales_history (shop_id, product_code, date);
CREATE INDEX IF NOT EXISTS idx_forecast_date
    ON forecast_history (date);
CREATE INDEX IF NOT EXISTS idx_forecast_shop_product_date
    ON forecast_history (shop_id, product_code, date);
CREATE INDEX IF NOT EXISTS idx_weather_date
    ON weather_daily (date);
"""


def apply_schema(conn: sqlite3.Connection) -> None:
    """Execute all DDL statements against *conn*.

    Uses ``executescript`` which issues an implicit COMMIT first, so call this
    before starting any data-write transaction.
    """
    conn.executescript(SCHEMA_SQL)
