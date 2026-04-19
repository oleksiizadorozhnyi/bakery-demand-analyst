"""bakery_loader.py

Load the public "French Bakery Daily Sales" Kaggle CSV, aggregate it to a
daily baseline per product, and select the best 90-day contiguous window.
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from datetime import date, timedelta
from typing import TYPE_CHECKING

import numpy as np

from bakery_analyst.config import settings

if TYPE_CHECKING:
    pass

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

PRODUCTS: list[str] = ["baguette", "croissant"]

ARTICLE_MAP: dict[str, str] = {
    "BAGUETTE": "baguette",
    "CROISSANT": "croissant",
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y")


def _parse_date(raw: str) -> date | None:
    """Try known date formats; return None if all fail."""
    raw = raw.strip()
    for fmt in _DATE_FORMATS:
        try:
            return date.fromisoformat(raw) if fmt == "%Y-%m-%d" else _strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _strptime(raw: str, fmt: str) -> date:
    """Parse *raw* with *fmt* using datetime.strptime."""
    import datetime as _dt
    return _dt.datetime.strptime(raw, fmt).date()


def _sniff_delimiter(path: str) -> str:
    """Peek at the first line to decide between comma and semicolon."""
    with open(path, newline="", encoding="utf-8-sig") as fh:
        first = fh.readline()
    return ";" if first.count(";") > first.count(",") else ","


def _aggregate_csv(path: str) -> dict[date, dict[str, int]]:
    """Read the CSV and return daily totals per product.

    Returns
    -------
    daily : dict[date, dict[str, int]]
        Mapping date → {product_code: total_quantity}.
        Only products in ARTICLE_MAP are included.

    Raises
    ------
    FileNotFoundError
        If *path* does not exist.
    ValueError
        If no matching rows are found, or a product is entirely absent.
    """
    import os

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Bakery CSV not found at {path}. "
            "Download from Kaggle: "
            "https://www.kaggle.com/datasets/matthieugimbert/french-bakery-daily-sales"
        )

    delimiter = _sniff_delimiter(path)

    # daily[date][product_code] → cumulative quantity
    daily: dict[date, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    bad_qty = 0
    total_rows = 0
    found_products: set[str] = set()

    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        # Normalise header names (strip whitespace, handle BOM residuals)
        if reader.fieldnames:
            reader.fieldnames = [f.strip() for f in reader.fieldnames]

        for row in reader:
            total_rows += 1

            # --- date ---
            raw_date = (row.get("date") or "").strip()
            day = _parse_date(raw_date)
            if day is None:
                continue  # unparseable date → skip silently

            # --- article ---
            raw_article = (row.get("article") or "").strip().upper()
            product_code = ARTICLE_MAP.get(raw_article)
            if product_code is None:
                continue  # not a product we care about

            # --- quantity ---
            raw_qty = (row.get("Quantity") or "").strip()
            try:
                # Quantity is stored as a float string ("1.0") in this dataset
                qty = int(float(raw_qty))
            except (ValueError, TypeError):
                bad_qty += 1
                continue

            if qty <= 0:
                continue

            daily[day][product_code] += qty
            found_products.add(product_code)

    if bad_qty:
        print(
            f"  [bakery_loader] Warning: {bad_qty} rows skipped due to "
            "unparseable Quantity values.",
            file=sys.stderr,
        )

    if not found_products:
        raise ValueError(
            f"No baguette or croissant rows found in {path}. "
            "Check article column values."
        )

    for product in PRODUCTS:
        if product not in found_products:
            raise ValueError(
                f"Product '{product}' not found in {path}. "
                "Check article column values."
            )

    # Convert to regular dicts for cleaner downstream handling
    result: dict[date, dict[str, int]] = {}
    for day, products in daily.items():
        result[day] = {p: products.get(p, 0) for p in PRODUCTS}

    unique_dates = len(result)
    print(
        f"  [bakery_loader] Loaded {total_rows} rows, "
        f"{len(found_products)} products, "
        f"{unique_dates} unique dates"
    )

    return result


def select_window(
    daily: dict[date, dict[str, int]],
    window_size: int = 90,
) -> tuple[date, date]:
    """Return (start_date, end_date) of the best 90-day window.

    'Best' = highest sum of (baguette + croissant) units over the window,
    among windows where both products appear on at least 70% of days
    (i.e. ≥ 63 days each have non-zero quantity for that product).

    If no window satisfies the 70% threshold, fall back to the window
    with the highest coverage (most non-zero days for both products combined).

    Parameters
    ----------
    daily:
        Mapping date → {product_code: quantity}.
    window_size:
        Number of calendar days in the window (default 90).

    Returns
    -------
    (start_date, end_date) : tuple[date, date]
        Both dates are inclusive; end_date = start_date + timedelta(window_size - 1).

    Raises
    ------
    ValueError
        If fewer than *window_size* contiguous calendar days are available.
    """
    if not daily:
        raise ValueError(
            f"Need at least {window_size} days of data; found only 0 days."
        )

    all_dates = sorted(daily.keys())

    # Build a complete calendar span and check for a contiguous run of at
    # least window_size days within the *data* span.
    span_start = all_dates[0]
    span_end = all_dates[-1]
    total_calendar_days = (span_end - span_start).days + 1

    if total_calendar_days < window_size:
        raise ValueError(
            f"Need at least {window_size} days of data; "
            f"found only {total_calendar_days} days."
        )

    # We slide a window over calendar dates (not just data dates).
    # For each window start we compute metrics using whatever data exists.
    min_coverage = 0.70 * window_size  # 63 days for window_size=90

    best_qualified: tuple[date, float, float] | None = None  # (start, total_units, coverage)
    best_fallback: tuple[date, float] | None = None  # (start, combined_coverage_days)

    candidate_start = span_start
    while True:
        candidate_end = candidate_start + timedelta(days=window_size - 1)
        if candidate_end > span_end:
            break

        total_units = 0
        baguette_nonzero = 0
        croissant_nonzero = 0

        for i in range(window_size):
            day = candidate_start + timedelta(days=i)
            day_data = daily.get(day)
            if day_data is not None:
                b = day_data.get("baguette", 0)
                c = day_data.get("croissant", 0)
                total_units += b + c
                if b > 0:
                    baguette_nonzero += 1
                if c > 0:
                    croissant_nonzero += 1

        combined_coverage = baguette_nonzero + croissant_nonzero

        if baguette_nonzero >= min_coverage and croissant_nonzero >= min_coverage:
            if (
                best_qualified is None
                or total_units > best_qualified[1]
            ):
                best_qualified = (candidate_start, float(total_units), float(combined_coverage))
        else:
            if (
                best_fallback is None
                or combined_coverage > best_fallback[1]
            ):
                best_fallback = (candidate_start, float(combined_coverage))

        candidate_start += timedelta(days=1)

    if best_qualified is not None:
        start = best_qualified[0]
    elif best_fallback is not None:
        start = best_fallback[0]
    else:
        raise ValueError(
            f"Need at least {window_size} days of data; "
            f"found only {total_calendar_days} days."
        )

    end = start + timedelta(days=window_size - 1)

    # Compute coverage percentages for the chosen window (for logging)
    b_nz = 0
    c_nz = 0
    for i in range(window_size):
        day = start + timedelta(days=i)
        day_data = daily.get(day)
        if day_data is not None:
            if day_data.get("baguette", 0) > 0:
                b_nz += 1
            if day_data.get("croissant", 0) > 0:
                c_nz += 1

    print(
        f"  [bakery_loader] Selected window: {start} → {end} "
        f"({window_size} days, "
        f"baguette coverage={b_nz * 100 // window_size}%, "
        f"croissant coverage={c_nz * 100 // window_size}%)"
    )

    return start, end


def _fill_window(
    daily: dict[date, dict[str, int]],
    start: date,
    end: date,
    rng: np.random.Generator,
) -> dict[date, dict[str, int]]:
    """Produce a complete day-by-day mapping for [start, end].

    Missing dates are forward-filled with ±10% uniform noise (min 1).
    A warning is printed to stderr for each filled date.

    Parameters
    ----------
    daily:
        Raw aggregated data (may have gaps).
    start, end:
        Inclusive window boundaries.
    rng:
        NumPy random Generator used for noise.

    Returns
    -------
    filled : dict[date, dict[str, int]]
        Every calendar date in [start, end] with both products present.
    """
    filled: dict[date, dict[str, int]] = {}
    prev: dict[str, int] = {p: 1 for p in PRODUCTS}  # sensible default

    current = start
    while current <= end:
        if current in daily:
            filled[current] = dict(daily[current])
            prev = filled[current]
        else:
            # Forward-fill with noise
            print(
                f"  [bakery_loader] Warning: date {current} missing in CSV; "
                "forward-filling from previous date.",
                file=sys.stderr,
            )
            noisy: dict[str, int] = {}
            for p in PRODUCTS:
                base = prev.get(p, 1)
                # ±10% uniform noise
                low = base * 0.90
                high = base * 1.10
                val = int(rng.uniform(low, high))
                noisy[p] = max(1, val)
            filled[current] = noisy
            prev = noisy

        current += timedelta(days=1)

    return filled


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_bakery_baseline(
    path: str,
    rng: np.random.Generator,
    window_size: int = 90,
) -> tuple[dict[date, dict[str, int]], date, date]:
    """Load, clean, aggregate, and window-select French Bakery CSV data.

    Parameters
    ----------
    path:
        Filesystem path to the CSV file.
    rng:
        NumPy random Generator used for noise when forward-filling missing dates.
    window_size:
        Number of calendar days in the selected window (default 90).

    Returns
    -------
    baseline : dict[date, dict[str, int]]
        Daily aggregated units per product for the selected window.
        Every calendar date in [start, end] is guaranteed to have both
        'baguette' and 'croissant' keys with non-negative integer values.
    start_date : date
        First date of the selected window.
    end_date : date
        Last date of the selected window.

    Raises
    ------
    FileNotFoundError
        If the CSV file does not exist at *path*.
    ValueError
        If the data does not contain the required products or enough days.
    """
    daily = _aggregate_csv(path)
    start, end = select_window(daily, window_size=window_size)
    baseline = _fill_window(daily, start, end, rng)
    return baseline, start, end
