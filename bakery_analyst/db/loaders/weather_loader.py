"""Fetch real Paris historical weather from the Open-Meteo Archive API.

Weather is cached locally as CSV to avoid repeated network calls.
If the network call fails, synthetic season-aware weather is returned instead.
"""

from __future__ import annotations

import csv
import json
import math
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

from bakery_analyst.config import settings

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WeatherRow:
    """One day of Paris weather data."""

    date: date
    temp: float
    rain_mm: float
    wind: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _date_range(start: date, end: date) -> list[date]:
    """Return every calendar date from *start* to *end* inclusive."""
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _read_cache(path: Path) -> dict[date, WeatherRow]:
    """Read the cache CSV and return a mapping of date -> WeatherRow."""
    rows: dict[date, WeatherRow] = {}
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            d = date.fromisoformat(row["date"])
            rows[d] = WeatherRow(
                date=d,
                temp=float(row["temp"]),
                rain_mm=float(row["rain_mm"]),
                wind=float(row["wind"]),
            )
    return rows


def _write_cache(path: Path, rows: dict[date, WeatherRow]) -> None:
    """Write *rows* to the cache CSV, overwriting any existing file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["date", "temp", "rain_mm", "wind"])
        writer.writeheader()
        for d in sorted(rows):
            r = rows[d]
            writer.writerow(
                {
                    "date": r.date.isoformat(),
                    "temp": r.temp,
                    "rain_mm": r.rain_mm,
                    "wind": r.wind,
                }
            )


def _cache_covers(cached: dict[date, WeatherRow], start: date, end: date) -> bool:
    """Return True if every date in [start, end] is present in *cached*."""
    return all(d in cached for d in _date_range(start, end))


def _interpolate_nulls(
    times: list[str],
    values: list[float | None],
    default: float,
) -> list[float]:
    """Replace None entries with interpolated or default values."""
    result: list[float | None] = list(values)
    n = len(result)

    for i in range(n):
        if result[i] is None:
            # Find previous non-None
            prev_idx: int | None = None
            for j in range(i - 1, -1, -1):
                if result[j] is not None:
                    prev_idx = j
                    break
            # Find next non-None
            next_idx: int | None = None
            for j in range(i + 1, n):
                if result[j] is not None:
                    next_idx = j
                    break

            if prev_idx is not None and next_idx is not None:
                # Linear interpolation
                span = next_idx - prev_idx
                frac = (i - prev_idx) / span
                result[i] = result[prev_idx] + frac * (result[next_idx] - result[prev_idx])  # type: ignore[operator]
            elif prev_idx is not None:
                result[i] = result[prev_idx]
            elif next_idx is not None:
                result[i] = result[next_idx]
            else:
                result[i] = default

    return result  # type: ignore[return-value]


def _fetch_from_api(start: date, end: date) -> dict[date, WeatherRow]:
    """Download weather data from Open-Meteo and return parsed rows."""
    start_str = start.isoformat()
    end_str = end.isoformat()

    print(
        f"  [weather_loader] Fetching Paris weather from Open-Meteo"
        f" ({start_str} \u2192 {end_str})..."
    )

    params = urllib.parse.urlencode(
        {
            "latitude": "48.8566",
            "longitude": "2.3522",
            "start_date": start_str,
            "end_date": end_str,
            "daily": "temperature_2m_mean,precipitation_sum,windspeed_10m_max",
            "timezone": "Europe/Paris",
        }
    )
    url = f"https://archive-api.open-meteo.com/v1/archive?{params}"

    with urllib.request.urlopen(url, timeout=30) as resp:
        if resp.status != 200:
            raise urllib.error.HTTPError(
                url, resp.status, "Non-200 response", resp.headers, None
            )
        payload = json.loads(resp.read().decode("utf-8"))

    daily = payload["daily"]
    times: list[str] = daily["time"]
    raw_temp: list[float | None] = daily["temperature_2m_mean"]
    raw_rain: list[float | None] = daily["precipitation_sum"]
    raw_wind: list[float | None] = daily["windspeed_10m_max"]

    # Handle nulls
    temp_vals = _interpolate_nulls(times, raw_temp, default=15.0)
    rain_vals = _interpolate_nulls(times, raw_rain, default=0.0)
    wind_vals = [10.0 if v is None else float(v) for v in raw_wind]

    rows: dict[date, WeatherRow] = {}
    for t, temp, rain, wind in zip(times, temp_vals, rain_vals, wind_vals):
        d = date.fromisoformat(t)
        rows[d] = WeatherRow(date=d, temp=float(temp), rain_mm=rain, wind=wind)

    return rows


def _generate_synthetic(
    start: date,
    end: date,
    rng: np.random.Generator,
) -> dict[date, WeatherRow]:
    """Generate synthetic season-aware Paris weather for [start, end]."""
    rows: dict[date, WeatherRow] = {}
    for d in _date_range(start, end):
        doy = d.timetuple().tm_yday
        temp_base = 16.0 + 8.0 * math.sin(2 * math.pi * (doy - 80) / 365)
        temp = float(temp_base + rng.normal(0.0, 2.5))
        rain_prob = 0.40 + 0.15 * math.cos(2 * math.pi * (doy - 15) / 365)
        rain_mm = float(rng.exponential(5.0)) if rng.random() < rain_prob else 0.0
        wind = float(rng.gamma(shape=2.0, scale=4.0))
        rows[d] = WeatherRow(date=d, temp=temp, rain_mm=rain_mm, wind=wind)
    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_weather(
    start_date: date,
    end_date: date,
    rng: np.random.Generator,
    cache_path: str | None = None,
) -> dict[date, WeatherRow]:
    """Return real (or synthetic fallback) Paris weather for [start_date, end_date].

    Fetches from Open-Meteo if not cached; writes cache on successful fetch.
    Falls back to synthetic weather if the network call fails.

    Parameters
    ----------
    start_date, end_date:
        Inclusive date range.
    rng:
        Used only for the synthetic fallback.
    cache_path:
        Override settings.weather_cache_path (useful in tests).
    """
    resolved_cache = Path(cache_path if cache_path is not None else settings.weather_cache_path)

    # --- Cache hit? ---
    if resolved_cache.exists():
        cached = _read_cache(resolved_cache)
        if _cache_covers(cached, start_date, end_date):
            print(f"  [weather_loader] Using cached weather from {resolved_cache}")
            return {d: cached[d] for d in _date_range(start_date, end_date)}

    # --- Fetch from API ---
    try:
        rows = _fetch_from_api(start_date, end_date)
    except Exception as exc:  # noqa: BLE001
        print(
            f"  [weather_loader] WARNING: Open-Meteo fetch failed ({exc})."
            f" Falling back to synthetic Paris weather.",
            file=sys.stderr,
        )
        return _generate_synthetic(start_date, end_date, rng)

    # --- Write cache (merge with any existing data) ---
    if resolved_cache.exists():
        try:
            existing = _read_cache(resolved_cache)
        except Exception:  # noqa: BLE001
            existing = {}
        existing.update(rows)
        rows_to_write = existing
    else:
        rows_to_write = rows

    _write_cache(resolved_cache, rows_to_write)
    print(f"  [weather_loader] Cached weather to {resolved_cache}")

    return {d: rows[d] for d in _date_range(start_date, end_date) if d in rows}
