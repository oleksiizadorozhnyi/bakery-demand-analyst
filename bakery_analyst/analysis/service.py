"""Analytics orchestration layer for bakery demand analytics.

Computes all metrics for each ValidatedPrediction, applies risk flags,
and returns a list of AnalysisRow objects.
"""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from bakery_analyst.config import settings
from bakery_analyst.models.domain_models import AnalysisRow, ValidatedPrediction
from bakery_analyst.repository.analytics_repository import (
    compute_temp_sales_correlation,
    fetch_bias_metrics,
    fetch_recent_bias,
    fetch_stockout_metrics,
    fetch_variability_metrics,
    fetch_waste_metrics,
)

# ---------------------------------------------------------------------------
# Risk flag thresholds
# ---------------------------------------------------------------------------
HIGH_WASTE_THRESHOLD: float = 0.20
FREQUENT_STOCKOUT_THRESHOLD: float = 0.15
HIGH_CV_THRESHOLD: float = 0.40
OVERFORECAST_RATIO_THRESHOLD: float = 0.65


def _r(value: float | None) -> float | None:
    """Round a float to 3 decimal places, or return None."""
    return round(value, 3) if value is not None else None


def _compute_windows(target_date: str) -> tuple[str, str, str]:
    """Return (main_window_start, recent_window_start, window_end) for the given target date.

    Args:
        target_date: ISO-format date string, e.g. ``"2026-01-15"``.

    Returns:
        A 3-tuple of ISO-format date strings:
        ``(main_window_start, recent_window_start, window_end)``.
    """
    td = date.fromisoformat(target_date)
    window_end = (td - timedelta(days=1)).isoformat()
    main_window_start = (td - timedelta(days=settings.main_window_days)).isoformat()
    recent_window_start = (td - timedelta(days=settings.recent_window_days)).isoformat()
    return main_window_start, recent_window_start, window_end


def _analyse_one(
    prediction: ValidatedPrediction,
    target_date: str,
    main_window_start: str,
    recent_window_start: str,
    window_end: str,
) -> AnalysisRow:
    """Compute metrics and risk flags for a single prediction.

    Args:
        prediction: A validated prediction object.
        target_date: The target date string (ISO format).
        main_window_start: Start of the main analysis window (ISO format).
        recent_window_start: Start of the recent bias window (ISO format).
        window_end: Shared end of both windows (ISO format).

    Returns:
        A fully populated :class:`AnalysisRow`.
    """
    shop_id = prediction.shop_id
    product_code = prediction.product_code

    # ------------------------------------------------------------------
    # Fetch all metrics
    # ------------------------------------------------------------------
    bias = fetch_bias_metrics(shop_id, product_code, main_window_start, window_end)
    recent_bias = fetch_recent_bias(shop_id, product_code, recent_window_start, window_end)
    waste = fetch_waste_metrics(shop_id, product_code, main_window_start, window_end)
    stockout = fetch_stockout_metrics(shop_id, product_code, main_window_start, window_end)
    variability = fetch_variability_metrics(shop_id, product_code, main_window_start, window_end)
    temp_corr = compute_temp_sales_correlation(shop_id, product_code, main_window_start, window_end)

    # ------------------------------------------------------------------
    # Extract individual values (raw, before rounding)
    # ------------------------------------------------------------------
    mean_signed_error: float | None = bias.get("mean_signed_error")
    mae: float | None = bias.get("mae")
    overforecast_ratio: float | None = bias.get("overforecast_ratio")
    recent_mean_signed_error: float | None = recent_bias.get("recent_mean_signed_error")
    waste_rate: float | None = waste.get("waste_rate")
    stockout_rate: float | None = stockout.get("stockout_rate")
    stddev_units_sold: float | None = variability.get("stddev_units_sold")
    coefficient_of_variation: float | None = variability.get("coefficient_of_variation")

    # ------------------------------------------------------------------
    # Derived metric
    # ------------------------------------------------------------------
    service_reliability: float | None = (
        1.0 - stockout_rate if stockout_rate is not None else None
    )

    # ------------------------------------------------------------------
    # Progress output
    # ------------------------------------------------------------------
    bias_display = f"{mean_signed_error:+.1f}" if mean_signed_error is not None else "N/A"
    waste_display = f"{waste_rate * 100:.1f}%" if waste_rate is not None else "N/A"
    stockout_display = f"{stockout_rate * 100:.1f}%" if stockout_rate is not None else "N/A"
    print(
        f"  \u2192 {shop_id} / {product_code}: "
        f"bias={bias_display}, waste={waste_display}, stockout={stockout_display}"
    )

    # ------------------------------------------------------------------
    # Risk flags
    # ------------------------------------------------------------------
    high_waste_flag = waste_rate is not None and waste_rate > HIGH_WASTE_THRESHOLD
    frequent_stockout_flag = stockout_rate is not None and stockout_rate > FREQUENT_STOCKOUT_THRESHOLD
    high_variability_flag = (
        coefficient_of_variation is not None
        and coefficient_of_variation > HIGH_CV_THRESHOLD
    )
    persistent_overforecast_flag = (
        overforecast_ratio is not None and overforecast_ratio > OVERFORECAST_RATIO_THRESHOLD
    )
    incomplete_prediction_flag = prediction.prediction_quality == "partial"

    return AnalysisRow(
        target_date=target_date,
        shop_id=shop_id,
        product_code=product_code,
        prediction_quality=prediction.prediction_quality,
        mean_signed_error=_r(mean_signed_error),
        overforecast_ratio=_r(overforecast_ratio),
        mae=_r(mae),
        waste_rate=_r(waste_rate),
        stockout_rate=_r(stockout_rate),
        service_reliability=_r(service_reliability),
        stddev_units_sold=_r(stddev_units_sold),
        coefficient_of_variation=_r(coefficient_of_variation),
        temp_sales_correlation=_r(temp_corr),
        recent_mean_signed_error=_r(recent_mean_signed_error),
        high_waste_flag=high_waste_flag,
        frequent_stockout_flag=frequent_stockout_flag,
        high_variability_flag=high_variability_flag,
        persistent_overforecast_flag=persistent_overforecast_flag,
        incomplete_prediction_flag=incomplete_prediction_flag,
    )


def run_analysis(
    predictions: list[ValidatedPrediction],
    target_date: str,
) -> list[AnalysisRow]:
    """Compute all metrics for each prediction and return an AnalysisRow list.

    For every :class:`ValidatedPrediction` the function fetches bias, waste,
    stockout, variability, and temperature-correlation metrics from the
    repository layer, derives ``service_reliability``, applies transparent
    risk flags, and collects the results into :class:`AnalysisRow` objects.

    Args:
        predictions: Validated predictions to analyse.
        target_date: ISO-format date string that defines the analysis windows,
            e.g. ``"2026-01-15"``.

    Returns:
        A list of :class:`AnalysisRow` objects, one per input prediction.
    """
    main_window_start, recent_window_start, window_end = _compute_windows(target_date)

    rows: list[AnalysisRow] = []
    for prediction in predictions:
        row = _analyse_one(
            prediction=prediction,
            target_date=target_date,
            main_window_start=main_window_start,
            recent_window_start=recent_window_start,
            window_end=window_end,
        )
        rows.append(row)

    return rows


def rows_to_csv(rows: list[AnalysisRow], path: str) -> None:
    """Write analysis rows to a CSV file using :class:`csv.DictWriter`.

    The CSV header is derived from the first row's ``as_dict()`` keys.  If
    *rows* is empty, an empty file is written with no header.

    Args:
        rows: Analysis rows to serialise.
        path: Destination file path (created or overwritten).
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        with open(path, "w", newline="", encoding="utf-8"):
            pass
        return

    with open(path, "w", newline="", encoding="utf-8") as fh:
        fieldnames = list(rows[0].as_dict().keys())
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_dict())
