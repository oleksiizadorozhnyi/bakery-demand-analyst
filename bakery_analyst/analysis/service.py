"""Analytics orchestration layer for bakery demand analytics.

Computes all metrics for each ValidatedPrediction, applies risk flags,
and returns a list of AnalysisRow objects.
"""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

from bakery_analyst.config import settings
from bakery_analyst.models.domain_models import AnalysisRow, ValidatedPrediction
from bakery_analyst.repository.analytics_repository import (
    compute_temp_sales_correlation,
    fetch_bias_metrics,
    fetch_recent_bias,
    fetch_recency_metrics,
    fetch_stockout_metrics,
    fetch_stockout_severity,
    fetch_variability_metrics,
    fetch_waste_metrics,
    fetch_window_coverage,
)

# ---------------------------------------------------------------------------
# Risk flag thresholds
# ---------------------------------------------------------------------------
HIGH_WASTE_THRESHOLD: float = 0.20
FREQUENT_STOCKOUT_THRESHOLD: float = 0.15
HIGH_CV_THRESHOLD: float = 0.40
OVERFORECAST_RATIO_THRESHOLD: float = 0.65


def _r(value: float | None, decimals: int = 3) -> float | None:
    """Round a float to *decimals* places, or return None."""
    return round(value, decimals) if value is not None else None


def _ri(value: int | None) -> int | None:
    """Pass through an int or None unchanged."""
    return value


def _compute_windows(target_date: str) -> tuple[str, str, str]:
    """Return (main_window_start, recent_window_start, window_end).

    window_end       = target_date - 1 day  (last day of observed history)
    main_window_start = window_end - main_window_days
    recent_window_start = window_end - recent_window_days

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
    """Compute all metrics and risk flags for a single prediction.

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
    # Fetch all metrics from the repository layer
    # ------------------------------------------------------------------
    bias = fetch_bias_metrics(shop_id, product_code, main_window_start, window_end)
    recent_bias = fetch_recent_bias(shop_id, product_code, recent_window_start, window_end)
    waste = fetch_waste_metrics(shop_id, product_code, main_window_start, window_end)
    stockout = fetch_stockout_metrics(shop_id, product_code, main_window_start, window_end)
    stockout_sev = fetch_stockout_severity(shop_id, product_code, main_window_start, window_end)
    variability = fetch_variability_metrics(shop_id, product_code, main_window_start, window_end)
    temp_corr = compute_temp_sales_correlation(shop_id, product_code, main_window_start, window_end)
    coverage = fetch_window_coverage(shop_id, product_code, main_window_start, window_end)
    recency = fetch_recency_metrics(shop_id, product_code, window_end)

    # ------------------------------------------------------------------
    # Extract individual values (raw, before rounding)
    # ------------------------------------------------------------------
    mean_signed_error: float | None = bias.get("mean_signed_error")
    mae: float | None = bias.get("mae")
    overforecast_ratio: float | None = bias.get("overforecast_ratio")
    recent_mean_signed_error: float | None = recent_bias.get("recent_mean_signed_error")
    waste_rate: float | None = waste.get("waste_rate")
    avg_daily_waste_units: float | None = waste.get("avg_daily_waste_units")
    stockout_rate: float | None = stockout.get("stockout_rate")
    stockout_severity_proxy: float | None = stockout_sev.get("stockout_severity_proxy")
    stddev_units_sold: float | None = variability.get("stddev_units_sold")
    coefficient_of_variation: float | None = variability.get("coefficient_of_variation")
    window_coverage_count: int | None = coverage.get("window_coverage_count")
    days_since_last_stockout: int | None = recency.get("days_since_last_stockout")
    days_since_last_waste: int | None = recency.get("days_since_last_waste")

    # ------------------------------------------------------------------
    # Bias-adjusted order suggestion
    #
    # Uses the 28-day bias as the primary correction because it is more
    # statistically stable than the 14-day recent bias.  The 14-day bias
    # is reported alongside so the LLM can flag cases where recent bias
    # diverges strongly and a more aggressive correction may be appropriate.
    #
    # Formula: pred_point - mean_signed_error
    #   If bias > 0 (over-forecast): suggested order < prediction.
    #   If bias < 0 (under-forecast): suggested order > prediction.
    # ------------------------------------------------------------------
    bias_adjusted_order: float | None = (
        round(prediction.pred_point - mean_signed_error, 1)
        if mean_signed_error is not None
        else None
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
    frequent_stockout_flag = (
        stockout_rate is not None and stockout_rate > FREQUENT_STOCKOUT_THRESHOLD
    )
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
        # Forecast error
        mean_signed_error=_r(mean_signed_error),
        recent_mean_signed_error=_r(recent_mean_signed_error),
        overforecast_ratio=_r(overforecast_ratio),
        mae=_r(mae),
        # Waste
        waste_rate=_r(waste_rate),
        avg_daily_waste_units=_r(avg_daily_waste_units, decimals=1),
        # Stockout
        stockout_rate=_r(stockout_rate),
        stockout_severity_proxy=_r(stockout_severity_proxy, decimals=1),
        # Variability (internal)
        stddev_units_sold=_r(stddev_units_sold),
        coefficient_of_variation=_r(coefficient_of_variation),
        # Temperature
        temp_sales_correlation=_r(temp_corr),
        # Actionability helpers
        bias_adjusted_order=bias_adjusted_order,
        window_coverage_count=_ri(window_coverage_count),
        days_since_last_stockout=_ri(days_since_last_stockout),
        days_since_last_waste=_ri(days_since_last_waste),
        # Risk flags
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
