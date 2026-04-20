"""Domain models used internally across the analytics and reporting layers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class ValidatedPrediction:
    """A prediction record after validation and quality classification.

    Attributes
    ----------
    prediction_quality:
        ``complete`` — all four forecast fields present and valid.
        ``partial``  — pred_point present but one or more quantiles missing.
    """

    shop_id: str
    product_code: str
    date: str
    pred_point: float
    pred_q50: float | None
    pred_q80: float | None
    pred_q90: float | None
    prediction_quality: Literal["complete", "partial"]


@dataclass
class AnalysisRow:
    """One row of the computed analytics table (one shop/product for target date).

    Metric categories
    -----------------
    Forecast error (model quality):
        mean_signed_error, recent_mean_signed_error, mae, overforecast_ratio
    Waste (operational impact):
        waste_rate, avg_daily_waste_units
    Stockout (operational risk):
        stockout_rate, stockout_severity_proxy
    Demand variability (internal, flag trigger only):
        stddev_units_sold, coefficient_of_variation
    Temperature signal:
        temp_sales_correlation
    Actionability helpers:
        bias_adjusted_order, window_coverage_count,
        days_since_last_stockout, days_since_last_waste
    Risk flags:
        high_waste_flag, frequent_stockout_flag, high_variability_flag,
        persistent_overforecast_flag, incomplete_prediction_flag
    """

    target_date: str
    shop_id: str
    product_code: str
    prediction_quality: Literal["complete", "partial"]

    # ------------------------------------------------------------------
    # Forecast error — model quality metrics
    # ------------------------------------------------------------------
    mean_signed_error: float | None
    """avg(pred_point - units_sold) over 28 days. Positive = over-forecast."""

    recent_mean_signed_error: float | None
    """Same formula over the most recent 14 days. Compare with mean_signed_error
    to detect whether bias is worsening, stable, or improving."""

    overforecast_ratio: float | None
    """Fraction of days where pred_point > units_sold. Indicates directional
    consistency of the bias."""

    mae: float | None
    """Mean absolute error over 28 days. Captures error magnitude regardless
    of direction."""

    # ------------------------------------------------------------------
    # Waste — operational impact
    # ------------------------------------------------------------------
    waste_rate: float | None
    """sum(waste_units) / sum(ordered_units). Relative waste burden."""

    avg_daily_waste_units: float | None
    """avg(waste_units) per day over the window. Absolute volume figure — easier
    to cost out than waste_rate alone."""

    # ------------------------------------------------------------------
    # Stockout — operational risk
    # ------------------------------------------------------------------
    stockout_rate: float | None
    """Fraction of days in the window with at least one stockout event."""

    stockout_severity_proxy: float | None
    """PROXY / ESTIMATE ONLY — NOT true lost demand.
    On stockout days, avg(pred_point - ordered_units). The forecast is used as a
    proxy for what true demand *may* have been. Do not present as a precise lost-
    sales figure; always label as an estimated gap based on forecast vs. order."""

    # ------------------------------------------------------------------
    # Demand variability — internal use (flag trigger), not foregrounded
    # ------------------------------------------------------------------
    stddev_units_sold: float | None
    """Standard deviation of units_sold. Note: censored by stockouts on days
    where units_sold = ordered_units, so this understates true demand variance
    when stockout_rate is high."""

    coefficient_of_variation: float | None
    """stddev / mean of units_sold. Used as the high_variability_flag trigger;
    not foregrounded in the client narrative due to censoring and low
    interpretability for non-technical readers."""

    # ------------------------------------------------------------------
    # Temperature signal
    # ------------------------------------------------------------------
    temp_sales_correlation: float | None
    """Pearson r between daily temperature and units_sold. 28-day window makes
    this noisy; only surface in report when |r| > 0.35."""

    # ------------------------------------------------------------------
    # Actionability helpers
    # ------------------------------------------------------------------
    bias_adjusted_order: float | None
    """pred_point - mean_signed_error (28-day bias).
    Suggested baseline order quantity for the next day, correcting for
    observed systematic over- or under-forecasting.
    Always present as a starting point, not a guarantee."""

    window_coverage_count: int | None
    """Number of calendar days in the main 28-day window that have usable
    sales data. Lower values reduce metric reliability and should prompt
    more cautious language in the report."""

    days_since_last_stockout: int | None
    """Days between window_end and the most recent stockout event in the full
    historical record. None if no stockout has ever been recorded. Used to
    distinguish active problems from fading ones."""

    days_since_last_waste: int | None
    """Days between window_end and the most recent day with waste_units > 0.
    None if no waste has ever been recorded."""

    # ------------------------------------------------------------------
    # Risk flags
    # ------------------------------------------------------------------
    high_waste_flag: bool = False
    frequent_stockout_flag: bool = False
    high_variability_flag: bool = False
    persistent_overforecast_flag: bool = False
    incomplete_prediction_flag: bool = False

    def as_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}
