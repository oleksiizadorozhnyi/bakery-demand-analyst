"""Domain models used internally across the analytics and reporting layers."""

from __future__ import annotations

from dataclasses import dataclass, field
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
    """One row of the computed analytics table (one shop/product for target date)."""

    target_date: str
    shop_id: str
    product_code: str
    prediction_quality: Literal["complete", "partial"]

    # Bias metrics (28-day window)
    mean_signed_error: float | None
    overforecast_ratio: float | None
    mae: float | None

    # Waste
    waste_rate: float | None

    # Reliability
    stockout_rate: float | None
    service_reliability: float | None

    # Demand variability
    stddev_units_sold: float | None
    coefficient_of_variation: float | None

    # Temperature correlation (isolated — computed in Python, not SQL)
    temp_sales_correlation: float | None

    # Recent bias (14-day)
    recent_mean_signed_error: float | None

    # Risk flags
    high_waste_flag: bool = False
    frequent_stockout_flag: bool = False
    high_variability_flag: bool = False
    persistent_overforecast_flag: bool = False
    incomplete_prediction_flag: bool = False

    def as_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}
