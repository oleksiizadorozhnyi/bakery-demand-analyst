"""Fetches predictions from the demand API and validates / classifies them.

Validation rules
----------------
Critical fields: shop_id, product_code, date, pred_point.
  → records missing any of these are silently dropped (logged to stderr).

Quantile fields: pred_q50, pred_q80, pred_q90.
  → if any are missing, the record is kept but marked prediction_quality="partial".
  → if all are present and quantile order holds, marked "complete".
  → if all quantiles present but ordering violated, they are set to None ("partial").
"""

from __future__ import annotations

import sys
from typing import Any

import httpx

from bakery_analyst.models.api_models import DemandResponse, PredictionRecord
from bakery_analyst.models.domain_models import ValidatedPrediction


def fetch_predictions(base_url: str, date_str: str, timeout: float = 10.0) -> DemandResponse:
    """Call GET /api/demand?date=<date_str> and return the parsed response.

    Raises
    ------
    httpx.HTTPStatusError
        When the server returns 4xx or 5xx.
    httpx.RequestError
        On network/timeout errors.
    """
    url = f"{base_url}/api/demand"
    response = httpx.get(url, params={"date": date_str}, timeout=timeout)
    response.raise_for_status()
    return DemandResponse.model_validate(response.json())


def validate_predictions(records: list[PredictionRecord]) -> list[ValidatedPrediction]:
    """Validate and quality-classify a list of raw prediction records.

    Returns a filtered list of :class:`ValidatedPrediction` objects with
    ``prediction_quality`` set to ``"complete"`` or ``"partial"``.
    """
    validated: list[ValidatedPrediction] = []

    for rec in records:
        # --- critical field check ---
        if not _has_critical_fields(rec):
            print(
                f"  [warn] Dropping record — missing critical field: {rec.model_dump()}",
                file=sys.stderr,
            )
            continue

        # --- quantile quality classification ---
        q50, q80, q90 = rec.pred_q50, rec.pred_q80, rec.pred_q90
        all_present = all(v is not None for v in (q50, q80, q90))

        if all_present:
            if _quantiles_ordered(q50, q80, q90):  # type: ignore[arg-type]
                quality = "complete"
            else:
                print(
                    f"  [warn] Quantile ordering violated for "
                    f"{rec.shop_id}/{rec.product_code} — dropping quantiles.",
                    file=sys.stderr,
                )
                q50 = q80 = q90 = None
                quality = "partial"
        else:
            quality = "partial"

        validated.append(
            ValidatedPrediction(
                shop_id=rec.shop_id,
                product_code=rec.product_code,
                date=rec.date,
                pred_point=rec.pred_point,
                pred_q50=q50,
                pred_q80=q80,
                pred_q90=q90,
                prediction_quality=quality,
            )
        )

    return validated


def _has_critical_fields(rec: PredictionRecord) -> bool:
    return bool(rec.shop_id and rec.product_code and rec.date and rec.pred_point is not None)


def _quantiles_ordered(q50: float, q80: float, q90: float) -> bool:
    return q50 <= q80 <= q90
