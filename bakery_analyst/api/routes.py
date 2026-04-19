"""Route handlers for the Bakery Demand Analytics API."""

import random
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from bakery_analyst.config import settings
from bakery_analyst.db.connection import db_session
from bakery_analyst.models.api_models import DemandResponse, HealthResponse, PredictionRecord

router = APIRouter()


def _validate_date(date_str: str) -> None:
    """Validate that *date_str* is a calendar-valid date in ``YYYY-MM-DD`` format.

    Args:
        date_str: The raw date string supplied by the caller.

    Raises:
        HTTPException: 422 if the string does not match ``YYYY-MM-DD`` or is
            not a real calendar date.
    """
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid date format: '{date_str}'. Expected YYYY-MM-DD.",
        )

    # Reject inputs like '2024-02-30' that strptime might silently accept on
    # some platforms by round-tripping back to the canonical string.
    if parsed.strftime("%Y-%m-%d") != date_str:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid calendar date: '{date_str}'.",
        )


def _apply_partial_simulation(row: dict) -> dict:
    """Optionally drop quantile fields from *row* to simulate incomplete data.

    When failure injection is enabled and the random draw falls below
    ``settings.partial_record_probability``, the quantile columns
    (``pred_q50``, ``pred_q80``, ``pred_q90``) are set to ``None``.
    The point forecast (``pred_point``) is always preserved.

    Args:
        row: A mutable mapping containing the forecast row fields.

    Returns:
        The same mapping, possibly with quantile fields set to ``None``.
    """
    if settings.failure_enabled and random.random() < settings.partial_record_probability:
        row["pred_q50"] = None
        row["pred_q80"] = None
        row["pred_q90"] = None
    return row


@router.get("/api/demand", response_model=DemandResponse)
async def get_demand(
    date: str = Query(..., description="Forecast date in YYYY-MM-DD format"),
) -> DemandResponse:
    """Return all demand forecasts stored for the given date.

    Args:
        date: Query parameter ``date`` — must be a valid calendar date in
            ``YYYY-MM-DD`` format.

    Returns:
        A :class:`~bakery_analyst.models.api_models.DemandResponse` containing
        the matched predictions.

    Raises:
        HTTPException: 422 if *date* is malformed or not a valid calendar date.
        HTTPException: 404 if no predictions exist for *date*.
    """
    _validate_date(date)

    with db_session() as conn:
        cursor = conn.execute(
            """
            SELECT shop_id, product_code, date,
                   pred_point, pred_q50, pred_q80, pred_q90
            FROM forecast_history
            WHERE date = ?
            """,
            (date,),
        )
        rows = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No predictions found for date {date}",
        )

    predictions: list[PredictionRecord] = [
        PredictionRecord(**_apply_partial_simulation(row)) for row in rows
    ]

    return DemandResponse(date=date, predictions=predictions)


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Return the current health status of the API.

    Returns:
        A :class:`~bakery_analyst.models.api_models.HealthResponse` with
        ``status="ok"`` and the configured database path.
    """
    return HealthResponse(status="ok", db_path=settings.db_path)
