"""Pydantic models for the demand API — both request validation and response shape."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class PredictionRecord(BaseModel):
    """One row returned by the demand API for a single shop/product."""

    shop_id: str
    product_code: str
    date: str
    pred_point: float = Field(..., ge=0)
    pred_q50: float | None = Field(None, ge=0)
    pred_q80: float | None = Field(None, ge=0)
    pred_q90: float | None = Field(None, ge=0)


class DemandResponse(BaseModel):
    """Top-level envelope returned by GET /api/demand."""

    date: str
    predictions: list[PredictionRecord]


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    db_path: str
