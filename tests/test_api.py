"""Smoke tests: FastAPI demand endpoint shape and error handling."""

from __future__ import annotations

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

# Redirect DB before importing the app
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ["DB_PATH"] = _tmp.name
os.environ["FAILURE_ENABLED"] = "false"

from bakery_analyst.api.app import create_app
from bakery_analyst.db.seed import seed_database


@pytest.fixture(scope="module")
def client():
    seed_database(force=True)
    app = create_app()
    with TestClient(app) as c:
        yield c
    os.unlink(_tmp.name)


@pytest.fixture(scope="module")
def a_seeded_date(client) -> str:
    """Return one date that is definitely in forecast_history."""
    from bakery_analyst.db.connection import get_connection
    conn = get_connection(_tmp.name)
    row = conn.execute("SELECT date FROM forecast_history ORDER BY date LIMIT 1").fetchone()
    conn.close()
    return row["date"]


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------

def test_health_returns_200(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# /api/demand — happy path
# ---------------------------------------------------------------------------

def test_demand_returns_200_for_seeded_date(client, a_seeded_date):
    r = client.get(f"/api/demand?date={a_seeded_date}")
    assert r.status_code == 200


def test_demand_envelope_shape(client, a_seeded_date):
    body = client.get(f"/api/demand?date={a_seeded_date}").json()
    assert "date" in body
    assert "predictions" in body
    assert isinstance(body["predictions"], list)
    assert len(body["predictions"]) > 0


def test_demand_record_has_required_fields(client, a_seeded_date):
    records = client.get(f"/api/demand?date={a_seeded_date}").json()["predictions"]
    for rec in records:
        assert "shop_id" in rec
        assert "product_code" in rec
        assert "date" in rec
        assert "pred_point" in rec
        assert rec["pred_point"] > 0


def test_demand_returns_six_records_for_seeded_date(client, a_seeded_date):
    """3 shops × 2 products = 6 records."""
    records = client.get(f"/api/demand?date={a_seeded_date}").json()["predictions"]
    assert len(records) == 6


# ---------------------------------------------------------------------------
# /api/demand — 404
# ---------------------------------------------------------------------------

def test_demand_404_for_unknown_date(client):
    r = client.get("/api/demand?date=2000-01-01")
    assert r.status_code == 404
    assert "detail" in r.json()


# ---------------------------------------------------------------------------
# /api/demand — 422 validation errors
# ---------------------------------------------------------------------------

def test_demand_422_bad_format(client):
    r = client.get("/api/demand?date=15-01-2025")
    assert r.status_code == 422


def test_demand_422_impossible_date(client):
    r = client.get("/api/demand?date=2025-13-45")
    assert r.status_code == 422


def test_demand_422_missing_param(client):
    r = client.get("/api/demand")
    assert r.status_code == 422
