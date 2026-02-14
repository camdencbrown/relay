"""Tests for RBAC enforcement with REQUIRE_AUTH=true."""

import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def auth_keys_and_client(_reset_db):
    """Create API keys through the API (auth disabled), then enable auth."""
    from src.config import get_settings
    from src.main import app

    with TestClient(app, raise_server_exceptions=False) as c:
        # Create keys while auth is disabled (REQUIRE_AUTH=false from conftest)
        resp = c.post("/api/v1/admin/api-keys", json={"name": "test-reader", "role": "reader"})
        reader_key = resp.json()["key"]
        resp = c.post("/api/v1/admin/api-keys", json={"name": "test-writer", "role": "writer"})
        writer_key = resp.json()["key"]
        resp = c.post("/api/v1/admin/api-keys", json={"name": "test-admin", "role": "admin"})
        admin_key = resp.json()["key"]

        # Enable auth - settings are checked dynamically per request
        os.environ["REQUIRE_AUTH"] = "true"
        get_settings.cache_clear()

        yield {
            "client": c,
            "reader_key": reader_key,
            "writer_key": writer_key,
            "admin_key": admin_key,
        }

    os.environ["REQUIRE_AUTH"] = "false"
    get_settings.cache_clear()


def test_missing_key_rejected(auth_keys_and_client):
    c = auth_keys_and_client["client"]
    resp = c.post(
        "/api/v1/pipeline/create",
        json={
            "name": "No Auth",
            "source": {"type": "csv_url", "url": "http://example.com/data.csv"},
            "destination": {"type": "s3", "bucket": "test", "path": "data/"},
        },
    )
    assert resp.status_code == 401


def test_reader_cannot_create_pipeline(auth_keys_and_client):
    c = auth_keys_and_client["client"]
    reader_key = auth_keys_and_client["reader_key"]
    resp = c.post(
        "/api/v1/pipeline/create",
        json={
            "name": "Reader Pipeline",
            "source": {"type": "csv_url", "url": "http://example.com/data.csv"},
            "destination": {"type": "s3", "bucket": "test", "path": "data/"},
        },
        headers={"X-API-Key": reader_key},
    )
    assert resp.status_code == 403
    assert "Insufficient permissions" in resp.json()["detail"]


def test_writer_can_create_pipeline(auth_keys_and_client):
    c = auth_keys_and_client["client"]
    writer_key = auth_keys_and_client["writer_key"]
    resp = c.post(
        "/api/v1/pipeline/create",
        json={
            "name": "Writer Pipeline",
            "source": {"type": "csv_url", "url": "http://example.com/data.csv"},
            "destination": {"type": "s3", "bucket": "test", "path": "data/"},
        },
        headers={"X-API-Key": writer_key},
    )
    assert resp.status_code == 200


def test_writer_cannot_delete_pipeline(auth_keys_and_client):
    c = auth_keys_and_client["client"]
    admin_key = auth_keys_and_client["admin_key"]
    writer_key = auth_keys_and_client["writer_key"]

    # Admin creates pipeline
    resp = c.post(
        "/api/v1/pipeline/create",
        json={
            "name": "Delete Test",
            "source": {"type": "csv_url", "url": "http://example.com/data.csv"},
            "destination": {"type": "s3", "bucket": "test", "path": "data/"},
        },
        headers={"X-API-Key": admin_key},
    )
    pipe_id = resp.json()["pipeline_id"]

    # Writer tries to delete
    resp = c.delete(
        f"/api/v1/pipeline/{pipe_id}",
        headers={"X-API-Key": writer_key},
    )
    assert resp.status_code == 403


def test_admin_can_do_everything(auth_keys_and_client):
    c = auth_keys_and_client["client"]
    admin_key = auth_keys_and_client["admin_key"]

    # Create pipeline
    resp = c.post(
        "/api/v1/pipeline/create",
        json={
            "name": "Admin Pipeline",
            "source": {"type": "csv_url", "url": "http://example.com/data.csv"},
            "destination": {"type": "s3", "bucket": "test", "path": "data/"},
        },
        headers={"X-API-Key": admin_key},
    )
    assert resp.status_code == 200
    pipe_id = resp.json()["pipeline_id"]

    # Delete pipeline
    resp = c.delete(
        f"/api/v1/pipeline/{pipe_id}",
        headers={"X-API-Key": admin_key},
    )
    assert resp.status_code == 200


def test_reader_can_read(auth_keys_and_client):
    """Readers can access read-only endpoints (no auth required on GET)."""
    c = auth_keys_and_client["client"]
    resp = c.get("/api/v1/pipeline/list")
    assert resp.status_code == 200
