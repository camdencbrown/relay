"""Tests for metadata endpoints"""


def test_metadata_not_found(client):
    resp = client.get("/api/v1/metadata/pipe-nonexistent")
    assert resp.status_code == 404


def test_pending_reviews_empty(client):
    resp = client.get("/api/v1/metadata/review/pending")
    assert resp.status_code == 200
    data = resp.json()
    assert data["pending_count"] == 0


def test_approve_column(client):
    resp = client.post(
        "/api/v1/metadata/review/approve",
        json={
            "column_name": "email",
            "description": "User email address",
            "business_meaning": "Primary contact email",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "approved"
