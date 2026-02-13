"""Tests for query/schema endpoints"""


def test_query_no_pipelines(client):
    resp = client.post("/api/v1/query", json={"pipelines": ["pipe-none"], "sql": "SELECT 1"})
    assert resp.status_code == 400


def test_schema_no_pipelines(client):
    resp = client.post("/api/v1/schema", json={"pipelines": ["pipe-none"]})
    assert resp.status_code == 200
    # Should return empty schemas (unknown pipeline is just skipped)
    data = resp.json()
    assert data["status"] == "success"


def test_export_no_results(client):
    resp = client.post(
        "/api/v1/export",
        json={"pipelines": ["pipe-none"], "sql": "SELECT 1", "format": "csv"},
    )
    # Should fail because pipeline doesn't exist
    assert resp.status_code in (400, 404)
