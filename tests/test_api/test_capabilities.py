"""Tests for /capabilities endpoint"""


def test_capabilities_returns_200(client):
    resp = client.get("/api/v1/capabilities")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Relay - Agent-Native Data Movement"
    assert "endpoints_summary" in data
    assert "query_engine" in data


def test_capabilities_has_version(client):
    resp = client.get("/api/v1/capabilities")
    data = resp.json()
    assert data["version"] == "2.0"


def test_capabilities_lists_sources(client):
    resp = client.get("/api/v1/capabilities")
    sources = resp.json()["sources"]
    types = [s["type"] for s in sources]
    assert "csv_url" in types
    assert "rest_api" in types
    assert "synthetic" in types
