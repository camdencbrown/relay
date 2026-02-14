"""Tests for analytics API endpoints."""


def _create_pipeline(client, name="Test Pipeline"):
    resp = client.post(
        "/api/v1/pipeline/create",
        json={
            "name": name,
            "source": {"type": "csv_url", "url": "http://example.com/data.csv"},
            "destination": {"type": "s3", "bucket": "test", "path": "data/"},
        },
    )
    return resp.json()["pipeline_id"]


def test_empty_summary(client):
    resp = client.get("/api/v1/analytics/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_events"] == 0
    assert data["event_counts"] == {}
    assert data["recent_events"] == []


def test_events_after_pipeline_create(client):
    _create_pipeline(client, "Analytics Test")

    resp = client.get("/api/v1/analytics/events?event_type=pipeline_created")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] >= 1
    assert data["events"][0]["event_type"] == "pipeline_created"


def test_summary_with_counts(client):
    _create_pipeline(client, "Pipeline A")
    _create_pipeline(client, "Pipeline B")

    resp = client.get("/api/v1/analytics/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["event_counts"]["pipeline_created"] >= 2
    assert data["total_events"] >= 2
    assert len(data["recent_events"]) >= 2


def test_events_filter_by_pipeline(client):
    pipe_id = _create_pipeline(client, "Filter Test")

    resp = client.get(f"/api/v1/analytics/events?pipeline_id={pipe_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] >= 1
    assert all(e["pipeline_id"] == pipe_id for e in data["events"])
