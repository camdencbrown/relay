"""Tests for pipeline CRUD endpoints"""


def test_create_pipeline(client):
    resp = client.post(
        "/api/v1/pipeline/create",
        json={
            "name": "Test CSV Pipeline",
            "source": {"type": "csv_url", "url": "http://example.com/data.csv"},
            "destination": {"type": "s3", "bucket": "test-bucket", "path": "test/"},
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "created"
    assert data["pipeline_id"].startswith("pipe-")
    assert data["table_name"] == "test_csv_pipeline"


def test_list_pipelines_empty(client):
    resp = client.get("/api/v1/pipeline/list")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["pipelines"] == []


def test_list_after_create(client):
    client.post(
        "/api/v1/pipeline/create",
        json={
            "name": "Pipeline A",
            "source": {"type": "csv_url", "url": "http://example.com/a.csv"},
            "destination": {"type": "s3", "bucket": "b", "path": "p/"},
        },
    )
    resp = client.get("/api/v1/pipeline/list")
    data = resp.json()
    assert data["total"] == 1
    assert data["pipelines"][0]["name"] == "Pipeline A"


def test_get_pipeline(client):
    create_resp = client.post(
        "/api/v1/pipeline/create",
        json={
            "name": "Get Test",
            "source": {"type": "csv_url", "url": "http://example.com/x.csv"},
            "destination": {"type": "s3", "bucket": "b", "path": "p/"},
        },
    )
    pid = create_resp.json()["pipeline_id"]
    resp = client.get(f"/api/v1/pipeline/{pid}")
    assert resp.status_code == 200
    assert resp.json()["name"] == "Get Test"


def test_get_pipeline_not_found(client):
    resp = client.get("/api/v1/pipeline/pipe-nonexistent")
    assert resp.status_code == 404


def test_delete_pipeline(client):
    create_resp = client.post(
        "/api/v1/pipeline/create",
        json={
            "name": "To Delete",
            "source": {"type": "csv_url", "url": "http://example.com/d.csv"},
            "destination": {"type": "s3", "bucket": "b", "path": "p/"},
        },
    )
    pid = create_resp.json()["pipeline_id"]
    resp = client.delete(f"/api/v1/pipeline/{pid}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "deleted"

    # Should be gone
    resp = client.get(f"/api/v1/pipeline/{pid}")
    assert resp.status_code == 404
