"""Tests for connection API endpoints"""

import pytest


def test_create_connection(client):
    resp = client.post("/api/v1/connection/create", json={
        "name": "test-mysql",
        "type": "mysql",
        "description": "Test MySQL connection",
        "credentials": {"host": "localhost", "username": "root", "password": "secret", "database": "testdb"},
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "created"
    conn = data["connection"]
    assert conn["name"] == "test-mysql"
    assert conn["type"] == "mysql"
    assert "credentials" not in conn  # credentials never returned


def test_create_duplicate_name(client):
    client.post("/api/v1/connection/create", json={
        "name": "dup-conn",
        "type": "postgres",
        "credentials": {"host": "localhost", "username": "u", "password": "p", "database": "db"},
    })
    resp = client.post("/api/v1/connection/create", json={
        "name": "dup-conn",
        "type": "postgres",
        "credentials": {"host": "other", "username": "u", "password": "p", "database": "db"},
    })
    assert resp.status_code == 409


def test_create_connection_invalid_name(client):
    resp = client.post("/api/v1/connection/create", json={
        "name": "1-invalid",
        "type": "mysql",
        "credentials": {"host": "localhost"},
    })
    assert resp.status_code == 422


def test_create_connection_invalid_type(client):
    resp = client.post("/api/v1/connection/create", json={
        "name": "valid-name",
        "type": "invalid_type",
        "credentials": {"host": "localhost"},
    })
    assert resp.status_code == 422


def test_list_connections(client):
    # Create two connections
    client.post("/api/v1/connection/create", json={
        "name": "conn-a",
        "type": "mysql",
        "credentials": {"host": "a"},
    })
    client.post("/api/v1/connection/create", json={
        "name": "conn-b",
        "type": "postgres",
        "credentials": {"host": "b"},
    })
    resp = client.get("/api/v1/connection/list")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2
    for conn in data["connections"]:
        assert "credentials" not in conn  # never returned


def test_get_connection(client):
    create_resp = client.post("/api/v1/connection/create", json={
        "name": "get-test",
        "type": "salesforce",
        "credentials": {"username": "u", "password": "p", "security_token": "t"},
    })
    conn_id = create_resp.json()["connection"]["id"]

    resp = client.get(f"/api/v1/connection/{conn_id}")
    assert resp.status_code == 200
    conn = resp.json()["connection"]
    assert conn["name"] == "get-test"
    assert "credentials" not in conn


def test_get_connection_not_found(client):
    resp = client.get("/api/v1/connection/conn-nonexistent")
    assert resp.status_code == 404


def test_update_connection(client):
    create_resp = client.post("/api/v1/connection/create", json={
        "name": "update-test",
        "type": "mysql",
        "credentials": {"host": "old-host", "username": "u", "password": "p", "database": "db"},
    })
    conn_id = create_resp.json()["connection"]["id"]

    resp = client.put(f"/api/v1/connection/{conn_id}", json={
        "description": "Updated description",
        "credentials": {"host": "new-host", "username": "u", "password": "new-p", "database": "db"},
    })
    assert resp.status_code == 200
    conn = resp.json()["connection"]
    assert conn["description"] == "Updated description"
    assert "credentials" not in conn


def test_update_connection_not_found(client):
    resp = client.put("/api/v1/connection/conn-nonexistent", json={"description": "x"})
    assert resp.status_code == 404


def test_delete_connection(client):
    create_resp = client.post("/api/v1/connection/create", json={
        "name": "del-test",
        "type": "mysql",
        "credentials": {"host": "h"},
    })
    conn_id = create_resp.json()["connection"]["id"]

    resp = client.delete(f"/api/v1/connection/{conn_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "deleted"

    # Verify gone
    resp = client.get(f"/api/v1/connection/{conn_id}")
    assert resp.status_code == 404


def test_delete_connection_blocked_by_pipeline(client):
    """Cannot delete a connection that is referenced by a pipeline."""
    create_resp = client.post("/api/v1/connection/create", json={
        "name": "in-use-conn",
        "type": "mysql",
        "credentials": {"host": "h", "username": "u", "password": "p", "database": "db"},
    })
    conn_id = create_resp.json()["connection"]["id"]

    # Create a pipeline that references this connection
    client.post("/api/v1/pipeline/create", json={
        "name": "Pipeline Using Conn",
        "source": {"type": "mysql", "connection": "in-use-conn", "query": "SELECT 1"},
        "destination": {"type": "s3", "bucket": "b", "path": "p/"},
    })

    resp = client.delete(f"/api/v1/connection/{conn_id}")
    assert resp.status_code == 409
    assert "used by pipelines" in resp.json()["detail"]


def test_pipeline_with_connection_hides_credentials(client):
    """Pipeline response should show connection name, not credentials."""
    client.post("/api/v1/connection/create", json={
        "name": "pipe-conn",
        "type": "mysql",
        "credentials": {"host": "secret-host", "username": "u", "password": "topsecret", "database": "db"},
    })

    create_resp = client.post("/api/v1/pipeline/create", json={
        "name": "Conn Pipeline",
        "source": {"type": "mysql", "connection": "pipe-conn", "query": "SELECT 1"},
        "destination": {"type": "s3", "bucket": "b", "path": "p/"},
    })
    assert create_resp.status_code == 200
    pipeline_id = create_resp.json()["pipeline_id"]

    # Fetch the pipeline and verify credentials aren't stored
    resp = client.get(f"/api/v1/pipeline/{pipeline_id}")
    assert resp.status_code == 200
    source = resp.json()["source"]
    assert source["connection"] == "pipe-conn"
    assert "password" not in source
    assert "topsecret" not in str(source)


def test_capabilities_includes_connections(client):
    resp = client.get("/api/v1/capabilities")
    assert resp.status_code == 200
    data = resp.json()
    assert "connections" in data
    assert "connection_create" in data["endpoints_summary"]
