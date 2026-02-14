"""Tests for admin API endpoints (dev mode - auth disabled)."""


def test_create_api_key(client):
    resp = client.post(
        "/api/v1/admin/api-keys",
        json={"name": "test-key", "description": "A test key", "role": "writer"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "created"
    assert data["key"].startswith("relay_")
    assert data["role"] == "writer"


def test_list_api_keys(client):
    client.post("/api/v1/admin/api-keys", json={"name": "key1", "role": "reader"})
    client.post("/api/v1/admin/api-keys", json={"name": "key2", "role": "admin"})

    resp = client.get("/api/v1/admin/api-keys")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] >= 2
    names = [k["name"] for k in data["api_keys"]]
    assert "key1" in names
    assert "key2" in names


def test_deactivate_api_key(client):
    create_resp = client.post("/api/v1/admin/api-keys", json={"name": "to-deactivate", "role": "writer"})
    # Get the key ID from listing
    list_resp = client.get("/api/v1/admin/api-keys")
    keys = list_resp.json()["api_keys"]
    key_id = next(k["id"] for k in keys if k["name"] == "to-deactivate")

    resp = client.delete(f"/api/v1/admin/api-keys/{key_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "deactivated"


def test_deactivate_not_found(client):
    resp = client.delete("/api/v1/admin/api-keys/99999")
    assert resp.status_code == 404


def test_create_invalid_role(client):
    resp = client.post(
        "/api/v1/admin/api-keys",
        json={"name": "bad-role", "role": "superadmin"},
    )
    assert resp.status_code == 422  # Pydantic validation error
