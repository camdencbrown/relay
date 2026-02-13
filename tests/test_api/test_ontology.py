"""Tests for ontology API endpoints"""

from datetime import datetime, timezone


def _create_pipeline(client, name="Test Pipeline", pipe_id=None):
    """Helper to create a pipeline via API."""
    resp = client.post(
        "/api/v1/pipeline/create",
        json={
            "name": name,
            "source": {"type": "csv_url", "url": "http://example.com/data.csv"},
            "destination": {"type": "s3", "bucket": "test", "path": "data/"},
        },
    )
    return resp.json()["pipeline_id"]


def _create_entity(client, name, pipeline_id, display_name=None):
    """Helper to create an entity."""
    resp = client.post(
        "/api/v1/ontology/entity",
        json={
            "name": name,
            "display_name": display_name or name.title(),
            "pipeline_id": pipeline_id,
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


# ------------------------------------------------------------------
# Ontology snapshot
# ------------------------------------------------------------------


def test_get_empty_ontology(client):
    resp = client.get("/api/v1/ontology")
    assert resp.status_code == 200
    data = resp.json()
    assert data["entities"] == []
    assert data["relationships"] == []
    assert data["metrics"] == []
    assert data["dimensions"] == []


# ------------------------------------------------------------------
# Entity CRUD
# ------------------------------------------------------------------


def test_create_entity(client):
    pipe_id = _create_pipeline(client, "Orders")
    resp = client.post(
        "/api/v1/ontology/entity",
        json={
            "name": "orders",
            "display_name": "Orders",
            "description": "Order records",
            "pipeline_id": pipe_id,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "orders"
    assert data["id"].startswith("ent-")
    assert data["status"] == "active"


def test_create_entity_missing_pipeline(client):
    resp = client.post(
        "/api/v1/ontology/entity",
        json={
            "name": "bad",
            "display_name": "Bad",
            "pipeline_id": "pipe-nonexistent",
        },
    )
    assert resp.status_code == 400
    assert "Pipeline not found" in resp.json()["detail"]


def test_create_entity_duplicate_name(client):
    pipe_id = _create_pipeline(client, "Orders")
    client.post(
        "/api/v1/ontology/entity",
        json={"name": "orders", "display_name": "Orders", "pipeline_id": pipe_id},
    )
    resp = client.post(
        "/api/v1/ontology/entity",
        json={"name": "orders", "display_name": "Orders 2", "pipeline_id": pipe_id},
    )
    assert resp.status_code == 409


def test_list_entities(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "entity_a", pipe_id)
    _create_entity(client, "entity_b", pipe_id)

    resp = client.get("/api/v1/ontology/entity/list")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


def test_list_entities_by_status(client):
    pipe_id = _create_pipeline(client)
    entity = _create_entity(client, "entity_status", pipe_id)

    # Update status to proposed
    client.put(f"/api/v1/ontology/entity/{entity['id']}", json={"status": "proposed"})

    active = client.get("/api/v1/ontology/entity/list?status=active").json()
    proposed = client.get("/api/v1/ontology/entity/list?status=proposed").json()
    assert len(active) == 0
    assert len(proposed) == 1


def test_get_entity(client):
    pipe_id = _create_pipeline(client)
    entity = _create_entity(client, "get_me", pipe_id)

    resp = client.get(f"/api/v1/ontology/entity/{entity['id']}")
    assert resp.status_code == 200
    assert resp.json()["name"] == "get_me"


def test_get_entity_not_found(client):
    resp = client.get("/api/v1/ontology/entity/ent-nonexistent")
    assert resp.status_code == 404


def test_get_entity_by_name(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "named_entity", pipe_id)

    resp = client.get("/api/v1/ontology/entity/by-name/named_entity")
    assert resp.status_code == 200
    assert resp.json()["name"] == "named_entity"


def test_get_entity_by_name_not_found(client):
    resp = client.get("/api/v1/ontology/entity/by-name/nonexistent")
    assert resp.status_code == 404


def test_update_entity(client):
    pipe_id = _create_pipeline(client)
    entity = _create_entity(client, "update_me", pipe_id)

    resp = client.put(
        f"/api/v1/ontology/entity/{entity['id']}",
        json={"display_name": "Updated Name", "description": "Updated desc"},
    )
    assert resp.status_code == 200
    assert resp.json()["display_name"] == "Updated Name"


def test_update_entity_no_updates(client):
    pipe_id = _create_pipeline(client)
    entity = _create_entity(client, "no_update", pipe_id)

    resp = client.put(f"/api/v1/ontology/entity/{entity['id']}", json={})
    assert resp.status_code == 400


def test_delete_entity(client):
    pipe_id = _create_pipeline(client)
    entity = _create_entity(client, "delete_me", pipe_id)

    resp = client.delete(f"/api/v1/ontology/entity/{entity['id']}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "deleted"

    resp = client.get(f"/api/v1/ontology/entity/{entity['id']}")
    assert resp.status_code == 404


def test_delete_entity_not_found(client):
    resp = client.delete("/api/v1/ontology/entity/ent-nonexistent")
    assert resp.status_code == 404


# ------------------------------------------------------------------
# Relationship CRUD
# ------------------------------------------------------------------


def test_create_relationship(client):
    pipe_id1 = _create_pipeline(client, "Orders")
    pipe_id2 = _create_pipeline(client, "Customers")
    _create_entity(client, "orders", pipe_id1)
    _create_entity(client, "customers", pipe_id2)

    resp = client.post(
        "/api/v1/ontology/relationship",
        json={
            "name": "orders_to_customers",
            "from_entity": "orders",
            "to_entity": "customers",
            "from_column": "customer_id",
            "to_column": "id",
            "relationship_type": "many_to_one",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"].startswith("rel-")
    assert data["from_entity"] == "orders"


def test_create_relationship_missing_entity(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "orders", pipe_id)

    resp = client.post(
        "/api/v1/ontology/relationship",
        json={
            "name": "bad_rel",
            "from_entity": "orders",
            "to_entity": "nonexistent",
            "from_column": "x",
            "to_column": "y",
        },
    )
    assert resp.status_code == 400


def test_list_relationships(client):
    pipe_id1 = _create_pipeline(client, "Orders")
    pipe_id2 = _create_pipeline(client, "Customers")
    _create_entity(client, "orders", pipe_id1)
    _create_entity(client, "customers", pipe_id2)
    client.post(
        "/api/v1/ontology/relationship",
        json={
            "name": "r1",
            "from_entity": "orders",
            "to_entity": "customers",
            "from_column": "customer_id",
            "to_column": "id",
        },
    )

    resp = client.get("/api/v1/ontology/relationship/list")
    assert resp.status_code == 200
    assert len(resp.json()) == 1

    resp = client.get("/api/v1/ontology/relationship/list?entity=orders")
    assert len(resp.json()) == 1

    resp = client.get("/api/v1/ontology/relationship/list?entity=nonexistent")
    assert len(resp.json()) == 0


def test_delete_relationship(client):
    pipe_id1 = _create_pipeline(client, "Orders")
    pipe_id2 = _create_pipeline(client, "Customers")
    _create_entity(client, "orders", pipe_id1)
    _create_entity(client, "customers", pipe_id2)
    resp = client.post(
        "/api/v1/ontology/relationship",
        json={
            "name": "to_delete",
            "from_entity": "orders",
            "to_entity": "customers",
            "from_column": "x",
            "to_column": "y",
        },
    )
    rel_id = resp.json()["id"]

    resp = client.delete(f"/api/v1/ontology/relationship/{rel_id}")
    assert resp.status_code == 200


# ------------------------------------------------------------------
# Metric CRUD
# ------------------------------------------------------------------


def test_create_metric(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "orders", pipe_id)

    resp = client.post(
        "/api/v1/ontology/metric",
        json={
            "name": "revenue",
            "display_name": "Revenue",
            "entity_name": "orders",
            "expression": "SUM(orders.total)",
            "format_type": "currency",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["id"].startswith("met-")


def test_create_metric_missing_entity(client):
    resp = client.post(
        "/api/v1/ontology/metric",
        json={
            "name": "bad_metric",
            "display_name": "Bad",
            "entity_name": "nonexistent",
            "expression": "COUNT(*)",
        },
    )
    assert resp.status_code == 400


def test_list_metrics(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "orders", pipe_id)
    client.post(
        "/api/v1/ontology/metric",
        json={
            "name": "m1",
            "display_name": "M1",
            "entity_name": "orders",
            "expression": "SUM(orders.x)",
        },
    )

    resp = client.get("/api/v1/ontology/metric/list")
    assert resp.status_code == 200
    assert len(resp.json()) == 1


def test_update_metric(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "orders", pipe_id)
    resp = client.post(
        "/api/v1/ontology/metric",
        json={
            "name": "upd_metric",
            "display_name": "Old",
            "entity_name": "orders",
            "expression": "SUM(orders.x)",
        },
    )
    met_id = resp.json()["id"]

    resp = client.put(f"/api/v1/ontology/metric/{met_id}", json={"display_name": "New"})
    assert resp.status_code == 200
    assert resp.json()["display_name"] == "New"


def test_delete_metric(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "orders", pipe_id)
    resp = client.post(
        "/api/v1/ontology/metric",
        json={
            "name": "del_metric",
            "display_name": "Del",
            "entity_name": "orders",
            "expression": "COUNT(*)",
        },
    )
    met_id = resp.json()["id"]

    resp = client.delete(f"/api/v1/ontology/metric/{met_id}")
    assert resp.status_code == 200


# ------------------------------------------------------------------
# Dimension CRUD
# ------------------------------------------------------------------


def test_create_dimension(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "customers", pipe_id)

    resp = client.post(
        "/api/v1/ontology/dimension",
        json={
            "name": "segment",
            "display_name": "Segment",
            "entity_name": "customers",
            "expression": "customers.segment",
            "dimension_type": "direct",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["id"].startswith("dim-")


def test_create_dimension_missing_entity(client):
    resp = client.post(
        "/api/v1/ontology/dimension",
        json={
            "name": "bad_dim",
            "display_name": "Bad",
            "entity_name": "nonexistent",
            "expression": "x.y",
        },
    )
    assert resp.status_code == 400


def test_list_dimensions(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "orders", pipe_id)
    client.post(
        "/api/v1/ontology/dimension",
        json={
            "name": "d1",
            "display_name": "D1",
            "entity_name": "orders",
            "expression": "orders.status",
        },
    )

    resp = client.get("/api/v1/ontology/dimension/list")
    assert resp.status_code == 200
    assert len(resp.json()) == 1


def test_update_dimension(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "orders", pipe_id)
    resp = client.post(
        "/api/v1/ontology/dimension",
        json={
            "name": "upd_dim",
            "display_name": "Old",
            "entity_name": "orders",
            "expression": "orders.x",
        },
    )
    dim_id = resp.json()["id"]

    resp = client.put(f"/api/v1/ontology/dimension/{dim_id}", json={"display_name": "New"})
    assert resp.status_code == 200
    assert resp.json()["display_name"] == "New"


def test_delete_dimension(client):
    pipe_id = _create_pipeline(client)
    _create_entity(client, "orders", pipe_id)
    resp = client.post(
        "/api/v1/ontology/dimension",
        json={
            "name": "del_dim",
            "display_name": "Del",
            "entity_name": "orders",
            "expression": "orders.x",
        },
    )
    dim_id = resp.json()["id"]

    resp = client.delete(f"/api/v1/ontology/dimension/{dim_id}")
    assert resp.status_code == 200


# ------------------------------------------------------------------
# Semantic Query
# ------------------------------------------------------------------


def test_semantic_query_empty_returns_error(client):
    resp = client.post("/api/v1/ontology/query", json={})
    assert resp.status_code == 400


# ------------------------------------------------------------------
# Proposals
# ------------------------------------------------------------------


def test_propose_for_pipeline(client):
    pipe_id = _create_pipeline(client, "Sales Data")

    resp = client.post(
        "/api/v1/ontology/propose",
        json={"pipeline_id": pipe_id, "include_relationships": False, "include_metrics": False},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] >= 1
    assert any(p["proposal_type"] == "entity" for p in data["proposals"])


def test_propose_nonexistent_pipeline(client):
    resp = client.post(
        "/api/v1/ontology/propose",
        json={"pipeline_id": "pipe-nonexistent"},
    )
    assert resp.status_code == 400


def test_list_proposals(client):
    pipe_id = _create_pipeline(client, "List Proposals")
    client.post(
        "/api/v1/ontology/propose",
        json={"pipeline_id": pipe_id, "include_relationships": False, "include_metrics": False},
    )

    resp = client.get("/api/v1/ontology/proposal/list")
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


def test_get_proposal(client):
    pipe_id = _create_pipeline(client, "Get Proposal")
    propose_resp = client.post(
        "/api/v1/ontology/propose",
        json={"pipeline_id": pipe_id, "include_relationships": False, "include_metrics": False},
    )
    proposal_id = propose_resp.json()["proposals"][0]["id"]

    resp = client.get(f"/api/v1/ontology/proposal/{proposal_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == proposal_id


def test_get_proposal_not_found(client):
    resp = client.get("/api/v1/ontology/proposal/prop-nonexistent")
    assert resp.status_code == 404


def test_ontology_snapshot_after_creation(client):
    pipe_id = _create_pipeline(client, "Snapshot Test")
    _create_entity(client, "snapshot_entity", pipe_id)

    resp = client.get("/api/v1/ontology")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["entities"]) == 1
    assert data["entities"][0]["name"] == "snapshot_entity"


# ------------------------------------------------------------------
# Capabilities includes ontology
# ------------------------------------------------------------------


def test_capabilities_includes_ontology(client):
    resp = client.get("/api/v1/capabilities")
    assert resp.status_code == 200
    data = resp.json()
    assert "ontology" in data
    assert "endpoints" in data["ontology"]
    assert "semantic_query" in data["ontology"]["endpoints"]
