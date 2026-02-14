"""Tests for lineage computation."""

from datetime import datetime, timezone

from src.lineage import compute_lineage, extract_column_references


# ------------------------------------------------------------------
# Column reference extraction
# ------------------------------------------------------------------


def test_simple_column_ref():
    assert extract_column_references("SUM(orders.total)") == ["orders.total"]


def test_multiple_column_refs():
    refs = extract_column_references("orders.total - orders.discount")
    assert "orders.total" in refs
    assert "orders.discount" in refs


def test_no_column_refs():
    assert extract_column_references("COUNT(*)") == []


def test_complex_expression():
    refs = extract_column_references("CASE WHEN orders.status = 'active' THEN orders.total ELSE 0 END")
    assert "orders.status" in refs
    assert "orders.total" in refs


def test_derived_expression():
    refs = extract_column_references("DATE_TRUNC('month', orders.created_at)")
    assert "orders.created_at" in refs


# ------------------------------------------------------------------
# Full lineage computation
# ------------------------------------------------------------------


def test_compute_lineage_full(storage):
    """Full lineage with pipeline, metrics, dimensions, relationships."""
    now = datetime.now(timezone.utc).isoformat()

    # Create pipeline
    storage.save_pipeline({
        "id": "pipe-orders",
        "name": "Orders Pipeline",
        "source": {"type": "csv_url", "url": "http://example.com/orders.csv"},
        "destination": {"type": "s3", "bucket": "b", "path": "p/"},
        "options": {},
        "schedule": {},
        "status": "active",
        "created_at": now,
    })
    storage.save_pipeline({
        "id": "pipe-cust",
        "name": "Customers Pipeline",
        "source": {"type": "csv_url", "url": "http://example.com/cust.csv"},
        "destination": {"type": "s3", "bucket": "b", "path": "p/"},
        "options": {},
        "schedule": {},
        "status": "active",
        "created_at": now,
    })

    # Create entities
    storage.save_entity({
        "id": "ent-orders",
        "name": "orders",
        "display_name": "Orders",
        "pipeline_id": "pipe-orders",
        "status": "active",
    })
    storage.save_entity({
        "id": "ent-cust",
        "name": "customers",
        "display_name": "Customers",
        "pipeline_id": "pipe-cust",
        "status": "active",
    })

    # Create metric
    storage.save_metric({
        "id": "met-rev",
        "name": "revenue",
        "display_name": "Revenue",
        "entity_name": "orders",
        "expression": "SUM(orders.total)",
        "status": "active",
    })

    # Create dimension
    storage.save_dimension({
        "id": "dim-month",
        "name": "order_month",
        "display_name": "Order Month",
        "entity_name": "orders",
        "expression": "DATE_TRUNC('month', orders.created_at)",
        "status": "active",
    })

    # Create relationship
    storage.save_relationship({
        "id": "rel-1",
        "name": "orders_to_customers",
        "from_entity": "orders",
        "to_entity": "customers",
        "from_column": "customer_id",
        "to_column": "id",
    })

    lineage = compute_lineage("orders", storage)
    assert lineage is not None
    assert lineage["entity"]["name"] == "orders"
    assert lineage["pipeline"]["id"] == "pipe-orders"
    assert lineage["source"]["type"] == "csv_url"
    assert len(lineage["metrics"]) == 1
    assert lineage["metrics"][0]["column_references"] == ["orders.total"]
    assert len(lineage["dimensions"]) == 1
    assert "orders.created_at" in lineage["dimensions"][0]["column_references"]
    assert lineage["relationships"]["outgoing"][0]["to_entity"] == "customers"
    assert lineage["downstream_entities"] == ["customers"]
    assert lineage["upstream_entities"] == []


def test_compute_lineage_with_incoming(storage):
    """Verify upstream entities via incoming relationships."""
    storage.save_entity({
        "id": "ent-a",
        "name": "entity_a",
        "display_name": "A",
        "pipeline_id": "pipe-x",
        "status": "active",
    })
    storage.save_entity({
        "id": "ent-b",
        "name": "entity_b",
        "display_name": "B",
        "pipeline_id": "pipe-y",
        "status": "active",
    })
    storage.save_relationship({
        "id": "rel-ab",
        "name": "a_to_b",
        "from_entity": "entity_a",
        "to_entity": "entity_b",
        "from_column": "b_id",
        "to_column": "id",
    })

    lineage = compute_lineage("entity_b", storage)
    assert lineage is not None
    assert lineage["upstream_entities"] == ["entity_a"]


def test_compute_lineage_not_found(storage):
    assert compute_lineage("nonexistent", storage) is None
