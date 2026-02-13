"""Tests for SQLite Storage"""

from datetime import datetime, timezone


def test_save_and_get_pipeline(storage):
    pipeline = {
        "id": "pipe-test1",
        "name": "Test Pipeline",
        "source": {"type": "csv_url", "url": "http://example.com/data.csv"},
        "destination": {"type": "s3", "bucket": "test", "path": "data/"},
        "options": {"format": "parquet"},
        "schedule": {"enabled": False},
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    storage.save_pipeline(pipeline)
    result = storage.get_pipeline("pipe-test1")
    assert result is not None
    assert result["name"] == "Test Pipeline"
    assert result["source"]["type"] == "csv_url"
    assert result["runs"] == []


def test_list_pipelines(storage):
    for i in range(3):
        storage.save_pipeline(
            {
                "id": f"pipe-{i}",
                "name": f"Pipeline {i}",
                "source": {"type": "csv_url"},
                "destination": {"type": "s3", "bucket": "b", "path": "p/"},
                "options": {},
                "schedule": {},
                "status": "active",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    pipelines = storage.list_pipelines()
    assert len(pipelines) == 3


def test_delete_pipeline(storage):
    storage.save_pipeline(
        {
            "id": "pipe-del",
            "name": "To Delete",
            "source": {},
            "destination": {},
            "options": {},
            "schedule": {},
            "status": "active",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    assert storage.delete_pipeline("pipe-del") is True
    assert storage.get_pipeline("pipe-del") is None


def test_add_and_update_run(storage):
    storage.save_pipeline(
        {
            "id": "pipe-run",
            "name": "Run Test",
            "source": {},
            "destination": {},
            "options": {},
            "schedule": {},
            "status": "active",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    storage.add_run("pipe-run", {"run_id": "run-1", "status": "running", "started_at": "2024-01-01T00:00:00"})
    pipeline = storage.get_pipeline("pipe-run")
    assert len(pipeline["runs"]) == 1
    assert pipeline["runs"][0]["status"] == "running"

    storage.update_run("pipe-run", "run-1", {"status": "success", "rows_processed": 100})
    pipeline = storage.get_pipeline("pipe-run")
    assert pipeline["runs"][0]["status"] == "success"
    assert pipeline["runs"][0]["rows_processed"] == 100


def test_metadata_crud(storage):
    metadata = {"pipeline_name": "test", "columns": [{"name": "id", "type": "int"}]}
    storage.save_metadata("pipe-meta", metadata)
    result = storage.get_metadata("pipe-meta")
    assert result is not None
    assert result["pipeline_name"] == "test"

    # Update
    metadata["row_count"] = 42
    storage.save_metadata("pipe-meta", metadata)
    result = storage.get_metadata("pipe-meta")
    assert result["row_count"] == 42


def test_column_knowledge(storage):
    storage.save_column_knowledge("email", "Email address of the user", "Primary contact", "admin")
    result = storage.get_column_knowledge("email")
    assert result is not None
    assert result["description"] == "Email address of the user"

    all_knowledge = storage.list_column_knowledge()
    assert "email" in all_knowledge


def test_get_nonexistent_pipeline(storage):
    assert storage.get_pipeline("nonexistent") is None


def test_get_nonexistent_metadata(storage):
    assert storage.get_metadata("nonexistent") is None


# ------------------------------------------------------------------
# Connection CRUD
# ------------------------------------------------------------------


def test_save_and_get_connection(storage):
    conn = {
        "id": "conn-test1",
        "name": "my-mysql",
        "type": "mysql",
        "credentials": {"host": "localhost", "username": "root", "password": "secret", "database": "db"},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    result = storage.save_connection(conn)
    assert result["name"] == "my-mysql"
    assert "credentials" not in result  # not included by default

    # Get without credentials
    fetched = storage.get_connection("conn-test1")
    assert fetched is not None
    assert fetched["name"] == "my-mysql"
    assert "credentials" not in fetched

    # Get with credentials
    fetched_full = storage.get_connection("conn-test1", include_credentials=True)
    assert fetched_full["credentials"]["password"] == "secret"


def test_get_connection_by_name(storage):
    storage.save_connection({
        "id": "conn-byname",
        "name": "prod-pg",
        "type": "postgres",
        "credentials": {"host": "pg.example.com", "password": "pw"},
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    result = storage.get_connection_by_name("prod-pg", include_credentials=True)
    assert result is not None
    assert result["credentials"]["host"] == "pg.example.com"

    assert storage.get_connection_by_name("nonexistent") is None


def test_list_connections(storage):
    for i in range(3):
        storage.save_connection({
            "id": f"conn-list-{i}",
            "name": f"conn-{i}",
            "type": "mysql",
            "credentials": {"host": f"host-{i}"},
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    conns = storage.list_connections()
    assert len(conns) == 3
    for c in conns:
        assert "credentials" not in c


def test_update_connection(storage):
    storage.save_connection({
        "id": "conn-upd",
        "name": "upd-conn",
        "type": "mysql",
        "credentials": {"host": "old", "password": "old-pw"},
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    result = storage.update_connection("conn-upd", {
        "description": "Updated",
        "credentials": {"host": "new", "password": "new-pw"},
    })
    assert result["description"] == "Updated"
    assert result["updated_at"] is not None

    # Verify credentials were re-encrypted
    full = storage.get_connection("conn-upd", include_credentials=True)
    assert full["credentials"]["password"] == "new-pw"


def test_delete_connection(storage):
    storage.save_connection({
        "id": "conn-del",
        "name": "del-conn",
        "type": "postgres",
        "credentials": {"host": "h"},
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    assert storage.delete_connection("conn-del") is True
    assert storage.get_connection("conn-del") is None
    assert storage.delete_connection("conn-del") is False


def test_list_pipelines_using_connection(storage):
    storage.save_pipeline({
        "id": "pipe-conn1",
        "name": "Uses Connection",
        "source": {"type": "mysql", "connection": "my-conn"},
        "destination": {"type": "s3", "bucket": "b", "path": "p/"},
        "options": {},
        "schedule": {},
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    storage.save_pipeline({
        "id": "pipe-noconn",
        "name": "No Connection",
        "source": {"type": "csv_url", "url": "http://example.com"},
        "destination": {"type": "s3", "bucket": "b", "path": "p/"},
        "options": {},
        "schedule": {},
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    using = storage.list_pipelines_using_connection("my-conn")
    assert len(using) == 1
    assert using[0]["name"] == "Uses Connection"


# ------------------------------------------------------------------
# Ontology Entity CRUD
# ------------------------------------------------------------------


def test_save_and_get_entity(storage):
    entity = {
        "id": "ent-test1",
        "name": "orders",
        "display_name": "Orders",
        "description": "Order records",
        "pipeline_id": "pipe-orders",
        "column_annotations": {"id": {"role": "primary_key"}},
        "status": "active",
    }
    result = storage.save_entity(entity)
    assert result["name"] == "orders"
    assert result["column_annotations"]["id"]["role"] == "primary_key"

    fetched = storage.get_entity("ent-test1")
    assert fetched is not None
    assert fetched["display_name"] == "Orders"


def test_get_entity_by_name(storage):
    storage.save_entity({
        "id": "ent-byname",
        "name": "customers",
        "display_name": "Customers",
        "pipeline_id": "pipe-cust",
    })
    result = storage.get_entity_by_name("customers")
    assert result is not None
    assert result["id"] == "ent-byname"
    assert storage.get_entity_by_name("nonexistent") is None


def test_list_entities(storage):
    for i in range(3):
        storage.save_entity({
            "id": f"ent-list-{i}",
            "name": f"entity_{i}",
            "display_name": f"Entity {i}",
            "pipeline_id": f"pipe-{i}",
            "status": "active" if i < 2 else "proposed",
        })
    all_ents = storage.list_entities()
    assert len(all_ents) == 3
    active = storage.list_entities(status="active")
    assert len(active) == 2


def test_update_entity(storage):
    storage.save_entity({
        "id": "ent-upd",
        "name": "to_update",
        "display_name": "Old Name",
        "pipeline_id": "pipe-x",
    })
    result = storage.update_entity("ent-upd", {"display_name": "New Name"})
    assert result["display_name"] == "New Name"
    assert result["updated_at"] is not None


def test_delete_entity(storage):
    storage.save_entity({
        "id": "ent-del",
        "name": "to_delete",
        "display_name": "Delete Me",
        "pipeline_id": "pipe-d",
    })
    assert storage.delete_entity("ent-del") is True
    assert storage.get_entity("ent-del") is None
    assert storage.delete_entity("ent-del") is False


def test_get_entity_for_pipeline(storage):
    storage.save_entity({
        "id": "ent-pipe",
        "name": "orders",
        "display_name": "Orders",
        "pipeline_id": "pipe-abc",
    })
    result = storage.get_entity_for_pipeline("pipe-abc")
    assert result["name"] == "orders"
    assert storage.get_entity_for_pipeline("pipe-nonexistent") is None


# ------------------------------------------------------------------
# Ontology Relationship CRUD
# ------------------------------------------------------------------


def test_save_and_get_relationship(storage):
    rel = {
        "id": "rel-test1",
        "name": "orders_to_customers",
        "from_entity": "orders",
        "to_entity": "customers",
        "from_column": "customer_id",
        "to_column": "id",
        "relationship_type": "many_to_one",
    }
    result = storage.save_relationship(rel)
    assert result["name"] == "orders_to_customers"

    fetched = storage.get_relationship("rel-test1")
    assert fetched["from_entity"] == "orders"


def test_list_relationships(storage):
    storage.save_relationship({
        "id": "rel-1",
        "name": "r1",
        "from_entity": "orders",
        "to_entity": "customers",
        "from_column": "customer_id",
        "to_column": "id",
    })
    storage.save_relationship({
        "id": "rel-2",
        "name": "r2",
        "from_entity": "orders",
        "to_entity": "products",
        "from_column": "product_id",
        "to_column": "id",
    })
    all_rels = storage.list_relationships()
    assert len(all_rels) == 2
    orders_rels = storage.list_relationships(entity_name="orders")
    assert len(orders_rels) == 2
    customer_rels = storage.list_relationships(entity_name="customers")
    assert len(customer_rels) == 1


def test_delete_relationship(storage):
    storage.save_relationship({
        "id": "rel-del",
        "name": "rdel",
        "from_entity": "a",
        "to_entity": "b",
        "from_column": "a_id",
        "to_column": "id",
    })
    assert storage.delete_relationship("rel-del") is True
    assert storage.get_relationship("rel-del") is None


# ------------------------------------------------------------------
# Ontology Metric CRUD
# ------------------------------------------------------------------


def test_save_and_get_metric(storage):
    metric = {
        "id": "met-test1",
        "name": "revenue",
        "display_name": "Revenue",
        "entity_name": "orders",
        "expression": "SUM(orders.total)",
        "format_type": "currency",
    }
    result = storage.save_metric(metric)
    assert result["name"] == "revenue"

    fetched = storage.get_metric("met-test1")
    assert fetched["expression"] == "SUM(orders.total)"


def test_get_metric_by_name(storage):
    storage.save_metric({
        "id": "met-byname",
        "name": "order_count",
        "display_name": "Order Count",
        "entity_name": "orders",
        "expression": "COUNT(*)",
    })
    result = storage.get_metric_by_name("order_count")
    assert result is not None
    assert storage.get_metric_by_name("nonexistent") is None


def test_list_metrics(storage):
    storage.save_metric({
        "id": "met-1",
        "name": "m1",
        "display_name": "M1",
        "entity_name": "orders",
        "expression": "SUM(orders.x)",
    })
    storage.save_metric({
        "id": "met-2",
        "name": "m2",
        "display_name": "M2",
        "entity_name": "customers",
        "expression": "COUNT(*)",
    })
    all_mets = storage.list_metrics()
    assert len(all_mets) == 2
    order_mets = storage.list_metrics(entity_name="orders")
    assert len(order_mets) == 1


def test_update_metric(storage):
    storage.save_metric({
        "id": "met-upd",
        "name": "mupd",
        "display_name": "Old",
        "entity_name": "orders",
        "expression": "SUM(orders.x)",
    })
    result = storage.update_metric("met-upd", {"display_name": "New", "expression": "AVG(orders.x)"})
    assert result["display_name"] == "New"
    assert result["expression"] == "AVG(orders.x)"


def test_delete_metric(storage):
    storage.save_metric({
        "id": "met-del",
        "name": "mdel",
        "display_name": "Del",
        "entity_name": "orders",
        "expression": "SUM(orders.x)",
    })
    assert storage.delete_metric("met-del") is True
    assert storage.get_metric("met-del") is None


# ------------------------------------------------------------------
# Ontology Dimension CRUD
# ------------------------------------------------------------------


def test_save_and_get_dimension(storage):
    dim = {
        "id": "dim-test1",
        "name": "customer_segment",
        "display_name": "Customer Segment",
        "entity_name": "customers",
        "expression": "customers.segment",
        "dimension_type": "direct",
    }
    result = storage.save_dimension(dim)
    assert result["name"] == "customer_segment"

    fetched = storage.get_dimension("dim-test1")
    assert fetched["expression"] == "customers.segment"


def test_get_dimension_by_name(storage):
    storage.save_dimension({
        "id": "dim-byname",
        "name": "order_month",
        "display_name": "Order Month",
        "entity_name": "orders",
        "expression": "DATE_TRUNC('month', orders.created_at)",
        "dimension_type": "derived",
    })
    result = storage.get_dimension_by_name("order_month")
    assert result is not None
    assert storage.get_dimension_by_name("nonexistent") is None


def test_list_dimensions(storage):
    storage.save_dimension({
        "id": "dim-1",
        "name": "d1",
        "display_name": "D1",
        "entity_name": "orders",
        "expression": "orders.status",
    })
    storage.save_dimension({
        "id": "dim-2",
        "name": "d2",
        "display_name": "D2",
        "entity_name": "customers",
        "expression": "customers.region",
    })
    all_dims = storage.list_dimensions()
    assert len(all_dims) == 2
    order_dims = storage.list_dimensions(entity_name="orders")
    assert len(order_dims) == 1


def test_update_dimension(storage):
    storage.save_dimension({
        "id": "dim-upd",
        "name": "dupd",
        "display_name": "Old",
        "entity_name": "orders",
        "expression": "orders.x",
    })
    result = storage.update_dimension("dim-upd", {"display_name": "New"})
    assert result["display_name"] == "New"


def test_delete_dimension(storage):
    storage.save_dimension({
        "id": "dim-del",
        "name": "ddel",
        "display_name": "Del",
        "entity_name": "orders",
        "expression": "orders.x",
    })
    assert storage.delete_dimension("dim-del") is True
    assert storage.get_dimension("dim-del") is None


# ------------------------------------------------------------------
# Ontology Proposal CRUD
# ------------------------------------------------------------------


def test_save_and_get_proposal(storage):
    proposal = {
        "id": "prop-test1",
        "proposal_type": "entity",
        "payload": {"name": "orders", "display_name": "Orders", "pipeline_id": "pipe-1"},
        "source_pipeline_id": "pipe-1",
        "proposed_by": "ai",
        "status": "pending",
    }
    result = storage.save_proposal(proposal)
    assert result["proposal_type"] == "entity"
    assert result["payload"]["name"] == "orders"

    fetched = storage.get_proposal("prop-test1")
    assert fetched is not None
    assert fetched["status"] == "pending"


def test_list_proposals(storage):
    storage.save_proposal({
        "id": "prop-1",
        "proposal_type": "entity",
        "payload": {"name": "e1"},
        "status": "pending",
    })
    storage.save_proposal({
        "id": "prop-2",
        "proposal_type": "metric",
        "payload": {"name": "m1"},
        "status": "approved",
    })
    all_props = storage.list_proposals()
    assert len(all_props) == 2
    pending = storage.list_proposals(status="pending")
    assert len(pending) == 1
    metrics = storage.list_proposals(proposal_type="metric")
    assert len(metrics) == 1


def test_update_proposal(storage):
    storage.save_proposal({
        "id": "prop-upd",
        "proposal_type": "entity",
        "payload": {"name": "e1"},
        "status": "pending",
    })
    result = storage.update_proposal("prop-upd", {"status": "approved", "reviewed_by": "admin"})
    assert result["status"] == "approved"
    assert result["reviewed_by"] == "admin"


# ------------------------------------------------------------------
# Ontology Snapshot
# ------------------------------------------------------------------


def test_ontology_snapshot(storage):
    storage.save_entity({
        "id": "ent-snap",
        "name": "orders",
        "display_name": "Orders",
        "pipeline_id": "pipe-1",
        "status": "active",
    })
    storage.save_entity({
        "id": "ent-snap2",
        "name": "proposed_thing",
        "display_name": "Proposed",
        "pipeline_id": "pipe-2",
        "status": "proposed",
    })
    storage.save_metric({
        "id": "met-snap",
        "name": "revenue",
        "display_name": "Revenue",
        "entity_name": "orders",
        "expression": "SUM(orders.total)",
        "status": "active",
    })
    snapshot = storage.get_ontology_snapshot()
    assert len(snapshot["entities"]) == 1  # only active
    assert snapshot["entities"][0]["name"] == "orders"
    assert len(snapshot["metrics"]) == 1
    assert len(snapshot["relationships"]) == 0
    assert len(snapshot["dimensions"]) == 0
