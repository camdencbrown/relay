"""Tests for SemanticQueryEngine"""

from unittest.mock import MagicMock, patch

import pytest

from src.semantic_query import SemanticQueryEngine
from src.storage import Storage


@pytest.fixture
def storage():
    return Storage()


@pytest.fixture
def setup_ontology(storage):
    """Set up a complete ontology with two entities, a relationship, metrics, and dimensions."""
    # Create pipelines for the entities
    from datetime import datetime, timezone

    storage.save_pipeline({
        "id": "pipe-orders",
        "name": "Orders Data",
        "source": {"type": "csv_url"},
        "destination": {"type": "s3", "bucket": "b", "path": "p/"},
        "options": {},
        "schedule": {},
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    storage.save_pipeline({
        "id": "pipe-customers",
        "name": "Customers Data",
        "source": {"type": "csv_url"},
        "destination": {"type": "s3", "bucket": "b", "path": "p/"},
        "options": {},
        "schedule": {},
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    # Entities
    storage.save_entity({
        "id": "ent-orders",
        "name": "orders",
        "display_name": "Orders",
        "pipeline_id": "pipe-orders",
        "status": "active",
    })
    storage.save_entity({
        "id": "ent-customers",
        "name": "customers",
        "display_name": "Customers",
        "pipeline_id": "pipe-customers",
        "status": "active",
    })

    # Relationship
    storage.save_relationship({
        "id": "rel-o2c",
        "name": "orders_to_customers",
        "from_entity": "orders",
        "to_entity": "customers",
        "from_column": "customer_id",
        "to_column": "id",
        "relationship_type": "many_to_one",
        "status": "active",
    })

    # Metrics
    storage.save_metric({
        "id": "met-revenue",
        "name": "revenue",
        "display_name": "Revenue",
        "entity_name": "orders",
        "expression": "SUM(orders.total)",
        "format_type": "currency",
        "status": "active",
    })
    storage.save_metric({
        "id": "met-count",
        "name": "order_count",
        "display_name": "Order Count",
        "entity_name": "orders",
        "expression": "COUNT(*)",
        "format_type": "number",
        "status": "active",
    })
    storage.save_metric({
        "id": "met-aov",
        "name": "avg_order_value",
        "display_name": "Avg Order Value",
        "entity_name": "orders",
        "expression": "${revenue} / NULLIF(${order_count}, 0)",
        "format_type": "currency",
        "status": "active",
    })

    # Dimensions
    storage.save_dimension({
        "id": "dim-segment",
        "name": "customer_segment",
        "display_name": "Customer Segment",
        "entity_name": "customers",
        "expression": "customers.segment",
        "dimension_type": "direct",
        "status": "active",
    })
    storage.save_dimension({
        "id": "dim-month",
        "name": "order_month",
        "display_name": "Order Month",
        "entity_name": "orders",
        "expression": "DATE_TRUNC('month', orders.created_at)",
        "dimension_type": "derived",
        "status": "active",
    })

    return storage


def test_resolve_metric(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    ontology = setup_ontology.get_ontology_snapshot()
    metrics_by_name = {m["name"]: m for m in ontology["metrics"]}

    expr, entity = engine._resolve_metric("revenue", metrics_by_name)
    assert expr == "SUM(orders.total)"
    assert entity == "orders"


def test_resolve_metric_composable(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    ontology = setup_ontology.get_ontology_snapshot()
    metrics_by_name = {m["name"]: m for m in ontology["metrics"]}

    expr, entity = engine._resolve_metric("avg_order_value", metrics_by_name)
    assert "SUM(orders.total)" in expr
    assert "COUNT(*)" in expr
    assert entity == "orders"


def test_resolve_metric_cycle_detection(storage, setup_ontology):
    # Add a circular metric
    setup_ontology.save_metric({
        "id": "met-circ-a",
        "name": "circular_a",
        "display_name": "Circular A",
        "entity_name": "orders",
        "expression": "${circular_b}",
        "status": "active",
    })
    setup_ontology.save_metric({
        "id": "met-circ-b",
        "name": "circular_b",
        "display_name": "Circular B",
        "entity_name": "orders",
        "expression": "${circular_a}",
        "status": "active",
    })

    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    ontology = setup_ontology.get_ontology_snapshot()
    metrics_by_name = {m["name"]: m for m in ontology["metrics"]}

    with pytest.raises(ValueError, match="Circular metric reference"):
        engine._resolve_metric("circular_a", metrics_by_name)


def test_resolve_dimension(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    ontology = setup_ontology.get_ontology_snapshot()
    dimensions_by_name = {d["name"]: d for d in ontology["dimensions"]}

    expr, entity = engine._resolve_dimension("customer_segment", dimensions_by_name)
    assert expr == "customers.segment"
    assert entity == "customers"


def test_resolve_unknown_metric(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    ontology = setup_ontology.get_ontology_snapshot()
    metrics_by_name = {m["name"]: m for m in ontology["metrics"]}

    with pytest.raises(ValueError, match="Unknown metric"):
        engine._resolve_metric("nonexistent", metrics_by_name)


def test_resolve_unknown_dimension(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    ontology = setup_ontology.get_ontology_snapshot()
    dimensions_by_name = {d["name"]: d for d in ontology["dimensions"]}

    with pytest.raises(ValueError, match="Unknown dimension"):
        engine._resolve_dimension("nonexistent", dimensions_by_name)


def test_build_entity_table_map(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    ontology = setup_ontology.get_ontology_snapshot()
    entities_by_name = {e["name"]: e for e in ontology["entities"]}

    mapping = engine._build_entity_table_map({"orders", "customers"}, entities_by_name)
    assert mapping["orders"] == "orders_data"
    assert mapping["customers"] == "customers_data"


def test_substitute_aliases(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    entity_table_map = {"orders": "orders_data", "customers": "customers_data"}

    result = engine._substitute_aliases("SUM(orders.total)", entity_table_map)
    assert result == "SUM(orders_data.total)"

    result = engine._substitute_aliases("customers.segment", entity_table_map)
    assert result == "customers_data.segment"


def test_build_join_graph_single_entity(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    entity_table_map = {"orders": "orders_data"}

    result = engine._build_join_graph({"orders"}, [], entity_table_map, {})
    assert result == "orders_data"


def test_build_join_graph_two_entities(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    ontology = setup_ontology.get_ontology_snapshot()
    entities_by_name = {e["name"]: e for e in ontology["entities"]}
    entity_table_map = {"orders": "orders_data", "customers": "customers_data"}

    result = engine._build_join_graph(
        {"orders", "customers"},
        ontology["relationships"],
        entity_table_map,
        entities_by_name,
    )
    assert "orders_data" in result
    assert "customers_data" in result
    assert "LEFT JOIN" in result
    assert "customer_id" in result


def test_structured_query_sql_generation(storage, setup_ontology):
    """Test that _resolve_structured generates correct SQL (mocking execute_query)."""
    mock_qe = MagicMock()
    mock_qe.execute_query.return_value = {
        "rows": [{"customer_segment": "enterprise", "revenue": 50000}],
        "columns": ["customer_segment", "revenue"],
        "row_count": 1,
        "execution_time_ms": 10.0,
        "pipelines_used": {"pipe-orders": "orders_data", "pipe-customers": "customers_data"},
        "query_executed": "",
    }

    engine = SemanticQueryEngine(setup_ontology, mock_qe)
    result = engine._resolve_structured(
        metrics=["revenue"],
        dimensions=["customer_segment"],
        filters=[],
        order_by=[],
        limit=100,
    )

    # Verify execute_query was called
    mock_qe.execute_query.assert_called_once()
    call_args = mock_qe.execute_query.call_args

    # Check pipeline IDs were passed
    pipeline_ids = call_args[0][0]
    assert "pipe-orders" in pipeline_ids
    assert "pipe-customers" in pipeline_ids

    # Check generated SQL
    sql = call_args[0][1]
    assert "SUM(orders_data.total) AS revenue" in sql
    assert "customers_data.segment AS customer_segment" in sql
    assert "GROUP BY" in sql
    assert "LEFT JOIN" in sql
    assert "LIMIT 100" in sql

    assert "generated_sql" in result
    assert "entities_used" in result


def test_structured_query_with_filters(storage, setup_ontology):
    mock_qe = MagicMock()
    mock_qe.execute_query.return_value = {
        "rows": [],
        "columns": [],
        "row_count": 0,
        "execution_time_ms": 5.0,
        "pipelines_used": {},
        "query_executed": "",
    }

    engine = SemanticQueryEngine(setup_ontology, mock_qe)
    engine._resolve_structured(
        metrics=["revenue"],
        dimensions=[],
        filters=["orders.created_at > '2024-01-01'"],
        order_by=["revenue DESC"],
        limit=50,
    )

    sql = mock_qe.execute_query.call_args[0][1]
    assert "WHERE" in sql
    assert "orders_data.created_at > '2024-01-01'" in sql
    assert "ORDER BY" in sql
    assert "LIMIT 50" in sql


def test_empty_query_raises(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    with pytest.raises(ValueError, match="At least one metric or dimension"):
        engine._resolve_structured([], [], [], [], None)


def test_nl_query_without_api_key(storage, setup_ontology):
    engine = SemanticQueryEngine(setup_ontology, MagicMock())
    with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
        engine.execute({"natural_language": "What's total revenue?"})


def test_parse_json_response():
    # Direct JSON
    result = SemanticQueryEngine._parse_json_response('{"metrics": ["revenue"]}')
    assert result == {"metrics": ["revenue"]}

    # Markdown code block
    result = SemanticQueryEngine._parse_json_response('```json\n{"metrics": ["revenue"]}\n```')
    assert result == {"metrics": ["revenue"]}

    # Invalid
    result = SemanticQueryEngine._parse_json_response("not json at all")
    assert result is None
