"""Tests for OntologyManager"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.ontology import OntologyManager
from src.storage import Storage


@pytest.fixture
def storage():
    return Storage()


@pytest.fixture
def pipeline_with_metadata(storage):
    """Create a pipeline with metadata for testing proposals."""
    storage.save_pipeline({
        "id": "pipe-sales",
        "name": "Sales Data",
        "description": "Sales transaction records",
        "source": {"type": "csv_url", "url": "http://example.com/sales.csv"},
        "destination": {"type": "s3", "bucket": "b", "path": "p/"},
        "options": {},
        "schedule": {},
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    storage.save_metadata("pipe-sales", {
        "pipeline_name": "Sales Data",
        "columns": [
            {"name": "id", "type": "int64", "semantic_type": "identifier", "unique_values": 1000, "null_percentage": 0},
            {"name": "customer_id", "type": "int64", "semantic_type": "identifier", "unique_values": 200, "null_percentage": 0},
            {"name": "total", "type": "float64", "semantic_type": "currency", "unique_values": 500, "null_percentage": 0},
            {"name": "quantity", "type": "int64", "semantic_type": "numeric", "unique_values": 50, "null_percentage": 0},
            {"name": "status", "type": "object", "semantic_type": "category", "unique_values": 5, "null_percentage": 0},
            {"name": "created_at", "type": "datetime64", "semantic_type": "datetime", "unique_values": 900, "null_percentage": 0},
            {"name": "region", "type": "object", "semantic_type": "text", "unique_values": 10, "null_percentage": 2},
        ],
    })
    return storage


def test_heuristic_propose_entity(pipeline_with_metadata):
    manager = OntologyManager(pipeline_with_metadata)
    proposals = manager.propose_for_pipeline("pipe-sales", include_relationships=False, include_metrics=False)

    # Should have at least an entity proposal
    entity_proposals = [p for p in proposals if p["proposal_type"] == "entity"]
    assert len(entity_proposals) == 1
    payload = entity_proposals[0]["payload"]
    assert payload["name"] == "sales_data"
    assert payload["pipeline_id"] == "pipe-sales"


def test_heuristic_propose_relationships(pipeline_with_metadata):
    # Create an existing "customers" entity for relationship detection
    pipeline_with_metadata.save_entity({
        "id": "ent-cust",
        "name": "customers",
        "display_name": "Customers",
        "pipeline_id": "pipe-cust",
        "status": "active",
    })

    manager = OntologyManager(pipeline_with_metadata)
    proposals = manager.propose_for_pipeline("pipe-sales", include_relationships=True, include_metrics=False)

    rel_proposals = [p for p in proposals if p["proposal_type"] == "relationship"]
    assert len(rel_proposals) >= 1
    rel_payload = rel_proposals[0]["payload"]
    assert rel_payload["from_entity"] == "sales_data"
    assert rel_payload["to_entity"] == "customers"
    assert rel_payload["from_column"] == "customer_id"
    assert rel_payload["to_column"] == "id"


def test_heuristic_propose_metrics(pipeline_with_metadata):
    manager = OntologyManager(pipeline_with_metadata)
    proposals = manager.propose_for_pipeline("pipe-sales", include_relationships=False, include_metrics=True)

    met_proposals = [p for p in proposals if p["proposal_type"] == "metric"]
    metric_names = [p["payload"]["name"] for p in met_proposals]

    # Should propose SUM/AVG for numeric columns (total, quantity)
    assert "total_total" in metric_names
    assert "avg_total" in metric_names
    assert "total_quantity" in metric_names
    assert "avg_quantity" in metric_names
    assert "sales_data_count" in metric_names

    # Check currency format for total
    total_metric = next(p for p in met_proposals if p["payload"]["name"] == "total_total")
    assert total_metric["payload"]["format_type"] == "currency"


def test_heuristic_propose_dimensions(pipeline_with_metadata):
    manager = OntologyManager(pipeline_with_metadata)
    proposals = manager.propose_for_pipeline("pipe-sales", include_relationships=False, include_metrics=True)

    dim_proposals = [p for p in proposals if p["proposal_type"] == "dimension"]
    dim_names = [p["payload"]["name"] for p in dim_proposals]

    # Should propose datetime-derived dimension
    assert "created_at_month" in dim_names

    # Should propose low-cardinality text dimensions
    assert "status" in dim_names
    assert "region" in dim_names


def test_auto_approve_dev_mode(pipeline_with_metadata):
    """In dev mode (require_auth=false), proposals are auto-approved and materialized."""
    manager = OntologyManager(pipeline_with_metadata)
    proposals = manager.propose_for_pipeline("pipe-sales", include_relationships=False, include_metrics=False)

    # All proposals should be approved (require_auth is false in tests)
    for p in proposals:
        assert p["status"] == "approved"

    # Entity should have been materialized
    entity = pipeline_with_metadata.get_entity_by_name("sales_data")
    assert entity is not None
    assert entity["status"] == "active"


def test_approve_proposal(storage):
    storage.save_pipeline({
        "id": "pipe-test",
        "name": "Test",
        "source": {},
        "destination": {},
        "options": {},
        "schedule": {},
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    # Create a pending proposal manually
    storage.save_proposal({
        "id": "prop-1",
        "proposal_type": "entity",
        "payload": {
            "name": "test_entity",
            "display_name": "Test Entity",
            "pipeline_id": "pipe-test",
            "status": "active",
        },
        "status": "pending",
        "proposed_by": "ai",
    })

    manager = OntologyManager(storage)
    result = manager.approve_proposal("prop-1")
    assert result["status"] == "approved"
    assert result["created"]["name"] == "test_entity"

    # Verify entity was created
    entity = storage.get_entity_by_name("test_entity")
    assert entity is not None


def test_reject_proposal(storage):
    storage.save_proposal({
        "id": "prop-rej",
        "proposal_type": "metric",
        "payload": {"name": "bad_metric"},
        "status": "pending",
    })

    manager = OntologyManager(storage)
    result = manager.reject_proposal("prop-rej", notes="Not useful")
    assert result["status"] == "rejected"

    proposal = storage.get_proposal("prop-rej")
    assert proposal["status"] == "rejected"
    assert proposal["review_notes"] == "Not useful"


def test_approve_non_pending_fails(storage):
    storage.save_proposal({
        "id": "prop-done",
        "proposal_type": "entity",
        "payload": {"name": "x"},
        "status": "approved",
    })

    manager = OntologyManager(storage)
    with pytest.raises(ValueError, match="not pending"):
        manager.approve_proposal("prop-done")


def test_propose_nonexistent_pipeline(storage):
    manager = OntologyManager(storage)
    with pytest.raises(ValueError, match="Pipeline not found"):
        manager.propose_for_pipeline("pipe-nonexistent")


def test_normalize_entity_name():
    assert OntologyManager._normalize_entity_name("Sales Data") == "sales_data"
    assert OntologyManager._normalize_entity_name("My-Pipeline-V2") == "my_pipeline_v2"
    assert OntologyManager._normalize_entity_name("  Test  ") == "test"


@patch("src.ontology.get_settings")
def test_ai_propose_fallback_on_error(mock_settings, pipeline_with_metadata):
    """When AI fails, falls back to heuristics."""
    settings = MagicMock()
    settings.anthropic_api_key = "sk-test"
    settings.require_auth = False
    mock_settings.return_value = settings

    with patch("src.ontology.OntologyManager._ai_propose", side_effect=Exception("AI failed")):
        manager = OntologyManager(pipeline_with_metadata)
        # The _ai_propose will be called but will raise, so heuristic fallback won't happen
        # through the normal path. Let's test the _ai_propose method directly instead.

    # Test that _ai_propose catches errors and falls back
    manager = OntologyManager(pipeline_with_metadata)
    pipeline = pipeline_with_metadata.get_pipeline("pipe-sales")
    metadata = pipeline_with_metadata.get_metadata("pipe-sales")

    with patch("anthropic.Anthropic") as mock_anthropic:
        mock_anthropic.side_effect = Exception("Connection failed")
        result = manager._ai_propose(pipeline, metadata, [], True, True)
        # Should fall back to heuristic results
        assert len(result) > 0
        entity_proposals = [p for p in result if p["type"] == "entity"]
        assert len(entity_proposals) >= 1
