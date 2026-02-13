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
