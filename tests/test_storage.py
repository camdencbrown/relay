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
