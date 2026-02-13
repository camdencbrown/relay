"""
Storage layer for Relay
SQLite-backed via SQLAlchemy (v2)
Same public API as v1 (dict in / dict out) so consumers don't break.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

from .database import get_db
from .encryption import encrypt_dict
from .models import ColumnKnowledge, Connection, DatasetMetadata, Pipeline, PipelineRun


class Storage:
    """SQLite-backed pipeline storage."""

    # ------------------------------------------------------------------
    # Pipeline CRUD
    # ------------------------------------------------------------------

    def save_pipeline(self, pipeline: Dict) -> None:
        with get_db() as db:
            row = Pipeline(
                id=pipeline["id"],
                name=pipeline["name"],
                description=pipeline.get("description", ""),
                type=pipeline.get("type", "regular"),
                status=pipeline.get("status", "active"),
                created_at=pipeline.get("created_at", datetime.now(timezone.utc).isoformat()),
                last_scheduled_run=pipeline.get("last_scheduled_run"),
            )
            if row.type == "transformation":
                row.config = pipeline.get("config", {})
            else:
                row.source = pipeline.get("source", {})
                row.destination = pipeline.get("destination", {})
                row.options = pipeline.get("options", {})
                row.schedule = pipeline.get("schedule", {})
            db.add(row)

    def update_pipeline(self, pipeline_id: str, updates: Dict) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
            if not row:
                return None
            for key, value in updates.items():
                if key == "source":
                    row.source = value
                elif key == "destination":
                    row.destination = value
                elif key == "options":
                    row.options = value
                elif key == "schedule":
                    row.schedule = value
                elif key == "config":
                    row.config = value
                elif hasattr(row, key):
                    setattr(row, key, value)
            return self.get_pipeline(pipeline_id)

    def get_pipeline(self, pipeline_id: str) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
            if not row:
                return None
            d = row.to_dict()
            # Attach runs
            runs = (
                db.query(PipelineRun)
                .filter(PipelineRun.pipeline_id == pipeline_id)
                .order_by(PipelineRun.id)
                .all()
            )
            d["runs"] = [r.to_dict() for r in runs]
            return d

    def list_pipelines(self) -> List[Dict]:
        with get_db() as db:
            rows = db.query(Pipeline).order_by(Pipeline.created_at).all()
            result = []
            for row in rows:
                d = row.to_dict()
                runs = (
                    db.query(PipelineRun)
                    .filter(PipelineRun.pipeline_id == row.id)
                    .order_by(PipelineRun.id)
                    .all()
                )
                d["runs"] = [r.to_dict() for r in runs]
                result.append(d)
            return result

    def delete_pipeline(self, pipeline_id: str) -> bool:
        with get_db() as db:
            db.query(PipelineRun).filter(PipelineRun.pipeline_id == pipeline_id).delete()
            deleted = db.query(Pipeline).filter(Pipeline.id == pipeline_id).delete()
            return deleted > 0

    # ------------------------------------------------------------------
    # Run tracking
    # ------------------------------------------------------------------

    def add_run(self, pipeline_id: str, run: Dict) -> None:
        with get_db() as db:
            row = PipelineRun(
                run_id=run["run_id"],
                pipeline_id=pipeline_id,
                status=run.get("status", "running"),
                started_at=run.get("started_at", datetime.now(timezone.utc).isoformat()),
                progress=run.get("progress", "Starting..."),
                streaming=run.get("streaming", False),
            )
            db.add(row)

    def update_run(self, pipeline_id: str, run_id: str, updates: Dict) -> Optional[Dict]:
        with get_db() as db:
            row = (
                db.query(PipelineRun)
                .filter(PipelineRun.pipeline_id == pipeline_id, PipelineRun.run_id == run_id)
                .first()
            )
            if not row:
                return None
            for key, value in updates.items():
                if key == "files_written":
                    row.files_written = value
                elif key in ("run_id", "pipeline_id"):
                    continue  # skip primary identifiers
                elif hasattr(row, key):
                    setattr(row, key, value)
            return row.to_dict()

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def get_metadata(self, pipeline_id: str) -> Optional[Dict]:
        with get_db() as db:
            row = (
                db.query(DatasetMetadata)
                .filter(DatasetMetadata.pipeline_id == pipeline_id)
                .first()
            )
            return row.content if row else None

    def save_metadata(self, pipeline_id: str, metadata: Dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as db:
            row = (
                db.query(DatasetMetadata)
                .filter(DatasetMetadata.pipeline_id == pipeline_id)
                .first()
            )
            if row:
                row.content = metadata
                row.updated_at = now
            else:
                row = DatasetMetadata(
                    pipeline_id=pipeline_id,
                    created_at=now,
                )
                row.content = metadata
                db.add(row)

    # ------------------------------------------------------------------
    # Column Knowledge Base
    # ------------------------------------------------------------------

    def get_column_knowledge(self, column_key: str) -> Optional[Dict]:
        with get_db() as db:
            row = (
                db.query(ColumnKnowledge)
                .filter(ColumnKnowledge.column_key == column_key)
                .first()
            )
            if not row:
                return None
            return {
                "description": row.description,
                "business_meaning": row.business_meaning,
                "verified_by": row.verified_by,
                "verified_at": row.verified_at,
            }

    def save_column_knowledge(
        self,
        column_key: str,
        description: str,
        business_meaning: str = None,
        verified_by: str = "user",
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as db:
            row = (
                db.query(ColumnKnowledge)
                .filter(ColumnKnowledge.column_key == column_key)
                .first()
            )
            if row:
                row.description = description
                row.business_meaning = business_meaning
                row.verified_by = verified_by
                row.verified_at = now
            else:
                row = ColumnKnowledge(
                    column_key=column_key,
                    description=description,
                    business_meaning=business_meaning,
                    verified_by=verified_by,
                    verified_at=now,
                )
                db.add(row)

    def list_column_knowledge(self) -> Dict[str, Dict]:
        with get_db() as db:
            rows = db.query(ColumnKnowledge).all()
            return {
                row.column_key: {
                    "description": row.description,
                    "business_meaning": row.business_meaning,
                    "verified_by": row.verified_by,
                    "verified_at": row.verified_at,
                }
                for row in rows
            }

    # ------------------------------------------------------------------
    # Connection CRUD
    # ------------------------------------------------------------------

    def save_connection(self, connection: Dict) -> Dict:
        with get_db() as db:
            row = Connection(
                id=connection["id"],
                name=connection["name"],
                type=connection["type"],
                description=connection.get("description", ""),
                credentials_encrypted=encrypt_dict(connection["credentials"]),
                created_at=connection.get("created_at", datetime.now(timezone.utc).isoformat()),
            )
            db.add(row)
            return row.to_dict()

    def get_connection(self, connection_id: str, include_credentials: bool = False) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(Connection).filter(Connection.id == connection_id).first()
            if not row:
                return None
            return row.to_dict(include_credentials=include_credentials)

    def get_connection_by_name(self, name: str, include_credentials: bool = False) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(Connection).filter(Connection.name == name).first()
            if not row:
                return None
            return row.to_dict(include_credentials=include_credentials)

    def list_connections(self) -> List[Dict]:
        with get_db() as db:
            rows = db.query(Connection).order_by(Connection.created_at).all()
            return [row.to_dict() for row in rows]

    def update_connection(self, connection_id: str, updates: Dict) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(Connection).filter(Connection.id == connection_id).first()
            if not row:
                return None
            if "credentials" in updates:
                row.credentials_encrypted = encrypt_dict(updates.pop("credentials"))
            if "description" in updates:
                row.description = updates["description"]
            if "last_tested_at" in updates:
                row.last_tested_at = updates["last_tested_at"]
            if "last_test_status" in updates:
                row.last_test_status = updates["last_test_status"]
            row.updated_at = datetime.now(timezone.utc).isoformat()
            return row.to_dict()

    def delete_connection(self, connection_id: str) -> bool:
        with get_db() as db:
            deleted = db.query(Connection).filter(Connection.id == connection_id).delete()
            return deleted > 0

    def list_pipelines_using_connection(self, connection_name: str) -> List[Dict]:
        with get_db() as db:
            rows = db.query(Pipeline).all()
            result = []
            for row in rows:
                source = row.source
                if source.get("connection") == connection_name:
                    result.append(row.to_dict())
            return result
