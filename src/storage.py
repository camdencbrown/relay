"""
Storage layer for Relay
SQLite-backed via SQLAlchemy (v2)
Same public API as v1 (dict in / dict out) so consumers don't break.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

from .database import get_db
from .encryption import encrypt_dict
from .models import (
    APIKey,
    ColumnKnowledge,
    Connection,
    DatasetMetadata,
    OntologyDimension,
    OntologyEntity,
    OntologyMetric,
    OntologyProposal,
    OntologyRelationship,
    Pipeline,
    PipelineRun,
    PlatformEvent,
)


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

    # ------------------------------------------------------------------
    # Ontology Entity CRUD
    # ------------------------------------------------------------------

    def save_entity(self, entity: Dict) -> Dict:
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as db:
            row = OntologyEntity(
                id=entity["id"],
                name=entity["name"],
                display_name=entity["display_name"],
                description=entity.get("description", ""),
                pipeline_id=entity["pipeline_id"],
                status=entity.get("status", "active"),
                proposed_by=entity.get("proposed_by", "user"),
                approved_by=entity.get("approved_by"),
                approved_at=entity.get("approved_at"),
                created_at=entity.get("created_at", now),
                updated_at=entity.get("updated_at"),
            )
            row.column_annotations = entity.get("column_annotations", {})
            db.add(row)
            return row.to_dict()

    def get_entity(self, entity_id: str) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyEntity).filter(OntologyEntity.id == entity_id).first()
            return row.to_dict() if row else None

    def get_entity_by_name(self, name: str) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyEntity).filter(OntologyEntity.name == name).first()
            return row.to_dict() if row else None

    def list_entities(self, status: str = None) -> List[Dict]:
        with get_db() as db:
            q = db.query(OntologyEntity)
            if status:
                q = q.filter(OntologyEntity.status == status)
            return [r.to_dict() for r in q.order_by(OntologyEntity.created_at).all()]

    def update_entity(self, entity_id: str, updates: Dict) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyEntity).filter(OntologyEntity.id == entity_id).first()
            if not row:
                return None
            if "column_annotations" in updates:
                row.column_annotations = updates.pop("column_annotations")
            for key, value in updates.items():
                if hasattr(row, key) and key not in ("id", "created_at"):
                    setattr(row, key, value)
            row.updated_at = datetime.now(timezone.utc).isoformat()
            return row.to_dict()

    def delete_entity(self, entity_id: str) -> bool:
        with get_db() as db:
            deleted = db.query(OntologyEntity).filter(OntologyEntity.id == entity_id).delete()
            return deleted > 0

    def get_entity_for_pipeline(self, pipeline_id: str) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyEntity).filter(OntologyEntity.pipeline_id == pipeline_id).first()
            return row.to_dict() if row else None

    # ------------------------------------------------------------------
    # Ontology Relationship CRUD
    # ------------------------------------------------------------------

    def save_relationship(self, rel: Dict) -> Dict:
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as db:
            row = OntologyRelationship(
                id=rel["id"],
                name=rel["name"],
                from_entity=rel["from_entity"],
                to_entity=rel["to_entity"],
                from_column=rel["from_column"],
                to_column=rel["to_column"],
                relationship_type=rel.get("relationship_type", "one_to_many"),
                description=rel.get("description", ""),
                status=rel.get("status", "active"),
                proposed_by=rel.get("proposed_by", "user"),
                created_at=rel.get("created_at", now),
            )
            db.add(row)
            return row.to_dict()

    def get_relationship(self, rel_id: str) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyRelationship).filter(OntologyRelationship.id == rel_id).first()
            return row.to_dict() if row else None

    def list_relationships(self, entity_name: str = None, status: str = None) -> List[Dict]:
        with get_db() as db:
            q = db.query(OntologyRelationship)
            if entity_name:
                q = q.filter(
                    (OntologyRelationship.from_entity == entity_name)
                    | (OntologyRelationship.to_entity == entity_name)
                )
            if status:
                q = q.filter(OntologyRelationship.status == status)
            return [r.to_dict() for r in q.order_by(OntologyRelationship.created_at).all()]

    def update_relationship(self, rel_id: str, updates: Dict) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyRelationship).filter(OntologyRelationship.id == rel_id).first()
            if not row:
                return None
            for key, value in updates.items():
                if hasattr(row, key) and key not in ("id", "created_at"):
                    setattr(row, key, value)
            return row.to_dict()

    def delete_relationship(self, rel_id: str) -> bool:
        with get_db() as db:
            deleted = db.query(OntologyRelationship).filter(OntologyRelationship.id == rel_id).delete()
            return deleted > 0

    # ------------------------------------------------------------------
    # Ontology Metric CRUD
    # ------------------------------------------------------------------

    def save_metric(self, metric: Dict) -> Dict:
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as db:
            row = OntologyMetric(
                id=metric["id"],
                name=metric["name"],
                display_name=metric["display_name"],
                description=metric.get("description", ""),
                entity_name=metric["entity_name"],
                expression=metric["expression"],
                format_type=metric.get("format_type", "number"),
                status=metric.get("status", "active"),
                proposed_by=metric.get("proposed_by", "user"),
                created_at=metric.get("created_at", now),
                updated_at=metric.get("updated_at"),
            )
            db.add(row)
            return row.to_dict()

    def get_metric(self, metric_id: str) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyMetric).filter(OntologyMetric.id == metric_id).first()
            return row.to_dict() if row else None

    def get_metric_by_name(self, name: str) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyMetric).filter(OntologyMetric.name == name).first()
            return row.to_dict() if row else None

    def list_metrics(self, entity_name: str = None, status: str = None) -> List[Dict]:
        with get_db() as db:
            q = db.query(OntologyMetric)
            if entity_name:
                q = q.filter(OntologyMetric.entity_name == entity_name)
            if status:
                q = q.filter(OntologyMetric.status == status)
            return [r.to_dict() for r in q.order_by(OntologyMetric.created_at).all()]

    def update_metric(self, metric_id: str, updates: Dict) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyMetric).filter(OntologyMetric.id == metric_id).first()
            if not row:
                return None
            for key, value in updates.items():
                if hasattr(row, key) and key not in ("id", "created_at"):
                    setattr(row, key, value)
            row.updated_at = datetime.now(timezone.utc).isoformat()
            return row.to_dict()

    def delete_metric(self, metric_id: str) -> bool:
        with get_db() as db:
            deleted = db.query(OntologyMetric).filter(OntologyMetric.id == metric_id).delete()
            return deleted > 0

    # ------------------------------------------------------------------
    # Ontology Dimension CRUD
    # ------------------------------------------------------------------

    def save_dimension(self, dim: Dict) -> Dict:
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as db:
            row = OntologyDimension(
                id=dim["id"],
                name=dim["name"],
                display_name=dim["display_name"],
                description=dim.get("description", ""),
                entity_name=dim["entity_name"],
                expression=dim["expression"],
                dimension_type=dim.get("dimension_type", "direct"),
                status=dim.get("status", "active"),
                proposed_by=dim.get("proposed_by", "user"),
                created_at=dim.get("created_at", now),
                updated_at=dim.get("updated_at"),
            )
            db.add(row)
            return row.to_dict()

    def get_dimension(self, dim_id: str) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyDimension).filter(OntologyDimension.id == dim_id).first()
            return row.to_dict() if row else None

    def get_dimension_by_name(self, name: str) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyDimension).filter(OntologyDimension.name == name).first()
            return row.to_dict() if row else None

    def list_dimensions(self, entity_name: str = None, status: str = None) -> List[Dict]:
        with get_db() as db:
            q = db.query(OntologyDimension)
            if entity_name:
                q = q.filter(OntologyDimension.entity_name == entity_name)
            if status:
                q = q.filter(OntologyDimension.status == status)
            return [r.to_dict() for r in q.order_by(OntologyDimension.created_at).all()]

    def update_dimension(self, dim_id: str, updates: Dict) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyDimension).filter(OntologyDimension.id == dim_id).first()
            if not row:
                return None
            for key, value in updates.items():
                if hasattr(row, key) and key not in ("id", "created_at"):
                    setattr(row, key, value)
            row.updated_at = datetime.now(timezone.utc).isoformat()
            return row.to_dict()

    def delete_dimension(self, dim_id: str) -> bool:
        with get_db() as db:
            deleted = db.query(OntologyDimension).filter(OntologyDimension.id == dim_id).delete()
            return deleted > 0

    # ------------------------------------------------------------------
    # Ontology Proposal CRUD
    # ------------------------------------------------------------------

    def save_proposal(self, proposal: Dict) -> Dict:
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as db:
            row = OntologyProposal(
                id=proposal["id"],
                proposal_type=proposal["proposal_type"],
                source_pipeline_id=proposal.get("source_pipeline_id"),
                proposed_by=proposal.get("proposed_by", "ai"),
                status=proposal.get("status", "pending"),
                reviewed_by=proposal.get("reviewed_by"),
                reviewed_at=proposal.get("reviewed_at"),
                review_notes=proposal.get("review_notes"),
                created_at=proposal.get("created_at", now),
            )
            row.payload = proposal["payload"]
            db.add(row)
            return row.to_dict()

    def get_proposal(self, proposal_id: str) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyProposal).filter(OntologyProposal.id == proposal_id).first()
            return row.to_dict() if row else None

    def list_proposals(self, status: str = None, proposal_type: str = None) -> List[Dict]:
        with get_db() as db:
            q = db.query(OntologyProposal)
            if status:
                q = q.filter(OntologyProposal.status == status)
            if proposal_type:
                q = q.filter(OntologyProposal.proposal_type == proposal_type)
            return [r.to_dict() for r in q.order_by(OntologyProposal.created_at).all()]

    def update_proposal(self, proposal_id: str, updates: Dict) -> Optional[Dict]:
        with get_db() as db:
            row = db.query(OntologyProposal).filter(OntologyProposal.id == proposal_id).first()
            if not row:
                return None
            if "payload" in updates:
                row.payload = updates.pop("payload")
            for key, value in updates.items():
                if hasattr(row, key) and key not in ("id", "created_at"):
                    setattr(row, key, value)
            return row.to_dict()

    # ------------------------------------------------------------------
    # Ontology Snapshot
    # ------------------------------------------------------------------

    def get_ontology_snapshot(self) -> Dict:
        entities = self.list_entities(status="active")
        relationships = self.list_relationships(status="active")

        # Build lineage summary: entity→pipeline mapping and relationship graph
        entity_pipeline_map = {e["name"]: e["pipeline_id"] for e in entities}
        relationship_graph = [
            {
                "from": r["from_entity"],
                "to": r["to_entity"],
                "type": r["relationship_type"],
                "name": r["name"],
            }
            for r in relationships
        ]

        return {
            "entities": entities,
            "relationships": relationships,
            "metrics": self.list_metrics(status="active"),
            "dimensions": self.list_dimensions(status="active"),
            "lineage_summary": {
                "entity_pipeline_map": entity_pipeline_map,
                "relationship_graph": relationship_graph,
            },
        }

    # ------------------------------------------------------------------
    # Platform Analytics
    # ------------------------------------------------------------------

    def record_event(
        self,
        event_type: str,
        pipeline_id: str = None,
        entity_name: str = None,
        run_id: str = None,
        user_key_prefix: str = None,
        metadata: Dict = None,
    ) -> Dict:
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as db:
            row = PlatformEvent(
                event_type=event_type,
                pipeline_id=pipeline_id,
                entity_name=entity_name,
                run_id=run_id,
                user_key_prefix=user_key_prefix,
                created_at=now,
            )
            row.event_metadata = metadata or {}
            db.add(row)
            db.flush()
            return row.to_dict()

    def list_events(
        self, event_type: str = None, pipeline_id: str = None, limit: int = 100
    ) -> List[Dict]:
        with get_db() as db:
            q = db.query(PlatformEvent)
            if event_type:
                q = q.filter(PlatformEvent.event_type == event_type)
            if pipeline_id:
                q = q.filter(PlatformEvent.pipeline_id == pipeline_id)
            rows = q.order_by(PlatformEvent.id.desc()).limit(limit).all()
            return [r.to_dict() for r in rows]

    def get_analytics_summary(self) -> Dict:
        with get_db() as db:
            from sqlalchemy import func

            counts = (
                db.query(PlatformEvent.event_type, func.count(PlatformEvent.id))
                .group_by(PlatformEvent.event_type)
                .all()
            )
            recent = (
                db.query(PlatformEvent)
                .order_by(PlatformEvent.id.desc())
                .limit(50)
                .all()
            )
            return {
                "event_counts": {t: c for t, c in counts},
                "total_events": sum(c for _, c in counts),
                "recent_events": [r.to_dict() for r in recent],
            }

    # ------------------------------------------------------------------
    # API Key Management
    # ------------------------------------------------------------------

    def list_api_keys(self) -> List[Dict]:
        with get_db() as db:
            rows = db.query(APIKey).order_by(APIKey.id).all()
            return [r.to_dict() for r in rows]

    def deactivate_api_key(self, key_id: int) -> bool:
        with get_db() as db:
            row = db.query(APIKey).filter(APIKey.id == key_id).first()
            if not row:
                return False
            row.active = False
            return True
