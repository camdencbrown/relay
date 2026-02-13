"""
SQLAlchemy models for Relay
"""

import json

from sqlalchemy import Boolean, Column, Float, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, default="")
    type = Column(String, default="regular")  # "regular" or "transformation"
    status = Column(String, default="active")
    source_json = Column(Text, default="{}")  # JSON blob
    destination_json = Column(Text, default="{}")  # JSON blob
    options_json = Column(Text, default="{}")  # JSON blob
    schedule_json = Column(Text, default="{}")  # JSON blob
    config_json = Column(Text, default="{}")  # For transformation pipelines
    last_scheduled_run = Column(String, nullable=True)
    created_at = Column(String, nullable=False)

    @property
    def source(self) -> dict:
        return json.loads(self.source_json) if self.source_json else {}

    @source.setter
    def source(self, value: dict):
        self.source_json = json.dumps(value)

    @property
    def destination(self) -> dict:
        return json.loads(self.destination_json) if self.destination_json else {}

    @destination.setter
    def destination(self, value: dict):
        self.destination_json = json.dumps(value)

    @property
    def options(self) -> dict:
        return json.loads(self.options_json) if self.options_json else {}

    @options.setter
    def options(self, value: dict):
        self.options_json = json.dumps(value)

    @property
    def schedule(self) -> dict:
        return json.loads(self.schedule_json) if self.schedule_json else {}

    @schedule.setter
    def schedule(self, value: dict):
        self.schedule_json = json.dumps(value)

    @property
    def config(self) -> dict:
        return json.loads(self.config_json) if self.config_json else {}

    @config.setter
    def config(self, value: dict):
        self.config_json = json.dumps(value)

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "type": self.type,
            "status": self.status,
            "created_at": self.created_at,
            "last_scheduled_run": self.last_scheduled_run,
        }
        if self.type == "transformation":
            d["config"] = self.config
        else:
            d["source"] = self.source
            d["destination"] = self.destination
            d["options"] = self.options
            d["schedule"] = self.schedule
        return d


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, nullable=False, index=True)
    pipeline_id = Column(String, nullable=False, index=True)
    status = Column(String, default="running")
    started_at = Column(String, nullable=False)
    completed_at = Column(String, nullable=True)
    progress = Column(String, default="Starting...")
    streaming = Column(Boolean, default=False)
    rows_processed = Column(Integer, nullable=True)
    chunks_processed = Column(Integer, nullable=True)
    output_file = Column(String, nullable=True)
    files_written_json = Column(Text, default="[]")
    duration_seconds = Column(Float, nullable=True)
    error = Column(Text, nullable=True)
    traceback = Column(Text, nullable=True)
    metadata_generated = Column(Boolean, default=False)
    columns_needing_review = Column(Integer, default=0)
    ai_enhanced = Column(Boolean, default=False)

    @property
    def files_written(self) -> list:
        return json.loads(self.files_written_json) if self.files_written_json else []

    @files_written.setter
    def files_written(self, value: list):
        self.files_written_json = json.dumps(value)

    def to_dict(self) -> dict:
        d = {
            "run_id": self.run_id,
            "pipeline_id": self.pipeline_id,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "progress": self.progress,
            "streaming": self.streaming,
        }
        if self.rows_processed is not None:
            d["rows_processed"] = self.rows_processed
        if self.chunks_processed is not None:
            d["chunks_processed"] = self.chunks_processed
        if self.output_file:
            d["output_file"] = self.output_file
        if self.files_written:
            d["files_written"] = self.files_written
        if self.duration_seconds is not None:
            d["duration_seconds"] = self.duration_seconds
        if self.error:
            d["error"] = self.error
        if self.traceback:
            d["traceback"] = self.traceback
        if self.metadata_generated:
            d["metadata_generated"] = self.metadata_generated
            d["columns_needing_review"] = self.columns_needing_review
            d["ai_enhanced"] = self.ai_enhanced
        return d


class DatasetMetadata(Base):
    __tablename__ = "dataset_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id = Column(String, nullable=False, unique=True, index=True)
    metadata_json = Column(Text, default="{}")
    created_at = Column(String, nullable=False)
    updated_at = Column(String, nullable=True)

    @property
    def content(self) -> dict:
        return json.loads(self.metadata_json) if self.metadata_json else {}

    @content.setter
    def content(self, value: dict):
        self.metadata_json = json.dumps(value)


class ColumnKnowledge(Base):
    __tablename__ = "column_knowledge"

    id = Column(Integer, primary_key=True, autoincrement=True)
    column_key = Column(String, nullable=False, unique=True, index=True)
    description = Column(Text, nullable=False)
    business_meaning = Column(Text, nullable=True)
    verified_by = Column(String, default="user")
    verified_at = Column(String, nullable=True)


class Connection(Base):
    __tablename__ = "connections"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False, unique=True, index=True)
    type = Column(String, nullable=False)
    description = Column(String, default="")
    credentials_encrypted = Column(Text, nullable=False)
    last_tested_at = Column(String, nullable=True)
    last_test_status = Column(String, nullable=True)
    created_at = Column(String, nullable=False)
    updated_at = Column(String, nullable=True)

    def to_dict(self, include_credentials: bool = False) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "description": self.description,
            "last_tested_at": self.last_tested_at,
            "last_test_status": self.last_test_status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        if include_credentials:
            from .encryption import decrypt_dict

            d["credentials"] = decrypt_dict(self.credentials_encrypted)
        return d


class APIKey(Base):
    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_hash = Column(String, nullable=False, unique=True, index=True)
    key_prefix = Column(String, nullable=False)  # First 12 chars for display
    name = Column(String, nullable=False)
    description = Column(String, default="")
    active = Column(Boolean, default=True)
    created_at = Column(String, nullable=False)


# ------------------------------------------------------------------
# Ontology Models
# ------------------------------------------------------------------


class OntologyEntity(Base):
    __tablename__ = "ontology_entities"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False, unique=True, index=True)
    display_name = Column(String, nullable=False)
    description = Column(Text, default="")
    pipeline_id = Column(String, nullable=False, index=True)
    column_annotations_json = Column(Text, default="{}")
    status = Column(String, default="active")
    proposed_by = Column(String, default="user")
    approved_by = Column(String, nullable=True)
    approved_at = Column(String, nullable=True)
    created_at = Column(String, nullable=False)
    updated_at = Column(String, nullable=True)

    @property
    def column_annotations(self) -> dict:
        return json.loads(self.column_annotations_json) if self.column_annotations_json else {}

    @column_annotations.setter
    def column_annotations(self, value: dict):
        self.column_annotations_json = json.dumps(value)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "display_name": self.display_name,
            "description": self.description,
            "pipeline_id": self.pipeline_id,
            "column_annotations": self.column_annotations,
            "status": self.status,
            "proposed_by": self.proposed_by,
            "approved_by": self.approved_by,
            "approved_at": self.approved_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class OntologyRelationship(Base):
    __tablename__ = "ontology_relationships"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    from_entity = Column(String, nullable=False)
    to_entity = Column(String, nullable=False)
    from_column = Column(String, nullable=False)
    to_column = Column(String, nullable=False)
    relationship_type = Column(String, default="one_to_many")
    description = Column(Text, default="")
    status = Column(String, default="active")
    proposed_by = Column(String, default="user")
    created_at = Column(String, nullable=False)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "from_entity": self.from_entity,
            "to_entity": self.to_entity,
            "from_column": self.from_column,
            "to_column": self.to_column,
            "relationship_type": self.relationship_type,
            "description": self.description,
            "status": self.status,
            "proposed_by": self.proposed_by,
            "created_at": self.created_at,
        }


class OntologyMetric(Base):
    __tablename__ = "ontology_metrics"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    display_name = Column(String, nullable=False)
    description = Column(Text, default="")
    entity_name = Column(String, nullable=False)
    expression = Column(Text, nullable=False)
    format_type = Column(String, default="number")
    status = Column(String, default="active")
    proposed_by = Column(String, default="user")
    created_at = Column(String, nullable=False)
    updated_at = Column(String, nullable=True)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "display_name": self.display_name,
            "description": self.description,
            "entity_name": self.entity_name,
            "expression": self.expression,
            "format_type": self.format_type,
            "status": self.status,
            "proposed_by": self.proposed_by,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class OntologyDimension(Base):
    __tablename__ = "ontology_dimensions"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    display_name = Column(String, nullable=False)
    description = Column(Text, default="")
    entity_name = Column(String, nullable=False)
    expression = Column(Text, nullable=False)
    dimension_type = Column(String, default="direct")
    status = Column(String, default="active")
    proposed_by = Column(String, default="user")
    created_at = Column(String, nullable=False)
    updated_at = Column(String, nullable=True)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "display_name": self.display_name,
            "description": self.description,
            "entity_name": self.entity_name,
            "expression": self.expression,
            "dimension_type": self.dimension_type,
            "status": self.status,
            "proposed_by": self.proposed_by,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class OntologyProposal(Base):
    __tablename__ = "ontology_proposals"

    id = Column(String, primary_key=True)
    proposal_type = Column(String, nullable=False)
    payload_json = Column(Text, nullable=False)
    source_pipeline_id = Column(String, nullable=True)
    proposed_by = Column(String, default="ai")
    status = Column(String, default="pending")
    reviewed_by = Column(String, nullable=True)
    reviewed_at = Column(String, nullable=True)
    review_notes = Column(Text, nullable=True)
    created_at = Column(String, nullable=False)

    @property
    def payload(self) -> dict:
        return json.loads(self.payload_json) if self.payload_json else {}

    @payload.setter
    def payload(self, value: dict):
        self.payload_json = json.dumps(value)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "proposal_type": self.proposal_type,
            "payload": self.payload,
            "source_pipeline_id": self.source_pipeline_id,
            "proposed_by": self.proposed_by,
            "status": self.status,
            "reviewed_by": self.reviewed_by,
            "reviewed_at": self.reviewed_at,
            "review_notes": self.review_notes,
            "created_at": self.created_at,
        }
