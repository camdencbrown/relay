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


class APIKey(Base):
    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_hash = Column(String, nullable=False, unique=True, index=True)
    key_prefix = Column(String, nullable=False)  # First 12 chars for display
    name = Column(String, nullable=False)
    description = Column(String, default="")
    active = Column(Boolean, default=True)
    created_at = Column(String, nullable=False)
