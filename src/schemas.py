"""
Pydantic request/response models for Relay API
Extracted from api.py
"""

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    type: str = Field(..., description="Source type: csv_url, json_url, synthetic, mysql, postgres, rest_api, salesforce")
    url: Optional[str] = Field(None, description="URL to fetch data from")

    model_config = {"extra": "allow"}


class DestinationConfig(BaseModel):
    type: str = Field(..., description="Destination type: s3")
    bucket: str = Field(..., description="S3 bucket name")
    path: str = Field(..., description="Path within bucket")


class PipelineOptions(BaseModel):
    format: str = Field(default="parquet", description="Output format: parquet, csv, json")
    compression: str = Field(default="gzip", description="Compression: gzip, none")


class ScheduleConfig(BaseModel):
    enabled: bool = Field(default=False, description="Enable automatic scheduling")
    interval: str = Field(default="daily", description="Interval: hourly, daily, weekly, custom")
    cron: Optional[str] = Field(None, description="Custom cron expression")
    timezone: str = Field(default="UTC", description="Timezone for scheduling")


class CreatePipelineRequest(BaseModel):
    name: str = Field(..., description="Pipeline name")
    description: Optional[str] = Field(None, description="Pipeline description")
    source: SourceConfig
    destination: DestinationConfig
    options: Optional[PipelineOptions] = None
    schedule: Optional[ScheduleConfig] = None


class TestSourceRequest(BaseModel):
    type: str = Field(..., description="Source type")
    url: str = Field(..., description="Source URL")


class QueryRequest(BaseModel):
    pipelines: List[str] = Field(..., description="List of pipeline IDs to query")
    sql: str = Field(..., description="SQL query to execute")
    limit: int = Field(default=1000, description="Maximum rows to return")


class SchemaRequest(BaseModel):
    pipelines: List[str] = Field(..., description="List of pipeline IDs to inspect")


class ExportRequest(BaseModel):
    pipelines: List[str] = Field(..., description="List of pipeline IDs to query")
    sql: str = Field(..., description="SQL query to execute")
    format: str = Field(default="csv", description="Export format: csv, json, excel")
    filename: Optional[str] = Field(None, description="Optional filename for download")


class ApproveColumnRequest(BaseModel):
    column_name: str
    description: str
    business_meaning: Optional[str] = None
    verified_by: str = "user"


class TransformationSource(BaseModel):
    type: str
    url: Optional[str] = None
    alias: str

    model_config = {"extra": "allow"}


class JoinConfig(BaseModel):
    left: str
    right: str
    on: str
    how: str = "left"


class AggregateConfig(BaseModel):
    group_by: List[str]
    metrics: Dict[str, str]


class TransformationPipelineConfig(BaseModel):
    name: str
    sources: List[TransformationSource]
    join: Optional[JoinConfig] = None
    aggregate: Optional[AggregateConfig] = None
    destination: DestinationConfig
    schedule: Optional[ScheduleConfig] = None
