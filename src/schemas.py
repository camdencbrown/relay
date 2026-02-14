"""
Pydantic request/response models for Relay API
Extracted from api.py
"""

from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class SourceConfig(BaseModel):
    type: str = Field(..., description="Source type: csv_url, json_url, synthetic, mysql, postgres, rest_api, salesforce")
    url: Optional[str] = Field(None, description="URL to fetch data from")
    connection: Optional[str] = Field(None, description="Named connection to use for credentials")

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
    connection: Optional[str] = Field(None, description="Named connection to use for credentials")

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


# ------------------------------------------------------------------
# Connection schemas
# ------------------------------------------------------------------

VALID_CONNECTION_TYPES = {"mysql", "postgres", "salesforce", "rest_api", "domo", "servicenow", "s3"}


class CreateConnectionRequest(BaseModel):
    name: str = Field(..., description="Connection name (alphanumeric, hyphens, underscores)")
    type: str = Field(..., description="Connection type: mysql, postgres, salesforce, rest_api, etc.")
    description: Optional[str] = Field(None, description="Human-readable description")
    credentials: Dict = Field(..., description="Credentials for the connection")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        import re

        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_-]{1,62}$", v):
            raise ValueError(
                "Name must start with a letter, contain only letters/numbers/hyphens/underscores, "
                "and be 2-63 characters long."
            )
        return v

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in VALID_CONNECTION_TYPES:
            raise ValueError(f"Invalid connection type '{v}'. Must be one of: {sorted(VALID_CONNECTION_TYPES)}")
        return v


class UpdateConnectionRequest(BaseModel):
    description: Optional[str] = None
    credentials: Optional[Dict] = None


# ------------------------------------------------------------------
# Ontology schemas
# ------------------------------------------------------------------


class CreateEntityRequest(BaseModel):
    name: str = Field(..., description="Canonical entity name (e.g. 'orders')")
    display_name: str = Field(..., description="Human-readable name")
    description: Optional[str] = Field(None, description="Entity description")
    pipeline_id: str = Field(..., description="Pipeline that sources this entity")
    column_annotations: Optional[Dict] = Field(None, description="Column role annotations")


class UpdateEntityRequest(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    pipeline_id: Optional[str] = None
    column_annotations: Optional[Dict] = None
    status: Optional[str] = None


class CreateRelationshipRequest(BaseModel):
    name: str = Field(..., description="Relationship name (e.g. 'orders_to_customers')")
    from_entity: str = Field(..., description="Source entity name")
    to_entity: str = Field(..., description="Target entity name")
    from_column: str = Field(..., description="Source join column")
    to_column: str = Field(..., description="Target join column")
    relationship_type: str = Field(default="one_to_many", description="one_to_one | one_to_many | many_to_many")
    description: Optional[str] = None


class CreateMetricRequest(BaseModel):
    name: str = Field(..., description="Metric name (e.g. 'revenue')")
    display_name: str = Field(..., description="Human-readable name")
    description: Optional[str] = None
    entity_name: str = Field(..., description="Entity this metric is scoped to")
    expression: str = Field(..., description="SQL expression (e.g. 'SUM(orders.total)')")
    format_type: str = Field(default="number", description="number | currency | percentage")


class UpdateMetricRequest(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    expression: Optional[str] = None
    format_type: Optional[str] = None
    status: Optional[str] = None


class CreateDimensionRequest(BaseModel):
    name: str = Field(..., description="Dimension name (e.g. 'customer_segment')")
    display_name: str = Field(..., description="Human-readable name")
    description: Optional[str] = None
    entity_name: str = Field(..., description="Entity this dimension belongs to")
    expression: str = Field(..., description="SQL expression (e.g. 'customers.segment')")
    dimension_type: str = Field(default="direct", description="direct | derived")


class UpdateDimensionRequest(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    expression: Optional[str] = None
    dimension_type: Optional[str] = None
    status: Optional[str] = None


class SemanticQueryRequest(BaseModel):
    metrics: Optional[List[str]] = Field(None, description="Metric names to compute")
    dimensions: Optional[List[str]] = Field(None, description="Dimension names to group by")
    filters: Optional[List[str]] = Field(None, description="SQL WHERE clauses")
    order_by: Optional[List[str]] = Field(None, description="ORDER BY expressions")
    limit: Optional[int] = Field(None, description="Row limit")
    natural_language: Optional[str] = Field(None, description="Natural language query (alternative to structured)")


class ReviewProposalRequest(BaseModel):
    action: str = Field(..., description="approve | reject")
    notes: Optional[str] = None


class ProposeOntologyRequest(BaseModel):
    pipeline_id: str = Field(..., description="Pipeline to analyze")
    include_relationships: bool = Field(default=True, description="Propose relationships to existing entities")
    include_metrics: bool = Field(default=True, description="Propose metrics and dimensions")


# ------------------------------------------------------------------
# Admin / RBAC schemas
# ------------------------------------------------------------------

VALID_ROLES = {"reader", "writer", "admin"}


class CreateAPIKeyRequest(BaseModel):
    name: str = Field(..., description="Name for the API key")
    description: Optional[str] = Field(None, description="Optional description")
    role: str = Field(default="writer", description="Role: reader, writer, or admin")

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        if v not in VALID_ROLES:
            raise ValueError(f"Invalid role '{v}'. Must be one of: {sorted(VALID_ROLES)}")
        return v
