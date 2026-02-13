"""
Relay API Routes
Agent-friendly, self-describing endpoints
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid

from .storage import Storage
from .pipeline import PipelineEngine
from .metadata import MetadataGenerator
from .auth import verify_api_key, optional_auth, auth_manager
from .dataset_search import DatasetSearch
from .transform import TransformationEngine
from .query import QueryEngine

# Create API router
router = APIRouter()

# Initialize storage and engine
storage = Storage()
engine = PipelineEngine(storage)
dataset_search = DatasetSearch(storage)
transform_engine = TransformationEngine(engine)
query_engine = QueryEngine(storage)

# ============================================================================
# Request/Response Models
# ============================================================================

class SourceConfig(BaseModel):
    type: str = Field(..., description="Source type: csv_url, json_url, synthetic, mysql, postgres, rest_api")
    url: Optional[str] = Field(None, description="URL to fetch data from (for csv_url, json_url, rest_api)")
    # Additional fields for different source types
    class Config:
        extra = "allow"  # Allow additional fields for different source types

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
    cron: Optional[str] = Field(None, description="Custom cron expression (if interval=custom)")
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
    limit: int = Field(default=1000, description="Maximum rows to return (safety limit)")

class SchemaRequest(BaseModel):
    pipelines: List[str] = Field(..., description="List of pipeline IDs to inspect")

# ============================================================================
# Endpoints
# ============================================================================

@router.get("/capabilities")
async def get_capabilities():
    """
    Self-describing endpoint - agent learns entire API from this
    
    This is THE key endpoint. Agent calls this first to understand
    what Relay can do and how to do it.
    """
    return {
        "version": "1.0",
        "name": "Relay - Agent-Native Data Movement",
        "description": "Data pipeline platform designed for AI agent interaction",
        "design_principle": "Agent reads once, understands forever",
        
        "query_engine": {
            "engine": "DuckDB",
            "version": "1.0+",
            "description": "In-memory SQL execution over S3 parquet files",
            "supported_features": [
                "Multi-table JOINs (INNER, LEFT, RIGHT, OUTER)",
                "Common Table Expressions (CTEs/WITH clauses)",
                "Window functions (ROW_NUMBER, RANK, LAG, LEAD)",
                "Aggregations (SUM, AVG, COUNT, MIN, MAX, GROUP_CONCAT)",
                "Subqueries and nested queries",
                "CASE statements and conditional logic",
                "String functions (SUBSTRING, CONCAT, UPPER, LOWER, TRIM)",
                "Date functions (EXTRACT, DATE_TRUNC, STRFTIME)",
                "Math functions (ROUND, CEIL, FLOOR, ABS)",
                "Type casting (CAST, TRY_CAST)"
            ],
            "date_handling": {
                "note": "Dates may be stored as strings - use SUBSTRING or CAST as needed",
                "examples": [
                    "WHERE order_date LIKE '2024%'",
                    "WHERE CAST(order_date AS DATE) >= '2024-01-01'",
                    "WHERE SUBSTRING(order_date, 1, 4) = '2024'"
                ]
            },
            "best_practices": [
                "Use /schema endpoint to see column types and sample values before querying",
                "Filter early in WHERE clause for better performance",
                "Use LIMIT for exploratory queries",
                "Table names are pipeline names with spaces→underscores, lowercase"
            ],
            "documentation": "https://duckdb.org/docs/sql/introduction"
        },
        
        "quick_start": {
            "bash": "curl -X POST http://localhost:8001/api/v1/pipeline/create -H 'Content-Type: application/json' -d @pipeline.json",
            "powershell": "Invoke-WebRequest -Uri http://localhost:8001/api/v1/pipeline/create -Method POST -ContentType 'application/json' -Body (Get-Content pipeline.json -Raw)",
            "python": "requests.post('http://localhost:8001/api/v1/pipeline/create', json=pipeline_config)"
        },
        
        "endpoints_summary": {
            "discovery": "GET /api/v1/capabilities (this endpoint)",
            "test": "POST /api/v1/test/source (verify source before creating pipeline)",
            "create": "POST /api/v1/pipeline/create (create new pipeline)",
            "list": "GET /api/v1/pipeline/list (list all pipelines)",
            "get": "GET /api/v1/pipeline/{id} (get pipeline details)",
            "run": "POST /api/v1/pipeline/{id}/run (execute pipeline)",
            "status": "GET /api/v1/pipeline/{id}/run/{run_id} (check execution status)",
            "delete": "DELETE /api/v1/pipeline/{id} (remove pipeline)",
            "metadata": "GET /api/v1/metadata/{id} (get data schema & semantics)",
            "search_datasets": "GET /api/v1/datasets/search?q=query (find datasets by keywords)",
            "join_suggestions": "GET /api/v1/datasets/join-suggestions?dataset1=id1&dataset2=id2 (discover join keys)",
            "create_transformation": "POST /api/v1/pipeline/create-transformation (join/aggregate multiple sources)",
            "query": "POST /api/v1/query (execute SQL over pipeline data, returns results directly)",
            "schema": "POST /api/v1/schema (get table schemas for SQL queries)",
            "export": "POST /api/v1/export (execute SQL and download as CSV/JSON/Excel for visualization)"
        },
        
        "workflow_examples": {
            "basic_workflow": {
                "description": "Load data → Query → Get results",
                "steps": [
                    {
                        "step": 1,
                        "action": "Create pipeline",
                        "endpoint": "POST /api/v1/pipeline/create",
                        "request": {
                            "name": "customers",
                            "source": {"type": "csv_url", "url": "https://example.com/customers.csv"},
                            "destination": {"type": "s3", "bucket": "my-bucket", "path": "data/customers/"}
                        },
                        "response": {"pipeline_id": "pipe-abc123", "status": "created"}
                    },
                    {
                        "step": 2,
                        "action": "Run pipeline",
                        "endpoint": "POST /api/v1/pipeline/pipe-abc123/run",
                        "response": {"run_id": "run-xyz789", "status": "started"}
                    },
                    {
                        "step": 3,
                        "action": "Query data",
                        "endpoint": "POST /api/v1/query",
                        "request": {
                            "pipelines": ["pipe-abc123"],
                            "sql": "SELECT * FROM customers LIMIT 10"
                        },
                        "response": {"rows": "[...data...]", "row_count": 10}
                    }
                ]
            },
            "multi_table_analysis": {
                "description": "Join multiple datasets and aggregate",
                "query_example": {
                    "endpoint": "POST /api/v1/query",
                    "request": {
                        "pipelines": ["pipe-customers", "pipe-orders", "pipe-items"],
                        "sql": "SELECT c.name, SUM(oi.quantity * oi.price) as total_spend FROM customers c JOIN orders o ON c.customer_id = o.customer_id JOIN order_items oi ON o.order_id = oi.order_id WHERE c.state = 'CA' GROUP BY c.name ORDER BY total_spend DESC LIMIT 10"
                    },
                    "response": {
                        "rows": [{"name": "John Doe", "total_spend": 1234.56}],
                        "row_count": 10,
                        "execution_time_ms": 245
                    }
                }
            },
            "export_for_visualization": {
                "description": "Export query results for charts",
                "example": {
                    "endpoint": "POST /api/v1/export",
                    "request": {
                        "pipelines": ["pipe-sales"],
                        "sql": "SELECT region, SUM(revenue) as total FROM sales GROUP BY region",
                        "format": "json",
                        "filename": "revenue_by_region.json"
                    },
                    "response": "Downloads JSON file with results"
                }
            }
        },
        
        "sources": [
            {
                "type": "csv_url",
                "description": "Fetch CSV from public URL",
                "example": "https://example.com/data.csv",
                "auth_required": False,
                "parameters": {
                    "url": "HTTP(S) URL to CSV file"
                }
            },
            {
                "type": "json_url", 
                "description": "Fetch JSON from public URL",
                "example": "https://api.example.com/data.json",
                "auth_required": False,
                "parameters": {
                    "url": "HTTP(S) URL to JSON file/API"
                }
            },
            {
                "type": "rest_api",
                "description": "Fetch from any REST API",
                "example": "https://api.github.com/repos/owner/repo",
                "auth_required": False,
                "parameters": {
                    "url": "API endpoint URL",
                    "method": "HTTP method (GET, POST, etc.)",
                    "headers": "Optional headers dict"
                }
            }
        ],
        
        "destinations": [
            {
                "type": "s3",
                "description": "AWS S3 bucket",
                "example": "s3://airbyte-poc-bucket-cb/relay/your-data/",
                "auth": "AWS credentials configured via environment variables",
                "configured_bucket": "airbyte-poc-bucket-cb",
                "note": "Use the configured_bucket above, or specify your own if you have access",
                "parameters": {
                    "bucket": "S3 bucket name (use configured_bucket for this instance)",
                    "path": "Path within bucket (e.g., 'relay/pipelines/')"
                }
            }
        ],
        
        "operations": [
            {
                "endpoint": "/capabilities",
                "method": "GET",
                "description": "This endpoint - discover all capabilities"
            },
            {
                "endpoint": "/pipeline/create",
                "method": "POST",
                "description": "Create a new data pipeline",
                "example": {
                    "name": "My Pipeline",
                    "source": {"type": "csv_url", "url": "https://..."},
                    "destination": {"type": "s3", "bucket": "my-bucket", "path": "data/"}
                }
            },
            {
                "endpoint": "/pipeline/list",
                "method": "GET", 
                "description": "List all pipelines"
            },
            {
                "endpoint": "/pipeline/{id}",
                "method": "GET",
                "description": "Get pipeline details and run history"
            },
            {
                "endpoint": "/pipeline/{id}/run",
                "method": "POST",
                "description": "Trigger pipeline execution"
            },
            {
                "endpoint": "/pipeline/{id}/run/{run_id}",
                "method": "GET",
                "description": "Check status of a specific run"
            },
            {
                "endpoint": "/test/source",
                "method": "POST",
                "description": "Test if source is accessible before creating pipeline",
                "example": {
                    "type": "csv_url",
                    "url": "https://example.com/data.csv"
                }
            }
        ],
        
        "patterns": {
            "consistent_structure": "All responses follow same pattern: status, data, next_steps",
            "smart_defaults": "Agent provides minimal info, Relay fills gaps intelligently",
            "clear_errors": "Errors include suggestions for fixing",
            "next_steps": "Every response tells agent what to do next"
        },
        
        "scheduling": {
            "description": "Pipelines can run automatically on a schedule",
            "intervals": [
                {"value": "hourly", "description": "Every hour"},
                {"value": "daily", "description": "Once per day (2 AM UTC by default)"},
                {"value": "weekly", "description": "Once per week (Monday 2 AM UTC)"},
                {"value": "custom", "description": "Custom cron expression"}
            ],
            "example": {
                "schedule": {
                    "enabled": True,
                    "interval": "daily",
                    "timezone": "America/Denver"
                }
            }
        },
        
        "getting_started": [
            "1. Test source: POST /test/source",
            "2. Create pipeline: POST /pipeline/create",
            "3. Run pipeline: POST /pipeline/{id}/run (or let schedule handle it)",
            "4. Check status: GET /pipeline/{id}"
        ]
    }

@router.post("/pipeline/create")
async def create_pipeline(request: CreatePipelineRequest):
    """
    Create a new data pipeline
    
    Agent provides source + destination, Relay handles the rest
    """
    try:
        # Generate pipeline ID
        pipeline_id = f"pipe-{uuid.uuid4().hex[:8]}"
        
        # Use default options if not provided
        options = request.options or PipelineOptions()
        schedule = request.schedule or ScheduleConfig()
        
        # Create pipeline object
        pipeline = {
            "id": pipeline_id,
            "name": request.name,
            "description": request.description or "",
            "source": request.source.dict(),
            "destination": request.destination.dict(),
            "options": options.dict(),
            "schedule": schedule.dict(),
            "status": "active",
            "created_at": datetime.utcnow().isoformat() + "Z",
            "runs": [],
            "last_scheduled_run": None
        }
        
        # Save to storage
        storage.save_pipeline(pipeline)
        
        # Generate table name for queries
        table_name = request.name.replace(" ", "_").replace("-", "_").lower()
        if table_name and table_name[0].isdigit():
            table_name = "t_" + table_name
        
        return {
            "status": "created",
            "pipeline_id": pipeline_id,
            "name": request.name,
            "table_name": table_name,
            "source": f"{request.source.type} → {request.source.url}",
            "destination": f"s3://{request.destination.bucket}/{request.destination.path}",
            "options": options.dict(),
            "query_example": f"SELECT * FROM {table_name} LIMIT 10",
            "next_steps": [
                f"Run pipeline: POST /pipeline/{pipeline_id}/run",
                f"View details: GET /pipeline/{pipeline_id}",
                f"List all: GET /pipeline/list",
                f"Query after execution: POST /query with pipelines=['{pipeline_id}']"
            ],
            "created_at": pipeline["created_at"]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail={
            "status": "error",
            "message": f"Failed to create pipeline: {str(e)}",
            "suggestion": "Check that all required fields are provided"
        })

@router.get("/pipeline/list")
async def list_pipelines():
    """List all pipelines"""
    pipelines = storage.list_pipelines()
    
    result_pipelines = []
    for p in pipelines:
        # Handle both regular and transformation pipelines
        pipeline_type = p.get("type", "regular")
        
        if pipeline_type == "transformation":
            # Transformation pipeline structure
            result_pipelines.append({
                "id": p["id"],
                "name": p["name"],
                "type": "transformation",
                "source_count": len(p.get("config", {}).get("sources", [])),
                "status": p["status"],
                "created_at": p["created_at"],
                "last_run": p["runs"][-1] if p.get("runs") else None,
                "total_runs": len(p.get("runs", []))
            })
        else:
            # Regular pipeline structure
            result_pipelines.append({
                "id": p["id"],
                "name": p["name"],
                "source_type": p["source"]["type"],
                "destination_type": p["destination"]["type"],
                "status": p["status"],
                "created_at": p["created_at"],
                "last_run": p["runs"][-1] if p["runs"] else None,
                "total_runs": len(p["runs"])
            })
    
    return {
        "pipelines": result_pipelines,
        "total": len(result_pipelines),
        "next_steps": [
            "Create new: POST /pipeline/create",
            "View details: GET /pipeline/{id}"
        ]
    }

@router.get("/pipeline/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    """Get pipeline details and run history"""
    pipeline = storage.get_pipeline(pipeline_id)
    
    if not pipeline:
        raise HTTPException(status_code=404, detail={
            "status": "not_found",
            "message": f"Pipeline {pipeline_id} not found",
            "suggestion": "Check pipeline ID or list all: GET /pipeline/list"
        })
    
    return {
        **pipeline,
        "next_steps": [
            f"Run pipeline: POST /pipeline/{pipeline_id}/run",
            "List all: GET /pipeline/list"
        ]
    }

@router.post("/pipeline/{pipeline_id}/run")
async def run_pipeline(pipeline_id: str, background_tasks: BackgroundTasks):
    """Trigger pipeline execution"""
    pipeline = storage.get_pipeline(pipeline_id)
    
    if not pipeline:
        raise HTTPException(status_code=404, detail={
            "status": "not_found",
            "message": f"Pipeline {pipeline_id} not found"
        })
    
    # Generate run ID
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    
    # Execute pipeline in background
    background_tasks.add_task(engine.execute_pipeline, pipeline_id, run_id)
    
    return {
        "status": "started",
        "pipeline_id": pipeline_id,
        "run_id": run_id,
        "started_at": datetime.utcnow().isoformat() + "Z",
        "message": "Pipeline execution started",
        "next_steps": [
            f"Check status: GET /pipeline/{pipeline_id}/run/{run_id}",
            f"View pipeline: GET /pipeline/{pipeline_id}"
        ]
    }

@router.get("/pipeline/{pipeline_id}/run/{run_id}")
async def get_run_status(pipeline_id: str, run_id: str):
    """Check status of a specific pipeline run"""
    pipeline = storage.get_pipeline(pipeline_id)
    
    if not pipeline:
        raise HTTPException(status_code=404, detail={
            "status": "not_found",
            "message": f"Pipeline {pipeline_id} not found"
        })
    
    # Find the run
    run = next((r for r in pipeline["runs"] if r["run_id"] == run_id), None)
    
    if not run:
        raise HTTPException(status_code=404, detail={
            "status": "not_found",
            "message": f"Run {run_id} not found for pipeline {pipeline_id}"
        })
    
    return {
        **run,
        "next_steps": [
            f"View pipeline: GET /pipeline/{pipeline_id}",
            "List all: GET /pipeline/list"
        ]
    }

@router.delete("/pipeline/{pipeline_id}")
async def delete_pipeline(pipeline_id: str):
    """Delete a pipeline"""
    pipeline = storage.get_pipeline(pipeline_id)
    
    if not pipeline:
        raise HTTPException(status_code=404, detail={
            "status": "not_found",
            "message": f"Pipeline {pipeline_id} not found"
        })
    
    storage.delete_pipeline(pipeline_id)
    
    return {
        "status": "deleted",
        "pipeline_id": pipeline_id,
        "message": f"Pipeline {pipeline['name']} deleted successfully"
    }

@router.post("/test/source")
async def test_source(request: TestSourceRequest):
    """Test if source is accessible"""
    try:
        result = await engine.test_source(request.type, request.url)
        
        return {
            "status": "accessible",
            "type": request.type,
            "url": request.url,
            "preview": result,
            "message": "Source is accessible and ready to use",
            "next_steps": [
                "Create pipeline: POST /pipeline/create"
            ]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "type": request.type,
            "url": request.url,
            "error": str(e),
            "suggestions": [
                "Check that URL is correct",
                "Verify URL is publicly accessible",
                "Ensure source type matches content (csv_url for CSV, json_url for JSON)"
            ]
        }

# ============================================================================
# Metadata & Semantic Layer Endpoints
# ============================================================================

@router.get("/metadata/{pipeline_id}")
async def get_metadata(pipeline_id: str):
    """Get metadata for a pipeline's data"""
    metadata_gen_instance = MetadataGenerator()
    metadata = metadata_gen_instance.load_metadata(pipeline_id)
    
    if not metadata:
        raise HTTPException(status_code=404, detail={
            "status": "not_found",
            "message": f"No metadata found for pipeline {pipeline_id}",
            "suggestion": "Run the pipeline first to generate metadata"
        })
    
    return metadata

@router.get("/metadata/review/pending")
async def get_pending_reviews():
    """Get all columns pending human review"""
    metadata_gen_instance = MetadataGenerator()
    pending = metadata_gen_instance.get_pending_reviews()
    
    return {
        "status": "success",
        "pending_count": len(pending),
        "pending_reviews": pending
    }

class ApproveColumnRequest(BaseModel):
    column_name: str
    description: str
    business_meaning: Optional[str] = None
    verified_by: str = "user"

@router.post("/metadata/review/approve")
async def approve_column(request: ApproveColumnRequest):
    """Approve a column description for knowledge base"""
    metadata_gen_instance = MetadataGenerator()
    
    metadata_gen_instance.approve_column(
        request.column_name,
        request.description,
        request.business_meaning,
        request.verified_by
    )
    
    return {
        "status": "approved",
        "column_name": request.column_name,
        "message": f"Column '{request.column_name}' approved and added to knowledge base",
        "next_effect": "Future pipelines with this column will use verified description"
    }

# ============================================================================
# Dataset Search & Discovery (V2)
# ============================================================================

@router.get("/datasets/search")
async def search_datasets(q: str, top_k: int = 5):
    """
    Search for datasets by natural language query
    
    Example: /datasets/search?q=user+activity
    """
    results = dataset_search.search(q, top_k)
    
    return {
        "status": "success",
        "query": q,
        "results_count": len(results),
        "results": results,
        "next_steps": "Use pipeline_id to get metadata or create transformation"
    }

@router.get("/datasets/join-suggestions")
async def get_join_suggestions(dataset1: str, dataset2: str):
    """
    Get suggestions for how to join two datasets
    
    Example: /datasets/join-suggestions?dataset1=pipe-123&dataset2=pipe-456
    """
    suggestions = dataset_search.get_join_suggestions(dataset1, dataset2)
    
    return {
        "status": "success",
        "dataset1": dataset1,
        "dataset2": dataset2,
        "suggestions_count": len(suggestions),
        "suggestions": suggestions,
        "next_steps": "Use suggested join keys in transformation pipeline"
    }

# ============================================================================
# Transformation Pipelines (V2)
# ============================================================================

class TransformationSource(BaseModel):
    type: str
    url: Optional[str] = None
    alias: str
    class Config:
        extra = "allow"

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

@router.post("/pipeline/create-transformation")
async def create_transformation_pipeline(config: TransformationPipelineConfig, background_tasks: BackgroundTasks):
    """
    Create a transformation pipeline that joins/aggregates multiple sources
    
    Example:
    {
        "name": "Users with Post Count",
        "sources": [
            {"type": "rest_api", "url": "https://api.com/users", "alias": "users"},
            {"type": "rest_api", "url": "https://api.com/posts", "alias": "posts"}
        ],
        "join": {
            "left": "users",
            "right": "posts",
            "on": "users.id = posts.userId",
            "how": "left"
        },
        "aggregate": {
            "group_by": ["users.name"],
            "metrics": {
                "post_count": "COUNT(posts.id)"
            }
        },
        "destination": {
            "type": "s3",
            "bucket": "my-bucket",
            "path": "relay/transformed/users_posts/"
        }
    }
    """
    
    # Create pipeline ID
    pipeline_id = f"pipe-{uuid.uuid4().hex[:8]}"
    
    # Execute transformation
    try:
        result_df = transform_engine.execute_transformation(config.dict())
        
        # Write to destination
        output_path = engine._write_destination(
            result_df,
            config.destination.dict(),
            {}
        )
        
        # Save pipeline config
        pipeline = {
            "id": pipeline_id,
            "name": config.name,
            "type": "transformation",
            "config": config.dict(),
            "created_at": datetime.utcnow().isoformat() + "Z",
            "status": "active"
        }
        storage.save_pipeline(pipeline)
        
        return {
            "status": "success",
            "pipeline_id": pipeline_id,
            "rows_processed": len(result_df),
            "output_path": output_path,
            "message": f"Transformation pipeline created and executed",
            "next_steps": [
                f"Query data at: {output_path}",
                f"View metadata: GET /api/v1/metadata/{pipeline_id}",
                f"Run again: POST /api/v1/pipeline/{pipeline_id}/run"
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Query Endpoints
# ============================================================================

@router.post("/query")
async def query_data(request: QueryRequest):
    """
    Execute SQL query over pipeline data
    
    This is the agent-native way to get answers from your data:
    - No storage configuration needed
    - Results returned directly as JSON
    - Queries execute in-memory using DuckDB
    - Automatic join support across multiple pipelines
    
    Example:
    {
        "pipelines": ["pipe-abc123", "pipe-def456"],
        "sql": "SELECT c.name, COUNT(*) as order_count FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.name ORDER BY order_count DESC LIMIT 10",
        "limit": 1000
    }
    
    Table names are derived from pipeline names (spaces replaced with underscores).
    Use GET /schema to see available tables and columns.
    """
    try:
        result = query_engine.execute_query(
            pipelines=request.pipelines,
            sql=request.sql,
            limit=request.limit
        )
        
        # Add helpful hints for empty results
        hints = []
        if result["row_count"] == 0:
            hints = [
                "Query returned 0 rows - check your filter conditions",
                "Use POST /schema to see sample values for columns",
                "Try removing filters one at a time to debug",
                "Check for case sensitivity (e.g., 'Completed' vs 'completed')",
                "Verify date format if filtering on dates"
            ]
        
        return {
            "status": "success",
            **result,
            "hints": hints if hints else None,
            "next_steps": [
                "Refine query if needed",
                "Use POST /schema to see available columns and sample values",
                "Increase limit if you need more rows"
            ] if result["row_count"] > 0 else hints
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/schema")
async def get_schema(request: SchemaRequest):
    """
    Get schema information for pipelines
    
    Returns table names, column names, types, and descriptions.
    Use this before writing SQL queries to understand what data is available.
    
    Example:
    {
        "pipelines": ["pipe-abc123", "pipe-def456"]
    }
    """
    try:
        schemas = query_engine.list_pipeline_schemas(request.pipelines)
        
        return {
            "status": "success",
            "schemas": schemas,
            "usage_example": {
                "sql": f"SELECT * FROM {list(schemas.values())[0]['table_alias'] if schemas else 'table_name'} LIMIT 10",
                "explanation": "Use table_alias values in your SQL queries"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============================================================================
# Export Endpoint
# ============================================================================

class ExportRequest(BaseModel):
    pipelines: List[str] = Field(..., description="List of pipeline IDs to query")
    sql: str = Field(..., description="SQL query to execute")
    format: str = Field(default="csv", description="Export format: csv, json, excel")
    filename: Optional[str] = Field(None, description="Optional filename for download")

@router.post("/export")
async def export_data(request: ExportRequest):
    """
    Execute query and export results in specified format
    
    Perfect for visualization tools - returns data ready for charts/dashboards.
    
    Formats:
    - csv: Comma-separated values (great for Excel, Tableau, spreadsheets)
    - json: JSON array (great for D3.js, Chart.js, web visualizations)
    - excel: Excel workbook (great for stakeholder reports)
    
    Example:
    {
        "pipelines": ["pipe-abc", "pipe-def"],
        "sql": "SELECT date, SUM(revenue) as total FROM sales GROUP BY date ORDER BY date",
        "format": "csv",
        "filename": "monthly_revenue.csv"
    }
    
    Returns download link or inline data depending on size.
    """
    try:
        from fastapi.responses import Response
        import pandas as pd
        from io import BytesIO, StringIO
        
        # Execute query
        result = query_engine.execute_query(
            pipelines=request.pipelines,
            sql=request.sql,
            limit=10000  # Higher limit for exports
        )
        
        if result["row_count"] == 0:
            raise HTTPException(status_code=404, detail="Query returned no results")
        
        # Convert to DataFrame
        df = pd.DataFrame(result["rows"])
        
        # Generate filename
        filename = request.filename or f"export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.{request.format}"
        
        # Export based on format
        if request.format == "csv":
            output = StringIO()
            df.to_csv(output, index=False)
            content = output.getvalue()
            media_type = "text/csv"
            
        elif request.format == "json":
            content = df.to_json(orient="records", indent=2)
            media_type = "application/json"
            
        elif request.format == "excel":
            output = BytesIO()
            df.to_excel(output, index=False, engine='openpyxl')
            content = output.getvalue()
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported format: {request.format}")
        
        # Return as downloadable file
        return Response(
            content=content,
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "X-Row-Count": str(result["row_count"]),
                "X-Execution-Time-Ms": str(result["execution_time_ms"])
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
