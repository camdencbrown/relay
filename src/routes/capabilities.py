"""
GET /capabilities - Self-describing discovery endpoint
"""

from fastapi import APIRouter

router = APIRouter()


@router.get("/capabilities")
async def get_capabilities():
    """Agent reads this once to understand the entire API."""
    return {
        "version": "2.0",
        "name": "Relay - Agent-Native Data Movement",
        "description": "Data pipeline platform designed for AI agent interaction",
        "design_principle": "Agent reads once, understands forever",
        "query_engine": {
            "engine": "DuckDB",
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
                "Type casting (CAST, TRY_CAST)",
            ],
            "best_practices": [
                "Use /schema endpoint to see column types and sample values before querying",
                "Filter early in WHERE clause for better performance",
                "Use LIMIT for exploratory queries",
                "Table names are pipeline names with spaces replaced by underscores, lowercase",
            ],
        },
        "endpoints_summary": {
            "discovery": "GET /api/v1/capabilities",
            "test": "POST /api/v1/test/source",
            "create": "POST /api/v1/pipeline/create",
            "list": "GET /api/v1/pipeline/list",
            "get": "GET /api/v1/pipeline/{id}",
            "run": "POST /api/v1/pipeline/{id}/run",
            "status": "GET /api/v1/pipeline/{id}/run/{run_id}",
            "delete": "DELETE /api/v1/pipeline/{id}",
            "metadata": "GET /api/v1/metadata/{id}",
            "search_datasets": "GET /api/v1/datasets/search?q=query",
            "join_suggestions": "GET /api/v1/datasets/join-suggestions?dataset1=id1&dataset2=id2",
            "create_transformation": "POST /api/v1/pipeline/create-transformation",
            "query": "POST /api/v1/query",
            "schema": "POST /api/v1/schema",
            "export": "POST /api/v1/export",
            "connection_create": "POST /api/v1/connection/create",
            "connection_list": "GET /api/v1/connection/list",
            "connection_get": "GET /api/v1/connection/{id}",
            "connection_update": "PUT /api/v1/connection/{id}",
            "connection_delete": "DELETE /api/v1/connection/{id}",
            "connection_test": "POST /api/v1/connection/{id}/test",
        },
        "sources": [
            {"type": "csv_url", "description": "Fetch CSV from public URL"},
            {"type": "json_url", "description": "Fetch JSON from public URL"},
            {"type": "rest_api", "description": "Fetch from any REST API"},
            {"type": "mysql", "description": "MySQL database source"},
            {"type": "postgres", "description": "PostgreSQL database source"},
            {"type": "salesforce", "description": "Salesforce SOQL source"},
            {"type": "synthetic", "description": "Generate test data"},
        ],
        "destinations": [
            {
                "type": "s3",
                "description": "AWS S3 bucket",
                "parameters": {
                    "bucket": "S3 bucket name",
                    "path": "Path within bucket",
                },
            }
        ],
        "connections": {
            "description": "Named, encrypted credential stores. Create once, reference by name in pipelines.",
            "supported_types": ["mysql", "postgres", "salesforce", "rest_api", "domo", "servicenow", "s3"],
            "workflow": [
                "1. Create connection: POST /connection/create with name, type, credentials",
                "2. Test connection: POST /connection/{id}/test",
                "3. Use in pipeline: set source.connection = 'connection-name' instead of inline credentials",
            ],
            "security": [
                "Credentials encrypted at rest with Fernet (AES-128-CBC + HMAC)",
                "Credentials never returned by any API endpoint",
                "Decrypted only in memory during pipeline execution",
                "Encryption key from environment variable, not in code or database",
            ],
            "example_create": {
                "name": "prod-mysql",
                "type": "mysql",
                "description": "Production MySQL database",
                "credentials": {
                    "host": "db.example.com",
                    "port": 3306,
                    "username": "readonly",
                    "password": "secret",
                    "database": "analytics",
                },
            },
            "example_pipeline_usage": {
                "source": {
                    "type": "mysql",
                    "connection": "prod-mysql",
                    "query": "SELECT * FROM orders WHERE created_at > '2024-01-01'",
                },
            },
        },
        "scheduling": {
            "intervals": ["hourly", "daily", "weekly", "custom"],
            "example": {"schedule": {"enabled": True, "interval": "daily", "timezone": "UTC"}},
        },
        "getting_started": [
            "1. (Optional) Create connection: POST /connection/create",
            "2. (Optional) Test connection: POST /connection/{id}/test",
            "3. Test source: POST /test/source",
            "4. Create pipeline: POST /pipeline/create (use connection name or inline credentials)",
            "5. Run pipeline: POST /pipeline/{id}/run",
            "6. Query data: POST /query",
        ],
    }
