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
            "description": "In-memory SQL execution over S3 or local parquet files",
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
            "analytics_summary": "GET /api/v1/analytics/summary",
            "analytics_events": "GET /api/v1/analytics/events",
            "lineage": "GET /api/v1/ontology/lineage/{entity_name}",
            "admin_create_key": "POST /api/v1/admin/api-keys",
            "admin_list_keys": "GET /api/v1/admin/api-keys",
            "admin_deactivate_key": "DELETE /api/v1/admin/api-keys/{id}",
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
            },
            {
                "type": "local",
                "description": "Local disk storage (set STORAGE_MODE=local)",
                "parameters": {
                    "bucket": "Logical bucket name (becomes directory)",
                    "path": "Path within bucket",
                },
            },
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
        "analytics": {
            "description": "Platform usage tracking. Proves value by recording events for pipelines, queries, and ontology operations.",
            "endpoints": {
                "summary": "GET /api/v1/analytics/summary",
                "events": "GET /api/v1/analytics/events?event_type=&pipeline_id=&limit=",
            },
            "tracked_events": [
                "pipeline_created", "pipeline_run_started", "pipeline_deleted",
                "query_executed", "entity_created", "metric_created",
                "dimension_created", "semantic_query_executed", "ontology_proposed",
            ],
        },
        "storage": {
            "description": "Configurable storage backend for pipeline output files.",
            "modes": {
                "s3": "Write parquet files to AWS S3 (default, requires AWS credentials)",
                "local": "Write parquet files to local disk (set STORAGE_MODE=local for zero-config demos)",
            },
            "configuration": {
                "STORAGE_MODE": "s3 or local (default: s3)",
                "LOCAL_STORAGE_PATH": "Directory for local mode (default: ./relay_data)",
            },
        },
        "lineage": {
            "description": "Entity→pipeline→source traceability. Read-only computed view.",
            "endpoint": "GET /api/v1/ontology/lineage/{entity_name}",
            "returns": [
                "entity details",
                "pipeline info (id, name, type, status)",
                "source configuration",
                "metrics with column_references",
                "dimensions with column_references",
                "relationships (outgoing, incoming)",
                "downstream_entities and upstream_entities",
            ],
        },
        "auth": {
            "description": "Role-based access control (RBAC) with reader < writer < admin hierarchy.",
            "roles": {
                "reader": "Read-only access to all GET endpoints",
                "writer": "Create and modify pipelines, connections, ontology objects",
                "admin": "Delete operations, API key management, proposal review",
            },
            "configuration": {
                "REQUIRE_AUTH": "Set to true to enforce API key authentication (default: false)",
            },
            "endpoints": {
                "create_key": "POST /api/v1/admin/api-keys (admin only)",
                "list_keys": "GET /api/v1/admin/api-keys (admin only)",
                "deactivate_key": "DELETE /api/v1/admin/api-keys/{id} (admin only)",
            },
        },
        "ontology": {
            "description": "Semantic ontology layer: named entities, relationships, metrics, and dimensions. "
            "Agents both consume and build the ontology.",
            "workflows": {
                "structured_query": {
                    "description": "Query using metric and dimension names instead of raw SQL",
                    "example": {
                        "metrics": ["revenue"],
                        "dimensions": ["customer_segment"],
                        "filters": ["orders.created_at > '2024-01-01'"],
                        "limit": 100,
                    },
                },
                "natural_language_query": {
                    "description": "Ask a question in plain English (requires ANTHROPIC_API_KEY)",
                    "example": {"natural_language": "What's revenue by customer segment?"},
                },
                "agent_building": [
                    "1. POST /ontology/propose with pipeline_id to generate proposals",
                    "2. GET /ontology/proposal/list to review proposals",
                    "3. POST /ontology/proposal/{id}/review to approve or reject",
                    "4. GET /ontology to see the full active ontology",
                    "5. POST /ontology/query to run semantic queries",
                    "6. GET /ontology/lineage/{entity_name} to trace entity lineage",
                ],
            },
            "endpoints": {
                "ontology_snapshot": "GET /api/v1/ontology",
                "entity_create": "POST /api/v1/ontology/entity",
                "entity_list": "GET /api/v1/ontology/entity/list",
                "entity_get": "GET /api/v1/ontology/entity/{id}",
                "entity_by_name": "GET /api/v1/ontology/entity/by-name/{name}",
                "entity_update": "PUT /api/v1/ontology/entity/{id}",
                "entity_delete": "DELETE /api/v1/ontology/entity/{id}",
                "relationship_create": "POST /api/v1/ontology/relationship",
                "relationship_list": "GET /api/v1/ontology/relationship/list",
                "relationship_delete": "DELETE /api/v1/ontology/relationship/{id}",
                "metric_create": "POST /api/v1/ontology/metric",
                "metric_list": "GET /api/v1/ontology/metric/list",
                "metric_update": "PUT /api/v1/ontology/metric/{id}",
                "metric_delete": "DELETE /api/v1/ontology/metric/{id}",
                "dimension_create": "POST /api/v1/ontology/dimension",
                "dimension_list": "GET /api/v1/ontology/dimension/list",
                "dimension_update": "PUT /api/v1/ontology/dimension/{id}",
                "dimension_delete": "DELETE /api/v1/ontology/dimension/{id}",
                "semantic_query": "POST /api/v1/ontology/query",
                "propose": "POST /api/v1/ontology/propose",
                "proposal_list": "GET /api/v1/ontology/proposal/list",
                "proposal_get": "GET /api/v1/ontology/proposal/{id}",
                "proposal_review": "POST /api/v1/ontology/proposal/{id}/review",
                "lineage": "GET /api/v1/ontology/lineage/{entity_name}",
            },
        },
        "getting_started": [
            "1. (Optional) Create connection: POST /connection/create",
            "2. (Optional) Test connection: POST /connection/{id}/test",
            "3. Test source: POST /test/source",
            "4. Create pipeline: POST /pipeline/create (use connection name or inline credentials)",
            "5. Run pipeline: POST /pipeline/{id}/run",
            "6. Query data: POST /query (raw SQL) or POST /ontology/query (semantic)",
            "7. View lineage: GET /ontology/lineage/{entity_name}",
            "8. View analytics: GET /analytics/summary",
        ],
    }
