# API Reference

Base URL: `http://localhost:8001/api/v1`

## Discovery

### GET /capabilities
Returns the full API specification. This is the entry point for agents.

## Pipelines

### POST /pipeline/create
Create a new pipeline.

**Body:**
```json
{
  "name": "my_data",
  "source": {"type": "csv_url", "url": "https://example.com/data.csv"},
  "destination": {"type": "s3", "bucket": "my-bucket", "path": "data/"}
}
```

### GET /pipeline/list
List all pipelines.

### GET /pipeline/{id}
Get pipeline details and run history.

### POST /pipeline/{id}/run
Trigger pipeline execution (runs in background).

### GET /pipeline/{id}/run/{run_id}
Check run status.

### DELETE /pipeline/{id}
Delete a pipeline and its runs.

### POST /test/source
Test source connectivity.

**Body:**
```json
{"type": "csv_url", "url": "https://example.com/data.csv"}
```

## Query

### POST /query
Execute SQL over pipeline data using DuckDB.

**Body:**
```json
{
  "pipelines": ["pipe-abc123"],
  "sql": "SELECT * FROM my_data LIMIT 10",
  "limit": 1000
}
```

### POST /schema
Get table schemas (column names, types, sample values).

### POST /export
Execute query and return results as CSV, JSON, or Excel file.

## Metadata

### GET /metadata/{pipeline_id}
Get dataset metadata (column profiling, AI descriptions).

### GET /metadata/review/pending
List columns awaiting human review.

### POST /metadata/review/approve
Approve a column description for the knowledge base.

## Search

### GET /datasets/search?q=keyword
Search datasets by keyword.

### GET /datasets/join-suggestions?dataset1=id1&dataset2=id2
Suggest join keys between two datasets.

## Transformations

### POST /pipeline/create-transformation
Create a multi-source transformation pipeline with joins and aggregations.

**Body:**
```json
{
  "name": "joined_data",
  "sources": [
    {"type": "rest_api", "url": "https://api.example.com/users", "alias": "users"},
    {"type": "rest_api", "url": "https://api.example.com/orders", "alias": "orders"}
  ],
  "join": {"left": "users", "right": "orders", "on": "users.id = orders.user_id", "how": "left"},
  "destination": {"type": "s3", "bucket": "my-bucket", "path": "transformed/"}
}
```

## Health

### GET /health
Returns service status, version, and component info.
