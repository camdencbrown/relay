# Relay V1 - Technical Specification

**Project:** Relay - Agent-Native Data Movement Platform  
**Version:** 1.0 (MVP)  
**Date:** 2026-02-05  
**Status:** In Development

---

## Vision Statement

**Relay is a data movement platform designed for agent interaction first.**

Agents should be able to move data between systems with a single, clear request - no complex configuration, no memorizing IDs, no debugging cryptic errors.

**Tagline:** "Where agents move data"

---

## V1 Scope - Prove the Concept

### Goal
Build the simplest possible version that proves the concept works and is genuinely easier for agents than existing tools (Airbyte).

### Success Criteria
1. Agent can create a data pipeline in **under 2 minutes**
2. Agent uses **one consistent pattern** for all operations
3. Agent doesn't need external documentation
4. System works **first try** (no trial/error)

### What V1 Includes

**One Source Type:**
- Public CSV/JSON URL (no auth needed - simplest start)

**One Destination:**
- AWS S3 (we already have creds/access)

**Core Operations:**
- Create pipeline
- List pipelines
- Check status
- Test connection

**Simple Web UI:**
- View all pipelines
- See pipeline status/runs
- Watch agent activity in real-time
- Build trust through visibility

**Not in V1:**
- Authentication (use env vars for now)
- Scheduling (manual trigger only)
- Complex sources (databases, APIs with auth)
- Multi-user (single agent only)

---

## Architecture

### High-Level Design

```
┌─────────────────────────────────────┐
│  Agent (you)                        │
│  ↓ simple requests                  │
├─────────────────────────────────────┤
│  Relay API                          │
│  - Self-describing endpoints        │
│  - Consistent patterns              │
│  - Smart defaults                   │
│  ↓                                  │
├─────────────────────────────────────┤
│  Pipeline Engine                    │
│  - Fetch from source                │
│  - Transform (optional)             │
│  - Write to destination             │
│  ↓                                  │
├─────────────────────────────────────┤
│  Storage                            │
│  - AWS S3 (we have access)          │
└─────────────────────────────────────┘
```

### Tech Stack

**Backend:**
- Python 3.12 (what we're already using)
- FastAPI (modern, simple REST API framework)
- Pandas (data manipulation)
- boto3 (AWS S3 access)
- Requests (HTTP fetching)

**Frontend (Simple UI):**
- Plain HTML + JavaScript (no frameworks - keep it simple)
- Served by FastAPI (static files)
- Real-time updates via polling or WebSocket
- Shows: pipelines, runs, agent activity

**Storage:**
- JSON file for pipeline metadata (simple start)
- S3 for actual data

**No Database Yet:**
- Keep it simple for V1
- Use `pipelines.json` file

---

## API Specification

### Core Principle: Self-Describing

Every endpoint tells the agent what it can do and how to do it.

### Base URL
```
http://localhost:8000/api/v1
```

### Authentication (V1)
```
X-Relay-Token: simple-token-for-now
```
Or read from environment variable (we can decide)

---

## Endpoints

### 1. GET `/capabilities`
**Purpose:** Agent asks "what can I do here?"

**Request:**
```http
GET /api/v1/capabilities
```

**Response:**
```json
{
  "version": "1.0",
  "name": "Relay - Agent-Native Data Movement",
  "sources": [
    {
      "type": "csv_url",
      "description": "Fetch CSV from public URL",
      "example": "https://example.com/data.csv",
      "auth_required": false
    },
    {
      "type": "json_url", 
      "description": "Fetch JSON from public URL",
      "example": "https://api.example.com/data.json",
      "auth_required": false
    }
  ],
  "destinations": [
    {
      "type": "s3",
      "description": "AWS S3 bucket",
      "example": "s3://bucket-name/path/",
      "auth": "AWS credentials via environment variables"
    }
  ],
  "operations": [
    {
      "endpoint": "/pipeline/create",
      "method": "POST",
      "description": "Create a new data pipeline"
    },
    {
      "endpoint": "/pipeline/list",
      "method": "GET", 
      "description": "List all pipelines"
    },
    {
      "endpoint": "/pipeline/{id}",
      "method": "GET",
      "description": "Get pipeline details"
    },
    {
      "endpoint": "/pipeline/{id}/run",
      "method": "POST",
      "description": "Trigger pipeline execution"
    },
    {
      "endpoint": "/test/source",
      "method": "POST",
      "description": "Test if source is accessible"
    }
  ]
}
```

**Why This Matters:**
Agent discovers entire API from one call. No external docs needed.

---

### 2. POST `/pipeline/create`
**Purpose:** Create a data pipeline

**Request:**
```json
{
  "name": "Iris Dataset Pipeline",
  "description": "Move Iris CSV to S3",
  "source": {
    "type": "csv_url",
    "url": "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
  },
  "destination": {
    "type": "s3",
    "bucket": "relay-data-poc",
    "path": "iris/"
  },
  "options": {
    "format": "parquet",  // Optional: parquet, csv, json
    "compression": "gzip"  // Optional: gzip, none
  }
}
```

**Response (Success):**
```json
{
  "status": "created",
  "pipeline_id": "pipe-001",
  "name": "Iris Dataset Pipeline",
  "source": "csv_url → https://archive.ics.uci.edu/...",
  "destination": "s3://relay-data-poc/iris/",
  "next_steps": [
    "Test: POST /pipeline/pipe-001/run",
    "Monitor: GET /pipeline/pipe-001/status"
  ],
  "created_at": "2026-02-05T17:45:00Z"
}
```

**Response (Error - Missing Info):**
```json
{
  "status": "needs_input",
  "missing": [
    {
      "field": "destination.bucket",
      "help": "Specify S3 bucket name"
    }
  ]
}
```

---

### 3. GET `/pipeline/list`
**Purpose:** List all pipelines

**Request:**
```http
GET /api/v1/pipeline/list
```

**Response:**
```json
{
  "pipelines": [
    {
      "id": "pipe-001",
      "name": "Iris Dataset Pipeline",
      "source_type": "csv_url",
      "destination_type": "s3",
      "status": "active",
      "last_run": "2026-02-05T17:50:00Z",
      "rows_processed": 150
    },
    {
      "id": "pipe-002",
      "name": "Customer Data Export",
      "source_type": "csv_url",
      "destination_type": "s3",
      "status": "pending",
      "last_run": null,
      "rows_processed": 0
    }
  ],
  "total": 2
}
```

---

### 4. GET `/pipeline/{id}`
**Purpose:** Get pipeline details

**Request:**
```http
GET /api/v1/pipeline/pipe-001
```

**Response:**
```json
{
  "id": "pipe-001",
  "name": "Iris Dataset Pipeline",
  "description": "Move Iris CSV to S3",
  "source": {
    "type": "csv_url",
    "url": "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
  },
  "destination": {
    "type": "s3",
    "bucket": "relay-data-poc",
    "path": "iris/",
    "format": "parquet",
    "compression": "gzip"
  },
  "status": "active",
  "created_at": "2026-02-05T17:45:00Z",
  "last_run": {
    "timestamp": "2026-02-05T17:50:00Z",
    "status": "success",
    "rows_processed": 150,
    "duration_seconds": 3.2,
    "output_file": "s3://relay-data-poc/iris/2026-02-05-175000.parquet"
  },
  "next_steps": [
    "Run again: POST /pipeline/pipe-001/run",
    "View data: aws s3 ls s3://relay-data-poc/iris/"
  ]
}
```

---

### 5. POST `/pipeline/{id}/run`
**Purpose:** Execute a pipeline

**Request:**
```http
POST /api/v1/pipeline/pipe-001/run
```

**Response:**
```json
{
  "status": "running",
  "run_id": "run-001",
  "pipeline_id": "pipe-001",
  "started_at": "2026-02-05T18:00:00Z",
  "progress": "Fetching source data...",
  "check_status": "GET /pipeline/pipe-001/run/run-001"
}
```

---

### 6. GET `/pipeline/{id}/run/{run_id}`
**Purpose:** Check run status

**Request:**
```http
GET /api/v1/pipeline/pipe-001/run/run-001
```

**Response (Running):**
```json
{
  "status": "running",
  "run_id": "run-001",
  "progress": "Writing to S3...",
  "started_at": "2026-02-05T18:00:00Z",
  "elapsed_seconds": 2.1
}
```

**Response (Complete):**
```json
{
  "status": "success",
  "run_id": "run-001",
  "started_at": "2026-02-05T18:00:00Z",
  "completed_at": "2026-02-05T18:00:03Z",
  "duration_seconds": 3.2,
  "rows_processed": 150,
  "output_file": "s3://relay-data-poc/iris/2026-02-05-180000.parquet",
  "next": "Download: aws s3 cp s3://relay-data-poc/iris/2026-02-05-180000.parquet ."
}
```

---

### 7. POST `/test/source`
**Purpose:** Test if source is accessible before creating pipeline

**Request:**
```json
{
  "type": "csv_url",
  "url": "https://example.com/data.csv"
}
```

**Response (Success):**
```json
{
  "status": "accessible",
  "type": "csv_url",
  "url": "https://example.com/data.csv",
  "preview": {
    "columns": ["col1", "col2", "col3"],
    "rows": 1000,
    "sample": [
      ["value1", "value2", "value3"],
      ["value4", "value5", "value6"]
    ]
  },
  "message": "Source is accessible and ready to use"
}
```

**Response (Error):**
```json
{
  "status": "error",
  "type": "csv_url",
  "url": "https://example.com/data.csv",
  "error": "HTTP 404: Not Found",
  "suggestion": "Check that URL is correct and publicly accessible"
}
```

---

## Data Pipeline Engine

### Pipeline Execution Flow

```python
# Simplified flow
def execute_pipeline(pipeline):
    # 1. Fetch source data
    if pipeline.source.type == "csv_url":
        df = pd.read_csv(pipeline.source.url)
    elif pipeline.source.type == "json_url":
        df = pd.read_json(pipeline.source.url)
    
    # 2. Apply transformations (if any)
    # (V1: none, just pass through)
    
    # 3. Write to destination
    if pipeline.destination.type == "s3":
        output_path = generate_output_path(pipeline)
        write_to_s3(df, output_path, format=pipeline.options.format)
    
    # 4. Return result
    return {
        "status": "success",
        "rows": len(df),
        "output": output_path
    }
```

### Error Handling

Every operation returns clear, actionable errors:

```json
{
  "status": "error",
  "error_code": "source_unreachable",
  "message": "Could not fetch data from source URL",
  "details": "HTTP 404: Not Found",
  "suggestions": [
    "Check that URL is correct",
    "Verify URL is publicly accessible",
    "Test with: POST /test/source"
  ]
}
```

---

## Security Considerations

### V1 Approach (Simple but Functional)

1. **AWS Credentials:**
   - Read from environment variables
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_DEFAULT_REGION`

2. **API Authentication:**
   - Simple token in header: `X-Relay-Token`
   - Or skip for localhost testing

3. **Public Sources Only:**
   - No credentials stored in V1
   - Only fetch from public URLs
   - This avoids credential management complexity

### Future Security (V2+):

- Proper credential vault
- OAuth for API sources
- Encryption at rest
- User authentication
- Role-based access control

---

## Storage Schema

### pipelines.json Structure

```json
{
  "pipelines": [
    {
      "id": "pipe-001",
      "name": "Iris Dataset Pipeline",
      "description": "Move Iris CSV to S3",
      "source": {
        "type": "csv_url",
        "url": "https://..."
      },
      "destination": {
        "type": "s3",
        "bucket": "relay-data-poc",
        "path": "iris/",
        "format": "parquet",
        "compression": "gzip"
      },
      "status": "active",
      "created_at": "2026-02-05T17:45:00Z",
      "runs": [
        {
          "run_id": "run-001",
          "started_at": "2026-02-05T18:00:00Z",
          "completed_at": "2026-02-05T18:00:03Z",
          "status": "success",
          "rows_processed": 150,
          "output_file": "s3://relay-data-poc/iris/2026-02-05-180000.parquet"
        }
      ]
    }
  ]
}
```

Simple JSON file for V1. Easy to read/write, no database needed.

---

## Implementation Plan

### Phase 1: Core API (Day 1)
- [ ] Set up FastAPI project structure
- [ ] Implement `/capabilities` endpoint
- [ ] Implement `/pipeline/create` endpoint
- [ ] Implement `/pipeline/list` endpoint
- [ ] Implement basic JSON storage (pipelines.json)

### Phase 2: Pipeline Engine (Day 2)
- [ ] Build CSV URL → S3 pipeline
- [ ] Implement `/pipeline/{id}/run` endpoint
- [ ] Add error handling
- [ ] Test with Iris dataset

### Phase 3: Testing & Polish (Day 3)
- [ ] Add `/test/source` endpoint
- [ ] Improve error messages
- [ ] Add validation
- [ ] Test full workflow

### Phase 4: Documentation (Day 4)
- [ ] Write agent usage guide
- [ ] Create example requests
- [ ] Document common patterns

---

## Success Metrics

### Compare to Airbyte Experience

**Airbyte (Today):**
- Time to first pipeline: 30 minutes
- Number of API calls: 8-10
- Configuration complexity: High (nested JSON, IDs)
- Errors encountered: 2-3 (region, auth, etc.)

**Relay V1 (Goal):**
- Time to first pipeline: **< 2 minutes**
- Number of API calls: **2-3**
- Configuration complexity: **Low (simple JSON)**
- Errors encountered: **0 (works first try)**

---

## Example: Agent Creates First Pipeline

### Agent Workflow:

```python
# 1. Discover capabilities
GET /capabilities
# Response tells agent everything available

# 2. Create pipeline
POST /pipeline/create
{
  "name": "Test Pipeline",
  "source": {"type": "csv_url", "url": "https://data.csv"},
  "destination": {"type": "s3", "bucket": "my-bucket", "path": "data/"}
}
# Response: pipeline created, here's the ID

# 3. Run pipeline
POST /pipeline/pipe-001/run
# Response: running, here's how to check status

# 4. Check result
GET /pipeline/pipe-001
# Response: success! 150 rows processed

# DONE in 3 API calls!
```

**Total time: ~2 minutes**

Compare to Airbyte: 30+ minutes, 10+ API calls, 3+ errors

---

## Future Enhancements (V2+)

Once V1 proves the concept works:

- **More Sources:** MySQL, Postgres, APIs with auth
- **Scheduling:** Cron-based automatic runs
- **Transformations:** Basic data cleaning/filtering
- **Monitoring:** Pipeline health, alerts
- **Multi-Agent:** Multiple agents, shared pipelines
- **Credential Management:** Secure vault
- **Semantic Layer:** Query data with natural language

---

## Why This Will Work

### 1. **Agent-Friendly Design**
- Self-describing API (agent learns in one call)
- Consistent patterns (same structure every time)
- Clear next steps (response tells agent what to do)

### 2. **Smart Defaults**
- Format: Parquet (efficient)
- Compression: GZIP (standard)
- Path: Auto-generated from pipeline name

### 3. **Forgiving Input**
- Accept partial configs, ask for missing pieces
- Helpful error messages with suggestions
- Test endpoints before committing

### 4. **Proven Components**
- Python + Pandas (we know this works)
- boto3 (we already used it successfully)
- FastAPI (modern, fast, easy)

---

## Next Steps

1. **Review this spec** - does it make sense?
2. **Set up project structure** - create `relay/` folder
3. **Build `/capabilities` endpoint** - prove self-describing works
4. **Build first pipeline** - CSV → S3 (we have the pieces)
5. **Test with you (agent)** - does it feel easier than Airbyte?

---

**Status:** Ready to build  
**Estimated Time:** 3-4 days for V1 MVP  
**Risk:** Low (using proven tech)  
**Potential:** High (solves real agent pain point)

Let's build Relay! 🚀
