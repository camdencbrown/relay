# Relay

**Agent-Native Data Movement and Analytics Platform**

Relay is a FastAPI + DuckDB + S3 platform built for AI agents to ingest, query, transform, and export data through a self-describing API. Point it at any data source, and it handles the rest.

## Features

- **Agent-First API** -- Single `/capabilities` endpoint teaches an agent the entire API
- **Multi-Source Ingestion** -- CSV/JSON URLs, REST APIs, MySQL, PostgreSQL, Salesforce, synthetic data
- **DuckDB Query Engine** -- Full SQL (JOINs, CTEs, window functions) over S3 Parquet files
- **Streaming Pipeline** -- Millions of rows with chunked processing and parallel S3 writes
- **AI Semantics** -- Anthropic Claude generates column descriptions and business meanings
- **SQLite Storage** -- Pipeline configs, run history, and metadata in a single database file
- **Auth** -- SHA-256 hashed API keys, optional enforcement via `REQUIRE_AUTH`

## Quick Start

### Local

```bash
# Clone and install
git clone <repo-url> && cd relay
pip install -e ".[dev,connectors,ai]"

# Configure
cp .env.example .env
# Edit .env with your AWS credentials

# Run
python -m src.main
```

### Docker

```bash
cp .env.example .env
# Edit .env
docker compose up --build
```

The API is available at `http://localhost:8001`. Start with `GET /api/v1/capabilities`.

## Architecture

```
src/
  main.py            # FastAPI app, lifespan, health check
  config.py          # pydantic-settings centralized config
  database.py        # SQLAlchemy session management
  models.py          # SQLAlchemy ORM models
  storage.py         # SQLite-backed CRUD (dict in/out)
  connectors.py      # ConnectorRegistry -- all source types
  pipeline.py        # Pipeline execution engine
  streaming.py       # Chunked write to S3/Postgres
  query.py           # DuckDB query engine
  transform.py       # DuckDB-based joins/aggregations
  metadata.py        # Column profiling & knowledge base
  ai_semantics.py    # Anthropic Claude integration
  auth.py            # API key hashing & FastAPI Depends
  s3.py              # S3 client factory
  routes/            # API endpoints split by domain
```

## Configuration

All settings are environment variables (see [docs/configuration.md](docs/configuration.md)):

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///relay.db` | SQLAlchemy database URL |
| `AWS_ACCESS_KEY_ID` | | AWS credentials for S3 |
| `AWS_SECRET_ACCESS_KEY` | | AWS credentials for S3 |
| `S3_BUCKET_NAME` | | Default S3 bucket |
| `REQUIRE_AUTH` | `false` | Enforce API key auth |
| `ANTHROPIC_API_KEY` | | Enable AI semantics |
| `LOG_LEVEL` | `INFO` | Logging level |

## API Overview

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/capabilities` | GET | Self-describing API discovery |
| `/api/v1/pipeline/create` | POST | Create a data pipeline |
| `/api/v1/pipeline/list` | GET | List all pipelines |
| `/api/v1/pipeline/{id}/run` | POST | Execute a pipeline |
| `/api/v1/query` | POST | SQL query over pipeline data |
| `/api/v1/schema` | POST | Get table schemas |
| `/api/v1/export` | POST | Export as CSV/JSON/Excel |
| `/api/v1/datasets/search` | GET | Search datasets |
| `/api/v1/pipeline/create-transformation` | POST | Join/aggregate sources |

See [docs/api.md](docs/api.md) for the full reference.

## Development

```bash
pip install -e ".[dev,connectors,ai]"
pytest                    # Run tests
pytest --cov=src          # With coverage
ruff check src/           # Lint
```

## License

MIT
