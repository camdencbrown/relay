# Configuration Reference

All configuration is via environment variables. Copy `.env.example` to `.env` to get started.

## Application

| Variable | Default | Description |
|---|---|---|
| `DEBUG` | `false` | Enable debug mode |
| `HOST` | `0.0.0.0` | Server bind host |
| `PORT` | `8001` | Server bind port |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `CORS_ORIGINS` | `*` | Comma-separated allowed CORS origins |

## Database

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///relay.db` | SQLAlchemy connection URL |

## AWS / S3

| Variable | Default | Description |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | | AWS secret key |
| `AWS_DEFAULT_REGION` | `us-west-1` | AWS region |
| `S3_BUCKET_NAME` | | Default S3 bucket name |

## Authentication

| Variable | Default | Description |
|---|---|---|
| `REQUIRE_AUTH` | `false` | When `true`, write endpoints require a valid API key |

API keys are generated via `src.auth.generate_api_key()` and stored as SHA-256 hashes in the database.

## AI Semantics

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | | Anthropic API key for Claude. When empty, AI features are disabled gracefully. |
