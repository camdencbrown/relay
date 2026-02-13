# Contributing to Relay

## Development Setup

```bash
git clone <repo-url> && cd relay
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,connectors,ai]"
cp .env.example .env
```

## Running Tests

```bash
pytest                          # All tests
pytest tests/test_storage.py    # Single file
pytest --cov=src                # With coverage
pytest -v                       # Verbose
```

## Linting

```bash
ruff check src/
ruff format src/    # Auto-format
```

## Project Structure

- `src/` -- Application source code
- `src/routes/` -- API endpoint files (one per domain)
- `tests/` -- Pytest test suite
- `tests/test_api/` -- Integration tests using FastAPI TestClient
- `scripts/` -- One-off utility scripts (e.g., migration)

## Pull Request Guidelines

1. Create a feature branch from `main`
2. Write tests for new functionality
3. Ensure `pytest` passes and `ruff check src/` is clean
4. Keep PRs focused -- one feature or fix per PR
5. Update documentation if changing public APIs
