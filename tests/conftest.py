"""
Test fixtures for Relay
"""

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

# Override settings BEFORE any src imports
os.environ["DATABASE_URL"] = "sqlite://"  # in-memory
os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""
os.environ["REQUIRE_AUTH"] = "false"
os.environ["ANTHROPIC_API_KEY"] = ""
os.environ["ENCRYPTION_KEY"] = "dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3ODk=  "  # will be overwritten below

# Generate a valid Fernet key for tests
from cryptography.fernet import Fernet

os.environ["ENCRYPTION_KEY"] = Fernet.generate_key().decode()

from src.config import Settings, get_settings
from src.database import init_db, _get_engine, _get_session_factory
from src.models import Base
from src.storage import Storage


@pytest.fixture(autouse=True)
def _reset_db():
    """Create a fresh in-memory database for each test."""
    import src.database as db_mod

    # Reset cached engine/session
    db_mod._engine = None
    db_mod._SessionLocal = None

    # Also reset cached settings
    get_settings.cache_clear()

    init_db()
    yield

    # Cleanup
    db_mod._engine = None
    db_mod._SessionLocal = None
    get_settings.cache_clear()


@pytest.fixture
def storage():
    return Storage()


@pytest.fixture
def client():
    # Import after env setup
    from src.main import app

    with TestClient(app, raise_server_exceptions=False) as c:
        yield c
