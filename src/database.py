"""
Database session management for Relay
"""

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import get_settings
from .models import Base

_engine = None
_SessionLocal = None


def _get_engine():
    global _engine
    if _engine is None:
        settings = get_settings()
        connect_args = {}
        if settings.database_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        _engine = create_engine(
            settings.database_url,
            connect_args=connect_args,
            pool_pre_ping=True,
        )
    return _engine


def _get_session_factory():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=_get_engine(), expire_on_commit=False)
    return _SessionLocal


def init_db() -> None:
    """Create all tables and run lightweight migrations. Safe to call multiple times."""
    engine = _get_engine()
    Base.metadata.create_all(bind=engine)
    _run_migrations(engine)


def _run_migrations(engine) -> None:
    """Add columns that create_all doesn't handle on existing tables."""
    import sqlalchemy

    with engine.connect() as conn:
        inspector = sqlalchemy.inspect(engine)
        # Add 'role' column to api_keys if missing (added in v2.1)
        if "api_keys" in inspector.get_table_names():
            columns = [c["name"] for c in inspector.get_columns("api_keys")]
            if "role" not in columns:
                conn.execute(sqlalchemy.text("ALTER TABLE api_keys ADD COLUMN role TEXT DEFAULT 'writer'"))
                conn.commit()


@contextmanager
def get_db():
    """Yield a SQLAlchemy session, auto-closing on exit."""
    factory = _get_session_factory()
    session: Session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
