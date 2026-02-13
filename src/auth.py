"""
Authentication for Relay
SHA-256 hashed API keys stored in SQLite
"""

import hashlib
import logging
import secrets
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

from .config import get_settings
from .database import get_db
from .models import APIKey

logger = logging.getLogger(__name__)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def generate_api_key(name: str, description: str = "") -> str:
    """Generate a new API key. Returns the raw key (store securely -- only shown once)."""
    raw_key = f"relay_{secrets.token_urlsafe(32)}"
    key_hash = _hash_key(raw_key)
    prefix = raw_key[:12]

    with get_db() as db:
        row = APIKey(
            key_hash=key_hash,
            key_prefix=prefix,
            name=name,
            description=description,
            active=True,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        db.add(row)

    logger.info(f"Generated API key for '{name}' (prefix: {prefix}...)")
    return raw_key


def validate_api_key(raw_key: str) -> bool:
    """Check if an API key is valid and active."""
    key_hash = _hash_key(raw_key)
    with get_db() as db:
        row = db.query(APIKey).filter(APIKey.key_hash == key_hash, APIKey.active == True).first()  # noqa: E712
        return row is not None


async def require_api_key(api_key: str = Security(api_key_header)) -> str:
    """FastAPI dependency: require a valid API key on write endpoints.

    When REQUIRE_AUTH=false (default), all requests pass through.
    """
    settings = get_settings()
    if not settings.require_auth:
        return "dev_mode"

    if api_key is None:
        raise HTTPException(status_code=401, detail="Missing API key. Provide X-API-Key header.")

    if not validate_api_key(api_key):
        raise HTTPException(status_code=403, detail="Invalid or revoked API key.")

    return api_key


async def optional_api_key(api_key: str = Security(api_key_header)) -> Optional[str]:
    """FastAPI dependency: optional auth (doesn't fail if no key)."""
    if api_key and validate_api_key(api_key):
        return api_key
    return None
