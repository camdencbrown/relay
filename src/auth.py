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

ROLE_HIERARCHY = {"reader": 0, "writer": 1, "admin": 2}


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def generate_api_key(name: str, description: str = "", role: str = "writer") -> str:
    """Generate a new API key. Returns the raw key (store securely -- only shown once)."""
    if role not in ROLE_HIERARCHY:
        raise ValueError(f"Invalid role '{role}'. Must be one of: {sorted(ROLE_HIERARCHY.keys())}")

    raw_key = f"relay_{secrets.token_urlsafe(32)}"
    key_hash = _hash_key(raw_key)
    prefix = raw_key[:12]

    with get_db() as db:
        row = APIKey(
            key_hash=key_hash,
            key_prefix=prefix,
            name=name,
            description=description,
            role=role,
            active=True,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        db.add(row)

    logger.info(f"Generated API key for '{name}' (prefix: {prefix}..., role: {role})")
    return raw_key


def validate_api_key(raw_key: str) -> bool:
    """Check if an API key is valid and active."""
    key_hash = _hash_key(raw_key)
    with get_db() as db:
        row = db.query(APIKey).filter(APIKey.key_hash == key_hash, APIKey.active == True).first()  # noqa: E712
        return row is not None


def get_api_key_record(raw_key: str) -> Optional[dict]:
    """Look up an API key and return its full record (as dict), or None."""
    key_hash = _hash_key(raw_key)
    with get_db() as db:
        row = db.query(APIKey).filter(APIKey.key_hash == key_hash, APIKey.active == True).first()  # noqa: E712
        return row.to_dict() if row else None


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


def require_role(min_role: str):
    """Factory that returns a FastAPI dependency enforcing a minimum role level.

    Usage: Depends(require_role("admin"))
    """
    min_level = ROLE_HIERARCHY[min_role]

    async def _dependency(api_key: str = Security(api_key_header)) -> str:
        settings = get_settings()
        if not settings.require_auth:
            return "dev_mode"

        if api_key is None:
            raise HTTPException(status_code=401, detail="Missing API key. Provide X-API-Key header.")

        record = get_api_key_record(api_key)
        if not record:
            raise HTTPException(status_code=403, detail="Invalid or revoked API key.")

        user_level = ROLE_HIERARCHY.get(record.get("role", "reader"), 0)
        if user_level < min_level:
            raise HTTPException(
                status_code=403,
                detail=f"Insufficient permissions. Requires '{min_role}' role or higher.",
            )

        return api_key

    return _dependency


async def optional_api_key(api_key: str = Security(api_key_header)) -> Optional[str]:
    """FastAPI dependency: optional auth (doesn't fail if no key)."""
    if api_key and validate_api_key(api_key):
        return api_key
    return None
