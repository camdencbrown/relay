"""
Admin endpoints - API key management.
"""

from fastapi import APIRouter, Depends, HTTPException

from ..auth import generate_api_key, require_role
from ..schemas import CreateAPIKeyRequest
from ..storage import Storage

router = APIRouter(prefix="/admin", tags=["admin"])

_storage = Storage()


@router.post("/api-keys")
async def create_api_key(req: CreateAPIKeyRequest, _key: str = Depends(require_role("admin"))):
    """Create a new API key. Returns the raw key once."""
    raw_key = generate_api_key(name=req.name, description=req.description or "", role=req.role)
    return {
        "status": "created",
        "key": raw_key,
        "name": req.name,
        "role": req.role,
        "message": "Store this key securely - it will not be shown again.",
    }


@router.get("/api-keys")
async def list_api_keys(_key: str = Depends(require_role("admin"))):
    """List all API keys (without raw key values)."""
    keys = _storage.list_api_keys()
    return {"api_keys": keys, "count": len(keys)}


@router.delete("/api-keys/{key_id}")
async def deactivate_api_key(key_id: int, _key: str = Depends(require_role("admin"))):
    """Deactivate an API key."""
    if not _storage.deactivate_api_key(key_id):
        raise HTTPException(status_code=404, detail=f"API key {key_id} not found")
    return {"status": "deactivated", "id": key_id}
