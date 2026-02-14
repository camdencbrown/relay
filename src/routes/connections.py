"""
Connection management endpoints
"""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

from ..auth import require_role
from ..connectors import ConnectorRegistry
from ..schemas import CreateConnectionRequest, UpdateConnectionRequest
from ..storage import Storage

router = APIRouter()
storage = Storage()


def _conn_id() -> str:
    return f"conn-{uuid.uuid4().hex[:8]}"


@router.post("/connection/create")
async def create_connection(req: CreateConnectionRequest, _key: str = Depends(require_role("writer"))):
    """Create a named, encrypted connection."""
    existing = storage.get_connection_by_name(req.name)
    if existing:
        raise HTTPException(status_code=409, detail=f"Connection with name '{req.name}' already exists")

    connection = {
        "id": _conn_id(),
        "name": req.name,
        "type": req.type,
        "description": req.description or "",
        "credentials": req.credentials,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    result = storage.save_connection(connection)
    return {"status": "created", "connection": result}


@router.get("/connection/list")
async def list_connections():
    """List all connections (credentials never included)."""
    connections = storage.list_connections()
    return {"connections": connections, "count": len(connections)}


@router.get("/connection/{connection_id}")
async def get_connection(connection_id: str):
    """Get connection details (credentials never included)."""
    conn = storage.get_connection(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
    return {"connection": conn}


@router.put("/connection/{connection_id}")
async def update_connection(
    connection_id: str, req: UpdateConnectionRequest, _key: str = Depends(require_role("writer"))
):
    """Update connection description or credentials."""
    existing = storage.get_connection(connection_id)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")

    updates = {}
    if req.description is not None:
        updates["description"] = req.description
    if req.credentials is not None:
        updates["credentials"] = req.credentials

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    result = storage.update_connection(connection_id, updates)
    return {"status": "updated", "connection": result}


@router.delete("/connection/{connection_id}")
async def delete_connection(connection_id: str, _key: str = Depends(require_role("admin"))):
    """Delete a connection. Blocked if pipelines reference it."""
    conn = storage.get_connection(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")

    pipelines = storage.list_pipelines_using_connection(conn["name"])
    if pipelines:
        pipeline_names = [p["name"] for p in pipelines]
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete: connection is used by pipelines: {pipeline_names}",
        )

    storage.delete_connection(connection_id)
    return {"status": "deleted", "id": connection_id}


@router.post("/connection/{connection_id}/test")
async def test_connection(connection_id: str, _key: str = Depends(require_role("writer"))):
    """Test a connection's connectivity."""
    conn = storage.get_connection(connection_id, include_credentials=True)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")

    result = ConnectorRegistry.test_connection(conn["type"], conn["credentials"])

    now = datetime.now(timezone.utc).isoformat()
    storage.update_connection(connection_id, {
        "last_tested_at": now,
        "last_test_status": result["status"],
    })

    return {"status": result["status"], "message": result["message"]}
