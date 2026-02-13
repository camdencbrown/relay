"""
Metadata endpoints
"""

from fastapi import APIRouter, Depends, HTTPException

from ..auth import require_api_key
from ..metadata import MetadataGenerator
from ..schemas import ApproveColumnRequest
from ..storage import Storage

router = APIRouter()

_storage = Storage()


@router.get("/metadata/{pipeline_id}")
async def get_metadata(pipeline_id: str):
    metadata = _storage.get_metadata(pipeline_id)
    if not metadata:
        # Fall back to file-based metadata for backwards compatibility
        gen = MetadataGenerator(_storage)
        metadata = gen.load_metadata(pipeline_id)
    if not metadata:
        raise HTTPException(
            status_code=404,
            detail={
                "status": "not_found",
                "message": f"No metadata found for pipeline {pipeline_id}",
                "suggestion": "Run the pipeline first to generate metadata",
            },
        )
    return metadata


@router.get("/metadata/review/pending")
async def get_pending_reviews():
    gen = MetadataGenerator(_storage)
    pending = gen.get_pending_reviews()
    return {"status": "success", "pending_count": len(pending), "pending_reviews": pending}


@router.post("/metadata/review/approve")
async def approve_column(
    request: ApproveColumnRequest,
    _key: str = Depends(require_api_key),
):
    gen = MetadataGenerator(_storage)
    gen.approve_column(
        request.column_name,
        request.description,
        request.business_meaning,
        request.verified_by,
    )
    return {
        "status": "approved",
        "column_name": request.column_name,
        "message": f"Column '{request.column_name}' approved and added to knowledge base",
    }
