"""
Transformation pipeline endpoint
"""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

from ..auth import require_role
from ..pipeline import PipelineEngine
from ..schemas import TransformationPipelineConfig
from ..storage import Storage
from ..transform import TransformationEngine

router = APIRouter()

_storage = Storage()
_engine = PipelineEngine(_storage)
_transform_engine = TransformationEngine(_engine)


@router.post("/pipeline/create-transformation")
async def create_transformation_pipeline(
    config: TransformationPipelineConfig,
    _key: str = Depends(require_role("writer")),
):
    pipeline_id = f"pipe-{uuid.uuid4().hex[:8]}"

    try:
        result_df = _transform_engine.execute_transformation(config.model_dump())
        output_path = _engine._write_destination(
            result_df,
            config.destination.model_dump(),
            {},
        )

        pipeline = {
            "id": pipeline_id,
            "name": config.name,
            "type": "transformation",
            "config": config.model_dump(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "active",
        }
        _storage.save_pipeline(pipeline)

        return {
            "status": "success",
            "pipeline_id": pipeline_id,
            "rows_processed": len(result_df),
            "output_path": output_path,
            "message": "Transformation pipeline created and executed",
            "next_steps": [
                f"Query data at: {output_path}",
                f"View metadata: GET /api/v1/metadata/{pipeline_id}",
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
