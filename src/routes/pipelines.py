"""
Pipeline CRUD routes
"""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from ..auth import require_api_key
from ..pipeline import PipelineEngine
from ..schemas import CreatePipelineRequest, PipelineOptions, ScheduleConfig, TestSourceRequest
from ..storage import Storage
from ..utils import sanitize_table_name

router = APIRouter()

# Shared instances
_storage = Storage()
_engine = PipelineEngine(_storage)


def get_storage() -> Storage:
    return _storage


def get_engine() -> PipelineEngine:
    return _engine


@router.post("/pipeline/create")
async def create_pipeline(
    request: CreatePipelineRequest,
    _key: str = Depends(require_api_key),
):
    try:
        pipeline_id = f"pipe-{uuid.uuid4().hex[:8]}"
        options = request.options or PipelineOptions()
        schedule = request.schedule or ScheduleConfig()

        pipeline = {
            "id": pipeline_id,
            "name": request.name,
            "description": request.description or "",
            "source": request.source.model_dump(),
            "destination": request.destination.model_dump(),
            "options": options.model_dump(),
            "schedule": schedule.model_dump(),
            "status": "active",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        _storage.save_pipeline(pipeline)
        table_name = sanitize_table_name(request.name)

        return {
            "status": "created",
            "pipeline_id": pipeline_id,
            "name": request.name,
            "table_name": table_name,
            "source": f"{request.source.type} -> {request.source.url}",
            "destination": f"s3://{request.destination.bucket}/{request.destination.path}",
            "options": options.model_dump(),
            "query_example": f"SELECT * FROM {table_name} LIMIT 10",
            "next_steps": [
                f"Run pipeline: POST /pipeline/{pipeline_id}/run",
                f"View details: GET /pipeline/{pipeline_id}",
                "List all: GET /pipeline/list",
            ],
            "created_at": pipeline["created_at"],
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"status": "error", "message": f"Failed to create pipeline: {e}"},
        )


@router.get("/pipeline/list")
async def list_pipelines():
    pipelines = _storage.list_pipelines()
    result_pipelines = []
    for p in pipelines:
        pipeline_type = p.get("type", "regular")
        if pipeline_type == "transformation":
            result_pipelines.append(
                {
                    "id": p["id"],
                    "name": p["name"],
                    "type": "transformation",
                    "source_count": len(p.get("config", {}).get("sources", [])),
                    "status": p["status"],
                    "created_at": p["created_at"],
                    "last_run": p["runs"][-1] if p.get("runs") else None,
                    "total_runs": len(p.get("runs", [])),
                }
            )
        else:
            result_pipelines.append(
                {
                    "id": p["id"],
                    "name": p["name"],
                    "source_type": p.get("source", {}).get("type", "unknown"),
                    "destination_type": p.get("destination", {}).get("type", "unknown"),
                    "status": p["status"],
                    "created_at": p["created_at"],
                    "last_run": p["runs"][-1] if p.get("runs") else None,
                    "total_runs": len(p.get("runs", [])),
                }
            )

    return {
        "pipelines": result_pipelines,
        "total": len(result_pipelines),
        "next_steps": ["Create new: POST /pipeline/create", "View details: GET /pipeline/{id}"],
    }


@router.get("/pipeline/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    pipeline = _storage.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(
            status_code=404,
            detail={"status": "not_found", "message": f"Pipeline {pipeline_id} not found"},
        )
    return {
        **pipeline,
        "next_steps": [
            f"Run pipeline: POST /pipeline/{pipeline_id}/run",
            "List all: GET /pipeline/list",
        ],
    }


@router.post("/pipeline/{pipeline_id}/run")
async def run_pipeline(
    pipeline_id: str,
    background_tasks: BackgroundTasks,
    _key: str = Depends(require_api_key),
):
    pipeline = _storage.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(
            status_code=404,
            detail={"status": "not_found", "message": f"Pipeline {pipeline_id} not found"},
        )

    run_id = f"run-{uuid.uuid4().hex[:8]}"
    background_tasks.add_task(_engine.execute_pipeline, pipeline_id, run_id)

    return {
        "status": "started",
        "pipeline_id": pipeline_id,
        "run_id": run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "next_steps": [
            f"Check status: GET /pipeline/{pipeline_id}/run/{run_id}",
            f"View pipeline: GET /pipeline/{pipeline_id}",
        ],
    }


@router.get("/pipeline/{pipeline_id}/run/{run_id}")
async def get_run_status(pipeline_id: str, run_id: str):
    pipeline = _storage.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(
            status_code=404,
            detail={"status": "not_found", "message": f"Pipeline {pipeline_id} not found"},
        )

    run = next((r for r in pipeline["runs"] if r["run_id"] == run_id), None)
    if not run:
        raise HTTPException(
            status_code=404,
            detail={"status": "not_found", "message": f"Run {run_id} not found"},
        )

    return {**run, "next_steps": [f"View pipeline: GET /pipeline/{pipeline_id}"]}


@router.delete("/pipeline/{pipeline_id}")
async def delete_pipeline(
    pipeline_id: str,
    _key: str = Depends(require_api_key),
):
    pipeline = _storage.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(
            status_code=404,
            detail={"status": "not_found", "message": f"Pipeline {pipeline_id} not found"},
        )
    _storage.delete_pipeline(pipeline_id)
    return {
        "status": "deleted",
        "pipeline_id": pipeline_id,
        "message": f"Pipeline {pipeline['name']} deleted successfully",
    }


@router.post("/test/source")
async def test_source(request: TestSourceRequest):
    try:
        result = await _engine.test_source(request.type, request.url)
        return {
            "status": "accessible",
            "type": request.type,
            "url": request.url,
            "preview": result,
            "message": "Source is accessible and ready to use",
            "next_steps": ["Create pipeline: POST /pipeline/create"],
        }
    except Exception as e:
        return {
            "status": "error",
            "type": request.type,
            "url": request.url,
            "error": str(e),
            "suggestions": [
                "Check that URL is correct",
                "Verify URL is publicly accessible",
                "Ensure source type matches content",
            ],
        }
