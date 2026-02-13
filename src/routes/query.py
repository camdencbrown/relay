"""
Query, Schema, and Export endpoints
"""

from datetime import datetime, timezone
from io import BytesIO, StringIO

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from ..query import QueryEngine
from ..schemas import ExportRequest, QueryRequest, SchemaRequest
from ..storage import Storage

router = APIRouter()

_storage = Storage()
_query_engine = QueryEngine(_storage)


@router.post("/query")
async def query_data(request: QueryRequest):
    try:
        result = _query_engine.execute_query(
            pipelines=request.pipelines,
            sql=request.sql,
            limit=request.limit,
        )

        hints = []
        if result["row_count"] == 0:
            hints = [
                "Query returned 0 rows - check your filter conditions",
                "Use POST /schema to see sample values for columns",
                "Try removing filters one at a time to debug",
            ]

        return {
            "status": "success",
            **result,
            "hints": hints if hints else None,
            "next_steps": (
                ["Refine query if needed", "Use POST /schema to see available columns"]
                if result["row_count"] > 0
                else hints
            ),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/schema")
async def get_schema(request: SchemaRequest):
    try:
        schemas = _query_engine.list_pipeline_schemas(request.pipelines)
        return {
            "status": "success",
            "schemas": schemas,
            "usage_example": {
                "sql": f"SELECT * FROM {list(schemas.values())[0]['table_alias'] if schemas else 'table_name'} LIMIT 10",
                "explanation": "Use table_alias values in your SQL queries",
            },
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/export")
async def export_data(request: ExportRequest):
    try:
        result = _query_engine.execute_query(
            pipelines=request.pipelines,
            sql=request.sql,
            limit=10000,
        )

        if result["row_count"] == 0:
            raise HTTPException(status_code=404, detail="Query returned no results")

        df = pd.DataFrame(result["rows"])
        filename = (
            request.filename
            or f"export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.{request.format}"
        )

        if request.format == "csv":
            output = StringIO()
            df.to_csv(output, index=False)
            content = output.getvalue()
            media_type = "text/csv"
        elif request.format == "json":
            content = df.to_json(orient="records", indent=2)
            media_type = "application/json"
        elif request.format == "excel":
            output = BytesIO()
            df.to_excel(output, index=False, engine="openpyxl")
            content = output.getvalue()
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported format: {request.format}")

        return Response(
            content=content,
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "X-Row-Count": str(result["row_count"]),
                "X-Execution-Time-Ms": str(result["execution_time_ms"]),
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
