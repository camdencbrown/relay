"""
Query engine for Relay
Execute SQL queries over pipeline data using DuckDB
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

import duckdb

from .config import get_settings
from .s3 import get_duckdb_s3_config
from .storage import Storage
from .utils import sanitize_table_name

logger = logging.getLogger(__name__)


class QueryEngine:
    """Execute SQL queries over pipeline data."""

    def __init__(self, storage: Storage):
        self.storage = storage

    def execute_query(self, pipelines: List[str], sql: str, limit: int = 1000) -> Dict[str, Any]:
        start_time = datetime.now()
        conn = duckdb.connect(":memory:")

        # Configure S3 access (only needed in S3 storage mode)
        settings = get_settings()
        if settings.storage_mode == "s3":
            s3_cfg = get_duckdb_s3_config()
            if s3_cfg:
                conn.execute(
                    f"""
                    CREATE SECRET secret1 (
                        TYPE S3,
                        KEY_ID '{s3_cfg["key_id"]}',
                        SECRET '{s3_cfg["secret"]}',
                        REGION '{s3_cfg["region"]}'
                    );
                    """
                )

        # Load pipeline data as views
        table_map = {}
        for pipe_id in pipelines:
            pipeline = self.storage.get_pipeline(pipe_id)
            if not pipeline:
                raise ValueError(f"Pipeline not found: {pipe_id}")

            runs = pipeline.get("runs", [])
            successful_runs = [r for r in runs if r["status"] == "success"]
            if not successful_runs:
                raise ValueError(f"No successful runs for pipeline: {pipe_id}")

            s3_path = successful_runs[-1].get("output_file")
            if not s3_path:
                raise ValueError(f"No output file for pipeline: {pipe_id}")

            table_name = sanitize_table_name(pipeline["name"])
            conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{s3_path}')")
            table_map[pipe_id] = table_name

        # Execute user query with limit
        limited_sql = sql
        if "LIMIT" not in sql.upper():
            limited_sql = f"{sql} LIMIT {limit}"

        try:
            result_df = conn.execute(limited_sql).fetchdf()
        except Exception as e:
            conn.close()
            raise ValueError(f"Query execution failed: {e}")

        conn.close()

        # Replace NaN/NaT with None for JSON serialization
        import math
        results = result_df.to_dict("records")
        for row in results:
            for k, v in row.items():
                if isinstance(v, float) and math.isnan(v):
                    row[k] = None
        execution_time = (datetime.now() - start_time).total_seconds() * 1000

        return {
            "rows": results,
            "columns": list(result_df.columns),
            "row_count": len(results),
            "execution_time_ms": round(execution_time, 2),
            "pipelines_used": table_map,
            "query_executed": limited_sql,
        }

    def list_pipeline_schemas(self, pipelines: List[str]) -> Dict[str, Any]:
        schemas = {}

        for pipe_id in pipelines:
            pipeline = self.storage.get_pipeline(pipe_id)
            if not pipeline:
                continue

            table_alias = sanitize_table_name(pipeline["name"])

            schemas[pipe_id] = {
                "name": pipeline["name"],
                "table_alias": table_alias,
                "source": pipeline.get("source", {}).get("type", "unknown"),
                "columns": [],
            }

            # Try to load metadata from storage
            metadata = self.storage.get_metadata(pipe_id)
            if metadata:
                for col in metadata.get("columns", []):
                    schemas[pipe_id]["columns"].append(
                        {
                            "name": col["name"],
                            "type": col.get("type"),
                            "semantic_type": col.get("semantic_type"),
                            "description": col.get("description"),
                            "sample_values": col.get("sample_values", []),
                            "null_percentage": col.get("null_percentage", 0),
                        }
                    )

        return schemas
