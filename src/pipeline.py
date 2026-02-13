"""
Pipeline execution engine
Handles data movement from source to destination
"""

import logging
import traceback
from datetime import datetime, timezone
from io import BytesIO, StringIO
from typing import Dict

import pandas as pd

from .connectors import ConnectorRegistry
from .metadata import MetadataGenerator
from .s3 import get_s3_client
from .storage import Storage

logger = logging.getLogger(__name__)


class PipelineEngine:
    """Executes data pipelines."""

    def __init__(self, storage: Storage):
        self.storage = storage
        self.s3_client = get_s3_client()

        # Lazy imports to avoid circular dependencies at module level
        from .ai_semantics import AISemantics
        from .streaming import StreamingPipeline

        self.streaming = StreamingPipeline(self.s3_client)
        self.metadata_gen = MetadataGenerator(self.storage)
        self.ai_semantics = AISemantics()

    async def test_source(self, source_type: str, url: str) -> Dict:
        """Test if source is accessible and preview data."""
        df = ConnectorRegistry.fetch_source({"type": source_type, "url": url})
        return {
            "columns": df.columns.tolist(),
            "rows": len(df),
            "sample": df.head(3).values.tolist(),
        }

    def execute_pipeline(self, pipeline_id: str, run_id: str) -> None:
        """Execute a pipeline (runs in background)."""
        pipeline = self.storage.get_pipeline(pipeline_id)
        if not pipeline:
            return

        # Auto-detect streaming vs in-memory
        streaming_option = pipeline.get("options", {}).get("streaming", "auto")
        if streaming_option == "auto":
            source_type = pipeline["source"]["type"]
            use_streaming = source_type in ("mysql", "postgres", "salesforce", "synthetic")
        else:
            use_streaming = streaming_option is True or streaming_option == "true"

        run = {
            "run_id": run_id,
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "progress": "Starting...",
            "streaming": use_streaming,
        }
        self.storage.add_run(pipeline_id, run)

        try:
            if use_streaming:
                self._execute_streaming(pipeline, run_id, run)
            else:
                self._execute_inmemory(pipeline, run_id, run)
        except Exception as e:
            run.update(
                {
                    "status": "failed",
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "progress": f"Failed: {e}",
                }
            )
            self.storage.update_run(pipeline_id, run_id, run)

    def _execute_inmemory(self, pipeline: Dict, run_id: str, run: Dict) -> None:
        pipeline_id = pipeline["id"]

        run["progress"] = "Fetching source data..."
        self.storage.update_run(pipeline_id, run_id, run)

        df = ConnectorRegistry.fetch_source(pipeline["source"])
        rows_fetched = len(df)

        run["progress"] = f"Writing {rows_fetched} rows to destination..."
        self.storage.update_run(pipeline_id, run_id, run)

        output_path = self._write_destination(df, pipeline["destination"], pipeline.get("options", {}))

        run.update(
            {
                "status": "success",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "rows_processed": rows_fetched,
                "output_file": output_path,
                "progress": "Complete",
            }
        )
        self._set_duration(run)
        self.storage.update_run(pipeline_id, run_id, run)

        # Generate metadata
        if pipeline.get("generate_metadata", True):
            try:
                metadata = self.metadata_gen.generate_metadata(df, pipeline["name"], pipeline["source"])
                if pipeline.get("ai_semantics", True):
                    sample = df.sample(min(100, len(df)))
                    metadata = self.ai_semantics.enhance_metadata(metadata, sample, context=pipeline["name"])
                self.metadata_gen.save_metadata(metadata, pipeline_id)
                run["metadata_generated"] = True
                run["columns_needing_review"] = metadata.get("columns_needing_review", 0)
                run["ai_enhanced"] = pipeline.get("ai_semantics", True)
                self.storage.update_run(pipeline_id, run_id, run)
            except Exception as e:
                logger.error(f"Metadata generation failed: {e}")

    def _execute_streaming(self, pipeline: Dict, run_id: str, run: Dict) -> None:
        pipeline_id = pipeline["id"]

        run["progress"] = "Starting streaming pipeline..."
        self.storage.update_run(pipeline_id, run_id, run)

        chunks = ConnectorRegistry.fetch_source_streaming(pipeline["source"])

        run["progress"] = "Streaming data..."
        self.storage.update_run(pipeline_id, run_id, run)

        result = self.streaming.write_destination_streaming(
            chunks, pipeline["destination"], pipeline.get("options", {})
        )

        run.update(
            {
                "status": "success",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "rows_processed": result["total_rows"],
                "chunks_processed": result.get("total_chunks", 0),
                "output_file": result.get("primary_file") or result.get("destination"),
                "files_written": result.get("files_written", []),
                "progress": "Complete",
            }
        )
        self._set_duration(run)
        self.storage.update_run(pipeline_id, run_id, run)

    def _write_destination(self, df: pd.DataFrame, destination: Dict, options: Dict) -> str:
        dest_type = destination["type"]
        if dest_type == "s3":
            return self._write_s3(df, destination, options)
        raise ValueError(f"Unsupported destination type: {dest_type}")

    def _write_s3(self, df: pd.DataFrame, destination: Dict, options: Dict) -> str:
        bucket = destination["bucket"]
        path = destination["path"].rstrip("/") + "/"
        fmt = options.get("format", "parquet")
        compression = options.get("compression", "gzip")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")

        if fmt == "parquet":
            filename = f"{timestamp}.parquet"
            buffer = BytesIO()
            comp = compression if compression != "none" else None
            df.to_parquet(buffer, compression=comp, index=False)
            buffer.seek(0)
        elif fmt == "csv":
            filename = f"{timestamp}.csv"
            if compression == "gzip":
                filename += ".gz"
                buffer = BytesIO()
                df.to_csv(buffer, index=False, compression="gzip")
            else:
                buffer = StringIO()
                df.to_csv(buffer, index=False)
            buffer.seek(0)
        elif fmt == "json":
            filename = f"{timestamp}.json"
            if compression == "gzip":
                filename += ".gz"
                buffer = BytesIO()
                df.to_json(buffer, orient="records", compression="gzip")
            else:
                buffer = StringIO()
                df.to_json(buffer, orient="records")
            buffer.seek(0)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        s3_key = path + filename
        content = buffer.getvalue()
        if isinstance(content, str):
            content = content.encode()
        self.s3_client.put_object(Bucket=bucket, Key=s3_key, Body=content)
        return f"s3://{bucket}/{s3_key}"

    @staticmethod
    def _set_duration(run: Dict) -> None:
        start = datetime.fromisoformat(run["started_at"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(run["completed_at"].replace("Z", "+00:00"))
        run["duration_seconds"] = (end - start).total_seconds()
