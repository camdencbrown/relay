"""
Streaming pipeline engine for large datasets
Handles writes; source fetching is delegated to ConnectorRegistry.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from io import BytesIO, StringIO
from typing import Any, Dict, Iterator

import pandas as pd

from .connectors import PostgresWriter

logger = logging.getLogger(__name__)


class StreamingPipeline:
    """Handles large dataset processing in chunks."""

    def __init__(self, s3_client):
        self.s3_client = s3_client

    def write_destination_streaming(
        self,
        chunks: Iterator[pd.DataFrame],
        destination: Dict,
        options: Dict,
    ) -> Dict[str, Any]:
        dest_type = destination["type"]
        use_parallel = options.get("parallel", True)

        if dest_type == "s3":
            if use_parallel:
                return self._write_s3_parallel(chunks, destination, options)
            return self._write_s3_streaming(chunks, destination, options)
        if dest_type == "postgres":
            return self._write_postgres_streaming(chunks, destination, options)
        raise ValueError(f"Streaming not supported for destination: {dest_type}")

    # ----- S3 sequential -----

    def _write_s3_streaming(
        self, chunks: Iterator[pd.DataFrame], destination: Dict, options: Dict
    ) -> Dict[str, Any]:
        bucket = destination["bucket"]
        path = destination["path"].rstrip("/") + "/"
        fmt = options.get("format", "parquet")
        compression = options.get("compression", "gzip")
        combine = options.get("combine_chunks", False)

        chunk_num = 0
        total_rows = 0
        files_written = []

        if combine:
            all_chunks = []
            for chunk in chunks:
                all_chunks.append(chunk)
                total_rows += len(chunk)
                chunk_num += 1
            combined = pd.concat(all_chunks, ignore_index=True)
            f = self._write_single_file(combined, bucket, path, fmt, compression)
            files_written.append(f)
        else:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
            for chunk in chunks:
                name = f"{ts}_chunk_{chunk_num:06d}"
                f = self._write_single_file(chunk, bucket, path + name, fmt, compression)
                files_written.append(f)
                total_rows += len(chunk)
                chunk_num += 1

        return {
            "total_rows": total_rows,
            "total_chunks": chunk_num,
            "files_written": files_written,
            "primary_file": files_written[0] if files_written else None,
        }

    # ----- S3 parallel -----

    def _write_s3_parallel(
        self, chunks: Iterator[pd.DataFrame], destination: Dict, options: Dict
    ) -> Dict[str, Any]:
        bucket = destination["bucket"]
        path = destination["path"].rstrip("/") + "/"
        fmt = options.get("format", "parquet")
        compression = options.get("compression", "gzip")
        combine = options.get("combine_chunks", False)

        all_chunks = list(chunks)
        total_chunks = len(all_chunks)

        if combine:
            combined = pd.concat(all_chunks, ignore_index=True)
            f = self._write_single_file(combined, bucket, path, fmt, compression)
            return {
                "total_rows": len(combined),
                "total_chunks": total_chunks,
                "files_written": [f],
                "primary_file": f,
            }

        workers = min(max(2, total_chunks // 10 + 1), 20)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")

        def _task(idx_chunk):
            idx, chunk = idx_chunk
            name = f"{ts}_chunk_{idx:06d}"
            out = self._write_single_file(chunk, bucket, path + name, fmt, compression)
            return {"file": out, "rows": len(chunk)}

        files_written = []
        total_rows = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_task, (i, c)): i for i, c in enumerate(all_chunks)}
            for future in as_completed(futures):
                result = future.result()
                files_written.append(result["file"])
                total_rows += result["rows"]

        return {
            "total_rows": total_rows,
            "total_chunks": total_chunks,
            "files_written": files_written,
            "primary_file": files_written[0] if files_written else None,
            "workers_used": workers,
        }

    # ----- single file write -----

    def _write_single_file(
        self, df: pd.DataFrame, bucket: str, path: str, fmt: str, compression: str
    ) -> str:
        if fmt == "parquet":
            filename = path if path.endswith(".parquet") else f"{path}.parquet"
            buffer = BytesIO()
            comp = compression if compression != "none" else None
            df.to_parquet(buffer, compression=comp, index=False)
            buffer.seek(0)
        elif fmt == "csv":
            filename = path if path.endswith(".csv") else f"{path}.csv"
            if compression == "gzip":
                filename += ".gz"
                buffer = BytesIO()
                df.to_csv(buffer, index=False, compression="gzip")
            else:
                buffer = StringIO()
                df.to_csv(buffer, index=False)
            buffer.seek(0)
        elif fmt == "json":
            filename = path if path.endswith(".json") else f"{path}.json"
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

        content = buffer.getvalue()
        if isinstance(content, str):
            content = content.encode()
        self.s3_client.put_object(Bucket=bucket, Key=filename, Body=content)
        return f"s3://{bucket}/{filename}"

    # ----- Postgres -----

    def _write_postgres_streaming(
        self, chunks: Iterator[pd.DataFrame], destination: Dict, options: Dict
    ) -> Dict[str, Any]:
        chunk_num = 0
        total_rows = 0
        for chunk in chunks:
            if_exists = destination.get("if_exists", "append") if chunk_num > 0 else destination.get("if_exists", "replace")
            PostgresWriter.write(chunk, destination, if_exists=if_exists)
            total_rows += len(chunk)
            chunk_num += 1

        return {
            "total_rows": total_rows,
            "total_chunks": chunk_num,
            "destination": f"postgres://{destination['host']}:{destination.get('port', 5432)}/{destination['database']}/{destination['table']}",
        }
