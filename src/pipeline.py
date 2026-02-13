"""
Pipeline execution engine
Handles data movement from source to destination
"""

import pandas as pd
import requests
import boto3
from datetime import datetime
from io import StringIO, BytesIO
import os
from typing import Dict, Any
import traceback

from .storage import Storage
from .streaming import StreamingPipeline
from .metadata import MetadataGenerator
from .ai_semantics import AISemantics

class PipelineEngine:
    """Executes data pipelines"""
    
    def __init__(self, storage: Storage):
        self.storage = storage
        
        # Initialize S3 client with explicit credentials fallback
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-west-1')
        
        if aws_access_key and aws_secret_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
        else:
            # Fall back to default credential chain (IAM roles, etc.)
            self.s3_client = boto3.client('s3', region_name=aws_region)
        
        # Initialize streaming pipeline
        self.streaming = StreamingPipeline(self.s3_client)
        
        # Initialize metadata generator
        self.metadata_gen = MetadataGenerator()
        
        # Initialize AI semantics
        self.ai_semantics = AISemantics()
    
    async def test_source(self, source_type: str, url: str) -> Dict:
        """Test if source is accessible and preview data"""
        if source_type == "csv_url":
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # Parse CSV
            df = pd.read_csv(StringIO(response.text))
            
            return {
                "columns": df.columns.tolist(),
                "rows": len(df),
                "sample": df.head(3).values.tolist()
            }
        
        elif source_type == "json_url":
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # Parse JSON
            df = pd.read_json(StringIO(response.text))
            
            return {
                "columns": df.columns.tolist(),
                "rows": len(df),
                "sample": df.head(3).values.tolist()
            }
        
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def execute_pipeline(self, pipeline_id: str, run_id: str):
        """Execute a pipeline (runs in background)"""
        pipeline = self.storage.get_pipeline(pipeline_id)
        
        if not pipeline:
            return
        
        # Check if streaming is enabled (or auto-detect)
        streaming_option = pipeline.get("options", {}).get("streaming", "auto")
        
        if streaming_option == "auto":
            # Auto-detect: use streaming for database sources, Salesforce, and synthetic data
            source_type = pipeline["source"]["type"]
            use_streaming = source_type in ["mysql", "postgres", "salesforce", "synthetic"]
        else:
            use_streaming = streaming_option == True or streaming_option == "true"
        
        # Create run record
        run = {
            "run_id": run_id,
            "status": "running",
            "started_at": datetime.utcnow().isoformat() + "Z",
            "progress": "Starting...",
            "streaming": use_streaming
        }
        self.storage.add_run(pipeline_id, run)
        
        try:
            if use_streaming:
                # Use streaming for large datasets
                self._execute_streaming(pipeline, run_id, run)
            else:
                # Use in-memory (original approach) for small datasets
                self._execute_inmemory(pipeline, run_id, run)
                
        except Exception as e:
            # Handle error
            run.update({
                "status": "failed",
                "completed_at": datetime.utcnow().isoformat() + "Z",
                "error": str(e),
                "traceback": traceback.format_exc(),
                "progress": f"Failed: {str(e)}"
            })
            self.storage.update_run(pipeline_id, run_id, run)
    
    def _execute_inmemory(self, pipeline: Dict, run_id: str, run: Dict):
        """Original in-memory execution (for datasets < 100k rows)"""
        pipeline_id = pipeline["id"]
        
        # Step 1: Fetch data from source
        run["progress"] = "Fetching source data..."
        self.storage.update_run(pipeline_id, run_id, run)
        
        df = self._fetch_source(pipeline["source"])
        rows_fetched = len(df)
        
        # Step 2: Write to destination
        run["progress"] = f"Writing {rows_fetched} rows to destination..."
        self.storage.update_run(pipeline_id, run_id, run)
        
        output_path = self._write_destination(
            df, 
            pipeline["destination"],
            pipeline["options"]
        )
        
        # Step 3: Complete
        run.update({
            "status": "success",
            "completed_at": datetime.utcnow().isoformat() + "Z",
            "rows_processed": rows_fetched,
            "output_file": output_path,
            "progress": "Complete"
        })
        
        # Calculate duration
        start_time = datetime.fromisoformat(run["started_at"].replace("Z", ""))
        end_time = datetime.fromisoformat(run["completed_at"].replace("Z", ""))
        run["duration_seconds"] = (end_time - start_time).total_seconds()
        
        self.storage.update_run(pipeline_id, run_id, run)
        
        # Generate metadata if enabled
        if pipeline.get("generate_metadata", True):
            try:
                metadata = self.metadata_gen.generate_metadata(
                    df,
                    pipeline["name"],
                    pipeline["source"]
                )
                
                # Enhance with AI if enabled
                if pipeline.get("ai_semantics", True):
                    sample = df.sample(min(100, len(df)))  # Sample for AI analysis
                    metadata = self.ai_semantics.enhance_metadata(
                        metadata,
                        sample,
                        context=pipeline["name"]
                    )
                
                self.metadata_gen.save_metadata(metadata, pipeline_id)
                
                # Update run with metadata info
                run["metadata_generated"] = True
                run["columns_needing_review"] = metadata.get("columns_needing_review", 0)
                run["ai_enhanced"] = pipeline.get("ai_semantics", True)
                self.storage.update_run(pipeline_id, run_id, run)
            except Exception as e:
                # Don't fail pipeline if metadata generation fails
                import logging
                logging.error(f"Metadata generation failed: {e}")
    
    def _execute_streaming(self, pipeline: Dict, run_id: str, run: Dict):
        """Streaming execution (for large datasets)"""
        pipeline_id = pipeline["id"]
        
        # Step 1: Start streaming from source
        run["progress"] = "Starting streaming pipeline..."
        self.storage.update_run(pipeline_id, run_id, run)
        
        chunks = self.streaming.fetch_source_streaming(pipeline["source"])
        
        # Step 2: Stream to destination
        run["progress"] = "Streaming data..."
        self.storage.update_run(pipeline_id, run_id, run)
        
        result = self.streaming.write_destination_streaming(
            chunks,
            pipeline["destination"],
            pipeline["options"]
        )
        
        # Step 3: Complete
        run.update({
            "status": "success",
            "completed_at": datetime.utcnow().isoformat() + "Z",
            "rows_processed": result["total_rows"],
            "chunks_processed": result.get("total_chunks", 0),
            "output_file": result.get("primary_file") or result.get("destination"),
            "files_written": result.get("files_written", []),
            "progress": "Complete"
        })
        
        # Calculate duration
        start_time = datetime.fromisoformat(run["started_at"].replace("Z", ""))
        end_time = datetime.fromisoformat(run["completed_at"].replace("Z", ""))
        run["duration_seconds"] = (end_time - start_time).total_seconds()
        
        self.storage.update_run(pipeline_id, run_id, run)
    
    def _fetch_source(self, source: Dict) -> pd.DataFrame:
        """Fetch data from source"""
        source_type = source["type"]
        
        if source_type == "csv_url":
            url = source["url"]
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return pd.read_csv(StringIO(response.text))
        
        elif source_type == "json_url":
            url = source["url"]
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return pd.read_json(StringIO(response.text))
        
        elif source_type == "rest_api":
            # Generic REST API connector
            url = source["url"]
            method = source.get("method", "GET")
            headers = source.get("headers", {})
            
            response = requests.request(method, url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different response structures
            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict):
                # Try common patterns for data arrays
                for key in ["data", "results", "items", "records"]:
                    if key in data and isinstance(data[key], list):
                        return pd.DataFrame(data[key])
                # Single object - wrap in list
                return pd.DataFrame([data])
            else:
                raise ValueError(f"Unsupported response type: {type(data)}")
        
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _write_destination(self, df: pd.DataFrame, destination: Dict, options: Dict) -> str:
        """Write data to destination"""
        dest_type = destination["type"]
        
        if dest_type == "s3":
            return self._write_s3(df, destination, options)
        else:
            raise ValueError(f"Unsupported destination type: {dest_type}")
    
    def _write_s3(self, df: pd.DataFrame, destination: Dict, options: Dict) -> str:
        """Write data to S3"""
        bucket = destination["bucket"]
        path = destination["path"].rstrip("/") + "/"
        format_type = options.get("format", "parquet")
        compression = options.get("compression", "gzip")
        
        # Generate filename with timestamp
        timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H%M%S")
        
        if format_type == "parquet":
            filename = f"{timestamp}.parquet"
            buffer = BytesIO()
            
            # Apply compression if requested
            comp = compression if compression != "none" else None
            df.to_parquet(buffer, compression=comp, index=False)
            
            buffer.seek(0)
            
        elif format_type == "csv":
            filename = f"{timestamp}.csv"
            if compression == "gzip":
                filename += ".gz"
                buffer = BytesIO()
                df.to_csv(buffer, index=False, compression="gzip")
            else:
                buffer = StringIO()
                df.to_csv(buffer, index=False)
            
            buffer.seek(0)
        
        elif format_type == "json":
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
            raise ValueError(f"Unsupported format: {format_type}")
        
        # Upload to S3
        s3_key = path + filename
        self.s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=buffer.getvalue() if isinstance(buffer, BytesIO) else buffer.getvalue().encode()
        )
        
        return f"s3://{bucket}/{s3_key}"
