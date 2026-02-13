"""
Streaming pipeline engine for large datasets
Handles millions of rows without loading everything into memory
Supports parallel processing for maximum throughput
"""

import pandas as pd
import requests
from typing import Dict, Iterator, Any, List
from io import StringIO, BytesIO
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

logger = logging.getLogger(__name__)

class StreamingPipeline:
    """Handles large dataset processing in chunks"""
    
    def __init__(self, s3_client):
        self.s3_client = s3_client
        self.chunk_size = 10000  # Configurable chunk size
        self.max_workers = 5  # Parallel workers (auto-scaled based on dataset size)
    
    def fetch_source_streaming(self, source: Dict) -> Iterator[pd.DataFrame]:
        """
        Fetch data from source in chunks
        Yields DataFrame chunks instead of loading all at once
        """
        source_type = source["type"]
        
        if source_type == "csv_url":
            yield from self._fetch_csv_streaming(source)
        
        elif source_type == "mysql":
            yield from self._fetch_mysql_streaming(source)
        
        elif source_type == "postgres":
            yield from self._fetch_postgres_streaming(source)
        
        elif source_type == "rest_api":
            # REST APIs typically return all at once or have pagination
            # For now, fetch all and yield as single chunk
            # TODO: Add pagination support per API
            yield self._fetch_rest_api_single(source)
        
        elif source_type == "synthetic":
            # Synthetic data generator
            yield from self._fetch_synthetic_streaming(source)
        
        elif source_type == "salesforce":
            # Salesforce uses bulk API which streams naturally
            yield from self._fetch_salesforce_streaming(source)
        
        else:
            raise ValueError(f"Streaming not supported for source type: {source_type}")
    
    def _fetch_csv_streaming(self, source: Dict) -> Iterator[pd.DataFrame]:
        """Stream CSV data in chunks"""
        url = source["url"]
        
        # Download CSV and read in chunks
        response = requests.get(url, timeout=30, stream=True)
        response.raise_for_status()
        
        # Read CSV in chunks
        for chunk in pd.read_csv(
            StringIO(response.text),
            chunksize=self.chunk_size
        ):
            yield chunk
    
    def _fetch_mysql_streaming(self, source: Dict) -> Iterator[pd.DataFrame]:
        """Stream MySQL data with LIMIT/OFFSET pagination"""
        from sqlalchemy import create_engine
        
        host = source["host"]
        port = source.get("port", 3306)
        database = source["database"]
        username = source["username"]
        password = source["password"]
        query = source.get("query", f"SELECT * FROM {source.get('table', 'table')}")
        
        # Create connection
        connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        
        # Stream with chunksize
        for chunk in pd.read_sql(query, engine, chunksize=self.chunk_size):
            yield chunk
        
        engine.dispose()
    
    def _fetch_postgres_streaming(self, source: Dict) -> Iterator[pd.DataFrame]:
        """Stream Postgres data with LIMIT/OFFSET pagination"""
        from sqlalchemy import create_engine
        
        host = source["host"]
        port = source.get("port", 5432)
        database = source["database"]
        username = source["username"]
        password = source["password"]
        query = source.get("query", f"SELECT * FROM {source.get('table', 'table')}")
        
        # Create connection
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        
        # Stream with chunksize
        for chunk in pd.read_sql(query, engine, chunksize=self.chunk_size):
            yield chunk
        
        engine.dispose()
    
    def _fetch_rest_api_single(self, source: Dict) -> pd.DataFrame:
        """Fetch from REST API (non-streaming for now)"""
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
            # Try common patterns
            for key in ["data", "results", "items", "records"]:
                if key in data and isinstance(data[key], list):
                    return pd.DataFrame(data[key])
            # Single object
            return pd.DataFrame([data])
        else:
            raise ValueError(f"Unsupported response type: {type(data)}")
    
    def _fetch_synthetic_streaming(self, source: Dict) -> Iterator[pd.DataFrame]:
        """Generate synthetic data in chunks"""
        from .connectors import SyntheticDataGenerator
        
        yield from SyntheticDataGenerator.generate_streaming(source, self.chunk_size)
    
    def _fetch_salesforce_streaming(self, source: Dict) -> Iterator[pd.DataFrame]:
        """Fetch from Salesforce in chunks using bulk API"""
        from simple_salesforce import Salesforce
        
        # Connect
        username = source["username"]
        password = source["password"]
        security_token = source.get("security_token", "")
        domain = source.get("domain", "login")
        
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain=domain
        )
        
        # Execute query - query_all handles pagination automatically
        query = source["query"]
        result = sf.query_all(query)
        
        # Process in chunks
        records = result['records']
        
        # Clean metadata
        for record in records:
            record.pop('attributes', None)
        
        # Yield in chunks
        for i in range(0, len(records), self.chunk_size):
            chunk = records[i:i + self.chunk_size]
            yield pd.DataFrame(chunk)
    
    def write_destination_streaming(
        self,
        chunks: Iterator[pd.DataFrame],
        destination: Dict,
        options: Dict
    ) -> Dict[str, Any]:
        """
        Write chunks to destination with optional parallelization
        Returns summary of what was written
        """
        dest_type = destination["type"]
        use_parallel = options.get("parallel", True)
        
        if dest_type == "s3":
            if use_parallel:
                return self._write_s3_parallel(chunks, destination, options)
            else:
                return self._write_s3_streaming(chunks, destination, options)
        elif dest_type == "postgres":
            # Postgres writes must be sequential to maintain order
            return self._write_postgres_streaming(chunks, destination, options)
        else:
            raise ValueError(f"Streaming not supported for destination: {dest_type}")
    
    def _write_s3_streaming(
        self,
        chunks: Iterator[pd.DataFrame],
        destination: Dict,
        options: Dict
    ) -> Dict[str, Any]:
        """Write chunks to S3 as separate files or combined"""
        bucket = destination["bucket"]
        path = destination["path"].rstrip("/") + "/"
        format_type = options.get("format", "parquet")
        compression = options.get("compression", "gzip")
        
        # Option 1: Multiple files (one per chunk)
        # Option 2: Single file (combine chunks)
        combine_chunks = options.get("combine_chunks", False)
        
        chunk_num = 0
        total_rows = 0
        files_written = []
        
        if combine_chunks:
            # Combine all chunks into single file
            all_chunks = []
            for chunk in chunks:
                all_chunks.append(chunk)
                total_rows += len(chunk)
                chunk_num += 1
                logger.info(f"Processed chunk {chunk_num}: {len(chunk)} rows")
            
            # Combine and write
            combined = pd.concat(all_chunks, ignore_index=True)
            output_file = self._write_single_file(
                combined, bucket, path, format_type, compression
            )
            files_written.append(output_file)
        
        else:
            # Write each chunk as separate file
            from datetime import datetime
            timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H%M%S")
            
            for chunk in chunks:
                chunk_filename = f"{timestamp}_chunk_{chunk_num:06d}"
                output_file = self._write_single_file(
                    chunk, bucket, path + chunk_filename, format_type, compression
                )
                
                files_written.append(output_file)
                total_rows += len(chunk)
                chunk_num += 1
                
                logger.info(f"Written chunk {chunk_num}: {len(chunk)} rows to {output_file}")
        
        return {
            "total_rows": total_rows,
            "total_chunks": chunk_num,
            "files_written": files_written,
            "primary_file": files_written[0] if files_written else None
        }
    
    def _write_single_file(
        self,
        df: pd.DataFrame,
        bucket: str,
        path: str,
        format_type: str,
        compression: str
    ) -> str:
        """Write a single DataFrame to S3"""
        from datetime import datetime
        
        # Add extension
        if format_type == "parquet":
            filename = path if path.endswith(".parquet") else f"{path}.parquet"
            buffer = BytesIO()
            comp = compression if compression != "none" else None
            df.to_parquet(buffer, compression=comp, index=False)
            buffer.seek(0)
        
        elif format_type == "csv":
            filename = path if path.endswith(".csv") else f"{path}.csv"
            if compression == "gzip":
                filename += ".gz"
                buffer = BytesIO()
                df.to_csv(buffer, index=False, compression="gzip")
            else:
                buffer = StringIO()
                df.to_csv(buffer, index=False)
            buffer.seek(0)
        
        elif format_type == "json":
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
            raise ValueError(f"Unsupported format: {format_type}")
        
        # Upload to S3
        s3_key = filename
        content = buffer.getvalue()
        if isinstance(content, str):
            content = content.encode()
        
        self.s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=content
        )
        
        return f"s3://{bucket}/{s3_key}"
    
    def _write_postgres_streaming(
        self,
        chunks: Iterator[pd.DataFrame],
        destination: Dict,
        options: Dict
    ) -> Dict[str, Any]:
        """Write chunks to Postgres"""
        from sqlalchemy import create_engine
        
        host = destination["host"]
        port = destination.get("port", 5432)
        database = destination["database"]
        username = destination["username"]
        password = destination["password"]
        table = destination["table"]
        if_exists = destination.get("if_exists", "append")
        
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        
        chunk_num = 0
        total_rows = 0
        
        for chunk in chunks:
            # First chunk might replace/fail, rest append
            write_mode = if_exists if chunk_num == 0 else "append"
            
            chunk.to_sql(table, engine, if_exists=write_mode, index=False)
            
            total_rows += len(chunk)
            chunk_num += 1
            logger.info(f"Written chunk {chunk_num}: {len(chunk)} rows to {table}")
        
        engine.dispose()
        
        return {
            "total_rows": total_rows,
            "total_chunks": chunk_num,
            "destination": f"postgres://{host}:{port}/{database}/{table}"
        }
    
    def _write_s3_parallel(
        self,
        chunks: Iterator[pd.DataFrame],
        destination: Dict,
        options: Dict
    ) -> Dict[str, Any]:
        """
        Write chunks to S3 in parallel for maximum throughput
        Auto-scales workers based on dataset size
        """
        bucket = destination["bucket"]
        path = destination["path"].rstrip("/") + "/"
        format_type = options.get("format", "parquet")
        compression = options.get("compression", "gzip")
        combine_chunks = options.get("combine_chunks", False)
        
        # Collect all chunks first (needed for parallel processing)
        logger.info("Collecting chunks for parallel processing...")
        all_chunks = list(chunks)
        total_chunks = len(all_chunks)
        
        # Auto-scale workers based on dataset size
        if total_chunks < 10:
            workers = 2
        elif total_chunks < 100:
            workers = 5
        elif total_chunks < 1000:
            workers = 10
        else:
            workers = 20  # Max 20 parallel workers
        
        logger.info(f"Processing {total_chunks} chunks with {workers} workers")
        
        if combine_chunks:
            # Combine all chunks into single file (no parallelization needed)
            logger.info("Combining all chunks into single file...")
            combined = pd.concat(all_chunks, ignore_index=True)
            output_file = self._write_single_file(
                combined, bucket, path, format_type, compression
            )
            
            return {
                "total_rows": len(combined),
                "total_chunks": total_chunks,
                "files_written": [output_file],
                "primary_file": output_file
            }
        
        else:
            # Write chunks in parallel
            timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H%M%S")
            
            def write_chunk_task(chunk_data):
                """Task for parallel execution"""
                chunk_num, chunk = chunk_data
                chunk_filename = f"{timestamp}_chunk_{chunk_num:06d}"
                output_file = self._write_single_file(
                    chunk, bucket, path + chunk_filename, format_type, compression
                )
                return {
                    "file": output_file,
                    "rows": len(chunk),
                    "chunk_num": chunk_num
                }
            
            # Process chunks in parallel
            files_written = []
            total_rows = 0
            
            with ThreadPoolExecutor(max_workers=workers) as executor:
                # Submit all tasks
                futures = {
                    executor.submit(write_chunk_task, (i, chunk)): i
                    for i, chunk in enumerate(all_chunks)
                }
                
                # Collect results as they complete
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        files_written.append(result["file"])
                        total_rows += result["rows"]
                        
                        # Progress logging
                        if (result["chunk_num"] + 1) % 100 == 0:
                            logger.info(f"Progress: {result['chunk_num'] + 1}/{total_chunks} chunks written")
                    
                    except Exception as e:
                        logger.error(f"Chunk write failed: {e}")
                        raise
            
            logger.info(f"Parallel write complete: {total_chunks} chunks, {total_rows} rows")
            
            return {
                "total_rows": total_rows,
                "total_chunks": total_chunks,
                "files_written": files_written,
                "primary_file": files_written[0] if files_written else None,
                "workers_used": workers
            }
