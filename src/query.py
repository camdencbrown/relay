"""
Query engine for Relay
Execute SQL queries over pipeline data using DuckDB
"""

import duckdb
import boto3
import os
from typing import List, Dict, Any
from datetime import datetime
from .storage import Storage

class QueryEngine:
    """Execute SQL queries over pipeline data"""
    
    def __init__(self, storage: Storage):
        self.storage = storage
        
        # Initialize S3 client
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
            self.s3_client = boto3.client('s3', region_name=aws_region)
    
    def execute_query(self, pipelines: List[str], sql: str, limit: int = 1000) -> Dict[str, Any]:
        """
        Execute SQL query over pipeline data
        
        Args:
            pipelines: List of pipeline IDs
            sql: SQL query to execute
            limit: Maximum rows to return (safety)
            
        Returns:
            Query results with metadata
        """
        start_time = datetime.now()
        
        # Create DuckDB connection (in-memory)
        conn = duckdb.connect(':memory:')
        
        # Configure S3 access
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-west-1')
        
        if aws_access_key and aws_secret_key:
            conn.execute(f"""
                CREATE SECRET secret1 (
                    TYPE S3,
                    KEY_ID '{aws_access_key}',
                    SECRET '{aws_secret_key}',
                    REGION '{aws_region}'
                );
            """)
        
        # Load pipeline data as views
        table_map = {}
        for pipe_id in pipelines:
            pipeline = self.storage.get_pipeline(pipe_id)
            
            if not pipeline:
                raise ValueError(f"Pipeline not found: {pipe_id}")
            
            # Get latest successful run
            runs = pipeline.get("runs", [])
            successful_runs = [r for r in runs if r["status"] == "success"]
            
            if not successful_runs:
                raise ValueError(f"No successful runs for pipeline: {pipe_id}")
            
            latest_run = successful_runs[-1]
            s3_path = latest_run.get("output_file")
            
            if not s3_path:
                raise ValueError(f"No output file for pipeline: {pipe_id}")
            
            # Create table alias (sanitized pipeline name)
            table_name = pipeline["name"].replace(" ", "_").replace("-", "_").lower()
            
            # Ensure table name doesn't start with a number (SQL syntax error)
            if table_name[0].isdigit():
                table_name = "t_" + table_name
            
            # Handle multiple files (streaming output)
            if s3_path.endswith("*"):
                # Pattern matching for multiple files
                conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{s3_path}')")
            else:
                # Single file
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
            raise ValueError(f"Query execution failed: {str(e)}")
        
        conn.close()
        
        # Convert to records
        results = result_df.to_dict('records')
        
        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return {
            "rows": results,
            "columns": list(result_df.columns),
            "row_count": len(results),
            "execution_time_ms": round(execution_time, 2),
            "pipelines_used": table_map,
            "query_executed": limited_sql
        }
    
    def list_pipeline_schemas(self, pipelines: List[str]) -> Dict[str, Any]:
        """
        Get schema information for pipelines
        Helps agents understand what tables/columns are available
        """
        schemas = {}
        
        for pipe_id in pipelines:
            pipeline = self.storage.get_pipeline(pipe_id)
            
            if not pipeline:
                continue
            
            # Get metadata if available
            metadata_path = f"metadata/{pipe_id}_metadata.json"
            
            # Create table alias
            table_alias = pipeline["name"].replace(" ", "_").replace("-", "_").lower()
            if table_alias[0].isdigit():
                table_alias = "t_" + table_alias
            
            schemas[pipe_id] = {
                "name": pipeline["name"],
                "table_alias": table_alias,
                "source": pipeline["source"]["type"],
                "columns": []
            }
            
            # Try to load metadata
            try:
                import json
                from pathlib import Path
                
                meta_file = Path(__file__).parent.parent / metadata_path
                
                if meta_file.exists():
                    with open(meta_file, 'r') as f:
                        metadata = json.load(f)
                        
                    for col in metadata["columns"]:
                        schemas[pipe_id]["columns"].append({
                            "name": col["name"],
                            "type": col["type"],
                            "semantic_type": col.get("semantic_type"),
                            "description": col.get("description"),
                            "sample_values": col.get("sample_values", []),
                            "null_percentage": col.get("null_percentage", 0)
                        })
            except Exception:
                # Metadata not available, that's OK
                pass
        
        return schemas
