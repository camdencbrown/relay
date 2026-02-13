"""
Transformation pipeline - Multi-source joins and aggregations
Enables agent-native data transformation
"""

import pandas as pd
import requests
from typing import Dict, List, Any
from io import StringIO


class TransformationEngine:
    """Handles multi-source transformations"""
    
    def __init__(self, pipeline_engine):
        self.pipeline_engine = pipeline_engine
    
    def execute_transformation(self, config: Dict) -> pd.DataFrame:
        """
        Execute a transformation pipeline that combines multiple sources
        
        Config format:
        {
            "sources": [
                {"type": "rest_api", "url": "...", "alias": "users"},
                {"type": "rest_api", "url": "...", "alias": "posts"}
            ],
            "join": {
                "left": "users",
                "right": "posts",
                "on": "users.id = posts.userId",
                "how": "left"  # left, right, inner, outer
            },
            "aggregate": {  # optional
                "group_by": ["users.name"],
                "metrics": {
                    "post_count": "COUNT(posts.id)",
                    "avg_length": "AVG(posts.body_length)"
                }
            }
        }
        """
        
        # Step 1: Fetch all sources
        dataframes = {}
        for source in config["sources"]:
            alias = source["alias"]
            df = self._fetch_source(source)
            dataframes[alias] = df
        
        # Step 2: Perform join if specified
        if "join" in config:
            result_df = self._perform_join(dataframes, config["join"])
        else:
            # If no join, just use first source
            result_df = list(dataframes.values())[0]
        
        # Step 3: Aggregate if specified
        if "aggregate" in config:
            result_df = self._perform_aggregate(result_df, config["aggregate"])
        
        return result_df
    
    def _fetch_source(self, source: Dict) -> pd.DataFrame:
        """Fetch data from a source"""
        source_type = source.get("type", "pipeline")
        
        # NEW: Support pipeline IDs
        if source_type == "pipeline" or "pipeline_id" in source:
            pipeline_id = source.get("pipeline_id") or source.get("id")
            if not pipeline_id:
                raise ValueError("Pipeline source requires 'pipeline_id' field")
            
            # Get pipeline from storage
            pipeline = self.pipeline_engine.storage.get_pipeline(pipeline_id)
            if not pipeline:
                raise ValueError(f"Pipeline not found: {pipeline_id}")
            
            # Get latest successful run
            runs = pipeline.get("runs", [])
            successful_runs = [r for r in runs if r["status"] == "success"]
            if not successful_runs:
                raise ValueError(f"No successful runs for pipeline: {pipeline_id}")
            
            latest_run = successful_runs[-1]
            s3_path = latest_run.get("output_file")
            if not s3_path:
                raise ValueError(f"No output file for pipeline: {pipeline_id}")
            
            # Read from S3 using pandas
            df = pd.read_parquet(s3_path)
            return df
        
        elif source_type == "rest_api" or source_type == "json_url":
            response = requests.get(source["url"], timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Handle different JSON structures
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Check if there's a 'results' or 'data' key
                if "results" in data:
                    df = pd.DataFrame(data["results"])
                elif "data" in data:
                    df = pd.DataFrame(data["data"])
                else:
                    # Treat dict as single row
                    df = pd.DataFrame([data])
            else:
                raise ValueError(f"Unexpected JSON structure: {type(data)}")
            
            return df
            
        elif source_type == "csv_url":
            response = requests.get(source["url"], timeout=30)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text))
            return df
        
        else:
            raise ValueError(f"Unsupported source type for transformation: {source_type}")
    
    def _perform_join(self, dataframes: Dict[str, pd.DataFrame], join_config: Dict) -> pd.DataFrame:
        """
        Perform join between two dataframes
        
        join_config:
        {
            "left": "users",
            "right": "posts",
            "on": "users.id = posts.userId",
            "how": "left"
        }
        """
        left_alias = join_config["left"]
        right_alias = join_config["right"]
        
        left_df = dataframes[left_alias]
        right_df = dataframes[right_alias]
        
        # Parse join condition: "users.id = posts.userId"
        on_clause = join_config["on"]
        parts = on_clause.split("=")
        left_key = parts[0].strip().split(".")[-1]  # Extract "id" from "users.id"
        right_key = parts[1].strip().split(".")[-1]  # Extract "userId" from "posts.userId"
        
        how = join_config.get("how", "left")
        
        # Perform the join
        result = left_df.merge(
            right_df,
            left_on=left_key,
            right_on=right_key,
            how=how,
            suffixes=("", "_right")
        )
        
        return result
    
    def _perform_aggregate(self, df: pd.DataFrame, agg_config: Dict) -> pd.DataFrame:
        """
        Perform aggregation
        
        agg_config:
        {
            "group_by": ["name"],
            "metrics": {
                "post_count": "COUNT(id)",
                "total_value": "SUM(value)"
            }
        }
        """
        group_by = agg_config["group_by"]
        metrics = agg_config["metrics"]
        
        # Build aggregation dict
        agg_dict = {}
        for metric_name, metric_expr in metrics.items():
            # Parse simple expressions like "COUNT(id)", "SUM(value)", "AVG(score)"
            if "COUNT(" in metric_expr:
                col = metric_expr.split("(")[1].split(")")[0]
                agg_dict[col] = ("count", metric_name)
            elif "SUM(" in metric_expr:
                col = metric_expr.split("(")[1].split(")")[0]
                agg_dict[col] = ("sum", metric_name)
            elif "AVG(" in metric_expr:
                col = metric_expr.split("(")[1].split(")")[0]
                agg_dict[col] = ("mean", metric_name)
            elif "MAX(" in metric_expr:
                col = metric_expr.split("(")[1].split(")")[0]
                agg_dict[col] = ("max", metric_name)
            elif "MIN(" in metric_expr:
                col = metric_expr.split("(")[1].split(")")[0]
                agg_dict[col] = ("min", metric_name)
        
        # Group and aggregate
        grouped = df.groupby(group_by)
        
        # Apply aggregations
        result_parts = []
        for col, (func, new_name) in agg_dict.items():
            if func == "count":
                result_parts.append(grouped[col].count().rename(new_name))
            elif func == "sum":
                result_parts.append(grouped[col].sum().rename(new_name))
            elif func == "mean":
                result_parts.append(grouped[col].mean().rename(new_name))
            elif func == "max":
                result_parts.append(grouped[col].max().rename(new_name))
            elif func == "min":
                result_parts.append(grouped[col].min().rename(new_name))
        
        # Combine results
        result = pd.concat(result_parts, axis=1).reset_index()
        
        return result
