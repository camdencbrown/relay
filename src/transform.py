"""
Transformation pipeline - Multi-source joins and aggregations
Uses DuckDB for SQL-based transformations instead of Pandas merge/groupby.
"""

import logging
from typing import Dict

import duckdb
import pandas as pd

from .connectors import ConnectorRegistry

logger = logging.getLogger(__name__)


class TransformationEngine:
    """Handles multi-source transformations using DuckDB."""

    def __init__(self, pipeline_engine):
        self.pipeline_engine = pipeline_engine

    def execute_transformation(self, config: Dict) -> pd.DataFrame:
        """Execute a transformation pipeline combining multiple sources."""
        conn = duckdb.connect(":memory:")

        try:
            # Step 1: Fetch all sources and register them as DuckDB tables
            for source in config["sources"]:
                alias = source["alias"]
                df = self._fetch_source(source)
                conn.register(alias, df)
                logger.info(f"Registered source '{alias}' with {len(df)} rows")

            # Step 2: Build and execute SQL
            sql = self._build_sql(config)
            logger.info(f"Executing transformation SQL: {sql}")
            result_df = conn.execute(sql).fetchdf()
            return result_df
        finally:
            conn.close()

    def _fetch_source(self, source: Dict) -> pd.DataFrame:
        source_type = source.get("type", "pipeline")

        if source_type == "pipeline" or "pipeline_id" in source:
            pipeline_id = source.get("pipeline_id") or source.get("id")
            if not pipeline_id:
                raise ValueError("Pipeline source requires 'pipeline_id' field")

            pipeline = self.pipeline_engine.storage.get_pipeline(pipeline_id)
            if not pipeline:
                raise ValueError(f"Pipeline not found: {pipeline_id}")

            runs = pipeline.get("runs", [])
            successful_runs = [r for r in runs if r["status"] == "success"]
            if not successful_runs:
                raise ValueError(f"No successful runs for pipeline: {pipeline_id}")

            s3_path = successful_runs[-1].get("output_file")
            if not s3_path:
                raise ValueError(f"No output file for pipeline: {pipeline_id}")

            return pd.read_parquet(s3_path)

        return ConnectorRegistry.fetch_source(source)

    def _build_sql(self, config: Dict) -> str:
        """Build a SQL query from the transformation config."""
        join_cfg = config.get("join")
        agg_cfg = config.get("aggregate")

        if not join_cfg and not agg_cfg:
            # No transformations, just return first source
            first_alias = config["sources"][0]["alias"]
            return f"SELECT * FROM {first_alias}"

        # Build FROM clause with JOIN
        if join_cfg:
            left = join_cfg["left"]
            right = join_cfg["right"]
            how = join_cfg.get("how", "left").upper()
            if how == "LEFT":
                how = "LEFT"
            elif how == "RIGHT":
                how = "RIGHT"
            elif how == "INNER":
                how = "INNER"
            elif how == "OUTER":
                how = "FULL OUTER"

            # Parse "users.id = posts.userId" into proper DuckDB ON clause
            on_clause = join_cfg["on"]
            from_clause = f"{left} {how} JOIN {right} ON {on_clause}"
        else:
            from_clause = config["sources"][0]["alias"]

        # Build SELECT with aggregation
        if agg_cfg:
            group_by = agg_cfg["group_by"]
            metrics = agg_cfg["metrics"]

            select_parts = list(group_by)
            for metric_name, metric_expr in metrics.items():
                select_parts.append(f"{metric_expr} AS {metric_name}")

            select_clause = ", ".join(select_parts)
            group_clause = ", ".join(group_by)
            return f"SELECT {select_clause} FROM {from_clause} GROUP BY {group_clause}"

        return f"SELECT * FROM {from_clause}"
