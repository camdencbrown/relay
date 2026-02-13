"""
Metadata generation for Relay datasets
Analyzes data and creates semantic layer
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)


class MetadataGenerator:
    """Generates and manages dataset metadata."""

    def __init__(self, storage=None):
        self.storage = storage

    def generate_metadata(
        self,
        df: pd.DataFrame,
        pipeline_name: str,
        source_info: Dict,
        sample_size: int = 1000,
    ) -> Dict[str, Any]:
        if len(df) > sample_size:
            sample_df = df.sample(min(sample_size, len(df)))
        else:
            sample_df = df

        # Load knowledge base from storage
        knowledge = {}
        if self.storage:
            knowledge = self.storage.list_column_knowledge()

        metadata = {
            "pipeline_name": pipeline_name,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "source": source_info,
            "columns": [],
        }

        for col in df.columns:
            col_metadata = self._analyze_column(col, df[col], sample_df[col])

            col_key = self._normalize_column_name(col)
            if col_key in knowledge:
                verified = knowledge[col_key]
                col_metadata.update(
                    {
                        "description": verified["description"],
                        "business_meaning": verified.get("business_meaning"),
                        "human_verified": True,
                        "verified_at": verified.get("verified_at"),
                        "needs_review": False,
                    }
                )
            else:
                col_metadata["needs_review"] = True
                col_metadata["human_verified"] = False

            metadata["columns"].append(col_metadata)

        metadata["columns_needing_review"] = sum(
            1 for c in metadata["columns"] if c.get("needs_review", False)
        )
        return metadata

    def save_metadata(self, metadata: Dict, pipeline_id: str) -> None:
        if self.storage:
            self.storage.save_metadata(pipeline_id, metadata)
        else:
            # Fallback to file
            path = Path(__file__).parent.parent / "metadata"
            path.mkdir(exist_ok=True)
            filepath = path / f"{pipeline_id}_metadata.json"
            with open(filepath, "w") as f:
                json.dump(metadata, f, indent=2)

    def load_metadata(self, pipeline_id: str) -> Dict | None:
        if self.storage:
            return self.storage.get_metadata(pipeline_id)
        # Fallback to file
        filepath = Path(__file__).parent.parent / "metadata" / f"{pipeline_id}_metadata.json"
        if filepath.exists():
            with open(filepath) as f:
                return json.load(f)
        return None

    def approve_column(
        self,
        column_name: str,
        description: str,
        business_meaning: str = None,
        verified_by: str = "user",
    ) -> None:
        col_key = self._normalize_column_name(column_name)
        if self.storage:
            self.storage.save_column_knowledge(col_key, description, business_meaning, verified_by)

    def get_pending_reviews(self) -> List[Dict]:
        """Get all columns pending review across all pipelines."""
        if not self.storage:
            return []
        pending = []
        for pipeline in self.storage.list_pipelines():
            metadata = self.storage.get_metadata(pipeline["id"])
            if not metadata:
                continue
            for col in metadata.get("columns", []):
                if col.get("needs_review", False):
                    pending.append(
                        {
                            "pipeline": metadata.get("pipeline_name", pipeline["name"]),
                            "column": col["name"],
                            "type": col.get("type"),
                            "semantic_type": col.get("semantic_type"),
                            "auto_description": col.get("auto_description"),
                            "sample_values": col.get("sample_values", []),
                        }
                    )
        return pending

    @staticmethod
    def _normalize_column_name(name: str) -> str:
        return name.lower().strip().replace(" ", "_")

    @staticmethod
    def _analyze_column(name: str, full_series: pd.Series, sample_series: pd.Series) -> Dict[str, Any]:
        total_count = len(full_series)
        null_count = int(full_series.isna().sum())
        null_percentage = round((null_count / total_count * 100) if total_count > 0 else 0, 2)
        dtype = str(full_series.dtype)
        unique_count = int(sample_series.nunique())
        sample_values = [str(v) for v in sample_series.dropna().head(5).tolist()]
        semantic_type = MetadataGenerator._infer_semantic_type(name, sample_series)

        metadata: Dict[str, Any] = {
            "name": name,
            "type": dtype,
            "semantic_type": semantic_type,
            "null_count": null_count,
            "null_percentage": null_percentage,
            "unique_values": unique_count,
            "sample_values": sample_values,
            "auto_description": MetadataGenerator._generate_auto_description(name, dtype, semantic_type),
        }

        if pd.api.types.is_numeric_dtype(sample_series):
            metadata.update(
                {
                    "min": float(sample_series.min()) if not sample_series.isna().all() else None,
                    "max": float(sample_series.max()) if not sample_series.isna().all() else None,
                    "mean": float(sample_series.mean()) if not sample_series.isna().all() else None,
                }
            )
        return metadata

    @staticmethod
    def _infer_semantic_type(name: str, series: pd.Series) -> str:
        n = name.lower()
        if "email" in n:
            return "email"
        if "phone" in n or "tel" in n:
            return "phone"
        if "date" in n or "time" in n:
            return "datetime"
        if "id" in n:
            return "identifier"
        if "name" in n:
            return "name"
        if "address" in n or "street" in n:
            return "address"
        if "zip" in n or "postal" in n:
            return "postal_code"
        if "amount" in n or "price" in n or "cost" in n:
            return "currency"
        if "percent" in n or "rate" in n:
            return "percentage"
        if pd.api.types.is_numeric_dtype(series):
            return "numeric"
        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"
        if pd.api.types.is_bool_dtype(series):
            return "boolean"
        return "text"

    @staticmethod
    def _generate_auto_description(name: str, dtype: str, semantic_type: str) -> str:
        readable = name.replace("_", " ").replace("-", " ").title()
        mapping = {
            "email": f"Email address - {readable}",
            "phone": f"Phone number - {readable}",
            "identifier": f"Unique identifier - {readable}",
            "currency": f"Monetary amount - {readable}",
            "datetime": f"Date/time value - {readable}",
        }
        return mapping.get(semantic_type, f"{readable} ({dtype})")
