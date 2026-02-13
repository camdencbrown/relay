"""
Metadata generation for Relay datasets
Analyzes data and creates semantic layer
"""

import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
import json
from pathlib import Path

class MetadataGenerator:
    """Generates and manages dataset metadata"""
    
    def __init__(self, storage_path: Path = None):
        if storage_path is None:
            storage_path = Path(__file__).parent.parent / "metadata"
        
        self.storage_path = storage_path
        self.storage_path.mkdir(exist_ok=True)
        
        # Knowledge base for verified column descriptions
        self.knowledge_base_path = self.storage_path / "knowledge_base.json"
        self.knowledge_base = self._load_knowledge_base()
    
    def _load_knowledge_base(self) -> Dict:
        """Load human-verified column descriptions"""
        if self.knowledge_base_path.exists():
            with open(self.knowledge_base_path, 'r') as f:
                return json.load(f)
        return {"verified_columns": {}}
    
    def _save_knowledge_base(self):
        """Save knowledge base"""
        with open(self.knowledge_base_path, 'w') as f:
            json.dump(self.knowledge_base, f, indent=2)
    
    def generate_metadata(
        self,
        df: pd.DataFrame,
        pipeline_name: str,
        source_info: Dict,
        sample_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Generate metadata for a dataset
        Uses sample for large datasets
        """
        # Sample data if large
        if len(df) > sample_size:
            sample_df = df.sample(min(sample_size, len(df)))
        else:
            sample_df = df
        
        metadata = {
            "pipeline_name": pipeline_name,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "row_count": len(df),
            "column_count": len(df.columns),
            "source": source_info,
            "columns": []
        }
        
        # Analyze each column
        for col in df.columns:
            col_metadata = self._analyze_column(col, df[col], sample_df[col])
            
            # Check if we have verified description in knowledge base
            col_key = self._normalize_column_name(col)
            if col_key in self.knowledge_base["verified_columns"]:
                verified = self.knowledge_base["verified_columns"][col_key]
                col_metadata.update({
                    "description": verified["description"],
                    "business_meaning": verified.get("business_meaning"),
                    "human_verified": True,
                    "verified_at": verified.get("verified_at"),
                    "needs_review": False
                })
            else:
                # Mark for human review
                col_metadata["needs_review"] = True
                col_metadata["human_verified"] = False
            
            metadata["columns"].append(col_metadata)
        
        # Count columns needing review
        metadata["columns_needing_review"] = sum(
            1 for c in metadata["columns"] if c.get("needs_review", False)
        )
        
        return metadata
    
    def _normalize_column_name(self, name: str) -> str:
        """Normalize column name for knowledge base lookup"""
        return name.lower().strip().replace(" ", "_")
    
    def _analyze_column(
        self,
        name: str,
        full_series: pd.Series,
        sample_series: pd.Series
    ) -> Dict[str, Any]:
        """Analyze a single column"""
        
        # Basic stats
        total_count = len(full_series)
        null_count = full_series.isna().sum()
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
        
        # Data type
        dtype = str(full_series.dtype)
        
        # Unique values (from sample)
        unique_count = sample_series.nunique()
        
        # Sample values (non-null, from sample) - convert to strings for JSON serialization
        sample_values = [str(v) for v in sample_series.dropna().head(5).tolist()]
        
        # Infer semantic type
        semantic_type = self._infer_semantic_type(name, sample_series)
        
        metadata = {
            "name": name,
            "type": dtype,
            "semantic_type": semantic_type,
            "null_count": int(null_count),
            "null_percentage": round(null_percentage, 2),
            "unique_values": int(unique_count),
            "sample_values": sample_values,
            "auto_description": self._generate_auto_description(name, dtype, semantic_type)
        }
        
        # Add type-specific stats
        if pd.api.types.is_numeric_dtype(sample_series):
            metadata.update({
                "min": float(sample_series.min()) if not sample_series.isna().all() else None,
                "max": float(sample_series.max()) if not sample_series.isna().all() else None,
                "mean": float(sample_series.mean()) if not sample_series.isna().all() else None
            })
        
        return metadata
    
    def _infer_semantic_type(self, name: str, series: pd.Series) -> str:
        """Infer semantic type from column name and data"""
        name_lower = name.lower()
        
        # Check name patterns
        if "email" in name_lower:
            return "email"
        elif "phone" in name_lower or "tel" in name_lower:
            return "phone"
        elif "date" in name_lower or "time" in name_lower:
            return "datetime"
        elif "id" in name_lower:
            return "identifier"
        elif "name" in name_lower:
            return "name"
        elif "address" in name_lower or "street" in name_lower:
            return "address"
        elif "zip" in name_lower or "postal" in name_lower:
            return "postal_code"
        elif "amount" in name_lower or "price" in name_lower or "cost" in name_lower:
            return "currency"
        elif "percent" in name_lower or "rate" in name_lower:
            return "percentage"
        
        # Check data patterns
        if pd.api.types.is_numeric_dtype(series):
            return "numeric"
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"
        elif pd.api.types.is_bool_dtype(series):
            return "boolean"
        else:
            return "text"
    
    def _generate_auto_description(self, name: str, dtype: str, semantic_type: str) -> str:
        """Generate automatic description"""
        # Format name for human reading
        readable_name = name.replace("_", " ").replace("-", " ").title()
        
        if semantic_type == "email":
            return f"Email address - {readable_name}"
        elif semantic_type == "phone":
            return f"Phone number - {readable_name}"
        elif semantic_type == "identifier":
            return f"Unique identifier - {readable_name}"
        elif semantic_type == "currency":
            return f"Monetary amount - {readable_name}"
        elif semantic_type == "datetime":
            return f"Date/time value - {readable_name}"
        else:
            return f"{readable_name} ({dtype})"
    
    def save_metadata(self, metadata: Dict, pipeline_id: str):
        """Save metadata to file"""
        filepath = self.storage_path / f"{pipeline_id}_metadata.json"
        with open(filepath, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def load_metadata(self, pipeline_id: str) -> Dict:
        """Load metadata from file"""
        filepath = self.storage_path / f"{pipeline_id}_metadata.json"
        if filepath.exists():
            with open(filepath, 'r') as f:
                return json.load(f)
        return None
    
    def approve_column(
        self,
        column_name: str,
        description: str,
        business_meaning: str = None,
        verified_by: str = "user"
    ):
        """Approve a column description for knowledge base"""
        col_key = self._normalize_column_name(column_name)
        
        self.knowledge_base["verified_columns"][col_key] = {
            "description": description,
            "business_meaning": business_meaning,
            "verified_by": verified_by,
            "verified_at": datetime.utcnow().isoformat() + "Z"
        }
        
        self._save_knowledge_base()
    
    def get_pending_reviews(self) -> List[Dict]:
        """Get all columns pending review across all pipelines"""
        pending = []
        
        for filepath in self.storage_path.glob("*_metadata.json"):
            with open(filepath, 'r') as f:
                metadata = json.load(f)
            
            for col in metadata.get("columns", []):
                if col.get("needs_review", False):
                    pending.append({
                        "pipeline": metadata["pipeline_name"],
                        "column": col["name"],
                        "type": col["type"],
                        "semantic_type": col["semantic_type"],
                        "auto_description": col.get("auto_description"),
                        "sample_values": col.get("sample_values", []),
                        "metadata_file": filepath.name
                    })
        
        return pending
