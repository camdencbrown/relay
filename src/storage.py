"""
Storage layer for Relay
Simple JSON file storage for V1
"""

import json
from pathlib import Path
from typing import List, Dict, Optional
import threading

class Storage:
    """Simple JSON file storage for pipelines"""
    
    def __init__(self, storage_path: Optional[Path] = None):
        if storage_path is None:
            # Default to pipelines.json in project root
            storage_path = Path(__file__).parent.parent / "pipelines.json"
        
        self.storage_path = storage_path
        self.lock = threading.Lock()
        
        # Initialize storage file if it doesn't exist
        if not self.storage_path.exists():
            self._write_data({"pipelines": []})
    
    def _read_data(self) -> Dict:
        """Read data from storage file"""
        with self.lock:
            with open(self.storage_path, 'r') as f:
                return json.load(f)
    
    def _write_data(self, data: Dict):
        """Write data to storage file"""
        with self.lock:
            with open(self.storage_path, 'w') as f:
                json.dump(data, f, indent=2)
    
    def save_pipeline(self, pipeline: Dict):
        """Save a new pipeline"""
        data = self._read_data()
        data["pipelines"].append(pipeline)
        self._write_data(data)
    
    def update_pipeline(self, pipeline_id: str, updates: Dict):
        """Update an existing pipeline"""
        data = self._read_data()
        
        for i, p in enumerate(data["pipelines"]):
            if p["id"] == pipeline_id:
                data["pipelines"][i].update(updates)
                self._write_data(data)
                return data["pipelines"][i]
        
        return None
    
    def get_pipeline(self, pipeline_id: str) -> Optional[Dict]:
        """Get a pipeline by ID"""
        data = self._read_data()
        
        for p in data["pipelines"]:
            if p["id"] == pipeline_id:
                return p
        
        return None
    
    def list_pipelines(self) -> List[Dict]:
        """List all pipelines"""
        data = self._read_data()
        return data["pipelines"]
    
    def delete_pipeline(self, pipeline_id: str) -> bool:
        """Delete a pipeline"""
        data = self._read_data()
        
        # Find and remove pipeline
        data["pipelines"] = [p for p in data["pipelines"] if p["id"] != pipeline_id]
        self._write_data(data)
        return True
    
    def add_run(self, pipeline_id: str, run: Dict):
        """Add a run to a pipeline's history"""
        pipeline = self.get_pipeline(pipeline_id)
        
        if pipeline:
            pipeline["runs"].append(run)
            self.update_pipeline(pipeline_id, {"runs": pipeline["runs"]})
    
    def update_run(self, pipeline_id: str, run_id: str, updates: Dict):
        """Update a specific run"""
        pipeline = self.get_pipeline(pipeline_id)
        
        if pipeline:
            for i, run in enumerate(pipeline["runs"]):
                if run["run_id"] == run_id:
                    pipeline["runs"][i].update(updates)
                    self.update_pipeline(pipeline_id, {"runs": pipeline["runs"]})
                    return pipeline["runs"][i]
        
        return None
