"""
Dataset semantic search
Enables agents to discover relevant datasets by natural language query
"""

from typing import List, Dict, Tuple
import re


class DatasetSearch:
    """
    Simple keyword-based dataset search
    (Can be upgraded to vector embeddings later)
    """
    
    def __init__(self, storage):
        self.storage = storage
    
    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        """
        Search for datasets matching the query
        
        Returns list of:
        {
            "pipeline_id": "pipe-123",
            "name": "users_dataset",
            "confidence": 0.85,
            "reason": "Matched keywords: user, name"
        }
        """
        
        # Get all pipelines
        pipelines = self.storage.list_pipelines()
        
        # Score each pipeline
        scored = []
        for pipeline in pipelines:
            score, reason = self._score_pipeline(pipeline, query)
            if score > 0:
                scored.append({
                    "pipeline_id": pipeline["id"],
                    "name": pipeline["name"],
                    "confidence": score,
                    "reason": reason,
                    "source_type": pipeline["source"]["type"],
                    "created_at": pipeline["created_at"]
                })
        
        # Sort by score descending
        scored.sort(key=lambda x: x["confidence"], reverse=True)
        
        # Return top K
        return scored[:top_k]
    
    def _score_pipeline(self, pipeline: Dict, query: str) -> Tuple[float, str]:
        """
        Score a pipeline against a query
        Returns (score, reason)
        """
        
        query_lower = query.lower()
        query_words = set(re.findall(r'\w+', query_lower))
        
        score = 0.0
        matched_keywords = []
        
        # Check pipeline name
        name_lower = pipeline["name"].lower()
        name_words = set(re.findall(r'\w+', name_lower))
        
        name_matches = query_words.intersection(name_words)
        if name_matches:
            score += 0.5 * len(name_matches)
            matched_keywords.extend(name_matches)
        
        # Check source URL/query for keywords
        source = pipeline["source"]
        source_text = ""
        
        if "url" in source and source["url"]:
            source_text = source["url"].lower()
        elif "query" in source and source["query"]:
            source_text = source["query"].lower()
        
        source_words = set(re.findall(r'\w+', source_text))
        source_matches = query_words.intersection(source_words)
        if source_matches:
            score += 0.3 * len(source_matches)
            matched_keywords.extend(source_matches)
        
        # Check metadata if available
        try:
            metadata = self.storage.get_metadata(pipeline["id"])
            if metadata:
                # Check column names
                for col in metadata.get("columns", []):
                    col_name = col["name"].lower()
                    if any(word in col_name for word in query_words):
                        score += 0.1
                        matched_keywords.append(col["name"])
        except:
            pass
        
        # Normalize score to 0-1 range
        score = min(score, 1.0)
        
        reason = f"Matched keywords: {', '.join(set(matched_keywords))}" if matched_keywords else "Low relevance"
        
        return score, reason
    
    def get_join_suggestions(self, pipeline_id_1: str, pipeline_id_2: str) -> List[Dict]:
        """
        Suggest possible join keys between two datasets
        
        Returns list of:
        {
            "left_column": "id",
            "right_column": "userId",
            "confidence": 0.8,
            "reason": "Name similarity: id <-> userId"
        }
        """
        
        try:
            metadata_1 = self.storage.get_metadata(pipeline_id_1)
            metadata_2 = self.storage.get_metadata(pipeline_id_2)
            
            if not metadata_1 or not metadata_2:
                return []
            
            suggestions = []
            
            columns_1 = metadata_1.get("columns", [])
            columns_2 = metadata_2.get("columns", [])
            
            for col1 in columns_1:
                for col2 in columns_2:
                    # Check for similar names
                    name1 = col1["name"].lower()
                    name2 = col2["name"].lower()
                    
                    confidence = 0.0
                    reasons = []
                    
                    # Exact match
                    if name1 == name2:
                        confidence = 0.95
                        reasons.append("Exact name match")
                    
                    # Common patterns: id <-> userId, account_id <-> accountId
                    elif self._names_similar(name1, name2):
                        confidence = 0.75
                        reasons.append(f"Name similarity: {name1} <-> {name2}")
                    
                    # Both are identifier types
                    if col1.get("semantic_type") == "identifier" and col2.get("semantic_type") == "identifier":
                        confidence += 0.1
                        reasons.append("Both are identifiers")
                    
                    if confidence > 0.5:
                        suggestions.append({
                            "left_column": col1["name"],
                            "right_column": col2["name"],
                            "confidence": min(confidence, 1.0),
                            "reason": "; ".join(reasons)
                        })
            
            # Sort by confidence
            suggestions.sort(key=lambda x: x["confidence"], reverse=True)
            
            return suggestions
            
        except Exception as e:
            return []
    
    def _names_similar(self, name1: str, name2: str) -> bool:
        """Check if two column names are similar"""
        
        # Remove common separators
        clean1 = re.sub(r'[_\-\s]', '', name1)
        clean2 = re.sub(r'[_\-\s]', '', name2)
        
        # Check if one contains the other
        if clean1 in clean2 or clean2 in clean1:
            return True
        
        # Check common patterns
        patterns = [
            (r'id$', r'id$'),
            (r'^id', r'id$'),
            (r'_id$', r'id$'),
        ]
        
        for p1, p2 in patterns:
            if re.search(p1, name1) and re.search(p2, name2):
                return True
        
        return False
