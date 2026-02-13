"""
Dataset semantic search
Enables agents to discover relevant datasets by natural language query
"""

import re
from typing import Dict, List, Tuple


class DatasetSearch:
    """Simple keyword-based dataset search."""

    def __init__(self, storage):
        self.storage = storage

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        pipelines = self.storage.list_pipelines()
        scored = []
        for pipeline in pipelines:
            score, reason = self._score_pipeline(pipeline, query)
            if score > 0:
                scored.append(
                    {
                        "pipeline_id": pipeline["id"],
                        "name": pipeline["name"],
                        "confidence": score,
                        "reason": reason,
                        "source_type": pipeline.get("source", {}).get("type", "unknown"),
                        "created_at": pipeline["created_at"],
                    }
                )
        scored.sort(key=lambda x: x["confidence"], reverse=True)
        return scored[:top_k]

    def _score_pipeline(self, pipeline: Dict, query: str) -> Tuple[float, str]:
        query_lower = query.lower()
        query_words = set(re.findall(r"\w+", query_lower))

        score = 0.0
        matched_keywords: list = []

        name_lower = pipeline["name"].lower()
        name_words = set(re.findall(r"\w+", name_lower))
        name_matches = query_words.intersection(name_words)
        if name_matches:
            score += 0.5 * len(name_matches)
            matched_keywords.extend(name_matches)

        source = pipeline.get("source", {})
        source_text = source.get("url", "") or source.get("query", "") or ""
        source_words = set(re.findall(r"\w+", source_text.lower()))
        source_matches = query_words.intersection(source_words)
        if source_matches:
            score += 0.3 * len(source_matches)
            matched_keywords.extend(source_matches)

        # Check metadata from storage (fixed: uses storage.get_metadata)
        try:
            metadata = self.storage.get_metadata(pipeline["id"])
            if metadata:
                for col in metadata.get("columns", []):
                    col_name = col["name"].lower()
                    if any(word in col_name for word in query_words):
                        score += 0.1
                        matched_keywords.append(col["name"])
        except Exception:
            pass

        score = min(score, 1.0)
        reason = (
            f"Matched keywords: {', '.join(set(matched_keywords))}"
            if matched_keywords
            else "Low relevance"
        )
        return score, reason

    def get_join_suggestions(self, pipeline_id_1: str, pipeline_id_2: str) -> List[Dict]:
        try:
            metadata_1 = self.storage.get_metadata(pipeline_id_1)
            metadata_2 = self.storage.get_metadata(pipeline_id_2)
            if not metadata_1 or not metadata_2:
                return []

            suggestions = []
            for col1 in metadata_1.get("columns", []):
                for col2 in metadata_2.get("columns", []):
                    name1 = col1["name"].lower()
                    name2 = col2["name"].lower()
                    confidence = 0.0
                    reasons: list = []

                    if name1 == name2:
                        confidence = 0.95
                        reasons.append("Exact name match")
                    elif self._names_similar(name1, name2):
                        confidence = 0.75
                        reasons.append(f"Name similarity: {name1} <-> {name2}")

                    if col1.get("semantic_type") == "identifier" and col2.get("semantic_type") == "identifier":
                        confidence += 0.1
                        reasons.append("Both are identifiers")

                    if confidence > 0.5:
                        suggestions.append(
                            {
                                "left_column": col1["name"],
                                "right_column": col2["name"],
                                "confidence": min(confidence, 1.0),
                                "reason": "; ".join(reasons),
                            }
                        )

            suggestions.sort(key=lambda x: x["confidence"], reverse=True)
            return suggestions
        except Exception:
            return []

    @staticmethod
    def _names_similar(name1: str, name2: str) -> bool:
        clean1 = re.sub(r"[_\-\s]", "", name1)
        clean2 = re.sub(r"[_\-\s]", "", name2)
        if clean1 in clean2 or clean2 in clean1:
            return True
        patterns = [(r"id$", r"id$"), (r"^id", r"id$"), (r"_id$", r"id$")]
        for p1, p2 in patterns:
            if re.search(p1, name1) and re.search(p2, name2):
                return True
        return False
