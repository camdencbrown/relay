"""
AI-powered semantic layer for Relay
Uses Anthropic Claude for column descriptions and dataset understanding.
Graceful degradation: no API key = no AI, never crashes.
"""

import json
import logging
import re
from typing import Dict, List

import pandas as pd

from .config import get_settings

logger = logging.getLogger(__name__)


class AISemantics:
    """Claude-powered semantic understanding of data."""

    def __init__(self):
        self._client = None
        self._available = None  # lazy check

    @property
    def available(self) -> bool:
        if self._available is None:
            settings = get_settings()
            if not settings.anthropic_api_key:
                self._available = False
            else:
                try:
                    import anthropic
                    self._client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
                    self._available = True
                except Exception as e:
                    logger.warning(f"Anthropic client init failed: {e}")
                    self._available = False
        return self._available

    def enhance_metadata(
        self,
        metadata: Dict,
        sample_df: pd.DataFrame,
        context: str = None,
    ) -> Dict:
        """Enhance metadata with AI-generated semantic descriptions."""
        if not self.available:
            return metadata

        columns_to_enhance = [
            col for col in metadata["columns"] if col.get("needs_review", False)
        ]
        if not columns_to_enhance:
            return metadata

        try:
            ai_descriptions = self._generate_descriptions(
                columns_to_enhance,
                sample_df,
                context or metadata.get("pipeline_name", "Unknown dataset"),
            )

            for col in metadata["columns"]:
                if col["name"] in ai_descriptions:
                    ai_desc = ai_descriptions[col["name"]]
                    col.update(
                        {
                            "ai_description": ai_desc.get("description"),
                            "ai_business_meaning": ai_desc.get("business_meaning"),
                            "ai_use_cases": ai_desc.get("use_cases"),
                            "ai_quality_notes": ai_desc.get("quality_notes"),
                            "ai_confidence": ai_desc.get("confidence", 0.7),
                        }
                    )
        except Exception as e:
            logger.error(f"AI enhancement failed (continuing without): {e}")

        return metadata

    def _generate_descriptions(
        self,
        columns: List[Dict],
        sample_df: pd.DataFrame,
        context: str,
    ) -> Dict[str, Dict]:
        column_info = []
        for col in columns:
            col_name = col["name"]
            if col_name in sample_df.columns:
                sample_values = sample_df[col_name].dropna().head(10).tolist()
                sample_values = [str(v) for v in sample_values]
            else:
                sample_values = col.get("sample_values", [])

            column_info.append(
                {
                    "name": col_name,
                    "type": col.get("type", "unknown"),
                    "semantic_type": col.get("semantic_type", "unknown"),
                    "sample_values": sample_values,
                    "null_percentage": col.get("null_percentage", 0),
                    "unique_values": col.get("unique_values", 0),
                }
            )

        prompt = self._build_prompt(context, column_info)
        response_text = self._call_claude(prompt)
        return self._parse_response(response_text)

    def _build_prompt(self, context: str, columns: List[Dict]) -> str:
        return (
            f"Analyze this dataset and provide semantic descriptions for each column.\n\n"
            f"Dataset context: {context}\n\n"
            f"Columns to analyze:\n{json.dumps(columns, indent=2)}\n\n"
            f"For each column, provide:\n"
            f"1. description: Clear, concise description\n"
            f"2. business_meaning: What this represents in business terms\n"
            f"3. use_cases: 2-3 common uses\n"
            f"4. quality_notes: Data quality observations\n"
            f"5. confidence: Your confidence 0.0-1.0\n\n"
            f"Respond ONLY with valid JSON mapping column names to their analysis objects."
        )

    def _call_claude(self, prompt: str) -> str:
        message = self._client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text

    @staticmethod
    def _parse_response(response: str) -> Dict[str, Dict]:
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            # Try extracting JSON from markdown code blocks
            match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", response, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except json.JSONDecodeError:
                    pass
            return {}
