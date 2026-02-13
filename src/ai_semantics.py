"""
AI-powered semantic layer for Relay
Uses LLM to generate rich column descriptions and business meanings
"""

import pandas as pd
from typing import Dict, List, Any
import json
import os

class AISemantics:
    """LLM-powered semantic understanding of data"""
    
    def __init__(self):
        # Will use OpenClaw's session context for LLM calls
        self.model = "default"  # Uses session's model
    
    def enhance_metadata(
        self,
        metadata: Dict,
        sample_df: pd.DataFrame,
        context: str = None
    ) -> Dict:
        """
        Enhance metadata with AI-generated semantic descriptions
        Only enhances columns that need review (not already verified)
        """
        columns_to_enhance = [
            col for col in metadata["columns"]
            if col.get("needs_review", False)
        ]
        
        if not columns_to_enhance:
            return metadata  # All columns already verified
        
        # Get AI descriptions
        ai_descriptions = self._generate_descriptions(
            columns_to_enhance,
            sample_df,
            context or metadata.get("pipeline_name", "Unknown dataset")
        )
        
        # Update metadata with AI descriptions
        for col in metadata["columns"]:
            if col["name"] in ai_descriptions:
                ai_desc = ai_descriptions[col["name"]]
                col.update({
                    "ai_description": ai_desc.get("description"),
                    "ai_business_meaning": ai_desc.get("business_meaning"),
                    "ai_use_cases": ai_desc.get("use_cases"),
                    "ai_quality_notes": ai_desc.get("quality_notes"),
                    "ai_confidence": ai_desc.get("confidence", 0.7)
                })
        
        return metadata
    
    def _generate_descriptions(
        self,
        columns: List[Dict],
        sample_df: pd.DataFrame,
        context: str
    ) -> Dict[str, Dict]:
        """
        Generate AI descriptions for columns
        Uses LLM to understand business meaning
        """
        # Build prompt with column information
        column_info = []
        for col in columns:
            col_name = col["name"]
            
            # Get sample data
            if col_name in sample_df.columns:
                sample_values = sample_df[col_name].dropna().head(10).tolist()
            else:
                sample_values = col.get("sample_values", [])
            
            column_info.append({
                "name": col_name,
                "type": col.get("type", "unknown"),
                "semantic_type": col.get("semantic_type", "unknown"),
                "sample_values": sample_values,
                "null_percentage": col.get("null_percentage", 0),
                "unique_values": col.get("unique_values", 0)
            })
        
        prompt = self._build_analysis_prompt(context, column_info)
        
        # Call LLM (through OpenClaw session)
        response = self._call_llm(prompt)
        
        # Parse response into structured descriptions
        descriptions = self._parse_llm_response(response, columns)
        
        return descriptions
    
    def _build_analysis_prompt(self, context: str, columns: List[Dict]) -> str:
        """Build prompt for LLM analysis"""
        
        prompt = f"""Analyze this dataset and provide semantic descriptions for each column.

Dataset context: {context}

Columns to analyze:
{json.dumps(columns, indent=2)}

For each column, provide:
1. **description**: Clear, concise description of what this column contains
2. **business_meaning**: What this represents in business/domain terms
3. **use_cases**: Common ways this data is used (2-3 examples)
4. **quality_notes**: Any data quality observations (nulls, patterns, anomalies)
5. **confidence**: Your confidence in this analysis (0.0-1.0)

Respond with valid JSON in this format:
{{
  "column_name_1": {{
    "description": "...",
    "business_meaning": "...",
    "use_cases": ["...", "..."],
    "quality_notes": "...",
    "confidence": 0.9
  }},
  "column_name_2": {{ ... }}
}}

Focus on:
- Practical business value
- How someone would actually use this data
- Data quality issues that matter
- Be specific based on sample values, not generic

Respond ONLY with valid JSON, no other text."""

        return prompt
    
    def _call_llm(self, prompt: str) -> str:
        """
        Call LLM through OpenClaw's session
        In production, this would use the sessions_send or similar mechanism
        For now, we'll create a simple file-based communication
        """
        # For MVP, we'll use a direct approach
        # In production, this would integrate with OpenClaw's session management
        
        # Create a request file for the agent to process
        request_file = os.path.join(
            os.path.dirname(__file__),
            "../ai_requests/pending_request.json"
        )
        os.makedirs(os.path.dirname(request_file), exist_ok=True)
        
        with open(request_file, 'w') as f:
            json.dump({
                "type": "semantic_analysis",
                "prompt": prompt,
                "status": "pending"
            }, f, indent=2)
        
        # For now, return a placeholder
        # In production, this would wait for the agent to process and respond
        return self._get_mock_response()
    
    def _get_mock_response(self) -> str:
        """
        Mock LLM response for testing
        In production, this would be actual LLM output
        """
        # This is a placeholder - in production, the actual LLM would provide this
        return json.dumps({
            "email": {
                "description": "Email address of the contact or user",
                "business_meaning": "Primary communication channel for customer outreach and account identification",
                "use_cases": [
                    "Marketing campaign targeting",
                    "User authentication and login",
                    "Customer support communications"
                ],
                "quality_notes": "Check for valid email format, watch for duplicates",
                "confidence": 0.95
            },
            "created_date": {
                "description": "Timestamp when this record was created in the system",
                "business_meaning": "Indicates when the customer/lead entered the pipeline",
                "use_cases": [
                    "Customer cohort analysis",
                    "Lead velocity tracking",
                    "Data freshness validation"
                ],
                "quality_notes": "Should never be null, check for future dates",
                "confidence": 0.92
            }
        })
    
    def _parse_llm_response(
        self,
        response: str,
        columns: List[Dict]
    ) -> Dict[str, Dict]:
        """Parse LLM JSON response"""
        try:
            # Try to parse as JSON
            parsed = json.loads(response)
            return parsed
        except json.JSONDecodeError:
            # If not valid JSON, try to extract JSON from response
            # LLMs sometimes wrap JSON in markdown code blocks
            import re
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', response, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group(1))
                except:
                    pass
            
            # Fallback: return empty dict (columns will stay as auto-generated)
            return {}
    
    def generate_dataset_summary(
        self,
        metadata: Dict,
        sample_df: pd.DataFrame
    ) -> str:
        """
        Generate a natural language summary of the entire dataset
        Useful for agents to quickly understand what data they're working with
        """
        prompt = f"""Provide a concise summary of this dataset for an AI agent.

Dataset: {metadata.get('pipeline_name', 'Unknown')}
Rows: {metadata.get('row_count', 0)}
Columns: {metadata.get('column_count', 0)}

Column details:
{json.dumps([
    {
        'name': col['name'],
        'type': col.get('semantic_type', col.get('type')),
        'description': col.get('ai_description') or col.get('description') or col.get('auto_description'),
        'sample': col.get('sample_values', [])[:3]
    }
    for col in metadata.get('columns', [])
], indent=2)}

Sample data (first 5 rows):
{sample_df.head(5).to_dict('records')}

Provide:
1. What this dataset represents
2. Key columns and their purpose
3. Potential use cases for this data
4. Any data quality concerns

Keep it concise (3-4 sentences) and actionable."""

        response = self._call_llm(prompt)
        return response
    
    def suggest_transformations(
        self,
        metadata: Dict,
        goal: str
    ) -> List[Dict]:
        """
        Suggest data transformations based on goal
        Example: "prepare for customer segmentation analysis"
        """
        prompt = f"""Given this dataset, suggest data transformations for: {goal}

Dataset columns:
{json.dumps([
    {
        'name': col['name'],
        'type': col.get('semantic_type'),
        'description': col.get('ai_description') or col.get('description')
    }
    for col in metadata.get('columns', [])
], indent=2)}

Suggest transformations in this JSON format:
{{
  "transformations": [
    {{
      "type": "filter",
      "column": "...",
      "condition": "...",
      "reason": "..."
    }},
    {{
      "type": "derive",
      "new_column": "...",
      "formula": "...",
      "reason": "..."
    }}
  ]
}}

Focus on transformations that would help achieve the goal."""

        response = self._call_llm(prompt)
        try:
            parsed = json.loads(response)
            return parsed.get("transformations", [])
        except:
            return []
