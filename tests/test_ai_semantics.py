"""Tests for AI semantics module"""

import pandas as pd

from src.ai_semantics import AISemantics


def test_not_available_without_key():
    ai = AISemantics()
    assert ai.available is False


def test_enhance_returns_metadata_unchanged_when_unavailable():
    ai = AISemantics()
    metadata = {
        "columns": [
            {"name": "id", "needs_review": True},
            {"name": "name", "needs_review": True},
        ]
    }
    sample = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

    result = ai.enhance_metadata(metadata, sample, context="test")
    # Should return metadata unchanged (no AI fields added)
    assert "ai_description" not in result["columns"][0]


def test_parse_response_valid_json():
    response = '{"col1": {"description": "test", "confidence": 0.9}}'
    result = AISemantics._parse_response(response)
    assert "col1" in result
    assert result["col1"]["description"] == "test"


def test_parse_response_markdown_wrapped():
    response = '```json\n{"col1": {"description": "test"}}\n```'
    result = AISemantics._parse_response(response)
    assert "col1" in result


def test_parse_response_invalid():
    result = AISemantics._parse_response("not json at all")
    assert result == {}
