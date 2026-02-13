"""Tests for ConnectorRegistry"""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.connectors import ConnectorRegistry


def test_supported_types():
    types = ConnectorRegistry.supported_types()
    assert "csv_url" in types
    assert "json_url" in types
    assert "rest_api" in types
    assert "synthetic" in types


def test_synthetic_fetch():
    source = {
        "type": "synthetic",
        "row_count": 50,
        "schema": {"id": "uuid", "name": "first_name", "amount": "currency"},
    }
    df = ConnectorRegistry.fetch_source(source)
    assert len(df) == 50
    assert "id" in df.columns
    assert "name" in df.columns
    assert "amount" in df.columns


def test_synthetic_streaming():
    source = {
        "type": "synthetic",
        "row_count": 25,
        "schema": {"id": "uuid"},
    }
    chunks = list(ConnectorRegistry.fetch_source_streaming(source, chunk_size=10))
    total = sum(len(c) for c in chunks)
    assert total == 25
    assert len(chunks) == 3  # 10 + 10 + 5


@patch("src.connectors.requests.get")
def test_csv_url_fetch(mock_get):
    mock_resp = MagicMock()
    mock_resp.text = "a,b\n1,2\n3,4\n"
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    df = ConnectorRegistry.fetch_source({"type": "csv_url", "url": "http://example.com/data.csv"})
    assert len(df) == 2
    assert list(df.columns) == ["a", "b"]


@patch("src.connectors.requests.request")
def test_rest_api_fetch_list(mock_request):
    mock_resp = MagicMock()
    mock_resp.json.return_value = [{"id": 1}, {"id": 2}]
    mock_resp.raise_for_status = MagicMock()
    mock_request.return_value = mock_resp

    df = ConnectorRegistry.fetch_source({"type": "rest_api", "url": "http://api.example.com/items"})
    assert len(df) == 2


@patch("src.connectors.requests.request")
def test_rest_api_fetch_dict_with_data_key(mock_request):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"data": [{"id": 1}]}
    mock_resp.raise_for_status = MagicMock()
    mock_request.return_value = mock_resp

    df = ConnectorRegistry.fetch_source({"type": "rest_api", "url": "http://api.example.com/items"})
    assert len(df) == 1


def test_unsupported_type():
    import pytest

    with pytest.raises(ValueError, match="Unsupported source type"):
        ConnectorRegistry.fetch_source({"type": "unknown_type"})
