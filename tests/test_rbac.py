"""Tests for RBAC auth module."""

import pytest

from src.auth import ROLE_HIERARCHY, generate_api_key, get_api_key_record, validate_api_key


def test_generate_with_role():
    key = generate_api_key("admin-key", role="admin")
    assert key.startswith("relay_")
    record = get_api_key_record(key)
    assert record is not None
    assert record["role"] == "admin"


def test_default_role():
    key = generate_api_key("writer-key")
    record = get_api_key_record(key)
    assert record["role"] == "writer"


def test_invalid_role():
    with pytest.raises(ValueError, match="Invalid role"):
        generate_api_key("bad", role="superadmin")


def test_role_hierarchy():
    assert ROLE_HIERARCHY["reader"] < ROLE_HIERARCHY["writer"]
    assert ROLE_HIERARCHY["writer"] < ROLE_HIERARCHY["admin"]


def test_api_key_to_dict():
    key = generate_api_key("dict-test", description="Test key", role="reader")
    record = get_api_key_record(key)
    assert "id" in record
    assert record["key_prefix"] == key[:12]
    assert record["name"] == "dict-test"
    assert record["description"] == "Test key"
    assert record["role"] == "reader"
    assert record["active"] is True
    assert "created_at" in record
