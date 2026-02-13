"""Tests for auth module"""

from src.auth import _hash_key, generate_api_key, validate_api_key


def test_hash_key_deterministic():
    assert _hash_key("test") == _hash_key("test")


def test_hash_key_differs():
    assert _hash_key("key1") != _hash_key("key2")


def test_generate_and_validate():
    key = generate_api_key("test-key", "A test key")
    assert key.startswith("relay_")
    assert validate_api_key(key) is True


def test_invalid_key():
    assert validate_api_key("relay_invalid_key_that_doesnt_exist") is False


def test_multiple_keys():
    key1 = generate_api_key("key1")
    key2 = generate_api_key("key2")
    assert key1 != key2
    assert validate_api_key(key1) is True
    assert validate_api_key(key2) is True
