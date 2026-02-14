"""Tests for encryption module"""

import os

import pytest
from cryptography.fernet import Fernet

from src.encryption import EncryptionError, decrypt, decrypt_dict, encrypt, encrypt_dict


def test_roundtrip():
    plaintext = "hello world"
    ciphertext = encrypt(plaintext)
    assert ciphertext != plaintext
    assert decrypt(ciphertext) == plaintext


def test_different_ciphertexts():
    """Each encryption produces a different ciphertext (due to IV/timestamp)."""
    plaintext = "same-input"
    c1 = encrypt(plaintext)
    c2 = encrypt(plaintext)
    assert c1 != c2
    assert decrypt(c1) == plaintext
    assert decrypt(c2) == plaintext


def test_dict_roundtrip():
    data = {"host": "localhost", "password": "secret123", "port": 3306}
    ciphertext = encrypt_dict(data)
    result = decrypt_dict(ciphertext)
    assert result == data


def test_missing_key(monkeypatch):
    monkeypatch.setenv("ENCRYPTION_KEY", "")
    from src.config import get_settings
    get_settings.cache_clear()

    with pytest.raises(EncryptionError, match="ENCRYPTION_KEY not set"):
        encrypt("test")

    # Restore
    get_settings.cache_clear()


def test_invalid_key(monkeypatch):
    monkeypatch.setenv("ENCRYPTION_KEY", "not-a-valid-fernet-key")
    from src.config import get_settings
    get_settings.cache_clear()

    with pytest.raises(EncryptionError, match="invalid"):
        encrypt("test")

    # Restore
    get_settings.cache_clear()


def test_key_generation():
    """Verify Fernet.generate_key() produces a usable key."""
    key = Fernet.generate_key()
    assert isinstance(key, bytes)
    f = Fernet(key)
    token = f.encrypt(b"test")
    assert f.decrypt(token) == b"test"
