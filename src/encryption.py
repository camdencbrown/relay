"""
Fernet symmetric encryption for connection credentials.
"""

import json

from cryptography.fernet import Fernet, InvalidToken

from .config import get_settings


class EncryptionError(Exception):
    pass


def _get_fernet() -> Fernet:
    key = get_settings().encryption_key
    if not key:
        raise EncryptionError(
            "ENCRYPTION_KEY not set. Generate one with: "
            'python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"'
        )
    try:
        return Fernet(key.encode())
    except Exception:
        raise EncryptionError("ENCRYPTION_KEY is invalid. Must be a valid Fernet key (base64-encoded 32 bytes).")


def encrypt(plaintext: str) -> str:
    """Encrypt a plaintext string, returning a ciphertext string."""
    f = _get_fernet()
    return f.encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    """Decrypt a ciphertext string, returning the original plaintext."""
    f = _get_fernet()
    try:
        return f.decrypt(ciphertext.encode()).decode()
    except InvalidToken:
        raise EncryptionError("Decryption failed. The encryption key may have changed or data is corrupted.")


def encrypt_dict(data: dict) -> str:
    """Encrypt a dict as JSON."""
    return encrypt(json.dumps(data))


def decrypt_dict(ciphertext: str) -> dict:
    """Decrypt a ciphertext string back to a dict."""
    return json.loads(decrypt(ciphertext))
