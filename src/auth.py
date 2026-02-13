"""
Authentication and Authorization for Relay
Simple API key based auth for V1
"""

from fastapi import HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from typing import Optional
import secrets
import json
from pathlib import Path

# API Key header
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

class AuthManager:
    """Manages API keys and authentication"""
    
    def __init__(self, keys_file: Path = None):
        if keys_file is None:
            keys_file = Path(__file__).parent.parent / "api_keys.json"
        
        self.keys_file = keys_file
        self.keys = self._load_keys()
    
    def _load_keys(self) -> dict:
        """Load API keys from file"""
        if self.keys_file.exists():
            with open(self.keys_file, 'r') as f:
                return json.load(f)
        return {"keys": {}}
    
    def _save_keys(self):
        """Save API keys to file"""
        self.keys_file.parent.mkdir(exist_ok=True)
        with open(self.keys_file, 'w') as f:
            json.dump(self.keys, f, indent=2)
    
    def generate_key(self, name: str, description: str = "") -> str:
        """Generate a new API key"""
        key = f"relay_{secrets.token_urlsafe(32)}"
        
        self.keys["keys"][key] = {
            "name": name,
            "description": description,
            "created_at": __import__('datetime').datetime.utcnow().isoformat() + "Z",
            "active": True
        }
        
        self._save_keys()
        return key
    
    def validate_key(self, key: str) -> bool:
        """Check if API key is valid"""
        if key in self.keys["keys"]:
            return self.keys["keys"][key].get("active", False)
        return False
    
    def revoke_key(self, key: str):
        """Revoke an API key"""
        if key in self.keys["keys"]:
            self.keys["keys"][key]["active"] = False
            self._save_keys()
    
    def list_keys(self) -> list:
        """List all API keys (masked)"""
        return [
            {
                "key": key[:15] + "..." + key[-5:],
                **info
            }
            for key, info in self.keys["keys"].items()
        ]

# Global auth manager
auth_manager = AuthManager()

async def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    """
    Verify API key from header
    Returns key if valid, raises HTTPException if not
    """
    # For V1, allow access without key (development mode)
    # Set RELAY_REQUIRE_AUTH=true to enforce
    import os
    if os.getenv("RELAY_REQUIRE_AUTH", "false").lower() != "true":
        return "dev_mode"
    
    if api_key is None:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Provide X-API-Key header."
        )
    
    if not auth_manager.validate_key(api_key):
        raise HTTPException(
            status_code=403,
            detail="Invalid or revoked API key"
        )
    
    return api_key

async def optional_auth(api_key: str = Security(api_key_header)) -> Optional[str]:
    """Optional authentication (doesn't fail if no key)"""
    if api_key and auth_manager.validate_key(api_key):
        return api_key
    return None

# Helper to generate initial admin key
def generate_admin_key() -> str:
    """Generate admin API key on first run"""
    admin_key = auth_manager.generate_key(
        "admin",
        "Initial admin key - generated on first run"
    )
    print("=" * 60)
    print("ADMIN API KEY GENERATED")
    print("=" * 60)
    print(f"Key: {admin_key}")
    print()
    print("Save this key! It will not be shown again.")
    print("Use it in requests: -H 'X-API-Key: {key}'")
    print()
    print("To require authentication, set: RELAY_REQUIRE_AUTH=true")
    print("=" * 60)
    return admin_key
