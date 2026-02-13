"""
Centralized configuration for Relay
Uses pydantic-settings to load from environment variables
"""

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # App
    app_name: str = "Relay"
    app_version: str = "2.0.0"
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = 8001
    log_level: str = "INFO"

    # Database
    database_url: str = "sqlite:///relay.db"

    # AWS / S3
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_default_region: str = "us-west-1"
    s3_bucket_name: str = ""

    # Auth
    require_auth: bool = False

    # AI
    anthropic_api_key: str = ""

    # CORS
    cors_origins: str = "*"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
    }


@lru_cache
def get_settings() -> Settings:
    """Cached settings singleton."""
    return Settings()
