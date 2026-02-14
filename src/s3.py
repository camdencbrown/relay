"""
S3 client factory for Relay
Centralizes S3 client creation and DuckDB S3 configuration
"""

from functools import lru_cache

import boto3

from .config import get_settings


@lru_cache
def get_s3_client():
    """Create and cache an S3 client using application settings."""
    settings = get_settings()
    if settings.aws_access_key_id and settings.aws_secret_access_key:
        kwargs = {
            "aws_access_key_id": settings.aws_access_key_id,
            "aws_secret_access_key": settings.aws_secret_access_key,
            "region_name": settings.aws_default_region,
        }
        if settings.aws_session_token:
            kwargs["aws_session_token"] = settings.aws_session_token
        return boto3.client("s3", **kwargs)
    return boto3.client("s3", region_name=settings.aws_default_region)


def get_duckdb_s3_config() -> dict:
    """Return dict of S3 credentials for DuckDB secret creation."""
    settings = get_settings()
    if settings.aws_access_key_id and settings.aws_secret_access_key:
        cfg = {
            "key_id": settings.aws_access_key_id,
            "secret": settings.aws_secret_access_key,
            "region": settings.aws_default_region,
        }
        if settings.aws_session_token:
            cfg["session_token"] = settings.aws_session_token
        return cfg
    return {}
