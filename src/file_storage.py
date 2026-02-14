"""
File storage abstraction for Relay.
Supports local disk or S3 based on STORAGE_MODE setting.
"""

from pathlib import Path

from .config import get_settings


def ensure_local_storage_dir() -> None:
    """Create the local storage directory if using local mode."""
    settings = get_settings()
    if settings.storage_mode == "local":
        Path(settings.local_storage_path).mkdir(parents=True, exist_ok=True)


def write_file(content: bytes, bucket: str, key: str, s3_client=None) -> str:
    """Write file content to local disk or S3.

    Returns absolute local path or s3:// URI.
    """
    settings = get_settings()

    if settings.storage_mode == "local":
        dest = Path(settings.local_storage_path) / bucket / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        return str(dest.resolve())

    # S3 mode
    s3_client.put_object(Bucket=bucket, Key=key, Body=content)
    return f"s3://{bucket}/{key}"
