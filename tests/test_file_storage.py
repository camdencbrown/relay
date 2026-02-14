"""Tests for file_storage module."""

import os
import tempfile
from unittest.mock import MagicMock

from src.file_storage import ensure_local_storage_dir, write_file


def test_write_file_local_mode(monkeypatch):
    """Local mode writes file to disk and returns absolute path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("STORAGE_MODE", "local")
        monkeypatch.setenv("LOCAL_STORAGE_PATH", tmpdir)
        from src.config import get_settings

        get_settings.cache_clear()

        path = write_file(b"hello world", "my-bucket", "data/test.parquet")

        assert os.path.isabs(path)
        assert os.path.exists(path)
        with open(path, "rb") as f:
            assert f.read() == b"hello world"

        get_settings.cache_clear()


def test_write_file_s3_mode(monkeypatch):
    """S3 mode delegates to s3_client.put_object and returns s3:// URI."""
    monkeypatch.setenv("STORAGE_MODE", "s3")
    from src.config import get_settings

    get_settings.cache_clear()

    mock_client = MagicMock()
    result = write_file(b"data", "bucket", "path/file.parquet", s3_client=mock_client)

    mock_client.put_object.assert_called_once_with(Bucket="bucket", Key="path/file.parquet", Body=b"data")
    assert result == "s3://bucket/path/file.parquet"

    get_settings.cache_clear()


def test_ensure_local_storage_dir(monkeypatch):
    """ensure_local_storage_dir creates directory in local mode."""
    with tempfile.TemporaryDirectory() as tmpdir:
        target = os.path.join(tmpdir, "sub", "relay_data")
        monkeypatch.setenv("STORAGE_MODE", "local")
        monkeypatch.setenv("LOCAL_STORAGE_PATH", target)
        from src.config import get_settings

        get_settings.cache_clear()

        ensure_local_storage_dir()
        assert os.path.isdir(target)

        get_settings.cache_clear()


def test_ensure_local_storage_dir_noop_s3(monkeypatch):
    """ensure_local_storage_dir does nothing in S3 mode."""
    monkeypatch.setenv("STORAGE_MODE", "s3")
    monkeypatch.setenv("LOCAL_STORAGE_PATH", "/tmp/should_not_create_this_relay_test")
    from src.config import get_settings

    get_settings.cache_clear()

    ensure_local_storage_dir()
    assert not os.path.exists("/tmp/should_not_create_this_relay_test")

    get_settings.cache_clear()
