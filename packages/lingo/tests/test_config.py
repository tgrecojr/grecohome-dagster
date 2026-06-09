"""Tests for LingoSettings."""

import pytest
from grecohome_lingo.config import LingoSettings


@pytest.mark.unit
class TestLingoSettings:
    def test_inherits_bronze_root(self):
        # BRONZE_ROOT comes from the test env (pyproject pytest-env).
        assert LingoSettings().bronze_root

    def test_defaults(self, monkeypatch):
        monkeypatch.delenv("GDRIVE_FOLDER_ID", raising=False)
        monkeypatch.delenv("GDRIVE_SERVICE_ACCOUNT_PATH", raising=False)
        monkeypatch.delenv("GDRIVE_POLL_INTERVAL_MINUTES", raising=False)
        s = LingoSettings()
        assert s.gdrive_folder_id == ""
        assert s.gdrive_service_account_path == ""
        assert s.gdrive_poll_interval_minutes == 5

    def test_reads_env(self, monkeypatch):
        monkeypatch.setenv("GDRIVE_FOLDER_ID", "abc123")
        monkeypatch.setenv("GDRIVE_SERVICE_ACCOUNT_PATH", "/secrets/lingo/sa.json")
        monkeypatch.setenv("GDRIVE_POLL_INTERVAL_MINUTES", "10")
        s = LingoSettings()
        assert s.gdrive_folder_id == "abc123"
        assert s.gdrive_service_account_path == "/secrets/lingo/sa.json"
        assert s.gdrive_poll_interval_minutes == 10
