"""Tests for LingoSettings."""

import pytest
from grecohome_lingo.config import LingoSettings
from pydantic import ValidationError


@pytest.mark.unit
class TestLingoSettings:
    def test_inherits_bronze_root(self):
        # BRONZE_ROOT comes from the test env (pyproject pytest-env).
        assert LingoSettings().bronze_root

    # @spec LINGO-SENSOR-004
    def test_poll_interval_defaults_to_five_minutes(self, monkeypatch):
        monkeypatch.delenv("GDRIVE_POLL_INTERVAL_MINUTES", raising=False)
        assert LingoSettings().gdrive_poll_interval_minutes == 5

    # @spec LINGO-AUTH-002
    @pytest.mark.parametrize("missing", ["GDRIVE_FOLDER_ID", "GDRIVE_SERVICE_ACCOUNT_PATH"])
    def test_folder_id_and_key_path_are_required(self, monkeypatch, missing):
        monkeypatch.delenv(missing, raising=False)
        with pytest.raises(ValidationError):
            LingoSettings()

    def test_reads_env(self, monkeypatch):
        monkeypatch.setenv("GDRIVE_FOLDER_ID", "abc123")
        monkeypatch.setenv("GDRIVE_SERVICE_ACCOUNT_PATH", "/secrets/lingo/sa.json")
        monkeypatch.setenv("GDRIVE_POLL_INTERVAL_MINUTES", "10")
        s = LingoSettings()
        assert s.gdrive_folder_id == "abc123"
        assert s.gdrive_service_account_path == "/secrets/lingo/sa.json"
        assert s.gdrive_poll_interval_minutes == 10
