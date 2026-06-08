"""Tests for BaseSubjectSettings and init_settings."""

import pytest

from grecohome_core.config import BaseSubjectSettings, init_settings


@pytest.mark.unit
class TestBaseSubjectSettings:
    def test_loads_bronze_root_from_env(self, monkeypatch):
        monkeypatch.setenv("BRONZE_ROOT", "/data/bronze")
        s = BaseSubjectSettings()
        assert s.bronze_root == "/data/bronze"

    def test_defaults(self, monkeypatch):
        monkeypatch.setenv("BRONZE_ROOT", "/data/bronze")
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        s = BaseSubjectSettings()
        assert s.log_level == "INFO"
        assert s.environment == "development"

    def test_missing_bronze_root_exits(self, monkeypatch):
        monkeypatch.delenv("BRONZE_ROOT", raising=False)
        with pytest.raises(SystemExit):
            init_settings(BaseSubjectSettings)

    def test_init_settings_returns_instance_when_valid(self, monkeypatch):
        monkeypatch.setenv("BRONZE_ROOT", "/data/bronze")
        s = init_settings(BaseSubjectSettings)
        assert isinstance(s, BaseSubjectSettings)
        assert s.bronze_root == "/data/bronze"
