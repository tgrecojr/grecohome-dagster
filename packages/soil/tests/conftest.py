"""Shared fixtures for soil tests."""

import pytest
from grecohome_soil.config import settings


@pytest.fixture(autouse=True)
def isolate_soil_bronze(tmp_path, monkeypatch):
    """Point bronze capture at a per-test temp dir (for capture/asset tests)."""
    root = str(tmp_path / "bronze")
    monkeypatch.setattr(settings, "bronze_root", root)
    return root
