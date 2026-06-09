"""Shared fixtures for lingo tests."""

import pytest
from grecohome_lingo.config import settings


@pytest.fixture(autouse=True)
def isolate_lingo_bronze(tmp_path, monkeypatch):
    """Point bronze capture at a per-test temp dir (for asset materialization tests)."""
    root = str(tmp_path / "bronze")
    monkeypatch.setattr(settings, "bronze_root", root)
    return root
