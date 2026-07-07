"""Shared fixtures for location tests.

Point the module-level ``settings`` at per-test temp dirs so checks/assets that read
it during execution stay isolated. Capture/promote unit tests pass paths explicitly
and don't need these, but the fixtures are cheap and keep the whole suite hermetic.
"""

import pytest
from grecohome_location.config import settings


@pytest.fixture
def bronze_root(tmp_path, monkeypatch):
    root = str(tmp_path / "bronze")
    monkeypatch.setattr(settings, "bronze_root", root)
    return root


@pytest.fixture
def capture_dir(tmp_path, monkeypatch):
    d = str(tmp_path / "relay")
    monkeypatch.setattr(settings, "relay_capture_dir", d)
    return d


@pytest.fixture
def state_dir(tmp_path, monkeypatch):
    d = str(tmp_path / "state")
    monkeypatch.setattr(settings, "location_state_dir", d)
    return d
