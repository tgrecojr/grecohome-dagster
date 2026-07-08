"""Shared fixtures for geocode tests.

Point the module-level ``settings`` at a per-test temp bronze dir so assets/discovery
that read it stay isolated. ``PHOTON_BASE_URL`` comes from the root pytest-env.
"""

import pytest

from grecohome_geocode.config import settings


@pytest.fixture
def bronze_root(tmp_path, monkeypatch):
    root = str(tmp_path / "bronze")
    monkeypatch.setattr(settings, "bronze_root", root)
    return root
