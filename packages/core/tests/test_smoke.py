"""Smoke test: the core package imports and exposes a version."""

import grecohome_core


def test_core_importable():
    assert grecohome_core.__version__
