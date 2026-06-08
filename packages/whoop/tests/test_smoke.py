"""Smoke test: the whoop package imports and exposes a version."""

import grecohome_whoop


def test_whoop_importable():
    assert grecohome_whoop.__version__
