"""Tests for capture_bronze's dedupe / ext flags (added for the Garmin subject)."""

import os

import pytest

from grecohome_core.bronze import capture_bronze


def _files(root: str) -> list[str]:
    return [
        os.path.join(d, n).replace(os.sep, "/")
        for d, _s, names in os.walk(root)
        for n in names
    ]


@pytest.mark.unit
class TestDedupeFlag:
    def test_dedupe_false_keeps_identical_payloads(self, tmp_path):
        root = str(tmp_path / "bronze")
        meta = {"content_type": "application/json"}
        a = capture_bronze("garmin", "sleep", b'{"x":1}', meta, bronze_root=root, dedupe=False)
        b = capture_bronze("garmin", "sleep", b'{"x":1}', meta, bronze_root=root, dedupe=False)
        # Immutable-source mode: both identical payloads are written.
        assert a and b and a != b
        payloads = [f for f in _files(root) if f.endswith(".json") and not f.endswith(".meta.json")]
        assert len(payloads) == 2

    def test_dedupe_true_default_still_dedupes(self, tmp_path):
        root = str(tmp_path / "bronze")
        meta = {"content_type": "application/json"}
        a = capture_bronze("whoop", "sleep", b'{"x":1}', meta, bronze_root=root)
        b = capture_bronze("whoop", "sleep", b'{"x":1}', meta, bronze_root=root)
        assert a is not None
        assert b is None  # deduped (default dedupe=True preserved for Whoop)


@pytest.mark.unit
class TestExtFlag:
    def test_ext_override(self, tmp_path):
        root = str(tmp_path / "bronze")
        path = capture_bronze(
            "garmin", "activity_fit", b"PK\x03\x04zipbytes",
            {"content_type": "application/zip", "capture_mode": "raw"},
            bronze_root=root, dedupe=False, ext="zip",
        )
        assert path is not None and path.endswith(".zip")

    def test_capture_mode_and_redacted_fields_flow_to_sidecar(self, tmp_path):
        import json
        import re

        root = str(tmp_path / "bronze")
        path = capture_bronze(
            "garmin", "user_settings", b'{"a":1}',
            {"content_type": "application/json", "capture_mode": "reserialized",
             "redacted_fields": ["profile.accessToken"]},
            bronze_root=root, dedupe=False,
        )
        sidecar = json.load(open(re.sub(r"\.json$", ".meta.json", path)))
        assert sidecar["capture_mode"] == "reserialized"
        assert sidecar["redacted_fields"] == ["profile.accessToken"]
