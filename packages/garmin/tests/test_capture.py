"""Tests for the Garmin capture adapter over core capture_bronze."""

import json
import os
import re

import pytest
from grecohome_garmin.capture import capture_json, capture_raw


def _files(root: str) -> list[str]:
    return [
        os.path.join(d, n).replace(os.sep, "/")
        for d, _s, names in os.walk(root)
        for n in names
    ]


def _sidecar(payload_path: str) -> dict:
    # core writes the sidecar as "{base}.meta.json" (strip the payload ext).
    base = re.sub(r"\.[^.]+$", "", payload_path)
    return json.load(open(base + ".meta.json"))


@pytest.mark.unit
class TestCaptureJson:
    def test_writes_reserialized_json_under_partition(self, tmp_path):
        root = str(tmp_path / "bronze")
        path = capture_json(
            "sleep", {"a": 1}, request_url="get_sleep_data",
            request_params={"cdate": "2025-01-05"}, bronze_root=root, dt="2025-01-05",
        )
        assert path is not None
        assert "/garmin/sleep/dt=2025-01-05/" in path
        assert path.endswith(".json")
        sc = _sidecar(path)
        assert sc["capture_mode"] == "reserialized"
        assert sc["source"] == "garmin"
        assert sc["processor"] == "grecohome-garmin"

    def test_no_dedup_keeps_identical_captures(self, tmp_path):
        root = str(tmp_path / "bronze")
        for _ in range(2):
            capture_json("sleep", {"a": 1}, request_url="get_sleep_data",
                         request_params={}, bronze_root=root, dt="2025-01-05")
        payloads = [f for f in _files(root) if f.endswith(".json") and not f.endswith(".meta.json")]
        assert len(payloads) == 2  # immutable source: both kept

    def test_screen_secrets_redacts_and_records(self, tmp_path):
        root = str(tmp_path / "bronze")
        path = capture_json(
            "user_settings", {"accessToken": "x", "name": "ok"},
            request_url="get_user_profile", request_params={}, bronze_root=root,
            screen_secrets=True,
        )
        payload = json.load(open(path))
        assert payload == {"name": "ok"}  # secret removed from stored bytes
        assert _sidecar(path)["redacted_fields"] == ["accessToken"]


@pytest.mark.unit
class TestCaptureRaw:
    def test_writes_raw_binary_with_ext(self, tmp_path):
        root = str(tmp_path / "bronze")
        path = capture_raw(
            "activity_fit", b"PK\x03\x04zipbytes", ext="zip", content_type="application/zip",
            request_url="download_activity",
            request_params={"activity_id": 1, "format": "ORIGINAL"},
            bronze_root=root, dt="2025-01-05",
        )
        assert path is not None and path.endswith(".zip")
        with open(path, "rb") as fh:
            assert fh.read() == b"PK\x03\x04zipbytes"  # true bytes, not reserialized
        assert _sidecar(path)["capture_mode"] == "raw"
