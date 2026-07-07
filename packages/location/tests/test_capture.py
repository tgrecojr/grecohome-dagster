"""Tests for the location bronze capture adapter."""

import hashlib
import json
import os

import pytest
from grecohome_location.capture import capture_location, iso_from_ms

OVERLAND = (
    b'{"locations":[{"type":"Feature","geometry":{"type":"Point",'
    b'"coordinates":[-75.1,39.8]},"properties":{"timestamp":"2026-07-07T12:00:00Z"}},'
    b'{"type":"Feature","geometry":{"type":"Point","coordinates":[-75.2,39.9]},'
    b'"properties":{"timestamp":"2026-07-07T12:05:00Z"}}]}'
)
OWNTRACKS = b'{"_type":"location","lat":39.8,"lon":-75.1,"tst":1751894460,"acc":5}'


def _sidecar(payload_path: str) -> dict:
    with open(os.path.splitext(payload_path)[0] + ".meta.json") as fh:
        return json.load(fh)


@pytest.mark.unit
class TestCaptureLocation:
    def test_overland_byte_identical_and_sidecar(self, tmp_path):
        bronze = str(tmp_path / "bronze")
        ms = 1751894460456
        name = "1751894460456_d4e5f6.json"
        path = capture_location(
            OVERLAND,
            collection="overland",
            dt="2026-07-07",
            received_ms=ms,
            staging_file=name,
            bronze_root=bronze,
        )
        assert path is not None
        assert os.path.join("location", "overland", "dt=2026-07-07") in path

        with open(path, "rb") as fh:
            stored = fh.read()
        # Byte-exact: the bronze payload IS the staging bytes.
        assert stored == OVERLAND
        assert hashlib.sha256(stored).hexdigest() == hashlib.sha256(OVERLAND).hexdigest()
        # Envelope preserved: one object, N locations (no batch explosion).
        assert len(json.loads(stored)["locations"]) == 2

        sc = _sidecar(path)
        assert sc["source"] == "location"
        assert sc["collection"] == "overland"
        assert sc["capture_mode"] == "raw"
        assert sc["ingest_route"] == "/overland"
        assert sc["received_unix_ms"] == ms
        assert sc["received_at"] == iso_from_ms(ms)
        assert sc["staging_file"] == name
        assert sc["redacted_fields"] == []
        assert sc["sha256"] == hashlib.sha256(OVERLAND).hexdigest()
        assert sc["byte_size"] == len(OVERLAND)
        # fetched_at (promote time) is stamped and distinct from received_at.
        assert "fetched_at" in sc and sc["fetched_at"] != sc["received_at"]

    def test_owntracks_byte_identical_and_sidecar(self, tmp_path):
        bronze = str(tmp_path / "bronze")
        ms = 1751894460456
        name = "1751894460456_a1b2c3.json"
        path = capture_location(
            OWNTRACKS,
            collection="owntracks",
            dt="2026-07-07",
            received_ms=ms,
            staging_file=name,
            bronze_root=bronze,
        )
        assert path is not None
        assert os.path.join("location", "owntracks", "dt=2026-07-07") in path
        with open(path, "rb") as fh:
            assert fh.read() == OWNTRACKS
        sc = _sidecar(path)
        assert sc["collection"] == "owntracks"
        assert sc["ingest_route"] == "/owntracks"
        assert sc["capture_mode"] == "raw"
        assert json.loads(OWNTRACKS)["_type"] == "location"

    def test_iso_from_ms_millisecond_precision(self):
        assert iso_from_ms(0) == "1970-01-01T00:00:00.000Z"
        assert iso_from_ms(1234) == "1970-01-01T00:00:01.234Z"

    def test_no_secret_in_sidecar(self, tmp_path):
        """The auth token is header/query-only at the relay; nothing secret is written."""
        bronze = str(tmp_path / "bronze")
        path = capture_location(
            OWNTRACKS,
            collection="owntracks",
            dt="2026-07-07",
            received_ms=1,
            staging_file="1_abcdef.json",
            bronze_root=bronze,
        )
        blob = json.dumps(_sidecar(path)).lower()
        for token in ("authorization", "bearer", "token", "secret", "password"):
            assert token not in blob
