"""Tests for the geocode bronze capture adapter."""

import glob
import hashlib
import json
import os

import pytest

from grecohome_geocode.capture import capture_reverse

pytestmark = pytest.mark.unit

RESP = (
    b'{"type":"FeatureCollection","features":[{"type":"Feature",'
    b'"geometry":{"type":"Point","coordinates":[-75.1,39.8]},'
    b'"properties":{"name":"Home","city":"Avondale"}}]}'
)


def _capture(root, **kw):
    defaults = dict(
        lat_e4=398000,
        lon_e4=-751000,
        query_lat=39.8,
        query_lon=-75.1,
        radius_km=0.05,
        language="en",
        dt="2026-07-07",
        bronze_root=root,
    )
    defaults.update(kw)
    return capture_reverse(RESP, **defaults)


def _sidecar(payload_path: str) -> dict:
    with open(os.path.splitext(payload_path)[0] + ".meta.json") as fh:
        return json.load(fh)


class TestCaptureReverse:
    def test_writes_byte_exact_payload(self, tmp_path):
        root = str(tmp_path / "bronze")
        path = _capture(root)
        assert path is not None
        assert os.path.join("geocode", "reverse", "dt=2026-07-07") in path
        with open(path, "rb") as fh:
            stored = fh.read()
        assert stored == RESP
        assert hashlib.sha256(stored).hexdigest() == hashlib.sha256(RESP).hexdigest()

    def test_sidecar_carries_cell_key(self, tmp_path):
        root = str(tmp_path / "bronze")
        sc = _sidecar(_capture(root))
        assert sc["source"] == "geocode"
        assert sc["collection"] == "reverse"
        assert sc["capture_mode"] == "raw"
        assert sc["lat_e4"] == 398000
        assert sc["lon_e4"] == -751000
        assert sc["query_lat"] == 39.8
        assert sc["query_lon"] == -75.1
        assert sc["cell_precision"] == 4
        assert sc["request_params"]["radius"] == 0.05

    def test_distinct_cells_identical_response_both_land(self, tmp_path):
        """dedupe=False: two distinct cells with identical bytes must BOTH be cached.

        Content-hash dedup would drop the second and leave that cell un-cached (re-looked-
        up forever); idempotency is cell-based, so both captures land.
        """
        root = str(tmp_path / "bronze")
        assert _capture(root, lat_e4=398000, lon_e4=-751000) is not None
        assert _capture(root, lat_e4=399000, lon_e4=-752000) is not None
        payloads = [
            f
            for f in glob.glob(os.path.join(root, "geocode", "reverse", "dt=*", "*.json"))
            if not f.endswith(".meta.json")
        ]
        assert len(payloads) == 2
