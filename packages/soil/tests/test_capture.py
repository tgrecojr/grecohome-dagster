"""Tests for the USCRN bronze capture adapter."""

import glob
import json
import os

import pytest
from grecohome_soil.capture import capture_hourly

ROWS = [
    "03761 20260609 0000 20260608 2000 2.0 1.9 2.1 1.8 0.0",
    "03761 20260609 0100 20260608 2100 1.8 1.7 1.9 1.6 0.0",
]
SRC_URL = "https://x/h02/2026/CRNH0203-2026-PA_Avondale_2_N.txt"


def _capture(rows, root):
    return capture_hourly(
        rows,
        station="PA_Avondale_2_N",
        partition_date="2026-06-09",
        year=2026,
        source_url=SRC_URL,
        bronze_root=root,
    )


def _payloads(root):
    return sorted(glob.glob(os.path.join(root, "uscrn", "hourly", "dt=2026-06-09", "*.txt")))


@pytest.mark.unit
class TestCaptureHourly:
    def test_writes_the_days_rows(self, isolate_soil_bronze):
        root = isolate_soil_bronze
        path = _capture(ROWS, root)
        assert path and os.path.exists(path)
        assert open(path, "rb").read() == ("\n".join(ROWS) + "\n").encode("utf-8")

    def test_sidecar_provenance(self, isolate_soil_bronze):
        root = isolate_soil_bronze
        path = _capture(ROWS, root)
        meta = json.load(open(path.removesuffix(".txt") + ".meta.json"))
        assert meta["source"] == "uscrn"
        assert meta["collection"] == "hourly"
        assert meta["request_params"]["station"] == "PA_Avondale_2_N"
        assert meta["request_params"]["utc_date"] == "20260609"
        assert meta["request_params"]["wbanno"] == "03761"
        assert meta["request_params"]["row_count"] == 2

    def test_skips_empty(self, isolate_soil_bronze):
        root = isolate_soil_bronze
        assert _capture([], root) is None
        assert _payloads(root) == []

    def test_dedup_then_new_row(self, isolate_soil_bronze):
        root = isolate_soil_bronze
        assert _capture(ROWS, root) is not None
        # Identical rows -> deduped, no second file.
        assert _capture(ROWS, root) is None
        assert len(_payloads(root)) == 1
        # A new hourly row -> different payload, captured.
        more = ROWS + ["03761 20260609 0200 20260608 2200 1.6 1.5 1.7 1.4 0.0"]
        assert _capture(more, root) is not None
        assert len(_payloads(root)) == 2
