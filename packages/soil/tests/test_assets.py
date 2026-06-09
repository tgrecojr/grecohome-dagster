"""Tests for the USCRN bronze asset (materialized against a mocked year file)."""

import glob
import os

import httpx
import pytest
import respx
from dagster import materialize
from grecohome_soil import fetch
from grecohome_soil.dagster.assets import uscrn_bronze_hourly

# Rows across two UTC dates; only the 2026-06-09 rows should be captured.
SAMPLE = "\n".join(
    [
        "03761 20260608 2300 20260608 1900 2.5 2.4 2.6 2.3 0.0",
        "03761 20260609 0000 20260608 2000 2.0 1.9 2.1 1.8 0.0",
        "03761 20260609 0100 20260608 2100 1.8 1.7 1.9 1.6 0.0",
    ]
)


def _payloads(root):
    return glob.glob(os.path.join(root, "uscrn", "hourly", "dt=2026-06-09", "*.txt"))


@pytest.mark.unit
class TestUscrnBronzeAsset:
    @respx.mock
    def test_materialize_writes_only_the_days_rows(self, isolate_soil_bronze):
        root = isolate_soil_bronze
        respx.get(fetch.year_file_url(2026)).mock(return_value=httpx.Response(200, text=SAMPLE))

        result = materialize([uscrn_bronze_hourly], partition_key="2026-06-09")
        assert result.success

        files = _payloads(root)
        assert len(files) == 1
        lines = open(files[0]).read().strip().splitlines()
        assert len(lines) == 2
        assert all(line.split()[1] == "20260609" for line in lines)

    @respx.mock
    def test_404_skips_without_writing(self, isolate_soil_bronze):
        root = isolate_soil_bronze
        respx.get(fetch.year_file_url(2026)).mock(return_value=httpx.Response(404))

        result = materialize([uscrn_bronze_hourly], partition_key="2026-06-09")
        assert result.success
        assert _payloads(root) == []
