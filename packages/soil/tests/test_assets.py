"""Tests for the USCRN bronze asset (materialized against mocked year files)."""

import glob
import json
import os

import httpx
import pytest
import respx
from dagster import materialize
from grecohome_soil import fetch
from grecohome_soil.dagster.assets import uscrn_bronze_hourly
from grecohome_soil.dagster.schedules import PARTITION_RANGE_END_TAG, PARTITION_RANGE_START_TAG

# Rows across two UTC dates; only the 2026-06-09 rows should be captured.
SAMPLE = "\n".join(
    [
        "03761 20260608 2300 20260608 1900 2.5 2.4 2.6 2.3 0.0",
        "03761 20260609 0000 20260608 2000 2.0 1.9 2.1 1.8 0.0",
        "03761 20260609 0100 20260608 2100 1.8 1.7 1.9 1.6 0.0",
    ]
)

# Year boundary: the 2026-01-01 00:00Z row is the LAST row of the 2025 file.
FILE_2025 = "\n".join(
    [
        "03761 20251231 2300 20251231 1800 1.0 1.0 1.0 1.0 0.0",
        "03761 20260101 0000 20251231 1900 0.5 0.5 0.5 0.5 0.0",
    ]
)
FILE_2026 = "\n".join(
    [
        "03761 20260101 0100 20251231 2000 0.4 0.4 0.4 0.4 0.0",
        "03761 20260101 0200 20251231 2100 0.3 0.3 0.3 0.3 0.0",
        "03761 20260102 0000 20260101 1900 0.2 0.2 0.2 0.2 0.0",
    ]
)


def _payloads(root, dt="2026-06-09"):
    return glob.glob(os.path.join(root, "uscrn", "hourly", f"dt={dt}", "*.txt"))


def _lines(path):
    return open(path).read().strip().splitlines()


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
        lines = _lines(files[0])
        assert len(lines) == 2
        assert all(line.split()[1] == "20260609" for line in lines)

    @respx.mock
    def test_404_skips_without_writing(self, isolate_soil_bronze):
        root = isolate_soil_bronze
        respx.get(fetch.year_file_url(2026)).mock(return_value=httpx.Response(404))

        result = materialize([uscrn_bronze_hourly], partition_key="2026-06-09")
        assert result.success
        assert _payloads(root) == []

    @respx.mock
    def test_jan_1_takes_the_midnight_row_from_the_prior_year_file(self, isolate_soil_bronze):
        """The ``YYYY0101 0000`` row lives in the previous year's file; it must not be lost."""
        root = isolate_soil_bronze
        respx.get(fetch.year_file_url(2025)).mock(return_value=httpx.Response(200, text=FILE_2025))
        respx.get(fetch.year_file_url(2026)).mock(return_value=httpx.Response(200, text=FILE_2026))

        assert materialize([uscrn_bronze_hourly], partition_key="2026-01-01").success

        files = _payloads(root, "2026-01-01")
        assert len(files) == 1
        assert [ln.split()[2] for ln in _lines(files[0])] == ["0000", "0100", "0200"]
        meta = json.load(open(files[0].removesuffix(".txt") + ".meta.json"))
        assert meta["request_params"]["year_files"] == [2025, 2026]

    @respx.mock
    def test_jan_1_survives_a_missing_prior_year_file(self, isolate_soil_bronze):
        root = isolate_soil_bronze
        respx.get(fetch.year_file_url(2025)).mock(return_value=httpx.Response(404))
        respx.get(fetch.year_file_url(2026)).mock(return_value=httpx.Response(200, text=FILE_2026))

        assert materialize([uscrn_bronze_hourly], partition_key="2026-01-01").success
        files = _payloads(root, "2026-01-01")
        assert [ln.split()[2] for ln in _lines(files[0])] == ["0100", "0200"]

    @respx.mock
    def test_partition_range_run_fetches_each_year_file_once(self, isolate_soil_bronze):
        """A range run (correction sweep / backfill chunk) slices every partition
        from one download per year file and captures each day separately."""
        root = isolate_soil_bronze
        r25 = respx.get(fetch.year_file_url(2025)).mock(
            return_value=httpx.Response(200, text=FILE_2025)
        )
        r26 = respx.get(fetch.year_file_url(2026)).mock(
            return_value=httpx.Response(200, text=FILE_2026)
        )

        result = materialize(
            [uscrn_bronze_hourly],
            tags={PARTITION_RANGE_START_TAG: "2025-12-31", PARTITION_RANGE_END_TAG: "2026-01-02"},
        )
        assert result.success
        assert r25.call_count == 1
        assert r26.call_count == 1

        assert [ln.split()[2] for ln in _lines(_payloads(root, "2025-12-31")[0])] == ["2300"]
        assert [ln.split()[2] for ln in _lines(_payloads(root, "2026-01-01")[0])] == [
            "0000",
            "0100",
            "0200",
        ]
        assert [ln.split()[2] for ln in _lines(_payloads(root, "2026-01-02")[0])] == ["0000"]

    @respx.mock
    def test_recapture_of_a_back_corrected_day_lands_as_a_new_file(self, isolate_soil_bronze):
        """NOAA fills a gap hour later: the re-slice differs -> a second capture."""
        root = isolate_soil_bronze
        route = respx.get(fetch.year_file_url(2026)).mock(
            return_value=httpx.Response(200, text=SAMPLE)
        )
        assert materialize([uscrn_bronze_hourly], partition_key="2026-06-09").success
        # Unchanged day -> deduped, still one file.
        assert materialize([uscrn_bronze_hourly], partition_key="2026-06-09").success
        assert len(_payloads(root)) == 1

        corrected = SAMPLE.replace("2.0 1.9 2.1 1.8 0.0", "2.2 2.1 2.3 2.0 0.0")
        route.mock(return_value=httpx.Response(200, text=corrected))
        assert materialize([uscrn_bronze_hourly], partition_key="2026-06-09").success
        assert len(_payloads(root)) == 2
