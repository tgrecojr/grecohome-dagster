"""Tests pinning USCRN behavior named in the Soil specs that had no dedicated test."""

from unittest.mock import patch

import httpx
import pytest
import respx
from dagster import materialize
from grecohome_soil import fetch
from grecohome_soil.dagster.assets import MAX_PARTITIONS_PER_RUN, uscrn_bronze_hourly
from grecohome_soil.dagster.checks import SOIL_CHECK_CONFIGS
from grecohome_soil.dagster.definitions import defs
from grecohome_soil.dagster.schedules import PARTITION_RANGE_END_TAG, PARTITION_RANGE_START_TAG

FILE_2026 = "\n".join(
    [
        "03761 20260105 0100 20260104 2000 0.4 0.4 0.4 0.4 0.0",
        "03761 20260106 0100 20260105 2000 0.3 0.3 0.3 0.3 0.0",
    ]
)


@pytest.mark.unit
class TestFetchAndRun:
    # @spec SOIL-CAPTURE-006
    @respx.mock
    def test_404_logs_a_warning(self):
        url = fetch.year_file_url(2026)
        respx.get(url).mock(return_value=httpx.Response(404))
        with patch.object(fetch.log, "warning") as warn:
            assert fetch.fetch_year_file(url) is None
        warn.assert_called_once()
        assert warn.call_args.args[0] == "uscrn year file not found"

    # @spec SOIL-CAPTURE-009
    @respx.mock
    def test_range_run_records_its_metadata(self, isolate_soil_bronze):
        respx.get(fetch.year_file_url(2026)).mock(return_value=httpx.Response(200, text=FILE_2026))
        result = materialize(
            [uscrn_bronze_hourly],
            tags={PARTITION_RANGE_START_TAG: "2026-01-05", PARTITION_RANGE_END_TAG: "2026-01-07"},
        )
        (mat,) = result.asset_materializations_for_node("uscrn_bronze_hourly")[-1:]
        md = {k: v.value for k, v in mat.metadata.items()}
        assert md["partitions"] == 3
        assert (md["first_partition"], md["last_partition"]) == ("2026-01-05", "2026-01-07")
        assert md["rows"] == 2 and md["captured"] == 2
        assert md["year_files"] == {"2026": "ok"}

    # @spec SOIL-CAPTURE-010
    def test_backfills_are_split_into_runs_of_at_most_366_partitions(self):
        assert MAX_PARTITIONS_PER_RUN == 366
        assert uscrn_bronze_hourly.backfill_policy.max_partitions_per_run == 366


@pytest.mark.unit
class TestCheckConfig:
    # @spec SOIL-CHECKS-001
    def test_uscrn_hourly_check_settings(self):
        (cfg,) = SOIL_CHECK_CONFIGS
        assert (cfg.source, cfg.collection, cfg.reader) == ("uscrn", "hourly", "txt")
        assert cfg.event_date_source == "partition"
        assert (cfg.cadence_hours, cfg.grace_hours, cfg.cadence_days) == (30.0, 12.0, 2)
        assert cfg.enabled_checks == {"freshness", "completeness", "schema", "content"}
        s = defs.get_schedule_def("uscrn_bronze_checks_hourly")
        assert s.job_name == "uscrn_bronze_checks_job"
        assert s.cron_schedule == "43 * * * *"
        assert str(s.execution_timezone) == "UTC"
