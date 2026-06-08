"""Tests that the Whoop code location's Definitions resolve cleanly."""

import pytest

from grecohome_whoop.dagster.definitions import defs


@pytest.mark.unit
class TestDefinitions:
    def test_asset_keys(self):
        keys = {str(k) for k in defs.resolve_asset_graph().get_all_asset_keys()}
        assert keys == {
            "AssetKey(['whoop_bronze_sleep'])",
            "AssetKey(['whoop_bronze_recovery'])",
            "AssetKey(['whoop_bronze_workout'])",
            "AssetKey(['whoop_bronze_cycle'])",
            "AssetKey(['whoop_bronze_snapshots'])",
        }

    def test_schedules_are_hourly_utc(self):
        for name in ("whoop_hourly", "whoop_snapshots_hourly"):
            s = defs.get_schedule_def(name)
            assert s.cron_schedule == "0 * * * *"
            assert str(s.execution_timezone) == "UTC"

    def test_jobs_present(self):
        for name in ("whoop_bronze_job", "whoop_snapshots_job"):
            assert defs.resolve_job_def(name) is not None
