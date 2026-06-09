"""Tests that the Garmin code location's Definitions resolve cleanly."""

import pytest
from grecohome_garmin import catalog
from grecohome_garmin.dagster.assets import DAILY_ASSETS, REFERENCE_ASSETS
from grecohome_garmin.dagster.definitions import defs


@pytest.mark.unit
class TestDefinitions:
    def test_one_asset_per_catalog_collection(self):
        keys = {str(k) for k in defs.resolve_asset_graph().get_all_asset_keys()}
        assert len(keys) == len(catalog.CATALOG)
        assert len(DAILY_ASSETS) + len(REFERENCE_ASSETS) == len(catalog.CATALOG)
        assert "AssetKey(['garmin_bronze_sleep'])" in keys
        assert "AssetKey(['garmin_bronze_activities'])" in keys
        assert "AssetKey(['garmin_bronze_devices'])" in keys

    def test_schedules_are_daily_utc(self):
        for name in ("garmin_daily", "garmin_reference"):
            s = defs.get_schedule_def(name)
            assert s.cron_schedule == "0 7 * * *"
            assert str(s.execution_timezone) == "UTC"

    def test_jobs_resolve(self):
        for name in ("garmin_daily_job", "garmin_reference_job"):
            assert defs.resolve_job_def(name) is not None
