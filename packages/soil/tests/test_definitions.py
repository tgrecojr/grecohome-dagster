"""Tests that the USCRN/soil code location's Definitions resolve cleanly."""

import pytest
from grecohome_soil.dagster.definitions import defs


@pytest.mark.unit
class TestDefinitions:
    def test_single_asset(self):
        keys = {str(k) for k in defs.resolve_asset_graph().get_all_asset_keys()}
        assert keys == {"AssetKey(['uscrn_bronze_hourly'])"}

    def test_capture_job_resolves(self):
        assert defs.resolve_job_def("uscrn_capture_job") is not None


@pytest.mark.unit
class TestAssetChecks:
    def _checks(self):
        ag = defs.resolve_asset_graph()
        return {(str(k.asset_key), k.name) for k in ag.asset_check_keys}

    def test_all_four_families_present(self):
        keys = self._checks()
        akey = "AssetKey(['uscrn_bronze_hourly'])"
        for fam in ("freshness", "completeness", "schema_drift", "content_health"):
            assert (akey, f"uscrn_hourly_{fam}") in keys

    def test_event_date_is_partition(self):
        from grecohome_soil.dagster.checks import SOIL_CHECK_CONFIGS

        cfg = SOIL_CHECK_CONFIGS[0]
        assert cfg.event_date_source == "partition"
        assert cfg.reader == "txt"

    def test_validation_job_and_schedule_registered(self):
        assert defs.resolve_job_def("uscrn_bronze_checks_job") is not None
        s = defs.get_schedule_def("uscrn_bronze_checks_hourly")
        assert s.cron_schedule == "0 * * * *"
