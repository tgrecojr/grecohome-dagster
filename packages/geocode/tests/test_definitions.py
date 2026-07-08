"""Tests that the geocode code location's Definitions resolve cleanly."""

import pytest

from grecohome_geocode.dagster.definitions import defs

pytestmark = pytest.mark.unit


class TestDefinitions:
    def test_single_asset(self):
        keys = {str(k) for k in defs.resolve_asset_graph().get_all_asset_keys()}
        assert "AssetKey(['geocode_bronze_reverse'])" in keys

    def test_capture_job_resolves(self):
        assert defs.resolve_job_def("geocode_capture_job") is not None

    def test_capture_schedule_every_30m(self):
        s = defs.get_schedule_def("geocode_reverse_every_30m")
        assert s.cron_schedule == "*/30 * * * *"


class TestAssetChecks:
    def _checks(self):
        ag = defs.resolve_asset_graph()
        return {(str(k.asset_key), k.name) for k in ag.asset_check_keys}

    def test_content_and_schema_checks_present(self):
        keys = self._checks()
        akey = "AssetKey(['geocode_bronze_reverse'])"
        assert (akey, "geocode_reverse_content_health") in keys
        assert (akey, "geocode_reverse_schema_drift") in keys

    def test_no_freshness_or_completeness(self):
        # A cache is event-driven; the API-polling checks are intentionally disabled.
        names = {n for _a, n in self._checks()}
        assert "geocode_reverse_freshness" not in names
        assert "geocode_reverse_completeness" not in names

    def test_checks_job_and_schedule_registered(self):
        assert defs.resolve_job_def("geocode_bronze_checks_job") is not None
        s = defs.get_schedule_def("geocode_bronze_checks_hourly")
        assert s.cron_schedule == "0 * * * *"
