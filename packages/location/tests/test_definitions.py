"""Tests that the location code location's Definitions resolve cleanly."""

import pytest
from grecohome_location.dagster.definitions import defs


@pytest.mark.unit
class TestDefinitions:
    def test_both_stream_assets(self):
        keys = {str(k) for k in defs.resolve_asset_graph().get_all_asset_keys()}
        assert keys == {
            "AssetKey(['location_bronze_overland'])",
            "AssetKey(['location_bronze_owntracks'])",
        }

    def test_promote_job_resolves(self):
        assert defs.resolve_job_def("location_promote_job") is not None

    def test_checks_job_and_schedules(self):
        assert defs.resolve_job_def("location_bronze_checks_job") is not None
        assert defs.get_schedule_def("location_promote_every_5m").cron_schedule == "*/5 * * * *"
        assert defs.get_schedule_def("location_bronze_checks_hourly").cron_schedule == "0 * * * *"


@pytest.mark.unit
class TestAssetChecks:
    def _names(self):
        ag = defs.resolve_asset_graph()
        return {(str(k.asset_key), k.name) for k in ag.asset_check_keys}

    def test_expected_checks_present(self):
        names = self._names()
        ov = "AssetKey(['location_bronze_overland'])"
        ot = "AssetKey(['location_bronze_owntracks'])"
        expected = {
            (ov, "location_overland_content_health"),
            (ov, "location_overland_schema_drift"),
            (ov, "location_overland_receipt_freshness"),
            (ov, "location_overland_promote_lag"),
            (ot, "location_owntracks_content_health"),
            (ot, "location_owntracks_receipt_freshness"),
            (ot, "location_owntracks_promote_lag"),
        }
        assert expected <= names

    def test_owntracks_has_no_schema_drift(self):
        """OwnTracks messages are polymorphic -> schema drift is deliberately skipped."""
        names = self._names()
        ot = "AssetKey(['location_bronze_owntracks'])"
        assert (ot, "location_owntracks_schema_drift") not in names

    def test_check_count(self):
        # 2 content + 1 schema (overland only) + 2 freshness + 2 lag
        location_checks = [n for n in self._names() if n[1].startswith("location_")]
        assert len(location_checks) == 7
