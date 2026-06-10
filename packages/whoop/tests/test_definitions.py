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


@pytest.mark.unit
class TestAssetChecks:
    def _check_keys(self):
        ag = defs.resolve_asset_graph()
        return {(str(k.asset_key), k.name) for k in ag.asset_check_keys}

    def test_range_collections_have_all_four_families(self):
        keys = self._check_keys()
        for coll, asset in (
            ("sleep", "whoop_bronze_sleep"),
            ("recovery", "whoop_bronze_recovery"),
            ("cycle", "whoop_bronze_cycle"),
        ):
            for family in ("freshness", "completeness", "schema_drift", "content_health"):
                assert (f"AssetKey(['{asset}'])", f"whoop_{coll}_{family}") in keys

    def test_workout_skips_content_health(self):
        keys = self._check_keys()
        akey = "AssetKey(['whoop_bronze_workout'])"
        assert (akey, "whoop_workout_freshness") in keys
        assert (akey, "whoop_workout_completeness") in keys
        assert (akey, "whoop_workout_content_health") not in keys  # intermittent

    def test_snapshots_have_schema_and_content_no_freshness(self):
        keys = self._check_keys()
        akey = "AssetKey(['whoop_bronze_snapshots'])"
        for coll in ("profile", "body_measurement"):
            assert (akey, f"whoop_{coll}_schema_drift") in keys
            assert (akey, f"whoop_{coll}_content_health") in keys
            # Freshness disabled: dedup'd snapshots make sidecar-freshness misleading.
            assert (akey, f"whoop_{coll}_freshness") not in keys
            # Current-only snapshots have no event timeline.
            assert (akey, f"whoop_{coll}_completeness") not in keys

    def test_total_check_count(self):
        # 3 range collections x4 + workout x3 + 2 snapshots x2 = 19
        assert len(self._check_keys()) == 19
