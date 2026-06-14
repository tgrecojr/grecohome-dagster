"""Tests that the Garmin code location's Definitions resolve cleanly."""

import pytest
from grecohome_garmin import catalog
from grecohome_garmin.dagster.assets import DAILY_ASSETS, REFERENCE_ASSETS
from grecohome_garmin.dagster.checks import EMPIRICALLY_EMPTY, GARMIN_CHECK_CONFIGS
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


@pytest.mark.unit
class TestAssetChecks:
    def _checks(self):
        ag = defs.resolve_asset_graph()
        return {(str(k.asset_key), k.name) for k in ag.asset_check_keys}

    def _cfg(self, collection):
        return next(c for c in GARMIN_CHECK_CONFIGS if c.collection == collection)

    def test_every_collection_has_freshness(self):
        # The universal signal is on every catalog collection that has an asset.
        keys = self._checks()
        for cfg in GARMIN_CHECK_CONFIGS:
            assert (f"AssetKey(['garmin_bronze_{cfg.collection}'])",
                    f"garmin_{cfg.collection}_freshness") in keys

    def test_important_collections_get_full_suite(self):
        keys = self._checks()
        for coll in ("sleep", "stress", "training_status"):
            for fam in ("freshness", "completeness", "schema_drift", "content_health"):
                assert (f"AssetKey(['garmin_bronze_{coll}'])", f"garmin_{coll}_{fam}") in keys

    def test_plain_collection_is_freshness_only(self):
        # A non-important, non-empty collection gets freshness only.
        cfg = self._cfg("floors")
        assert cfg.enabled_checks == frozenset({"freshness"})
        assert not cfg.expected_empty

    def test_empirically_empty_overrides_catalog_flags(self):
        # These are ~always empty in the live tree but NOT skip-flagged in the
        # catalog; expected_empty must still be set from the content sweep.
        for coll in ("training_readiness", "body_battery_events", "running_tolerance",
                     "goals", "max_metrics"):
            assert coll in EMPIRICALLY_EMPTY
            assert self._cfg(coll).expected_empty, coll

    def test_catalog_skip_flag_marks_expected_empty(self):
        # hrv is skip_if_none in the catalog -> expected_empty regardless.
        assert self._cfg("hrv").expected_empty

    def test_validation_job_and_schedule_registered(self):
        assert defs.resolve_job_def("garmin_bronze_checks_job") is not None
        s = defs.get_schedule_def("garmin_bronze_checks_hourly")
        assert s.cron_schedule == "0 * * * *"

    def test_excluded_endpoints_get_no_checks(self, monkeypatch):
        # A collection turned off via FETCH_EXCLUDE never writes again, so it must
        # not get a (freshness) check that would inevitably age out and page.
        from grecohome_garmin.dagster import checks as checks_mod

        class _Sel:
            def is_selected(self, name: str) -> bool:
                return name != "menstrual_calendar"

        monkeypatch.setattr(checks_mod, "settings", _Sel())
        assert checks_mod._config_for(catalog.get("menstrual_calendar")) is None
        # Selected collections are unaffected.
        assert checks_mod._config_for(catalog.get("sleep")) is not None
