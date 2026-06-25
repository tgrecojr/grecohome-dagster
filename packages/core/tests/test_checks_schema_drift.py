"""Tests for the schema-drift asset check and its baseline storage."""

import os

import pytest
from dagster import AssetCheckSeverity, AssetKey

from grecohome_core.checks import build_schema_drift_check
from grecohome_core.checks.config import CollectionCheckConfig

KEY = AssetKey("garmin_bronze_sleep")


def _cfg(**kw) -> CollectionCheckConfig:
    base = dict(source="garmin", collection="sleep", asset_key=KEY,
                reader="json", unnest_records=False)
    base.update(kw)
    return CollectionCheckConfig(**base)


@pytest.fixture
def monitor_dir(tmp_path) -> str:
    d = tmp_path / "monitor"
    d.mkdir()
    return str(d)


@pytest.mark.unit
class TestSchemaDrift:
    def test_first_run_sets_baseline(self, capture, bronze_root, monitor_dir):
        capture("garmin", "sleep", {"dailySleepDTO": {}, "remSleepData": True},
                dt="2024-12-01")
        check = build_schema_drift_check(_cfg(), bronze_root, monitor_dir)
        res = check()
        assert res.passed
        assert res.metadata["status"].value == "baseline_set"
        # Baseline written OUTSIDE bronze_root.
        baseline = os.path.join(monitor_dir, "schema_baselines", "garmin", "sleep.json")
        assert os.path.exists(baseline)
        assert not baseline.startswith(bronze_root)

    def test_matching_schema_passes(self, capture, bronze_root, monitor_dir):
        capture("garmin", "sleep", {"dailySleepDTO": {}, "remSleepData": True},
                dt="2024-12-01")
        check = build_schema_drift_check(_cfg(), bronze_root, monitor_dir)
        check()  # set baseline
        res = check()  # second run, same shape
        assert res.passed
        assert res.metadata["status"].value == "ok"

    def test_drift_fails_error(self, capture, bronze_root, monitor_dir):
        capture("garmin", "sleep", {"dailySleepDTO": {}, "remSleepData": True},
                dt="2024-12-01")
        check = build_schema_drift_check(_cfg(), bronze_root, monitor_dir)
        check()  # baseline = {dailySleepDTO, remSleepData}
        # A newer partition with an added top-level key.
        capture("garmin", "sleep", {"dailySleepDTO": {}, "remSleepData": True, "newField": 1},
                dt="2024-12-02")
        res = check()
        assert not res.passed
        assert res.severity == AssetCheckSeverity.ERROR
        assert res.metadata["status"].value == "drift"

    def test_sparse_stub_on_newest_day_does_not_drift(self, capture, bronze_root, monitor_dir):
        # A full day defines the baseline...
        full = {"dailySleepDTO": {}, "remSleepData": True, "sleepHeartRate": [],
                "wellnessEpochSPO2DataDTOList": []}
        capture("garmin", "sleep", full, dt="2024-12-01")
        check = build_schema_drift_check(_cfg(), bronze_root, monitor_dir)
        check()  # baseline = the 4 full-day keys
        # ...then the newest partition is a not-yet-synced stub missing the optional
        # sensor sections (and carrying a thin-day-only field). The signature must be
        # taken from the richer earlier day, so this does NOT page.
        capture("garmin", "sleep", {"dailySleepDTO": {}, "displayName": "x"},
                dt="2024-12-02")
        res = check()
        assert res.passed
        assert res.metadata["status"].value == "ok"

    def test_real_added_field_still_drifts(self, capture, bronze_root, monitor_dir):
        # Guard the other direction: a genuinely richer payload (new contract field)
        # must still be caught — the richest representative is the new shape.
        capture("garmin", "sleep", {"dailySleepDTO": {}, "remSleepData": True},
                dt="2024-12-01")
        check = build_schema_drift_check(_cfg(), bronze_root, monitor_dir)
        check()  # baseline = 2 keys
        capture("garmin", "sleep",
                {"dailySleepDTO": {}, "remSleepData": True, "newContractField": 1},
                dt="2024-12-02")
        res = check()
        assert not res.passed
        assert res.severity == AssetCheckSeverity.ERROR
        assert res.metadata["status"].value == "drift"

    def test_disabled_without_monitor_dir(self, capture, bronze_root):
        capture("garmin", "sleep", {"dailySleepDTO": {}}, dt="2024-12-01")
        check = build_schema_drift_check(_cfg(), bronze_root, None)
        res = check()
        assert res.passed
        assert res.metadata["status"].value == "disabled"

    def test_refuses_baseline_inside_bronze_root(self, capture, bronze_root):
        capture("garmin", "sleep", {"dailySleepDTO": {}}, dt="2024-12-01")
        # monitor_dir pointed *inside* bronze -> the check must refuse to write.
        check = build_schema_drift_check(_cfg(), bronze_root, bronze_root)
        res = check()
        assert not res.passed
        assert res.metadata["status"].value == "error"
