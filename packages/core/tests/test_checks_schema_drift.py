"""Tests for the schema-drift asset check and its baseline storage."""

import os

import pytest
from dagster import AssetCheckSeverity, AssetKey

from grecohome_core.checks import build_schema_drift_check
from grecohome_core.checks.config import CollectionCheckConfig

KEY = AssetKey("garmin_bronze_sleep")


def _read_baseline_sig(path: str) -> list[str] | None:
    """Read the stored ``signature`` list from a baseline file (test helper)."""
    import json

    with open(path) as fh:
        return json.load(fh).get("signature")


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

    def test_empty_payload_does_not_set_baseline(self, capture, bronze_root, monitor_dir):
        # A sparse collection (e.g. max_metrics) writes an empty [] most days. An
        # empty payload has no schema, so it must not record a baseline — otherwise
        # the first real capture would "drift" against the empty baseline.
        capture("garmin", "max_metrics", [], dt="2024-12-01")
        check = build_schema_drift_check(
            _cfg(collection="max_metrics"), bronze_root, monitor_dir
        )
        res = check()
        assert res.passed
        assert res.metadata["status"].value == "no_payload"
        baseline = os.path.join(monitor_dir, "schema_baselines", "garmin", "max_metrics.json")
        assert not os.path.exists(baseline)

    def test_real_payload_after_empty_days_sets_baseline_not_drift(
        self, capture, bronze_root, monitor_dir
    ):
        # Empty days first, then the first real capture. The real payload defines the
        # baseline (pass); it must not be read as drift against the empty days.
        capture("garmin", "max_metrics", [], dt="2024-12-01")
        capture("garmin", "max_metrics", [], dt="2024-12-02")
        check = build_schema_drift_check(
            _cfg(collection="max_metrics"), bronze_root, monitor_dir
        )
        check()  # empty window -> no baseline yet
        capture(
            "garmin", "max_metrics",
            [{"userId": 1, "generic": {}, "cycling": None, "heatAltitudeAcclimation": {}}],
            dt="2024-12-03",
        )
        res = check()
        assert res.passed
        assert res.metadata["status"].value == "baseline_set"

    def test_interspersed_empty_day_does_not_drift(self, capture, bronze_root, monitor_dir):
        # Once a real baseline exists, a later empty day (a rest day with no new
        # VO2max) must not drift — the empty payload is ignored, the richer earlier
        # day still represents the schema.
        real = [{"userId": 1, "generic": {}, "cycling": None, "heatAltitudeAcclimation": {}}]
        capture("garmin", "max_metrics", real, dt="2024-12-01")
        check = build_schema_drift_check(
            _cfg(collection="max_metrics"), bronze_root, monitor_dir
        )
        check()  # baseline = the 4 real keys
        capture("garmin", "max_metrics", [], dt="2024-12-02")  # empty rest day, newest
        res = check()
        assert res.passed
        assert res.metadata["status"].value == "ok"

    def test_empty_baseline_self_heals(self, capture, bronze_root, monitor_dir):
        # The max_metrics incident: a baseline poisoned with [] (recorded during a
        # long empty stretch, before empties were skipped) must self-heal — the next
        # real payload re-records it and passes, no host file surgery.
        import json as _json

        baseline_file = os.path.join(
            monitor_dir, "schema_baselines", "garmin", "max_metrics.json"
        )
        os.makedirs(os.path.dirname(baseline_file), exist_ok=True)
        with open(baseline_file, "w") as fh:
            _json.dump({"source": "garmin", "collection": "max_metrics", "signature": []}, fh)

        capture(
            "garmin", "max_metrics",
            [{"userId": 1, "generic": {}, "cycling": None, "heatAltitudeAcclimation": {}}],
            dt="2024-12-03",
        )
        check = build_schema_drift_check(
            _cfg(collection="max_metrics"), bronze_root, monitor_dir
        )
        res = check()
        assert res.passed
        assert res.metadata["status"].value == "baseline_set"
        # The poisoned [] baseline was replaced with the real 4-key signature.
        assert _read_baseline_sig(baseline_file) == [
            "cycling", "generic", "heatAltitudeAcclimation", "userId"
        ]

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
