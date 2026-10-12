"""Tests for Garmin's skip-identical policy across re-captures (mocked client).

Garmin re-captures each trailing day on every daily tick. Single-request endpoints
that always return a value skip a payload identical to the folder's newest capture;
fan-out paths and skip-flagged endpoints keep every capture.
"""

import itertools
import os
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from grecohome_garmin import catalog
from grecohome_garmin.config import GarminSettings
from grecohome_garmin.pull import GarminPuller

from grecohome_core.bronze import capture as core_capture

DAY = "2025-01-05"


@pytest.fixture(autouse=True)
def _distinct_capture_times(monkeypatch):
    """Give each capture its own millisecond.

    The core writer orders a folder's captures by the fetch millis in the filename;
    back-to-back test captures can share a millisecond, making "newest" ambiguous.
    Real re-captures are a day (or at least the 2 s rate-limit pace) apart.
    """
    clock = itertools.count(core_capture._utc_now_ms())  # real now, then +1 ms each
    monkeypatch.setattr(core_capture, "_utc_now_ms", lambda: next(clock))


def _puller(tmp_path, client):
    settings = GarminSettings(rate_limit_seconds=0.0)
    return GarminPuller(client, settings, bronze_root=str(tmp_path / "bronze"))


def _payloads(tmp_path, collection: str) -> list[str]:
    base = os.path.join(str(tmp_path / "bronze"), "garmin", collection)
    if not os.path.isdir(base):
        return []
    return [
        os.path.join(d, n).replace(os.sep, "/")
        for d, _s, names in os.walk(base)
        for n in names
        if not n.endswith(".meta.json")
    ]


def _activity_client(activity_ids: list[int], per_activity_return) -> MagicMock:
    client = MagicMock()
    client.get_activities_by_date.return_value = [{"activityId": a} for a in activity_ids]
    for _name, method in catalog.PER_ACTIVITY:
        setattr(client, method, MagicMock(return_value=per_activity_return))
    client.download_activity.return_value = b"FITDATA"
    return client


@pytest.mark.unit
class TestSkipIdenticalOn:
    # @spec GARMIN-CAPTURE-004
    def test_daily_identical_recapture_is_skipped_changes_are_kept(self, tmp_path):
        client = MagicMock()
        p = _puller(tmp_path, client)
        ep = catalog.get("sleep")
        for value in ({"v": 1}, {"v": 1}, {"v": 2}, {"v": 2}, {"v": 1}):
            client.get_sleep_data.return_value = value
            p.pull_endpoint(ep, cdate=DAY)
        # v1 written, v1 skipped, v2 written, v2 skipped, v1 (a revert) written.
        assert len(_payloads(tmp_path, "sleep")) == 3

    def test_range_identical_recapture_is_skipped(self, tmp_path):
        client = MagicMock()
        client.get_body_battery.return_value = [{"v": 1}]
        p = _puller(tmp_path, client)
        for _ in range(3):
            p.pull_endpoint(catalog.get("body_battery"), start=DAY, end=DAY, dt=DAY)
        assert len(_payloads(tmp_path, "body_battery")) == 1

    def test_empty_unflagged_identical_recapture_is_skipped(self, tmp_path):
        client = MagicMock()
        client.get_training_readiness.return_value = []
        p = _puller(tmp_path, client)
        for _ in range(2):
            p.pull_endpoint(catalog.get("training_readiness"), cdate=DAY)
        assert len(_payloads(tmp_path, "training_readiness")) == 1

    def test_activities_list_identical_recapture_is_skipped(self, tmp_path):
        p = _puller(tmp_path, _activity_client([111], {"ok": 1}))
        for _ in range(2):
            p.pull_activities(DAY, DAY, dt=DAY)
        assert len(_payloads(tmp_path, "activities")) == 1

    def test_activities_list_skip_still_fans_out(self, tmp_path):
        client = _activity_client([111], {"ok": 1})
        p = _puller(tmp_path, client)
        for _ in range(2):
            p.pull_activities(DAY, DAY, dt=DAY)
        assert client.get_activity.call_count == 2

    def test_static_reference_identical_same_day_is_skipped(self, tmp_path):
        client = MagicMock()
        client.get_race_predictions.return_value = {"5k": 1500}
        p = _puller(tmp_path, client)
        for _ in range(2):
            p.pull_endpoint(catalog.get("race_predictions"))
        assert len(_payloads(tmp_path, "race_predictions")) == 1

    def test_weekly_reference_identical_same_day_is_skipped(self, tmp_path):
        client = MagicMock()
        client.get_weekly_steps.return_value = [{"w": 1}]
        p = _puller(tmp_path, client)
        for _ in range(2):
            p.pull_endpoint(catalog.get("weekly_steps"), end=DAY)
        assert len(_payloads(tmp_path, "weekly_steps")) == 1


@pytest.mark.unit
class TestKeepEveryCapture:
    # @spec GARMIN-CAPTURE-005
    @pytest.mark.parametrize(
        ("name", "method", "kwargs"),
        [
            ("lifestyle_logging", "get_lifestyle_logging_data", {"cdate": DAY}),  # skip_if_empty
            ("hrv", "get_hrv_data", {"cdate": DAY}),  # skip_if_none
            ("cycling_ftp", "get_cycling_ftp", {}),  # static, skip_if_empty
        ],
    )
    def test_skip_flagged_endpoint_keeps_identical(self, tmp_path, name, method, kwargs):
        client = MagicMock()
        getattr(client, method).return_value = {"v": 1}
        p = _puller(tmp_path, client)
        for _ in range(2):
            p.pull_endpoint(catalog.get(name), **kwargs)
        assert len(_payloads(tmp_path, catalog.get(name).collection)) == 2

    def test_skip_flagged_per_profile_keeps_identical(self, tmp_path):
        client = MagicMock()
        client.get_userprofile_settings.return_value = {"id": 999}
        client.get_gear.return_value = [{"uuid": "g1"}]
        p = _puller(tmp_path, client)
        for _ in range(2):
            p.pull_per_profile(catalog.get("gear"))
        assert len(_payloads(tmp_path, "gear")) == 2

    # @spec GARMIN-CAPTURE-006
    def test_per_activity_fan_out_keeps_identical_across_activities(self, tmp_path):
        # Two activities on one day returning identical (empty) bodies are two
        # distinct requests; both answers are kept, as are both FIT downloads.
        p = _puller(tmp_path, _activity_client([111, 222], []))
        p.pull_activities(DAY, DAY, dt=DAY)
        for name, _method in catalog.PER_ACTIVITY:
            assert len(_payloads(tmp_path, name)) == 2, name
        assert len(_payloads(tmp_path, "activity_fit")) == 2

    def test_per_activity_fan_out_keeps_identical_recaptures(self, tmp_path):
        p = _puller(tmp_path, _activity_client([111], {"ok": 1}))
        for _ in range(2):
            p.pull_activities(DAY, DAY, dt=DAY)
        assert len(_payloads(tmp_path, "activity_summary")) == 2
        assert len(_payloads(tmp_path, "activity_fit")) == 2

    # @spec GARMIN-CAPTURE-007
    def test_per_device_keeps_identical_across_devices_and_recaptures(self, tmp_path):
        client = MagicMock()
        client.get_devices.return_value = [{"deviceId": 42}, {"deviceId": 43}]
        client.get_device_settings.return_value = {"s": 1}
        p = _puller(tmp_path, client)
        for _ in range(2):
            p.pull_per_device(catalog.get("device_settings"))
        assert len(_payloads(tmp_path, "device_settings")) == 4

    def test_per_device_range_keeps_identical(self, tmp_path):
        client = MagicMock()
        client.get_devices.return_value = [{"deviceId": 42}, {"deviceId": 43}]
        client.get_device_solar_data.return_value = [{"solar": 1}]
        p = _puller(tmp_path, client)
        p.pull_per_device(catalog.get("device_solar"), start=DAY, end=DAY, dt=DAY)
        assert len(_payloads(tmp_path, "device_solar")) == 2

    # @spec GARMIN-CAPTURE-008
    def test_goals_keep_identical_answers_per_status(self, tmp_path):
        client = MagicMock()
        client.get_goals.return_value = []
        p = _puller(tmp_path, client)
        for _ in range(2):
            p.pull_endpoint(catalog.get("goals"))
        assert len(_payloads(tmp_path, "goals")) == 2 * len(catalog.GOAL_STATUSES)


@pytest.mark.unit
class TestFolderPlacement:
    # @spec GARMIN-CHECKS-002
    def test_identical_payload_into_new_partition_is_written(self, tmp_path):
        # Each newly completed partition's first capture always writes, even when
        # the previous day's payload was byte-identical -- freshness stays fed.
        client = MagicMock()
        client.get_sleep_data.return_value = {"v": 1}
        p = _puller(tmp_path, client)
        p.pull_endpoint(catalog.get("sleep"), cdate="2025-01-05")
        p.pull_endpoint(catalog.get("sleep"), cdate="2025-01-06")
        folders = sorted(f.split("/")[-2] for f in _payloads(tmp_path, "sleep"))
        assert folders == ["dt=2025-01-05", "dt=2025-01-06"]

    # @spec GARMIN-CAPTURE-009
    def test_reference_capture_lands_in_utc_fetch_date_folder(self, tmp_path):
        client = MagicMock()
        client.get_race_predictions.return_value = {"5k": 1500}
        p = _puller(tmp_path, client)
        before = datetime.now(UTC).strftime("%Y-%m-%d")
        p.pull_endpoint(catalog.get("race_predictions"))
        after = datetime.now(UTC).strftime("%Y-%m-%d")
        (path,) = _payloads(tmp_path, "race_predictions")
        assert path.split("/")[-2] in {f"dt={before}", f"dt={after}"}
