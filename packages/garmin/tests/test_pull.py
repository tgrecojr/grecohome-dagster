"""Tests for the catalog-driven pull helpers (mocked Garmin client)."""

import os
from unittest.mock import MagicMock

import pytest
from grecohome_garmin import catalog
from grecohome_garmin.config import GarminSettings
from grecohome_garmin.pull import GarminPuller


def _settings() -> GarminSettings:
    # rate_limit_seconds=0 so tests don't sleep; bronze_root comes from test env.
    return GarminSettings(rate_limit_seconds=0.0)


def _puller(tmp_path, client):
    return GarminPuller(client, _settings(), bronze_root=str(tmp_path / "bronze"))


def _payloads(root: str, collection: str) -> list[str]:
    base = os.path.join(root, "garmin", collection)
    if not os.path.isdir(base):
        return []
    return [
        os.path.join(d, n)
        for d, _s, names in os.walk(base)
        for n in names
        if not n.endswith(".meta.json")
    ]


@pytest.mark.unit
class TestPullEndpoint:
    def test_daily_captures_under_event_date(self, tmp_path):
        client = MagicMock()
        client.get_sleep_data.return_value = {"x": 1}
        p = _puller(tmp_path, client)
        p.pull_endpoint(catalog.get("sleep"), cdate="2025-01-05")
        files = _payloads(str(tmp_path / "bronze"), "sleep")
        assert len(files) == 1
        assert "/dt=2025-01-05/" in files[0].replace(os.sep, "/")

    def test_none_is_not_captured(self, tmp_path):
        client = MagicMock()
        client.get_hrv_data.return_value = None
        p = _puller(tmp_path, client)
        p.pull_endpoint(catalog.get("hrv"), cdate="2025-01-05")
        assert _payloads(str(tmp_path / "bronze"), "hrv") == []

    def test_empty_with_skip_flag_not_captured(self, tmp_path):
        client = MagicMock()
        client.get_lifestyle_logging_data.return_value = []  # skip_if_empty=True
        p = _puller(tmp_path, client)
        p.pull_endpoint(catalog.get("lifestyle_logging"), cdate="2025-01-05")
        assert _payloads(str(tmp_path / "bronze"), "lifestyle_logging") == []

    def test_empty_without_skip_flag_is_captured(self, tmp_path):
        # training_readiness has no skip flag: an empty 200 is a faithful record.
        client = MagicMock()
        client.get_training_readiness.return_value = []
        p = _puller(tmp_path, client)
        p.pull_endpoint(catalog.get("training_readiness"), cdate="2025-01-05")
        assert len(_payloads(str(tmp_path / "bronze"), "training_readiness")) == 1

    def test_exception_propagates(self, tmp_path):
        client = MagicMock()
        client.get_sleep_data.side_effect = RuntimeError("boom")
        p = _puller(tmp_path, client)
        with pytest.raises(RuntimeError, match="boom"):
            p.pull_endpoint(catalog.get("sleep"), cdate="2025-01-05")

    def test_range_uses_single_day_window(self, tmp_path):
        client = MagicMock()
        client.get_body_battery.return_value = [{"v": 1}]
        p = _puller(tmp_path, client)
        p.pull_endpoint(
            catalog.get("body_battery"), start="2025-01-05", end="2025-01-05", dt="2025-01-05"
        )
        client.get_body_battery.assert_called_once_with("2025-01-05", "2025-01-05")
        assert len(_payloads(str(tmp_path / "bronze"), "body_battery")) == 1

    def test_goals_loops_statuses(self, tmp_path):
        client = MagicMock()
        client.get_goals.return_value = [{"g": 1}]
        p = _puller(tmp_path, client)
        p.pull_endpoint(catalog.get("goals"))
        assert client.get_goals.call_count == len(catalog.GOAL_STATUSES)
        assert len(_payloads(str(tmp_path / "bronze"), "goals")) == len(catalog.GOAL_STATUSES)


@pytest.mark.unit
class TestFanOutAndPerEntity:
    def test_activities_fan_out_and_fit_download(self, tmp_path):
        client = MagicMock()
        client.get_activities_by_date.return_value = [{"activityId": 111}]
        for _name, method in catalog.PER_ACTIVITY:
            setattr(client, method, MagicMock(return_value={"ok": 1}))
        client.download_activity.return_value = b"FITDATA"
        p = _puller(tmp_path, client)
        p.pull_activities("2025-01-05", "2025-01-05", dt="2025-01-05")

        root = str(tmp_path / "bronze")
        assert len(_payloads(root, "activities")) == 1
        assert len(_payloads(root, "activity_summary")) == 1
        fit = _payloads(root, "activity_fit")
        assert len(fit) == 1 and fit[0].endswith(".zip")

    def test_per_device_loops_devices(self, tmp_path):
        client = MagicMock()
        client.get_devices.return_value = [{"deviceId": 42}, {"deviceId": 43}]
        client.get_device_settings.return_value = {"s": 1}
        p = _puller(tmp_path, client)
        p.pull_per_device(catalog.get("device_settings"))
        assert client.get_device_settings.call_count == 2
        assert len(_payloads(str(tmp_path / "bronze"), "device_settings")) == 2

    def test_per_profile_uses_profile_id(self, tmp_path):
        client = MagicMock()
        client.get_userprofile_settings.return_value = {"id": 999}
        client.get_gear.return_value = [{"uuid": "g1"}]
        p = _puller(tmp_path, client)
        p.pull_per_profile(catalog.get("gear"))
        client.get_gear.assert_called_once_with(999)
        assert len(_payloads(str(tmp_path / "bronze"), "gear")) == 1
