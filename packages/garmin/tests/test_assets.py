"""Tests that materializing garmin assets writes bronze (mocked Garmin client)."""

import os
from unittest.mock import MagicMock

import pytest
from dagster import materialize
from grecohome_garmin import catalog
from grecohome_garmin.dagster.assets import ASSET_BY_COLLECTION


def _payloads(root: str, collection: str) -> list[str]:
    base = os.path.join(root, "garmin", collection)
    if not os.path.isdir(base):
        return []
    return [
        os.path.join(d, n).replace(os.sep, "/")
        for d, _s, names in os.walk(base)
        for n in names
        if not n.endswith(".meta.json")
    ]


@pytest.mark.integration
class TestMaterialize:
    def test_daily_collection_writes_partitioned_bronze(self, isolate_garmin_bronze):
        client = MagicMock()
        client.get_sleep_data.return_value = {"x": 1}
        result = materialize(
            [ASSET_BY_COLLECTION["sleep"]],
            partition_key="2025-01-05",
            resources={"garmin": client},
        )
        assert result.success
        files = _payloads(isolate_garmin_bronze, "sleep")
        assert len(files) == 1
        assert "/dt=2025-01-05/" in files[0]

    def test_activities_asset_fans_out(self, isolate_garmin_bronze):
        client = MagicMock()
        client.get_activities_by_date.return_value = [{"activityId": 111}]
        for _name, method in catalog.PER_ACTIVITY:
            setattr(client, method, MagicMock(return_value={"ok": 1}))
        client.download_activity.return_value = b"FITDATA"
        result = materialize(
            [ASSET_BY_COLLECTION["activities"]],
            partition_key="2025-01-05",
            resources={"garmin": client},
        )
        assert result.success
        assert len(_payloads(isolate_garmin_bronze, "activities")) == 1
        assert len(_payloads(isolate_garmin_bronze, "activity_summary")) == 1  # fan-out
        fit = _payloads(isolate_garmin_bronze, "activity_fit")
        assert len(fit) == 1 and fit[0].endswith(".zip")

    def test_reference_collection_writes_bronze(self, isolate_garmin_bronze):
        client = MagicMock()
        client.get_devices.return_value = [{"deviceId": 1, "name": "watch"}]
        result = materialize(
            [ASSET_BY_COLLECTION["devices"]],
            resources={"garmin": client},
        )
        assert result.success
        assert len(_payloads(isolate_garmin_bronze, "devices")) == 1

    # @spec GARMIN-CAPTURE-012
    def test_run_records_capture_outcome_counts(self, isolate_garmin_bronze):
        client = MagicMock()
        client.get_activities_by_date.return_value = [{"activityId": 111}]
        for _name, method in catalog.PER_ACTIVITY:
            setattr(client, method, MagicMock(return_value={"ok": 1}))
        client.download_activity.return_value = b"FITDATA"
        asset = ASSET_BY_COLLECTION["activities"]
        first = materialize([asset], partition_key="2025-01-05", resources={"garmin": client})
        again = materialize([asset], partition_key="2025-01-05", resources={"garmin": client})
        captures = 1 + len(catalog.PER_ACTIVITY) + len(catalog.PER_ACTIVITY_DOWNLOAD)
        m1 = first.asset_materializations_for_node(asset.key.to_python_identifier())[0].metadata
        m2 = again.asset_materializations_for_node(asset.key.to_python_identifier())[0].metadata
        assert m1["written"].value == captures and m1["failed"].value == 0
        assert m2["written"].value == 0 and m2["skipped_identical"].value == captures

    # @spec GARMIN-CAPTURE-012
    def test_reference_run_records_capture_outcome_counts(self, isolate_garmin_bronze):
        client = MagicMock()
        client.get_devices.return_value = [{"deviceId": 1}]
        asset = ASSET_BY_COLLECTION["devices"]
        result = materialize([asset], resources={"garmin": client})
        md = result.asset_materializations_for_node(asset.key.to_python_identifier())[0].metadata
        assert (md["written"].value, md["skipped_identical"].value, md["failed"].value) == (1, 0, 0)

