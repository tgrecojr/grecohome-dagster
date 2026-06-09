"""Tests for the Drive-watching sensor (mocked Drive + ephemeral instance)."""

from unittest.mock import MagicMock, patch

import pytest
from dagster import DagsterInstance, SkipReason, build_sensor_context
from grecohome_lingo.dagster import sensors


def _eval(instance, files):
    with (
        patch("grecohome_lingo.drive.get_drive_service", return_value=MagicMock()),
        patch("grecohome_lingo.drive.list_csv_files", return_value=files),
    ):
        ctx = build_sensor_context(instance=instance)
        return sensors.lingo_drive_sensor(ctx)


@pytest.mark.unit
class TestLingoDriveSensor:
    def test_adds_partitions_and_runs_for_all_when_empty(self):
        inst = DagsterInstance.ephemeral()
        result = _eval(inst, [{"id": "f1"}, {"id": "f2"}])
        assert sorted(rr.partition_key for rr in result.run_requests) == ["f1", "f2"]
        assert sorted(result.dynamic_partitions_requests[0].partition_keys) == ["f1", "f2"]
        # run_key == file_id (capture-once)
        assert all(rr.run_key == rr.partition_key for rr in result.run_requests)

    def test_skips_already_captured_files(self):
        inst = DagsterInstance.ephemeral()
        inst.add_dynamic_partitions("lingo_files", ["f1"])
        result = _eval(inst, [{"id": "f1"}, {"id": "f2"}])
        assert [rr.partition_key for rr in result.run_requests] == ["f2"]
        assert result.dynamic_partitions_requests[0].partition_keys == ["f2"]

    def test_skip_reason_when_no_new_files(self):
        inst = DagsterInstance.ephemeral()
        inst.add_dynamic_partitions("lingo_files", ["f1"])
        result = _eval(inst, [{"id": "f1"}])
        assert isinstance(result, SkipReason)
