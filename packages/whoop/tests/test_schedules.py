"""Tests for the Whoop schedules."""

from datetime import UTC, datetime

import pytest
from dagster import build_schedule_context

from grecohome_whoop.config import settings
from grecohome_whoop.dagster.definitions import defs
from grecohome_whoop.dagster.schedules import whoop_hourly, whoop_snapshots_hourly


def _context(when: datetime):
    return build_schedule_context(
        scheduled_execution_time=when, repository_def=defs.get_repository_def()
    )


@pytest.mark.unit
class TestWhoopHourly:
    def test_emits_trailing_window_partitions(self):
        when = datetime(2026, 6, 8, 14, 0, tzinfo=UTC)
        result = whoop_hourly.evaluate_tick(_context(when))
        keys = [rr.partition_key for rr in result.run_requests]
        # reconcile_window_days (7) + 1 = 8 trailing days, including the current day.
        assert len(keys) == settings.reconcile_window_days + 1
        assert keys[-1] == "2026-06-08"
        assert keys[0] == "2026-06-01"

    def test_run_keys_are_hour_scoped(self):
        when = datetime(2026, 6, 8, 14, 0, tzinfo=UTC)
        result = whoop_hourly.evaluate_tick(_context(when))
        run_keys = [rr.run_key for rr in result.run_requests]
        # Distinct per (partition, hour) so re-emitting a partition each tick is a new run.
        assert all(rk.endswith("-20260608T14") for rk in run_keys)
        assert len(set(run_keys)) == len(run_keys)


@pytest.mark.unit
class TestWhoopSnapshotsHourly:
    def test_emits_single_snapshot_request(self):
        when = datetime(2026, 6, 8, 14, 0, tzinfo=UTC)
        result = whoop_snapshots_hourly.evaluate_tick(_context(when))
        assert len(result.run_requests) == 1
        assert result.run_requests[0].run_key == "snapshots-20260608T14"
