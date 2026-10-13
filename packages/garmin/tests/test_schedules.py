"""Tests for the Garmin schedules (daily trailing re-capture window)."""

from datetime import UTC, datetime

import pytest
from dagster import build_schedule_context
from grecohome_garmin.config import settings
from grecohome_garmin.dagster.definitions import defs
from grecohome_garmin.dagster.schedules import garmin_daily, garmin_reference


def _ctx(when: datetime):
    return build_schedule_context(
        scheduled_execution_time=when, repository_def=defs.get_repository_def()
    )


def _tick(when: datetime):
    return garmin_daily.evaluate_tick(_ctx(when)).run_requests


@pytest.mark.unit
class TestGarminDaily:
    # @spec GARMIN-CAPTURE-SCHED-001, GARMIN-CAPTURE-SCHED-002
    def test_trailing_window_of_completed_partitions(self):
        rrs = _tick(datetime(2026, 6, 9, 7, tzinfo=UTC))
        keys = [r.partition_key for r in rrs]
        assert len(rrs) == settings.lookback_days
        # end_offset=0: only completed days; today (06-09) is excluded.
        assert keys[-1] == "2026-06-08"
        assert "2026-06-09" not in keys

    # @spec GARMIN-CAPTURE-SCHED-003
    def test_consecutive_ticks_recapture_overlapping_partitions_as_distinct_runs(self):
        day1 = {r.partition_key: r.run_key for r in _tick(datetime(2026, 6, 9, 7, tzinfo=UTC))}
        day2 = {r.partition_key: r.run_key for r in _tick(datetime(2026, 6, 10, 7, tzinfo=UTC))}
        overlap = set(day1) & set(day2)
        assert len(overlap) == settings.lookback_days - 1
        # A partition re-emitted on a later tick must be a new run, not deduped.
        assert all(day1[k] != day2[k] for k in overlap)

    # @spec GARMIN-CAPTURE-SCHED-004
    def test_run_key_is_partition_plus_tick(self):
        rrs = _tick(datetime(2026, 6, 9, 7, tzinfo=UTC))
        assert all(r.run_key == f"{r.partition_key}-20260609T07" for r in rrs)


@pytest.mark.unit
class TestGarminReference:
    # @spec GARMIN-CAPTURE-SCHED-005
    def test_single_dated_request(self):
        result = garmin_reference.evaluate_tick(_ctx(datetime(2026, 6, 9, 7, tzinfo=UTC)))
        assert len(result.run_requests) == 1
        assert result.run_requests[0].run_key == "garmin-reference-20260609"
