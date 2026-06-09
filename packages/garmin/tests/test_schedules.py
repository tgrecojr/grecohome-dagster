"""Tests for the Garmin schedules (capture-once daily window)."""

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


@pytest.mark.unit
class TestGarminDaily:
    def test_capture_once_trailing_window(self):
        result = garmin_daily.evaluate_tick(_ctx(datetime(2026, 6, 9, 7, tzinfo=UTC)))
        rrs = result.run_requests
        assert len(rrs) == settings.lookback_days
        # run-once: run_key equals the partition key (no hour suffix).
        assert all(r.run_key == r.partition_key for r in rrs)
        # end_offset=0: only completed days; today (06-09) is excluded.
        assert rrs[-1].partition_key == "2026-06-08"
        assert "2026-06-09" not in {r.partition_key for r in rrs}


@pytest.mark.unit
class TestGarminReference:
    def test_single_dated_request(self):
        result = garmin_reference.evaluate_tick(_ctx(datetime(2026, 6, 9, 7, tzinfo=UTC)))
        assert len(result.run_requests) == 1
        assert result.run_requests[0].run_key == "garmin-reference-20260609"
