"""The sleep rebuild job and its daily schedule."""

from __future__ import annotations

import pytest
from dagster import AssetKey, DefaultScheduleStatus

from grecohome_silver.dagster.definitions import defs

pytestmark = pytest.mark.unit


# @spec SILVER-SLEEP-SCHED-001
def test_sleep_job_rebuilds_three_assets_and_runs_their_checks() -> None:
    job = defs.resolve_job_def("silver_sleep_job")
    assets = {
        AssetKey(n) for n in ("silver_sleep_garmin", "silver_sleep_whoop", "silver_sleep")
    }
    assert set(job.asset_layer.selected_asset_keys) == assets
    # Dagster names each check's op "<asset>_<check>".
    ops = {n.name for n in job.graph.nodes}
    assert {
        "silver_sleep_garmin_garmin_night_unique_nonnull",
        "silver_sleep_whoop_whoop_id_unique_night_nonnull",
        "silver_sleep_sleep_night_unique_nonnull", "silver_sleep_sleep_value_ranges",
        "silver_sleep_sleep_join_sanity", "silver_sleep_sleep_coverage_split",
        "silver_sleep_garmin_garmin_coverage_vs_bronze",
        "silver_sleep_garmin_silver_sleep_garmin_freshness",
        "silver_sleep_whoop_silver_sleep_whoop_freshness",
        "silver_sleep_silver_sleep_freshness",
    } <= ops


# @spec SILVER-SLEEP-SCHED-001
def test_sleep_schedule_is_daily_0600_utc_and_defaults_stopped() -> None:
    s = defs.get_schedule_def("silver_sleep_daily")
    assert s.cron_schedule == "0 6 * * *"
    assert str(s.execution_timezone) == "UTC"
    assert s.job_name == "silver_sleep_job"
    assert s.default_status == DefaultScheduleStatus.STOPPED
