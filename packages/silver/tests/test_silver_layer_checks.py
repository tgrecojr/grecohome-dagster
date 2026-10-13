"""The layer-wide silver checks job and schedule."""

from __future__ import annotations

import pytest
from dagster import DefaultScheduleStatus

from grecohome_silver.dagster.definitions import defs

pytestmark = pytest.mark.unit


def _silver_assets() -> set[str]:
    return {
        k.to_user_string()
        for k in defs.resolve_asset_graph().get_all_asset_keys()
        if k.to_user_string().startswith("silver_")
    }


# @spec SILVER-CHECKS-JOB-001
def test_checks_job_runs_every_silver_tables_checks_and_no_assets() -> None:
    ops = {n.name for n in defs.resolve_job_def("silver_checks_job").graph.nodes}
    assets = _silver_assets()
    assert assets, "expected silver assets"
    assert not (assets & ops), "the checks job must not materialize assets"
    for a in assets:  # every table, every family, has its freshness check in the job
        assert f"{a}_{a}_freshness" in ops, a


# @spec SILVER-CHECKS-JOB-002
def test_checks_schedule_daily_0700_and_starts_running() -> None:
    s = defs.get_schedule_def("silver_checks_daily")
    assert s.job_name == "silver_checks_job"
    assert s.cron_schedule == "0 7 * * *" and str(s.execution_timezone) == "UTC"
    assert s.default_status == DefaultScheduleStatus.RUNNING
