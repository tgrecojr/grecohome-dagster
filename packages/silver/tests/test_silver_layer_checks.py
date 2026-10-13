"""The layer-wide checks job and schedule, and the table freshness builder."""

from __future__ import annotations

import os
import time
from unittest.mock import patch

import pytest
from dagster import AssetCheckSeverity, AssetKey, DefaultScheduleStatus, asset

from grecohome_silver.dagster.definitions import defs
from grecohome_silver.dagster.freshness import MAX_AGE_HOURS, table_freshness_check

pytestmark = pytest.mark.unit


@asset(name="silver_probe")
def _probe() -> None:
    """A stand-in asset for building a freshness check."""


def _check(path: str):
    return table_freshness_check(_probe, lambda: path)


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


# @spec SILVER-CHECKS-FRESH-001
def test_freshness_name_age_and_limit(tmp_path) -> None:
    path = str(tmp_path / "t.parquet")
    open(path, "w").close()
    check = _check(path)
    assert {s.name for s in check.check_specs} == {"silver_probe_freshness"}
    assert {s.asset_key for s in check.check_specs} == {AssetKey("silver_probe")}
    for age_h, passed in ((0, True), (MAX_AGE_HOURS, True), (MAX_AGE_HOURS + 0.5, False)):
        old = time.time() - age_h * 3600 + (1 if age_h == MAX_AGE_HOURS else 0)
        os.utime(path, (old, old))
        r = check()
        assert r.passed is passed and r.severity == AssetCheckSeverity.ERROR
        assert r.metadata["max_age_hours"].value == 36.0 and "age_hours" in r.metadata


# @spec SILVER-CHECKS-FRESH-002, SILVER-CHECKS-FRESH-003
def test_freshness_missing_table_fails_at_error_and_alerts(tmp_path) -> None:
    check = _check(str(tmp_path / "absent.parquet"))
    with patch("grecohome_core.checks.alerting._logger.error") as err:
        r = check()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR
    err.assert_called_once()
    assert err.call_args.kwargs["check"] == "silver_probe_freshness"
