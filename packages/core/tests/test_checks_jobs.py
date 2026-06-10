"""Tests for the checks-only validation job/schedule builders."""

import pytest
from dagster import (
    AssetCheckResult,
    AssetKey,
    Definitions,
    asset,
    asset_check,
)

from grecohome_core.checks.jobs import build_bronze_checks_job, build_bronze_checks_schedule


@asset
def _demo_asset() -> int:
    return 1


@asset_check(asset=_demo_asset, name="demo_check")
def _demo_check() -> AssetCheckResult:
    return AssetCheckResult(passed=True)


@pytest.mark.unit
class TestChecksJob:
    def test_job_runs_checks_without_materializing_asset(self):
        job = build_bronze_checks_job([_demo_check], name="demo_checks_job")
        defs = Definitions(assets=[_demo_asset], asset_checks=[_demo_check], jobs=[job])
        jd = defs.resolve_job_def("demo_checks_job")

        result = jd.execute_in_process()
        assert result.success
        # The asset is NOT materialized — only the check runs.
        assert result.get_asset_materialization_events() == []
        evals = result.get_asset_check_evaluations()
        assert [(str(e.asset_key), e.check_name, e.passed) for e in evals] == [
            (str(AssetKey("_demo_asset")), "demo_check", True)
        ]

    def test_schedule_defaults_hourly_utc(self):
        job = build_bronze_checks_job([_demo_check], name="demo_checks_job")
        sched = build_bronze_checks_schedule(job, name="demo_checks_hourly")
        assert sched.name == "demo_checks_hourly"
        assert sched.cron_schedule == "0 * * * *"
        assert str(sched.execution_timezone) == "UTC"

    def test_schedule_name_defaults_to_job(self):
        job = build_bronze_checks_job([_demo_check], name="demo_checks_job")
        sched = build_bronze_checks_schedule(job)
        assert sched.name == "demo_checks_job_schedule"
