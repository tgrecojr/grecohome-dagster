"""Tests for the checks-only validation job/schedule builders."""

import inspect

import pytest
from dagster import (
    AssetCheckResult,
    AssetKey,
    DefaultScheduleStatus,
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


# Named so its step sorts before ``demo_check``: the run must get past a failure.
@asset_check(asset=_demo_asset, name="aaa_raising_check")
def _raising_check() -> AssetCheckResult:
    raise RuntimeError("boom")


@pytest.mark.unit
class TestChecksJob:
    # @spec CORE-CHECKS-JOB-001
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

    # @spec CORE-CHECKS-JOB-003
    def test_job_uses_the_in_process_executor(self):
        job = build_bronze_checks_job([_demo_check], name="demo_checks_job")
        defs = Definitions(assets=[_demo_asset], asset_checks=[_demo_check], jobs=[job])
        assert defs.resolve_job_def("demo_checks_job").executor_def.name == "in_process"

    # @spec CORE-CHECKS-JOB-003
    def test_a_raising_check_fails_only_its_own_step(self):
        job = build_bronze_checks_job([_raising_check, _demo_check], name="demo_checks_job")
        defs = Definitions(
            assets=[_demo_asset], asset_checks=[_raising_check, _demo_check], jobs=[job]
        )
        result = defs.resolve_job_def("demo_checks_job").execute_in_process(raise_on_error=False)
        assert not result.success
        assert [e.step_key for e in result.get_step_failure_events()] == [
            "_demo_asset_aaa_raising_check"
        ]
        # The step queued behind the failure still ran and reported.
        assert [(e.check_name, e.passed) for e in result.get_asset_check_evaluations()] == [
            ("demo_check", True)
        ]


@pytest.mark.unit
class TestChecksSchedule:
    # @spec CORE-CHECKS-JOB-002
    def test_schedule_runs_the_given_cron_in_utc_and_starts_running(self):
        job = build_bronze_checks_job([_demo_check], name="demo_checks_job")
        sched = build_bronze_checks_schedule(
            job, name="demo_checks_hourly", cron_schedule="37 * * * *"
        )
        assert sched.name == "demo_checks_hourly"
        assert sched.cron_schedule == "37 * * * *"
        assert str(sched.execution_timezone) == "UTC"
        assert sched.default_status == DefaultScheduleStatus.RUNNING

    # @spec CORE-CHECKS-JOB-002
    def test_schedule_name_defaults_to_job(self):
        job = build_bronze_checks_job([_demo_check], name="demo_checks_job")
        sched = build_bronze_checks_schedule(job, cron_schedule="37 * * * *")
        assert sched.name == "demo_checks_job_schedule"

    # @spec CORE-CHECKS-JOB-004
    def test_cron_is_a_required_keyword_argument(self):
        param = inspect.signature(build_bronze_checks_schedule).parameters["cron_schedule"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        assert param.default is inspect.Parameter.empty
        job = build_bronze_checks_job([_demo_check], name="demo_checks_job")
        with pytest.raises(TypeError):
            build_bronze_checks_schedule(job)  # type: ignore[call-arg]

    # @spec CORE-CHECKS-JOB-005
    @pytest.mark.parametrize(
        "cron",
        [
            "0 * * * *",  # the host's cron minute
            "* * * * *",
            "*/15 * * * *",
            "5,35 * * * *",
            "5-10 * * * *",
            "60 * * * *",
            "-7 * * * *",
            "",
        ],
    )
    def test_rejects_a_cron_without_one_literal_off_minute(self, cron):
        job = build_bronze_checks_job([_demo_check], name="demo_checks_job")
        with pytest.raises(ValueError):
            build_bronze_checks_schedule(job, cron_schedule=cron)

    # @spec CORE-CHECKS-JOB-005
    @pytest.mark.parametrize("cron", ["1 * * * *", "07 3-21/6 * * *", "59 23 * * 0"])
    def test_accepts_one_literal_minute_from_1_to_59(self, cron):
        job = build_bronze_checks_job([_demo_check], name="demo_checks_job")
        assert build_bronze_checks_schedule(job, cron_schedule=cron).cron_schedule == cron
