"""Gold mart checks: failure cases, missing marts, freshness, jobs and the checks-only job."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from dagster import AssetCheckSeverity, DefaultScheduleStatus, materialize

from grecohome_core.silver import connect
from grecohome_gold.config import settings
from grecohome_gold.dagster import checks as wellness_checks
from grecohome_gold.dagster.assets import WELLNESS_PARQUET, gold_daily_wellness, gold_path
from grecohome_gold.dagster.definitions import defs

pytestmark = pytest.mark.unit

MARTS = {
    "wellness": (wellness_checks, "wellness_day_unique_nonnull", "wellness_value_ranges",
                 "wellness_coverage", lambda: gold_path(WELLNESS_PARQUET),
                 ("day_strain", 22.0), "gold_daily_wellness_freshness"),
}


@pytest.fixture
def roots(silver_root, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "silver_root", silver_root)
    monkeypatch.setattr(settings, "gold_root", str(tmp_path / "gold"))


@pytest.fixture
def result(roots):
    res = materialize([gold_daily_wellness])
    assert res.success
    return res


def _rewrite(path: str, select: str) -> None:
    tmp = path + ".rewrite.parquet"
    con = connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{path}')")
    con.execute(f"COPY ({select}) TO '{tmp}' (FORMAT parquet)")
    os.replace(tmp, path)


# @spec GOLD-WELLNESS-MART-010
def test_materialization_metadata(result) -> None:
    w = result.asset_materializations_for_node("gold_daily_wellness")[0].metadata
    assert set(w) == {"rows", "path"}


# @spec GOLD-WELLNESS-CHECKS-001
@pytest.mark.parametrize("mart", MARTS)
def test_unique_check_fails_on_duplicate_day(result, mart) -> None:
    mod, unique, _, _, path, *_ = MARTS[mart]
    assert getattr(mod, unique)().passed
    _rewrite(path(), "SELECT * FROM t UNION ALL (SELECT * FROM t LIMIT 1)")
    r = getattr(mod, unique)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec GOLD-WELLNESS-CHECKS-002
@pytest.mark.parametrize("mart", MARTS)
def test_range_check_fails_out_of_bounds(result, mart) -> None:
    mod, _, ranges, _, path, (col, value), _ = MARTS[mart]
    assert getattr(mod, ranges)().passed
    _rewrite(path(), f"SELECT * REPLACE ({value} AS {col}) FROM t")
    r = getattr(mod, ranges)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec GOLD-WELLNESS-CHECKS-003
@pytest.mark.parametrize("mart", MARTS)
def test_coverage_warns_only_when_empty(result, mart) -> None:
    mod, _, _, coverage, path, *_ = MARTS[mart]
    assert getattr(mod, coverage)().passed
    _rewrite(path(), "SELECT * FROM t WHERE false")
    r = getattr(mod, coverage)()
    assert not r.passed and r.severity == AssetCheckSeverity.WARN


# @spec GOLD-WELLNESS-CHECKS-005, GOLD-WELLNESS-CHECKS-006
def test_missing_mart_fails_every_check_at_error_and_alerts(roots) -> None:
    checks = wellness_checks.ALL_CHECKS
    with patch("grecohome_core.checks.alerting._logger.error") as err:
        for check in checks:
            r = check()
            assert not r.passed and r.severity == AssetCheckSeverity.ERROR
    assert err.call_count == len(checks)


# @spec GOLD-WELLNESS-CHECKS-004
@pytest.mark.parametrize("mart", MARTS)
def test_freshness_check_defined_and_fresh(result, mart) -> None:
    mod, *_, freshness = MARTS[mart]
    assert getattr(mod, freshness)().passed


# @spec GOLD-WELLNESS-SCHED-001
def test_job_runs_its_checks_and_schedule_defaults_stopped() -> None:
    asset, job, schedule, cron = (
        "gold_daily_wellness", "gold_wellness_job", "gold_wellness_daily", "30 7 * * *"
    )
    ops = {n.name for n in defs.resolve_job_def(job).graph.nodes}
    assert asset in ops and f"{asset}_{asset}_freshness" in ops
    s = defs.get_schedule_def(schedule)
    assert s.cron_schedule == cron and str(s.execution_timezone) == "UTC"
    assert s.default_status == DefaultScheduleStatus.STOPPED


# @spec GOLD-CHECKS-JOB-001
def test_checks_job_runs_every_marts_checks_and_no_assets() -> None:
    ops = {n.name for n in defs.resolve_job_def("gold_checks_job").graph.nodes}
    asset = "gold_daily_wellness"
    assert asset not in ops
    assert f"{asset}_{asset}_freshness" in ops


# @spec GOLD-CHECKS-JOB-002
def test_checks_schedule_daily_0805_and_starts_running() -> None:
    s = defs.get_schedule_def("gold_checks_daily")
    assert s.job_name == "gold_checks_job"
    assert s.cron_schedule == "5 8 * * *" and str(s.execution_timezone) == "UTC"
    assert s.default_status == DefaultScheduleStatus.RUNNING
