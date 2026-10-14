"""Gold mart checks: failure cases, missing marts, freshness, jobs and the checks-only job."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from dagster import AssetCheckSeverity, DefaultScheduleStatus, materialize

from grecohome_core.silver import connect
from grecohome_gold.config import settings
from grecohome_gold.dagster import checks as wellness_checks
from grecohome_gold.dagster import weather_checks
from grecohome_gold.dagster.assets import WELLNESS_PARQUET, gold_daily_wellness, gold_path
from grecohome_gold.dagster.definitions import defs
from grecohome_gold.dagster.weather_assets import (
    WEATHER_PARQUET,
    gold_daily_weather,
    gold_weather_path,
)

pytestmark = pytest.mark.unit

MARTS = {
    "wellness": (wellness_checks, "wellness_day_unique_nonnull", "wellness_value_ranges",
                 "wellness_coverage", lambda: gold_path(WELLNESS_PARQUET),
                 ("day_strain", 22.0), "gold_daily_wellness_freshness"),
    "weather": (weather_checks, "weather_day_unique_nonnull", "weather_value_ranges",
                "weather_coverage", lambda: gold_weather_path(WEATHER_PARQUET),
                ("rh_mean_pct", 101.0), "gold_daily_weather_freshness"),
}


@pytest.fixture
def both_silver(silver_root, weather_silver_root):
    """One SILVER_ROOT holding the wellness tables and silver_weather."""
    src = os.path.join(weather_silver_root, "weather", "silver_weather.parquet")
    dst = os.path.join(silver_root, "weather", "silver_weather.parquet")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    os.replace(src, dst)
    return silver_root


@pytest.fixture
def roots(both_silver, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "silver_root", both_silver)
    monkeypatch.setattr(settings, "gold_root", str(tmp_path / "gold"))
    monkeypatch.setattr(settings, "gold_weather_min_valid_hours", 2)


@pytest.fixture
def result(roots):
    res = materialize([gold_daily_wellness, gold_daily_weather])
    assert res.success
    return res


def _rewrite(path: str, select: str) -> None:
    tmp = path + ".rewrite.parquet"
    con = connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{path}')")
    con.execute(f"COPY ({select}) TO '{tmp}' (FORMAT parquet)")
    os.replace(tmp, path)


# @spec GOLD-WELLNESS-MART-010, GOLD-WEATHER-MART-006
def test_materialization_metadata(result) -> None:
    w = result.asset_materializations_for_node("gold_daily_wellness")[0].metadata
    assert set(w) == {"rows", "tir_range", "path"} and w["tir_range"].value == "70-140"
    x = result.asset_materializations_for_node("gold_daily_weather")[0].metadata
    assert set(x) == {"rows", "gdd_base_f", "min_valid_hours", "path"}


# @spec GOLD-WELLNESS-CHECKS-001, GOLD-WEATHER-CHECKS-001
@pytest.mark.parametrize("mart", MARTS)
def test_unique_check_fails_on_duplicate_day(result, mart) -> None:
    mod, unique, _, _, path, *_ = MARTS[mart]
    assert getattr(mod, unique)().passed
    _rewrite(path(), "SELECT * FROM t UNION ALL (SELECT * FROM t LIMIT 1)")
    r = getattr(mod, unique)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec GOLD-WELLNESS-CHECKS-002, GOLD-WEATHER-CHECKS-002
@pytest.mark.parametrize("mart", MARTS)
def test_range_check_fails_out_of_bounds(result, mart) -> None:
    mod, _, ranges, _, path, (col, value), _ = MARTS[mart]
    assert getattr(mod, ranges)().passed
    _rewrite(path(), f"SELECT * REPLACE ({value} AS {col}) FROM t")
    r = getattr(mod, ranges)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec GOLD-WELLNESS-CHECKS-003, GOLD-WEATHER-CHECKS-003
@pytest.mark.parametrize("mart", MARTS)
def test_coverage_warns_only_when_empty(result, mart) -> None:
    mod, _, _, coverage, path, *_ = MARTS[mart]
    assert getattr(mod, coverage)().passed
    _rewrite(path(), "SELECT * FROM t WHERE false")
    r = getattr(mod, coverage)()
    assert not r.passed and r.severity == AssetCheckSeverity.WARN


# @spec GOLD-WELLNESS-CHECKS-005, GOLD-WELLNESS-CHECKS-006
# @spec GOLD-WEATHER-CHECKS-005, GOLD-WEATHER-CHECKS-006
@pytest.mark.parametrize("checks", [wellness_checks.ALL_CHECKS, weather_checks.WEATHER_CHECKS])
def test_missing_mart_fails_every_check_at_error_and_alerts(roots, checks) -> None:
    with patch("grecohome_core.checks.alerting._logger.error") as err:
        for check in checks:
            r = check()
            assert not r.passed and r.severity == AssetCheckSeverity.ERROR
    assert err.call_count == len(checks)


# @spec GOLD-WELLNESS-CHECKS-004, GOLD-WEATHER-CHECKS-004
@pytest.mark.parametrize("mart", MARTS)
def test_freshness_check_defined_and_fresh(result, mart) -> None:
    mod, *_, freshness = MARTS[mart]
    assert getattr(mod, freshness)().passed


# @spec GOLD-WELLNESS-SCHED-001, GOLD-WEATHER-SCHED-001
@pytest.mark.parametrize("asset, job, schedule, cron", [
    ("gold_daily_wellness", "gold_wellness_job", "gold_wellness_daily", "30 7 * * *"),
    ("gold_daily_weather", "gold_weather_job", "gold_weather_daily", "40 7 * * *"),
])
def test_job_runs_its_checks_and_schedule_defaults_stopped(asset, job, schedule, cron) -> None:
    ops = {n.name for n in defs.resolve_job_def(job).graph.nodes}
    assert asset in ops and f"{asset}_{asset}_freshness" in ops
    s = defs.get_schedule_def(schedule)
    assert s.cron_schedule == cron and str(s.execution_timezone) == "UTC"
    assert s.default_status == DefaultScheduleStatus.STOPPED


# @spec GOLD-CHECKS-JOB-001
def test_checks_job_runs_every_marts_checks_and_no_assets() -> None:
    ops = {n.name for n in defs.resolve_job_def("gold_checks_job").graph.nodes}
    for asset in ("gold_daily_wellness", "gold_daily_weather"):
        assert asset not in ops
        assert f"{asset}_{asset}_freshness" in ops


# @spec GOLD-CHECKS-JOB-002
def test_checks_schedule_daily_0800_and_starts_running() -> None:
    s = defs.get_schedule_def("gold_checks_daily")
    assert s.job_name == "gold_checks_job"
    assert s.cron_schedule == "0 8 * * *" and str(s.execution_timezone) == "UTC"
    assert s.default_status == DefaultScheduleStatus.RUNNING
