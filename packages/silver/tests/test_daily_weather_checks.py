"""Weather, daily, body and fitness: metadata, check failure cases, missing tables, jobs."""

from __future__ import annotations

import json
import os
from unittest.mock import patch

import pytest
from dagster import AssetCheckSeverity, DefaultScheduleStatus, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import body_checks, daily_checks, fitness_checks, weather_checks
from grecohome_silver.dagster.body_assets import BODY_PARQUET, body_path, silver_body
from grecohome_silver.dagster.daily_assets import DAILY_PARQUET, daily_path, silver_daily
from grecohome_silver.dagster.definitions import defs
from grecohome_silver.dagster.fitness_assets import FITNESS_PARQUET, fitness_path, silver_fitness
from grecohome_silver.dagster.weather_assets import WEATHER_PARQUET, silver_weather, weather_path

pytestmark = pytest.mark.unit


def _put(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(text)


def _uscrn_line() -> str:
    f = ["-9999.0"] * 38
    f[0], f[1], f[2] = "03761", "20260610", "1200"
    f[9] = "20.0"
    f[13] = "-99999.0"  # SOLARAD sentinel
    for i in range(28, 33):  # soil-moisture sentinels (fields 29..33)
        f[i] = "-99.0"
    for pos in (15, 22, 24, 26, 28):
        f[pos - 1] = "0"
    return " ".join(f)


@pytest.fixture
def roots(tmp_path, monkeypatch):
    b = tmp_path / "bronze"
    _put(str(b / "uscrn/hourly/dt=2026-06-10/hourly_1700000000000_a.txt"), _uscrn_line() + "\n")
    _put(str(b / "garmin/user_summary/dt=2026-06-10/user_summary_1700000000000_a.json"),
         json.dumps({"calendarDate": "2026-06-10", "totalSteps": 9000, "restingHeartRate": 50}))
    _put(str(b / "garmin/daily_weigh_ins/dt=2026-06-10/daily_weigh_ins_1700000000000_a.json"),
         json.dumps({"dateWeightList": [{"samplePk": 5, "calendarDate": "2026-06-10",
                                         "timestampGMT": 1781000000000, "weight": 80000,
                                         "bmi": 25.0}]}))
    _put(str(b / "garmin/max_metrics/dt=2026-06-10/max_metrics_1700000000000_a.json"),
         json.dumps([{"generic": {"vo2MaxValue": 50.0, "calendarDate": "2026-06-10"}}]))
    _put(str(b / "garmin/race_predictions/dt=2026-06-10/race_predictions_1700000000000_a.json"),
         json.dumps({"time5K": 1500, "timeMarathon": 15000}))
    monkeypatch.setattr(settings, "bronze_root", str(b))
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))


@pytest.fixture
def result(roots):
    res = materialize([silver_weather, silver_daily, silver_body, silver_fitness])
    assert res.success
    return res


# (module, unique, ranges, coverage, path, (bad column, bad value))
TABLES = {
    "weather": (weather_checks, "weather_obs_unique_nonnull", "weather_value_ranges",
                "weather_coverage_vs_bronze", lambda: weather_path(WEATHER_PARQUET),
                ("air_temp_c", 61.0)),
    "daily": (daily_checks, "daily_date_unique_nonnull", "daily_value_ranges",
              "daily_coverage_vs_bronze", lambda: daily_path(DAILY_PARQUET),
              ("resting_heart_rate", 151)),
    "body": (body_checks, "body_sample_unique_nonnull", "body_value_ranges",
             "body_coverage_vs_bronze", lambda: body_path(BODY_PARQUET), ("weight_kg", 80000.0)),
    "fitness": (fitness_checks, "fitness_day_unique_nonnull", "fitness_value_ranges",
                "fitness_coverage", lambda: fitness_path(FITNESS_PARQUET),
                ("vo2max_running", 91.0)),
}


def _rewrite(path: str, select: str) -> None:
    tmp = path + ".rewrite.parquet"
    con = connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{path}')")
    con.execute(f"COPY ({select}) TO '{tmp}' (FORMAT parquet)")
    os.replace(tmp, path)


# @spec SILVER-WEATHER-TABLE-007, SILVER-DAILY-SUMMARY-005, SILVER-DAILY-BODY-006
# @spec SILVER-DAILY-FITNESS-005
@pytest.mark.parametrize("node", ["silver_weather", "silver_daily", "silver_body",
                                  "silver_fitness"])
def test_materialization_metadata(result, node) -> None:
    meta = result.asset_materializations_for_node(node)[0].metadata
    assert set(meta) == {"rows", "bronze_files", "path"}
    assert meta["bronze_files"].value >= 1 and meta["rows"].value >= 1


# @spec SILVER-WEATHER-CHECKS-001, SILVER-DAILY-CHECKS-001, SILVER-DAILY-CHECKS-004
# @spec SILVER-DAILY-CHECKS-007
@pytest.mark.parametrize("table", TABLES)
def test_unique_check_passes_then_fails_on_duplicate(result, table) -> None:
    mod, unique, *_ = TABLES[table]
    assert getattr(mod, unique)().passed
    _rewrite(TABLES[table][4](), "SELECT * FROM t UNION ALL (SELECT * FROM t LIMIT 1)")
    r = getattr(mod, unique)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-WEATHER-CHECKS-002, SILVER-DAILY-CHECKS-002, SILVER-DAILY-CHECKS-005
# @spec SILVER-DAILY-CHECKS-008
@pytest.mark.parametrize("table", TABLES)
def test_range_check_passes_then_fails_out_of_bounds(result, table) -> None:
    mod, _, ranges, _, path, (col, value) = TABLES[table]
    assert getattr(mod, ranges)().passed
    _rewrite(path(), f"SELECT * REPLACE ({value} AS {col}) FROM t")
    r = getattr(mod, ranges)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-WEATHER-CHECKS-003, SILVER-DAILY-CHECKS-003, SILVER-DAILY-CHECKS-006
# @spec SILVER-DAILY-CHECKS-009
@pytest.mark.parametrize("table", TABLES)
def test_coverage_check_warns_when_silver_loses_rows(result, table) -> None:
    mod, _, _, coverage, path, _ = TABLES[table]
    assert getattr(mod, coverage)().passed
    _rewrite(path(), "SELECT * FROM t WHERE false")
    r = getattr(mod, coverage)()
    assert not r.passed and r.severity == AssetCheckSeverity.WARN


# @spec SILVER-WEATHER-CHECKS-005, SILVER-WEATHER-CHECKS-006
# @spec SILVER-DAILY-CHECKS-011, SILVER-DAILY-CHECKS-012
@pytest.mark.parametrize("checks", [weather_checks.WEATHER_CHECKS, daily_checks.DAILY_CHECKS,
                                    body_checks.BODY_CHECKS, fitness_checks.FITNESS_CHECKS])
def test_missing_table_fails_every_check_at_error_and_alerts(roots, checks) -> None:
    with patch("grecohome_core.checks.alerting._logger.error") as err:
        for check in checks:
            r = check()
            assert not r.passed and r.severity == AssetCheckSeverity.ERROR
    assert err.call_count == len(checks)


# @spec SILVER-WEATHER-CHECKS-004, SILVER-DAILY-CHECKS-010
@pytest.mark.parametrize("mod, name", [
    (weather_checks, "silver_weather_freshness"), (daily_checks, "silver_daily_freshness"),
    (body_checks, "silver_body_freshness"), (fitness_checks, "silver_fitness_freshness"),
])
def test_freshness_check_defined_and_fresh(result, mod, name) -> None:
    check = getattr(mod, name)
    assert check in getattr(mod, [n for n in dir(mod) if n.endswith("_CHECKS")][0])
    assert check().passed


# @spec SILVER-WEATHER-SCHED-001, SILVER-DAILY-SCHED-001
@pytest.mark.parametrize("asset, job, schedule, cron", [
    ("silver_weather", "silver_weather_job", "silver_weather_daily", "55 6 * * *"),
    ("silver_daily", "silver_daily_job", "silver_daily_daily", "40 6 * * *"),
    ("silver_body", "silver_body_job", "silver_body_daily", "42 6 * * *"),
    ("silver_fitness", "silver_fitness_job", "silver_fitness_daily", "44 6 * * *"),
])
def test_job_runs_its_checks_and_schedule_defaults_stopped(asset, job, schedule, cron) -> None:
    ops = {n.name for n in defs.resolve_job_def(job).graph.nodes}
    assert asset in ops and f"{asset}_{asset}_freshness" in ops
    s = defs.get_schedule_def(schedule)
    assert s.cron_schedule == cron and str(s.execution_timezone) == "UTC"
    assert s.default_status == DefaultScheduleStatus.STOPPED
