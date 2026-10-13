"""Workouts, splits and Whoop workouts: metadata, check failure cases, missing tables,
freshness, jobs."""

from __future__ import annotations

import json
import os
import time
from unittest.mock import patch

import pytest
from dagster import AssetCheckSeverity, DefaultScheduleStatus, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import whoop_workouts_checks, workout_checks, workout_splits_checks
from grecohome_silver.dagster.definitions import defs
from grecohome_silver.dagster.whoop_workouts_assets import (
    WHOOP_WORKOUTS_PARQUET,
    silver_whoop_workouts,
    whoop_workouts_path,
)
from grecohome_silver.dagster.workout_assets import WORKOUTS_PARQUET, silver_workouts, workouts_path
from grecohome_silver.dagster.workout_splits_assets import (
    SPLITS_PARQUET,
    silver_workout_splits,
    splits_path,
)

pytestmark = pytest.mark.unit


def _put(path, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(obj, fh)


@pytest.fixture
def roots(tmp_path, monkeypatch):
    b = tmp_path / "bronze"
    act = {"activityId": 11, "activityName": "Run", "activityType": {"typeKey": "running"},
           "startTimeLocal": "2026-06-11 07:00:00", "startTimeGMT": "2026-06-11 11:00:00",
           "duration": 1800.0, "distance": 5000.0, "calories": 400.0,
           "averageHR": 150, "maxHR": 175}
    _put(str(b / "garmin/activities/dt=2026-06-11/activities_1700000000000_a.json"), [act])
    lap = {"lapIndex": 1, "duration": 300.0, "distance": 1000.0, "averageSpeed": 3.3,
           "averageHR": 150, "maxHR": 160}
    _put(str(b / "garmin/activity_splits/dt=2026-06-11/activity_splits_1700000000000_a.json"),
         {"activityId": 11, "lapDTOs": [lap, {**lap, "lapIndex": 2}]})
    wk = {"id": "w-1", "start": "2026-06-11T12:00:00.000Z", "end": "2026-06-11T13:00:00.000Z",
          "timezone_offset": "-04:00", "updated_at": "2026-06-11T14:00:00.000Z",
          "sport_name": "weightlifting", "sport_id": 45, "score_state": "SCORED",
          "score": {"strain": 8.0, "average_heart_rate": 110, "max_heart_rate": 150,
                    "kilojoule": 900.0}}
    _put(str(b / "whoop/workout/dt=2026-06-11/workout_1700000000000_a.json"), {"records": [wk]})
    monkeypatch.setattr(settings, "bronze_root", str(b))
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))


@pytest.fixture
def result(roots):
    res = materialize([silver_workouts, silver_workout_splits, silver_whoop_workouts])
    assert res.success
    return res


TABLES = {
    "workouts": (workout_checks, "workouts_id_unique_nonnull", "workouts_value_ranges",
                 "workouts_coverage_vs_bronze", lambda: workouts_path(WORKOUTS_PARQUET),
                 "silver_workouts_freshness", ("max_hr", 241)),
    "splits": (workout_splits_checks, "splits_lap_unique_nonnull", "splits_value_ranges",
               "splits_coverage_vs_bronze", lambda: splits_path(SPLITS_PARQUET),
               "silver_workout_splits_freshness", ("lap_index", 0)),
    "whoop": (whoop_workouts_checks, "whoop_workout_unique_nonnull",
              "whoop_workout_value_ranges", "whoop_workout_coverage_vs_bronze",
              lambda: whoop_workouts_path(WHOOP_WORKOUTS_PARQUET),
              "silver_whoop_workouts_freshness", ("strain", 21.5)),
}


def _rewrite(path: str, select: str) -> None:
    tmp = path + ".rewrite.parquet"
    con = connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{path}')")
    con.execute(f"COPY ({select}) TO '{tmp}' (FORMAT parquet)")
    os.replace(tmp, path)


# @spec SILVER-WORKOUTS-GARMIN-006, SILVER-WORKOUTS-SPLITS-004, SILVER-WORKOUTS-WHOOP-006
@pytest.mark.parametrize("node", ["silver_workouts", "silver_workout_splits",
                                  "silver_whoop_workouts"])
def test_materialization_metadata(result, node) -> None:
    meta = result.asset_materializations_for_node(node)[0].metadata
    assert set(meta) == {"rows", "bronze_files", "path"}
    assert meta["bronze_files"].value == 1 and meta["rows"].value >= 1


# @spec SILVER-WORKOUTS-CHECKS-001, SILVER-WORKOUTS-CHECKS-004, SILVER-WORKOUTS-CHECKS-007
@pytest.mark.parametrize("table", TABLES)
def test_unique_check_passes_then_fails_on_duplicate(result, table) -> None:
    mod, unique, *_ = TABLES[table]
    assert getattr(mod, unique)().passed
    _rewrite(TABLES[table][4](), "SELECT * FROM t UNION ALL (SELECT * FROM t LIMIT 1)")
    r = getattr(mod, unique)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-WORKOUTS-CHECKS-002, SILVER-WORKOUTS-CHECKS-005, SILVER-WORKOUTS-CHECKS-008
@pytest.mark.parametrize("table", TABLES)
def test_range_check_passes_then_fails_out_of_bounds(result, table) -> None:
    mod, _, ranges, _, path, _, (col, value) = TABLES[table]
    assert getattr(mod, ranges)().passed
    _rewrite(path(), f"SELECT * REPLACE ({value} AS {col}) FROM t")
    r = getattr(mod, ranges)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-WORKOUTS-CHECKS-002
@pytest.mark.parametrize("col, value", [("duration_sec", 604800.0), ("distance_m", 1000000.0),
                                        ("calories", -1.0)])
def test_workouts_range_bounds(result, col, value) -> None:
    _rewrite(workouts_path(WORKOUTS_PARQUET), f"SELECT * REPLACE ({value} AS {col}) FROM t")
    assert not workout_checks.workouts_value_ranges().passed


# @spec SILVER-WORKOUTS-CHECKS-003, SILVER-WORKOUTS-CHECKS-006, SILVER-WORKOUTS-CHECKS-009
@pytest.mark.parametrize("table", TABLES)
def test_coverage_check_warns_when_silver_loses_rows(result, table) -> None:
    mod, _, _, coverage, path, *_ = TABLES[table]
    assert getattr(mod, coverage)().passed
    _rewrite(path(), "SELECT * FROM t WHERE false")
    r = getattr(mod, coverage)()
    assert not r.passed and r.severity == AssetCheckSeverity.WARN


# @spec SILVER-WORKOUTS-CHECKS-011, SILVER-WORKOUTS-CHECKS-012
@pytest.mark.parametrize("checks", [workout_checks.WORKOUT_CHECKS,
                                    workout_splits_checks.WORKOUT_SPLITS_CHECKS,
                                    whoop_workouts_checks.WHOOP_WORKOUTS_CHECKS])
def test_missing_table_fails_every_check_at_error_and_alerts(roots, checks) -> None:
    with patch("grecohome_core.checks.alerting._logger.error") as err:
        for check in checks:
            r = check()
            assert not r.passed and r.severity == AssetCheckSeverity.ERROR
    assert err.call_count == len(checks)


# @spec SILVER-WORKOUTS-CHECKS-010
@pytest.mark.parametrize("table", TABLES)
@pytest.mark.parametrize("age_h, passed", [(0, True), (35.5, True), (36.5, False)])
def test_freshness_fails_past_36_hours(result, table, age_h, passed) -> None:
    mod, *_, path, freshness, _ = TABLES[table]
    old = time.time() - age_h * 3600
    os.utime(path(), (old, old))
    r = getattr(mod, freshness)()
    assert r.passed is passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-WORKOUTS-SCHED-001
@pytest.mark.parametrize("asset, job, schedule, cron", [
    ("silver_workouts", "silver_workouts_job", "silver_workouts_daily", "45 6 * * *"),
    ("silver_workout_splits", "silver_workout_splits_job", "silver_workout_splits_daily",
     "47 6 * * *"),
    ("silver_whoop_workouts", "silver_whoop_workouts_job", "silver_whoop_workouts_daily",
     "53 6 * * *"),
])
def test_job_runs_its_checks_and_schedule_defaults_stopped(asset, job, schedule, cron) -> None:
    ops = {n.name for n in defs.resolve_job_def(job).graph.nodes}
    assert asset in ops and f"{asset}_{asset}_freshness" in ops
    assert len([o for o in ops if o.startswith(f"{asset}_")]) == 4  # 3 checks + freshness
    s = defs.get_schedule_def(schedule)
    assert s.cron_schedule == cron and str(s.execution_timezone) == "UTC"
    assert s.default_status == DefaultScheduleStatus.STOPPED
