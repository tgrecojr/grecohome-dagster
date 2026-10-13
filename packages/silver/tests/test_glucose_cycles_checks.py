"""Glucose, recovery and strain: metadata, check failure cases, missing tables, freshness, jobs."""

from __future__ import annotations

import json
import os
import time
from unittest.mock import patch

import pytest
from dagster import AssetCheckSeverity, DefaultScheduleStatus, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import glucose_checks, recovery_checks, strain_checks
from grecohome_silver.dagster.definitions import defs
from grecohome_silver.dagster.glucose_assets import GLUCOSE_PARQUET, glucose_path, silver_glucose
from grecohome_silver.dagster.recovery_assets import (
    RECOVERY_PARQUET,
    recovery_path,
    silver_recovery,
)
from grecohome_silver.dagster.strain_assets import STRAIN_PARQUET, silver_strain, strain_path

pytestmark = pytest.mark.unit

_HEADER = "Time of Glucose Reading [T=(local time) +/- (time zone offset)],Measurement(mg/dL)"


def _put(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(text)


@pytest.fixture
def roots(tmp_path, monkeypatch):
    bronze = tmp_path / "bronze"
    _put(str(bronze / "lingo/glucose/dt=2026-06-12/glucose_1700000000000_a.csv"),
         f"{_HEADER}\n2026-06-11T18:05-04:00,105\n2026-06-11T18:20-04:00,110\n")
    rec = {"cycle_id": 7, "sleep_id": "s-7", "created_at": "2026-06-11T11:00:00.000Z",
           "updated_at": "2026-06-11T11:30:00.000Z", "score_state": "SCORED",
           "score": {"recovery_score": 60.0, "resting_heart_rate": 55.0}}
    _put(str(bronze / "whoop/recovery/dt=2026-06-11/recovery_1700000000000_a.json"),
         json.dumps({"records": [rec]}))
    cyc = {"id": 7, "start": "2026-06-11T10:00:00.000Z", "end": None,
           "timezone_offset": "-04:00", "updated_at": "2026-06-11T20:00:00.000Z",
           "score_state": "SCORED", "score": {"strain": 9.0, "kilojoule": 5000.0}}
    _put(str(bronze / "whoop/cycle/dt=2026-06-11/cycle_1700000000000_a.json"),
         json.dumps({"records": [cyc]}))
    monkeypatch.setattr(settings, "bronze_root", str(bronze))
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))


@pytest.fixture
def result(roots):
    res = materialize([silver_glucose, silver_recovery, silver_strain])
    assert res.success
    return res


# (checks module, unique check, range check, coverage check, parquet path, freshness check)
TABLES = {
    "glucose": (glucose_checks, "glucose_reading_unique_nonnull", "glucose_value_range",
                "glucose_coverage_vs_bronze", lambda: glucose_path(GLUCOSE_PARQUET),
                "silver_glucose_freshness"),
    "recovery": (recovery_checks, "recovery_cycle_unique_nonnull", "recovery_value_ranges",
                 "recovery_coverage_vs_bronze", lambda: recovery_path(RECOVERY_PARQUET),
                 "silver_recovery_freshness"),
    "strain": (strain_checks, "strain_cycle_unique_nonnull", "strain_value_ranges",
               "strain_coverage_vs_bronze", lambda: strain_path(STRAIN_PARQUET),
               "silver_strain_freshness"),
}
BAD_VALUE = {"glucose": ("mgdl", 601), "recovery": ("hrv_rmssd_milli", 501.0),
             "strain": ("day_strain", 21.5)}


def _rewrite(path: str, select: str) -> None:
    tmp = path + ".rewrite.parquet"
    con = connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{path}')")
    con.execute(f"COPY ({select}) TO '{tmp}' (FORMAT parquet)")
    os.replace(tmp, path)


# @spec SILVER-GLUCOSE-TABLE-006, SILVER-CYCLES-RECOVERY-007, SILVER-CYCLES-STRAIN-006
@pytest.mark.parametrize("node", ["silver_glucose", "silver_recovery", "silver_strain"])
def test_materialization_metadata(result, node) -> None:
    meta = result.asset_materializations_for_node(node)[0].metadata
    assert set(meta) == {"rows", "bronze_files", "path"}
    assert meta["bronze_files"].value == 1 and meta["rows"].value >= 1


# @spec SILVER-GLUCOSE-CHECKS-001, SILVER-CYCLES-CHECKS-001, SILVER-CYCLES-CHECKS-004
@pytest.mark.parametrize("table", TABLES)
def test_unique_check_passes_then_fails_on_duplicate(result, table) -> None:
    mod, unique, _, _, path, _ = TABLES[table]
    assert getattr(mod, unique)().passed
    _rewrite(path(), "SELECT * FROM t UNION ALL (SELECT * FROM t LIMIT 1)")
    r = getattr(mod, unique)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-GLUCOSE-CHECKS-002, SILVER-CYCLES-CHECKS-002, SILVER-CYCLES-CHECKS-005
@pytest.mark.parametrize("table", TABLES)
def test_range_check_passes_then_fails_out_of_bounds(result, table) -> None:
    mod, _, ranges, _, path, _ = TABLES[table]
    assert getattr(mod, ranges)().passed
    col, value = BAD_VALUE[table]
    _rewrite(path(), f"SELECT * REPLACE ({value} AS {col}) FROM t")
    r = getattr(mod, ranges)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-GLUCOSE-CHECKS-003, SILVER-CYCLES-CHECKS-003, SILVER-CYCLES-CHECKS-006
@pytest.mark.parametrize("table", TABLES)
def test_coverage_check_warns_when_silver_loses_rows(result, table) -> None:
    mod, _, _, coverage, path, _ = TABLES[table]
    assert getattr(mod, coverage)().passed
    _rewrite(path(), "SELECT * FROM t WHERE false")
    r = getattr(mod, coverage)()
    assert not r.passed and r.severity == AssetCheckSeverity.WARN
    if table in ("glucose", "strain"):
        assert "distinct_days" in r.metadata


# @spec SILVER-GLUCOSE-CHECKS-005, SILVER-GLUCOSE-CHECKS-006
# @spec SILVER-CYCLES-CHECKS-008, SILVER-CYCLES-CHECKS-009
@pytest.mark.parametrize("checks", [glucose_checks.GLUCOSE_CHECKS, recovery_checks.RECOVERY_CHECKS,
                                    strain_checks.STRAIN_CHECKS])
def test_missing_table_fails_every_check_at_error_and_alerts(roots, checks) -> None:
    with patch("grecohome_core.checks.alerting._logger.error") as err:
        for check in checks:
            r = check()
            assert not r.passed and r.severity == AssetCheckSeverity.ERROR
    assert err.call_count == len(checks)


# @spec SILVER-GLUCOSE-CHECKS-004, SILVER-CYCLES-CHECKS-007
@pytest.mark.parametrize("table", TABLES)
@pytest.mark.parametrize("age_h, passed", [(0, True), (35.5, True), (36.5, False)])
def test_freshness_fails_past_36_hours(result, table, age_h, passed) -> None:
    mod, *_, path, freshness = TABLES[table]
    old = time.time() - age_h * 3600
    os.utime(path(), (old, old))
    r = getattr(mod, freshness)()
    assert r.passed is passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-GLUCOSE-SCHED-001, SILVER-CYCLES-SCHED-001
@pytest.mark.parametrize("asset, job, schedule, cron", [
    ("silver_glucose", "silver_glucose_job", "silver_glucose_daily", "30 6 * * *"),
    ("silver_recovery", "silver_recovery_job", "silver_recovery_daily", "50 6 * * *"),
    ("silver_strain", "silver_strain_job", "silver_strain_daily", "52 6 * * *"),
])
def test_job_runs_its_checks_and_schedule_defaults_stopped(asset, job, schedule, cron) -> None:
    ops = {n.name for n in defs.resolve_job_def(job).graph.nodes}
    assert asset in ops
    assert f"{asset}_{asset}_freshness" in ops
    assert len([o for o in ops if o.startswith(f"{asset}_")]) == 4  # 3 checks + freshness
    s = defs.get_schedule_def(schedule)
    assert s.cron_schedule == cron and str(s.execution_timezone) == "UTC"
    assert s.default_status == DefaultScheduleStatus.STOPPED
