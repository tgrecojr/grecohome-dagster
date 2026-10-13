"""Sleep asset checks: failure cases, missing tables, alerting and table freshness."""

from __future__ import annotations

import os
import time
from unittest.mock import patch

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import checks as checks_mod
from grecohome_silver.dagster.assets import (
    GARMIN_PARQUET,
    UNIFIED_PARQUET,
    WHOOP_PARQUET,
    silver_path,
    silver_sleep,
    silver_sleep_garmin,
    silver_sleep_whoop,
)

pytestmark = pytest.mark.unit

FRESHNESS = (
    ("silver_sleep_garmin_freshness", GARMIN_PARQUET),
    ("silver_sleep_whoop_freshness", WHOOP_PARQUET),
    ("silver_sleep_freshness", UNIFIED_PARQUET),
)


@pytest.fixture
def roots(bronze_root, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "bronze_root", bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))


@pytest.fixture
def result(roots):
    res = materialize([silver_sleep_garmin, silver_sleep_whoop, silver_sleep])
    assert res.success
    return res


def _rewrite(filename: str, select: str) -> None:
    """Replace a written table with ``select`` run over its current contents (``t``)."""
    path = silver_path(filename)
    tmp = path + ".rewrite.parquet"
    con = connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{path}')")
    con.execute(f"COPY ({select}) TO '{tmp}' (FORMAT parquet)")
    os.replace(tmp, path)


def _meta(res, name: str) -> dict:
    mats = res.asset_materializations_for_node(name)
    return {k: v.value for k, v in mats[0].metadata.items()}


# @spec SILVER-SLEEP-GARMIN-005, SILVER-SLEEP-WHOOP-006, SILVER-SLEEP-JOIN-003
def test_materialization_metadata(result) -> None:
    for name, filename in (("silver_sleep_garmin", GARMIN_PARQUET),
                           ("silver_sleep_whoop", WHOOP_PARQUET)):
        meta = _meta(result, name)
        assert set(meta) == {"rows", "bronze_files", "path"}
        assert meta["path"] == silver_path(filename) and meta["bronze_files"] > 0
    meta = _meta(result, "silver_sleep")
    assert set(meta) == {"rows", "path"} and meta["rows"] == 4


# @spec SILVER-SLEEP-CHECKS-001, SILVER-SLEEP-CHECKS-002, SILVER-SLEEP-CHECKS-003
@pytest.mark.parametrize("check, filename", [
    ("garmin_night_unique_nonnull", GARMIN_PARQUET),
    ("whoop_id_unique_night_nonnull", WHOOP_PARQUET),
    ("sleep_night_unique_nonnull", UNIFIED_PARQUET),
])
def test_uniqueness_checks_fail_on_duplicate(result, check, filename) -> None:
    _rewrite(filename, "SELECT * FROM t UNION ALL (SELECT * FROM t LIMIT 1)")
    r = getattr(checks_mod, check)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-SLEEP-CHECKS-001, SILVER-SLEEP-CHECKS-002, SILVER-SLEEP-CHECKS-003
@pytest.mark.parametrize("check, filename", [
    ("garmin_night_unique_nonnull", GARMIN_PARQUET),
    ("whoop_id_unique_night_nonnull", WHOOP_PARQUET),
    ("sleep_night_unique_nonnull", UNIFIED_PARQUET),
])
def test_uniqueness_checks_fail_on_null_night(result, check, filename) -> None:
    _rewrite(filename, "SELECT * REPLACE (NULL::DATE AS night_date) FROM t LIMIT 1")
    r = getattr(checks_mod, check)()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-SLEEP-CHECKS-004
@pytest.mark.parametrize("col, value", [
    ("garmin_sleep_score", -1), ("whoop_efficiency_pct", 100.5),
    ("garmin_awake_min", 1440.0), ("whoop_rem_min", -0.1),
])
def test_range_check_fails_out_of_bounds(result, col, value) -> None:
    _rewrite(UNIFIED_PARQUET, f"SELECT * REPLACE ({value} AS {col}) FROM t")
    r = checks_mod.sleep_value_ranges()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-SLEEP-CHECKS-005
def test_join_sanity_fails_on_sourceless_row_and_reports_single_source(result) -> None:
    ok = checks_mod.sleep_join_sanity()
    assert ok.passed
    assert ok.metadata["recent_single_source_nights"].value == 1  # 2025-12-22, Whoop only
    _rewrite(UNIFIED_PARQUET, "SELECT * REPLACE (false AS has_garmin, false AS has_whoop) FROM t")
    r = checks_mod.sleep_join_sanity()
    assert not r.passed and r.severity == AssetCheckSeverity.WARN


# @spec SILVER-SLEEP-CHECKS-007
def test_garmin_coverage_fails_when_silver_loses_nights(result) -> None:
    _rewrite(GARMIN_PARQUET, "SELECT * FROM t LIMIT 1")
    r = checks_mod.garmin_coverage_vs_bronze()
    assert not r.passed and r.severity == AssetCheckSeverity.WARN


# @spec SILVER-SLEEP-CHECKS-008, SILVER-SLEEP-CHECKS-009
def test_missing_tables_fail_every_check_at_error_and_alert(roots) -> None:
    with patch("grecohome_core.checks.alerting._logger.error") as err:
        for check in checks_mod.SLEEP_CHECKS:
            r = check()
            assert not r.passed and r.severity == AssetCheckSeverity.ERROR, check
    assert err.call_count == len(checks_mod.SLEEP_CHECKS)
    assert {c.args[0] for c in err.call_args_list} == {"asset_check_failed"}


# @spec SILVER-SLEEP-CHECKS-010
@pytest.mark.parametrize("check, filename", FRESHNESS)
def test_freshness_passes_on_a_fresh_table(result, check, filename) -> None:
    assert getattr(checks_mod, check)().passed


# @spec SILVER-SLEEP-CHECKS-010
@pytest.mark.parametrize("check, filename", FRESHNESS)
@pytest.mark.parametrize("age_h, passed", [(35.5, True), (36.5, False)])
def test_freshness_fails_past_36_hours(result, check, filename, age_h, passed) -> None:
    old = time.time() - age_h * 3600
    os.utime(silver_path(filename), (old, old))
    r = getattr(checks_mod, check)()
    assert r.passed is passed
    assert r.severity == AssetCheckSeverity.ERROR


# @spec SILVER-SLEEP-CHECKS-010
def test_freshness_checks_are_registered() -> None:
    names = {c.check_specs_by_output_name[k].name
             for c in checks_mod.SLEEP_CHECKS for k in c.check_specs_by_output_name}
    assert {name for name, _ in FRESHNESS} <= names
