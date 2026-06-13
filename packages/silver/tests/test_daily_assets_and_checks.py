"""Asset materialization + asset-check tests for silver daily-summary."""

from __future__ import annotations

import json
import os

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import daily_checks as checks_mod
from grecohome_silver.dagster.daily_assets import DAILY_PARQUET, daily_path, silver_daily

pytestmark = pytest.mark.unit


def _summary(cal_date: str, **over) -> dict:
    obj = {
        "calendarDate": cal_date, "totalSteps": 9000, "totalDistanceMeters": 6500.0,
        "activeKilocalories": 600.0, "restingHeartRate": 52, "maxHeartRate": 150,
        "averageStressLevel": 35, "bodyBatteryHighestValue": 95, "averageSpo2": 96,
    }
    obj.update(over)
    return obj


def _write(root: str, dt: str, ms: int, obj: dict, short: str = "aa") -> None:
    pdir = os.path.join(root, "garmin", "user_summary", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"user_summary_{ms}_{short}.json"), "w") as fh:
        json.dump(obj, fh)


@pytest.fixture
def daily_bronze_root(tmp_path) -> str:
    root = str(tmp_path / "bronze")
    # 2026-06-10 captured twice (dedup -> 12000 steps); 2026-06-11 once.
    _write(root, "2026-06-10", 1_700_000_000000, _summary("2026-06-10", totalSteps=9000), short="a")
    _write(root, "2026-06-10", 1_700_000_999000, _summary("2026-06-10", totalSteps=12000),
           short="b")
    _write(root, "2026-06-11", 1_700_001_000000, _summary("2026-06-11"))
    return root


@pytest.fixture
def materialized(daily_bronze_root, tmp_path, monkeypatch) -> str:
    monkeypatch.setattr(settings, "bronze_root", daily_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    assert materialize([silver_daily]).success
    return settings.silver_root


def _count(path: str) -> int:
    return int(connect().execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0])


def test_materializes_and_dedups(materialized) -> None:
    assert _count(daily_path(DAILY_PARQUET)) == 2  # 06-10 deduped, 06-11


def test_rebuild_idempotent(daily_bronze_root, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", daily_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    path = daily_path(DAILY_PARQUET)
    q = f"SELECT * FROM read_parquet('{path}') ORDER BY activity_date"
    assert materialize([silver_daily]).success
    one = connect().execute(q).fetchall()
    assert materialize([silver_daily]).success
    two = connect().execute(q).fetchall()
    assert one == two


def test_checks_pass(materialized) -> None:
    assert checks_mod.daily_date_unique_nonnull().passed
    assert checks_mod.daily_value_ranges().passed
    cov = checks_mod.daily_coverage_vs_bronze()
    assert cov.passed and cov.severity == AssetCheckSeverity.WARN
    assert cov.metadata["silver_days"].value == 2


def test_range_check_catches_bad_steps(materialized) -> None:
    path = daily_path(DAILY_PARQUET)
    con = connect()
    con.execute(
        f"COPY (SELECT * REPLACE (-5 AS total_steps) FROM read_parquet('{path}')) "
        f"TO '{path}' (FORMAT parquet)"
    )
    r = checks_mod.daily_value_ranges()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


def test_refuses_write_under_bronze_root(daily_bronze_root, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", daily_bronze_root)
    monkeypatch.setattr(settings, "silver_root", os.path.join(daily_bronze_root, "nested"))
    assert not materialize([silver_daily], raise_on_error=False).success
