"""Asset materialization + asset-check tests for silver Whoop workouts."""

from __future__ import annotations

import json
import os

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import whoop_workouts_checks as checks_mod
from grecohome_silver.dagster.whoop_workouts_assets import (
    WHOOP_WORKOUTS_PARQUET,
    silver_whoop_workouts,
    whoop_workouts_path,
)

pytestmark = pytest.mark.unit


def _workout(wid: str, start: str, updated_at: str, sport: str = "weightlifting",
             strain: float = 10.0) -> dict:
    return {
        "id": wid, "start": start, "end": start, "timezone_offset": "-05:00",
        "sport_name": sport, "sport_id": 45, "created_at": updated_at,
        "updated_at": updated_at, "score_state": "SCORED",
        "score": {"strain": strain, "kilojoule": 900.0, "average_heart_rate": 120,
                  "max_heart_rate": 150},
    }


def _write(root: str, dt: str, ms: int, records: list[dict], short: str = "aa") -> None:
    pdir = os.path.join(root, "whoop", "workout", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"workout_{ms}_{short}.json"), "w") as fh:
        json.dump({"records": records}, fh)


@pytest.fixture
def whoop_workouts_bronze_root(tmp_path) -> str:
    root = str(tmp_path / "bronze")
    # w-1 rescored (strain -> 12); w-2 once.
    _write(root, "2026-02-09", 1_700_000_000000,
           [_workout("w-1", "2026-02-09T15:00:00.000Z", "2026-02-09T16:00:00.000Z", strain=9.0)],
           short="v1")
    _write(root, "2026-02-09", 1_700_000_999000,
           [_workout("w-1", "2026-02-09T15:00:00.000Z", "2026-02-09T20:00:00.000Z", strain=12.0)],
           short="v2")
    _write(root, "2026-05-07", 1_700_001_000000,
           [_workout("w-2", "2026-05-07T15:00:00.000Z", "2026-05-07T16:00:00.000Z", "running")])
    return root


@pytest.fixture
def materialized(whoop_workouts_bronze_root, tmp_path, monkeypatch) -> str:
    monkeypatch.setattr(settings, "bronze_root", whoop_workouts_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    assert materialize([silver_whoop_workouts]).success
    return settings.silver_root


def _count(path: str) -> int:
    return int(connect().execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0])


def test_materializes_and_dedups(materialized) -> None:
    assert _count(whoop_workouts_path(WHOOP_WORKOUTS_PARQUET)) == 2  # w-1 deduped, w-2


def test_rebuild_idempotent(whoop_workouts_bronze_root, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", whoop_workouts_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    path = whoop_workouts_path(WHOOP_WORKOUTS_PARQUET)
    q = f"SELECT * FROM read_parquet('{path}') ORDER BY workout_id"
    assert materialize([silver_whoop_workouts]).success
    one = connect().execute(q).fetchall()
    assert materialize([silver_whoop_workouts]).success
    two = connect().execute(q).fetchall()
    assert one == two


def test_checks_pass(materialized) -> None:
    assert checks_mod.whoop_workout_unique_nonnull().passed
    assert checks_mod.whoop_workout_value_ranges().passed
    cov = checks_mod.whoop_workout_coverage_vs_bronze()
    assert cov.passed and cov.severity == AssetCheckSeverity.WARN
    assert cov.metadata["silver_workouts"].value == 2
    assert cov.metadata["distinct_sports"].value == 2  # weightlifting + running


def test_range_check_catches_bad_strain(materialized) -> None:
    path = whoop_workouts_path(WHOOP_WORKOUTS_PARQUET)
    con = connect()
    con.execute(
        f"COPY (SELECT * REPLACE (99.0 AS strain) FROM read_parquet('{path}')) "
        f"TO '{path}' (FORMAT parquet)"
    )
    r = checks_mod.whoop_workout_value_ranges()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


def test_refuses_write_under_bronze_root(whoop_workouts_bronze_root, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", whoop_workouts_bronze_root)
    monkeypatch.setattr(settings, "silver_root", os.path.join(whoop_workouts_bronze_root, "nested"))
    assert not materialize([silver_whoop_workouts], raise_on_error=False).success
