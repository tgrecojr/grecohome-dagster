"""Asset materialization + asset-check tests for silver workout splits."""

from __future__ import annotations

import json
import os

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import workout_splits_checks as checks_mod
from grecohome_silver.dagster.workout_splits_assets import (
    SPLITS_PARQUET,
    silver_workout_splits,
    splits_path,
)

pytestmark = pytest.mark.unit


def _lap(idx: int, distance: float = 1000.0, avg_hr: int = 150) -> dict:
    return {
        "lapIndex": idx, "startTimeGMT": "2024-01-01T12:00:00.0", "duration": 300.0,
        "movingDuration": 295.0, "distance": distance, "averageSpeed": 3.33, "maxSpeed": 4.0,
        "averageHR": avg_hr, "maxHR": avg_hr + 15, "calories": 50.0,
    }


def _write(root: str, activity_id: int, ms: int, laps: list[dict], short: str = "aa") -> None:
    pdir = os.path.join(root, "garmin", "activity_splits", "dt=2024-01-01")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"activity_splits_{ms}_{short}.json"), "w") as fh:
        json.dump({"activityId": activity_id, "lapDTOs": laps, "eventDTOs": []}, fh)


@pytest.fixture
def splits_bronze_root(tmp_path) -> str:
    root = str(tmp_path / "bronze")
    # Activity 555 re-pulled (lap 1 dedups to distance 1100); activity 777 has two laps.
    _write(root, 555, 1_700_000_000000, [_lap(1, 1000.0)], short="early")
    _write(root, 555, 1_700_000_999000, [_lap(1, 1100.0)], short="late")
    _write(root, 777, 1_700_001_000000, [_lap(1), _lap(2)])
    return root


@pytest.fixture
def materialized(splits_bronze_root, tmp_path, monkeypatch) -> str:
    monkeypatch.setattr(settings, "bronze_root", splits_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    assert materialize([silver_workout_splits]).success
    return settings.silver_root


def _count(path: str) -> int:
    return int(connect().execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0])


def test_materializes_and_dedups(materialized) -> None:
    assert _count(splits_path(SPLITS_PARQUET)) == 3  # 555/lap1 deduped + 777/lap1 + 777/lap2


def test_rebuild_idempotent(splits_bronze_root, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", splits_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    path = splits_path(SPLITS_PARQUET)
    q = f"SELECT * FROM read_parquet('{path}') ORDER BY activity_id, lap_index"
    assert materialize([silver_workout_splits]).success
    one = connect().execute(q).fetchall()
    assert materialize([silver_workout_splits]).success
    two = connect().execute(q).fetchall()
    assert one == two


def test_checks_pass(materialized) -> None:
    assert checks_mod.splits_lap_unique_nonnull().passed
    assert checks_mod.splits_value_ranges().passed
    cov = checks_mod.splits_coverage_vs_bronze()
    assert cov.passed and cov.severity == AssetCheckSeverity.WARN
    assert cov.metadata["silver_laps"].value == 3
    assert cov.metadata["distinct_activities"].value == 2


def test_range_check_catches_bad_hr(materialized) -> None:
    path = splits_path(SPLITS_PARQUET)
    con = connect()
    con.execute(
        f"COPY (SELECT * REPLACE (999 AS avg_hr) FROM read_parquet('{path}')) "
        f"TO '{path}' (FORMAT parquet)"
    )
    r = checks_mod.splits_value_ranges()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


def test_refuses_write_under_bronze_root(splits_bronze_root, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", splits_bronze_root)
    monkeypatch.setattr(settings, "silver_root", os.path.join(splits_bronze_root, "nested"))
    assert not materialize([silver_workout_splits], raise_on_error=False).success
