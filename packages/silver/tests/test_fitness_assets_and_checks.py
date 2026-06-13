"""Asset materialization + asset-check tests for silver fitness."""

from __future__ import annotations

import json
import os

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import fitness_checks as checks_mod
from grecohome_silver.dagster.fitness_assets import FITNESS_PARQUET, fitness_path, silver_fitness

pytestmark = pytest.mark.unit


def _write(root: str, coll: str, dt: str, ms: int, obj, short: str = "aa") -> None:
    pdir = os.path.join(root, "garmin", coll, f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"{coll}_{ms}_{short}.json"), "w") as fh:
        json.dump(obj, fh)


def _status(code: int = 3, load: int = 250) -> dict:
    return {"mostRecentTrainingStatus": {"latestTrainingStatusData": {
        "3381453277": {"trainingStatus": code, "weeklyTrainingLoad": load}}}}


@pytest.fixture
def fitness_bronze_root(tmp_path) -> str:
    root = str(tmp_path / "bronze")
    _write(root, "max_metrics", "2026-06-06", 1_700_000_000000,
           [{"generic": {"vo2MaxValue": 48.0}, "cycling": {"vo2MaxValue": None}}])
    _write(root, "max_metrics", "2026-06-06", 1_700_000_999000,
           [{"generic": {"vo2MaxValue": 52.0}, "cycling": {"vo2MaxValue": None}}], short="late")
    _write(root, "training_status", "2026-06-06", 1_700_000_000000, _status())
    _write(root, "training_status", "2026-06-07", 1_700_001_000000, _status())
    _write(root, "race_predictions", "2026-06-07", 1_700_001_000000,
           {"time5K": 1541, "time10K": 3200, "timeHalfMarathon": 7100, "timeMarathon": 15000})
    return root


@pytest.fixture
def materialized(fitness_bronze_root, tmp_path, monkeypatch) -> str:
    monkeypatch.setattr(settings, "bronze_root", fitness_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    assert materialize([silver_fitness]).success
    return settings.silver_root


def _count(path: str) -> int:
    return int(connect().execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0])


def test_materializes_two_snapshot_days(materialized) -> None:
    assert _count(fitness_path(FITNESS_PARQUET)) == 2  # 2026-06-06 and -07


def test_rebuild_idempotent(fitness_bronze_root, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", fitness_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    path = fitness_path(FITNESS_PARQUET)
    q = f"SELECT * FROM read_parquet('{path}') ORDER BY snapshot_date"
    assert materialize([silver_fitness]).success
    one = connect().execute(q).fetchall()
    assert materialize([silver_fitness]).success
    two = connect().execute(q).fetchall()
    assert one == two


def test_checks_pass(materialized) -> None:
    assert checks_mod.fitness_day_unique_nonnull().passed
    assert checks_mod.fitness_value_ranges().passed
    cov = checks_mod.fitness_coverage()
    assert cov.passed and cov.severity == AssetCheckSeverity.WARN
    assert cov.metadata["snapshot_days"].value == 2
    assert cov.metadata["days_with_vo2max"].value == 1  # only 2026-06-06 has VO2max


def test_range_check_catches_bad_vo2max(materialized) -> None:
    path = fitness_path(FITNESS_PARQUET)
    con = connect()
    con.execute(
        f"COPY (SELECT * REPLACE (999.0 AS vo2max_running) FROM read_parquet('{path}')) "
        f"TO '{path}' (FORMAT parquet)"
    )
    r = checks_mod.fitness_value_ranges()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


def test_refuses_write_under_bronze_root(fitness_bronze_root, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", fitness_bronze_root)
    monkeypatch.setattr(settings, "silver_root", os.path.join(fitness_bronze_root, "nested"))
    assert not materialize([silver_fitness], raise_on_error=False).success
