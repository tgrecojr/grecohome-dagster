"""Asset materialization + asset-check tests for silver body."""

from __future__ import annotations

import json
import os

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import body_checks as checks_mod
from grecohome_silver.dagster.body_assets import BODY_PARQUET, body_path, silver_body

pytestmark = pytest.mark.unit


def _weighin(pk: int, cal_date: str, weight_g: int = 80000) -> dict:
    return {
        "samplePk": pk, "calendarDate": cal_date, "timestampGMT": 1700000000000,
        "weight": weight_g, "bmi": 27.0, "bodyFat": 22.5, "bodyWater": 55.0,
        "boneMass": 3200, "muscleMass": 33000, "sourceType": "INDEX_SCALE",
    }


def _write(root: str, dt: str, ms: int, weigh_ins: list[dict], short: str = "aa") -> None:
    pdir = os.path.join(root, "garmin", "daily_weigh_ins", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"daily_weigh_ins_{ms}_{short}.json"), "w") as fh:
        json.dump({"dateWeightList": weigh_ins}, fh)


@pytest.fixture
def body_bronze_root(tmp_path) -> str:
    root = str(tmp_path / "bronze")
    # Weigh-in 1 recurs in two files (overlap window) -> dedup to latest (81 kg); 2 once.
    _write(root, "2026-03-30", 1_700_000_000000, [_weighin(1, "2026-03-30", 80000)], short="a")
    _write(root, "2026-03-31", 1_700_000_999000, [_weighin(1, "2026-03-30", 81000)], short="b")
    _write(root, "2026-03-31", 1_700_001_000000, [_weighin(2, "2026-03-31", 80500)])
    return root


@pytest.fixture
def materialized(body_bronze_root, tmp_path, monkeypatch) -> str:
    monkeypatch.setattr(settings, "bronze_root", body_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    assert materialize([silver_body]).success
    return settings.silver_root


def _count(path: str) -> int:
    return int(connect().execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0])


def test_materializes_and_dedups(materialized) -> None:
    assert _count(body_path(BODY_PARQUET)) == 2  # weigh-in 1 deduped, weigh-in 2


def test_rebuild_idempotent(body_bronze_root, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", body_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    path = body_path(BODY_PARQUET)
    q = f"SELECT * FROM read_parquet('{path}') ORDER BY sample_pk"
    assert materialize([silver_body]).success
    one = connect().execute(q).fetchall()
    assert materialize([silver_body]).success
    two = connect().execute(q).fetchall()
    assert one == two


def test_checks_pass(materialized) -> None:
    assert checks_mod.body_sample_unique_nonnull().passed
    assert checks_mod.body_value_ranges().passed
    cov = checks_mod.body_coverage_vs_bronze()
    assert cov.passed and cov.severity == AssetCheckSeverity.WARN
    assert cov.metadata["silver_weighins"].value == 2


def test_range_check_catches_grams_as_kg(materialized) -> None:
    """The unit guard fires if weight is left in grams (80000 'kg')."""
    path = body_path(BODY_PARQUET)
    con = connect()
    con.execute(
        f"COPY (SELECT * REPLACE (80000.0 AS weight_kg) FROM read_parquet('{path}')) "
        f"TO '{path}' (FORMAT parquet)"
    )
    r = checks_mod.body_value_ranges()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


def test_refuses_write_under_bronze_root(body_bronze_root, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", body_bronze_root)
    monkeypatch.setattr(settings, "silver_root", os.path.join(body_bronze_root, "nested"))
    assert not materialize([silver_body], raise_on_error=False).success
