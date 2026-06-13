"""Asset materialization + asset-check tests for silver strain."""

from __future__ import annotations

import json
import os

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import strain_checks as checks_mod
from grecohome_silver.dagster.strain_assets import STRAIN_PARQUET, silver_strain, strain_path

pytestmark = pytest.mark.unit


def _write(root: str, dt: str, ms: int, records: list[dict], short: str = "aa") -> None:
    pdir = os.path.join(root, "whoop", "cycle", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"cycle_{ms}_{short}.json"), "w") as fh:
        json.dump({"records": records}, fh)


def _cycle(cid: int, start: str, updated_at: str, strain: float = 12.0) -> dict:
    return {
        "id": cid, "start": start, "end": start, "timezone_offset": "-04:00",
        "created_at": updated_at, "updated_at": updated_at, "score_state": "SCORED",
        "score": {"strain": strain, "kilojoule": 8000.0,
                  "average_heart_rate": 120, "max_heart_rate": 170},
    }


@pytest.fixture
def strain_bronze_root(tmp_path) -> str:
    root = str(tmp_path / "bronze")
    # Cycle 1 captured twice (rescore -> strain 15); cycle 2 once.
    _write(root, "2026-06-10", 1_700_000_000000,
           [_cycle(1, "2026-06-10T11:00:00.000Z", "2026-06-10T12:00:00.000Z", 10.0)], short="v1")
    _write(root, "2026-06-10", 1_700_000_999000,
           [_cycle(1, "2026-06-10T11:00:00.000Z", "2026-06-10T22:00:00.000Z", 15.0)], short="v2")
    _write(root, "2026-06-11", 1_700_001_000000,
           [_cycle(2, "2026-06-11T11:00:00.000Z", "2026-06-11T20:00:00.000Z", 8.0)])
    return root


@pytest.fixture
def materialized(strain_bronze_root, tmp_path, monkeypatch) -> str:
    monkeypatch.setattr(settings, "bronze_root", strain_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    assert materialize([silver_strain]).success
    return settings.silver_root


def _count(path: str) -> int:
    return int(connect().execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0])


def test_materializes_and_dedups(materialized) -> None:
    assert _count(strain_path(STRAIN_PARQUET)) == 2  # cycle 1 deduped, cycle 2


def test_rebuild_idempotent(strain_bronze_root, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", strain_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    path = strain_path(STRAIN_PARQUET)
    assert materialize([silver_strain]).success
    one = connect().execute(f"SELECT * FROM read_parquet('{path}') ORDER BY cycle_id").fetchall()
    assert materialize([silver_strain]).success
    two = connect().execute(f"SELECT * FROM read_parquet('{path}') ORDER BY cycle_id").fetchall()
    assert one == two


def test_checks_pass(materialized) -> None:
    assert checks_mod.strain_cycle_unique_nonnull().passed
    assert checks_mod.strain_value_ranges().passed
    cov = checks_mod.strain_coverage_vs_bronze()
    assert cov.passed and cov.severity == AssetCheckSeverity.WARN
    assert cov.metadata["silver_cycles"].value == 2


def test_range_check_catches_bad_strain(materialized) -> None:
    path = strain_path(STRAIN_PARQUET)
    con = connect()
    con.execute(
        f"COPY (SELECT * REPLACE (99.0 AS day_strain) FROM read_parquet('{path}')) "
        f"TO '{path}' (FORMAT parquet)"
    )
    r = checks_mod.strain_value_ranges()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


def test_refuses_write_under_bronze_root(strain_bronze_root, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", strain_bronze_root)
    monkeypatch.setattr(settings, "silver_root", os.path.join(strain_bronze_root, "nested"))
    assert not materialize([silver_strain], raise_on_error=False).success
