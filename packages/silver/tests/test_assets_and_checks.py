"""Asset materialization + asset-check tests over the synthetic bronze tree.

Exercises the full path: materialize the three assets (Parquet written under a temp
SILVER_ROOT), then run each asset check against the output and assert pass/severity.
"""

from __future__ import annotations

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import assets as assets_mod
from grecohome_silver.dagster import checks as checks_mod
from grecohome_silver.dagster.assets import (
    UNIFIED_PARQUET,
    silver_path,
    silver_sleep,
    silver_sleep_garmin,
    silver_sleep_whoop,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def materialized(bronze_root, tmp_path, monkeypatch):
    """Point settings at the synthetic bronze + a temp silver root, then rebuild."""
    silver_root = str(tmp_path / "silver")
    monkeypatch.setattr(settings, "bronze_root", bronze_root)
    monkeypatch.setattr(settings, "silver_root", silver_root)
    result = materialize([silver_sleep_garmin, silver_sleep_whoop, silver_sleep])
    assert result.success
    return silver_root


def _rows(path: str) -> list[dict]:
    con = connect()
    cur = con.execute(f"SELECT * FROM read_parquet('{path}')")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def test_all_assets_materialize_and_write_parquet(materialized) -> None:
    rows = _rows(silver_path(UNIFIED_PARQUET))
    nights = {r["night_date"].isoformat() for r in rows}
    assert nights == {"2022-06-06", "2024-01-01", "2025-12-20", "2025-12-22"}


def test_rebuild_is_idempotent(bronze_root, tmp_path, monkeypatch) -> None:
    """Re-materializing yields byte-identical row content (pure projection)."""
    monkeypatch.setattr(settings, "bronze_root", bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    assert materialize([silver_sleep_garmin, silver_sleep_whoop, silver_sleep]).success
    first = _rows(silver_path(UNIFIED_PARQUET))
    assert materialize([silver_sleep_garmin, silver_sleep_whoop, silver_sleep]).success
    second = _rows(silver_path(UNIFIED_PARQUET))
    assert first == second


def test_uniqueness_checks_pass(materialized) -> None:
    for check in (
        checks_mod.garmin_night_unique_nonnull,
        checks_mod.whoop_id_unique_night_nonnull,
        checks_mod.sleep_night_unique_nonnull,
    ):
        r = check()
        assert r.passed, check.__name__


def test_range_check_passes(materialized) -> None:
    r = checks_mod.sleep_value_ranges()
    assert r.passed
    assert r.metadata["out_of_range_rows"].value == 0


def test_join_sanity_and_coverage_split(materialized) -> None:
    sanity = checks_mod.sleep_join_sanity()
    assert sanity.passed  # no fully-null rows
    assert sanity.severity == AssetCheckSeverity.WARN

    split = checks_mod.sleep_coverage_split()
    assert split.metadata["both"].value == 1
    assert split.metadata["garmin_only"].value == 2
    assert split.metadata["whoop_only"].value == 1


def test_garmin_coverage_matches_bronze(materialized) -> None:
    r = checks_mod.garmin_coverage_vs_bronze()
    assert r.passed
    # 3 distinct calendarDates in bronze (2024-01-01 twice, 2022-06-06, 2025-12-20).
    assert r.metadata["silver_nights"].value == 3
    assert r.metadata["bronze_distinct_nights"].value == 3


def test_range_check_catches_unit_bug(materialized, monkeypatch) -> None:
    """Sanity that the range check actually fails on an out-of-range value."""
    # Rewrite the unified parquet with an impossible percentage, then re-check.
    path = silver_path(UNIFIED_PARQUET)
    con = connect()
    con.execute(
        f"COPY (SELECT * REPLACE (999.0 AS whoop_performance_pct) FROM read_parquet('{path}')) "
        f"TO '{path}' (FORMAT parquet)"
    )
    assert assets_mod  # keep import referenced
    r = checks_mod.sleep_value_ranges()
    assert not r.passed
    assert r.severity == AssetCheckSeverity.ERROR
