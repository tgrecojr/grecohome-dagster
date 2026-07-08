"""Asset materialization + asset-check tests for silver location.

Materializes ``silver_location`` over a synthetic bronze tree (Parquet under a temp
SILVER_ROOT), then runs each asset check against the output and asserts pass/severity.
"""

from __future__ import annotations

import json
import os

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import location_checks as checks_mod
from grecohome_silver.dagster.location_assets import (
    LOCATION_PARQUET,
    location_path,
    silver_location,
)

pytestmark = pytest.mark.unit


def _write(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(obj, fh)


@pytest.fixture
def location_bronze(tmp_path) -> str:
    root = str(tmp_path / "bronze")
    dt = "dt=2026-07-07"
    # Overland: two fixes; OwnTracks: one fix.
    _write(
        os.path.join(root, "location", "overland", dt, "overland_1700000000000_a.json"),
        {"locations": [
            {"geometry": {"coordinates": [-75.1, 39.8]},
             "properties": {"timestamp": "2026-07-07T12:00:00Z", "horizontal_accuracy": 5}},
            {"geometry": {"coordinates": [-75.2, 39.9]},
             "properties": {"timestamp": "2026-07-07T12:05:00Z"}},
        ]},
    )
    _write(
        os.path.join(root, "location", "owntracks", dt, "owntracks_1700000000000_a.json"),
        {"_type": "location", "lat": 40.0, "lon": -75.3, "tst": 1751894460, "acc": 8},
    )
    # geocode cache: one of the overland cells resolved.
    stem = os.path.join(root, "geocode", "reverse", dt, "reverse_1700000000000_a")
    os.makedirs(os.path.dirname(stem), exist_ok=True)
    with open(stem + ".json", "w") as fh:
        json.dump({"type": "FeatureCollection", "features": [
            {"type": "Feature", "properties": {"name": "Home", "city": "Avondale"}}]}, fh)
    with open(stem + ".meta.json", "w") as fh:
        json.dump({"lat_e4": 398000, "lon_e4": -751000, "fetched_at_unix_ms": 1700000000000}, fh)
    return root


@pytest.fixture
def materialized(location_bronze, tmp_path, monkeypatch) -> str:
    silver_root = str(tmp_path / "silver")
    monkeypatch.setattr(settings, "bronze_root", location_bronze)
    monkeypatch.setattr(settings, "silver_root", silver_root)
    result = materialize([silver_location])
    assert result.success
    return silver_root


def _rows(path: str) -> list[dict]:
    con = connect()
    cur = con.execute(f"SELECT * FROM read_parquet('{path}')")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def test_materializes_and_writes_parquet(materialized) -> None:
    rows = _rows(location_path(LOCATION_PARQUET))
    assert len(rows) == 3
    streams = sorted(r["source_stream"] for r in rows)
    assert streams == ["overland", "overland", "owntracks"]
    geocoded = [r for r in rows if r["geocoded"]]
    assert len(geocoded) == 1
    assert geocoded[0]["geo_name"] == "Home"


def test_rebuild_is_idempotent(location_bronze, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", location_bronze)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    assert materialize([silver_location]).success
    first = _rows(location_path(LOCATION_PARQUET))
    assert materialize([silver_location]).success
    assert first == _rows(location_path(LOCATION_PARQUET))


def test_unique_and_range_checks_pass(materialized) -> None:
    assert checks_mod.location_fix_unique_nonnull().passed
    assert checks_mod.location_coord_range().passed


def test_coverage_check_metadata(materialized) -> None:
    r = checks_mod.location_coverage_vs_bronze()
    assert r.passed
    assert r.severity == AssetCheckSeverity.WARN
    assert r.metadata["silver_fixes"].value == 3
    assert r.metadata["geocoded_fixes"].value == 1
    assert r.metadata["named_fixes"].value == 1


def test_range_check_catches_bad_coord(materialized) -> None:
    path = location_path(LOCATION_PARQUET)
    con = connect()
    con.execute(
        f"COPY (SELECT * REPLACE (999.0 AS lat) FROM read_parquet('{path}')) "
        f"TO '{path}' (FORMAT parquet)"
    )
    r = checks_mod.location_coord_range()
    assert not r.passed
    assert r.severity == AssetCheckSeverity.ERROR
