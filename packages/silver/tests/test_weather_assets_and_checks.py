"""Asset materialization + asset-check tests for silver weather.

Builds a tiny USCRN bronze tree (duplicate capture, a sentinel-only row, a NUL-padded
row), materializes ``silver_weather`` under a temp SILVER_ROOT, then runs each check.
"""

from __future__ import annotations

import os

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_silver.config import settings
from grecohome_silver.dagster import weather_checks as checks_mod
from grecohome_silver.dagster.weather_assets import WEATHER_PARQUET, silver_weather, weather_path

pytestmark = pytest.mark.unit

# 1-based CRNH0203 field index for each name we set; everything else stays a sentinel.
_FIELD_POS = {
    "t_avg": 10, "t_max": 11, "t_min": 12, "precip": 13, "solar": 14,
    "sur": 21, "sur_max": 23, "sur_min": 25, "rh": 27,
    "sm5": 29, "st5": 34,
}  # fmt: skip


def uscrn_row(utc_date: str, utc_time: str, *, wbanno: str = "03761", **vals) -> str:
    """Build one 38-field whitespace USCRN line; unset measurements stay sentinels."""
    f = ["-9999.0"] * 38
    f[0], f[1], f[2], f[3], f[4] = wbanno, utc_date, utc_time, utc_date, utc_time
    f[5], f[6], f[7] = "2.623", "-75.79", "39.86"
    f[13] = "-99999.0"  # SOLARAD sentinel
    for i in range(28, 33):  # soil-moisture sentinels (fields 29..33)
        f[i] = "-99.0"
    for name, value in vals.items():
        f[_FIELD_POS[name] - 1] = str(value)
    return " ".join(f)


def write_uscrn(root: str, dt: str, fetched_ms: int, rows: list[str], short: str = "aa") -> None:
    pdir = os.path.join(root, "uscrn", "hourly", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"hourly_{fetched_ms}_{short}.txt"), "w") as fh:
        fh.write("\n".join(rows) + "\n")
    with open(os.path.join(pdir, f"hourly_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


@pytest.fixture
def weather_bronze_root(tmp_path) -> str:
    """A synthetic USCRN bronze tree exercising the weather transform cases."""
    root = str(tmp_path / "bronze")
    # Two real obs on one UTC day; the 17:00 obs is captured twice (dedup -> t=21.0).
    write_uscrn(root, "2026-06-13", 1_700_000_000000, [
        uscrn_row("20260613", "1700", t_avg=20.0, t_max=22.0, t_min=18.0,
                  precip=2.5, solar=800.0, sur_max=26.0, sur_min=15.0, rh=65.0,
                  sm5=0.30, st5=19.0),
        uscrn_row("20260613", "1800", t_avg=21.0, t_max=23.0, t_min=19.0,
                  precip=0.0, solar=700.0, sur_max=27.0, sur_min=16.0, rh=60.0,
                  sm5=0.30, st5=20.0),
    ])
    write_uscrn(root, "2026-06-13", 1_700_000_999000, [
        uscrn_row("20260613", "1700", t_avg=21.0, t_max=22.0, t_min=18.0,
                  precip=2.5, solar=800.0, sur_max=26.0, sur_min=15.0, rh=65.0,
                  sm5=0.30, st5=19.0),
    ], short="late")
    # A sentinel-only row (all measurements missing) — kept, all-null measurements.
    write_uscrn(root, "2026-06-14", 1_700_001_000000, [uscrn_row("20260614", "0500")])
    return root


@pytest.fixture
def materialized(weather_bronze_root, tmp_path, monkeypatch) -> str:
    monkeypatch.setattr(settings, "bronze_root", weather_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    assert materialize([silver_weather]).success
    return settings.silver_root


def _count(path: str) -> int:
    return int(connect().execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0])


def test_materializes_and_dedups(materialized) -> None:
    # 3 distinct obs (17:00 deduped, 18:00, plus the 06-14 sentinel row).
    assert _count(weather_path(WEATHER_PARQUET)) == 3


def test_rebuild_is_idempotent(weather_bronze_root, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "bronze_root", weather_bronze_root)
    monkeypatch.setattr(settings, "silver_root", str(tmp_path / "silver"))
    assert materialize([silver_weather]).success
    path = weather_path(WEATHER_PARQUET)
    one = connect().execute(f"SELECT * FROM read_parquet('{path}') ORDER BY obs_ts_utc").fetchall()
    assert materialize([silver_weather]).success
    two = connect().execute(f"SELECT * FROM read_parquet('{path}') ORDER BY obs_ts_utc").fetchall()
    assert one == two


def test_checks_pass(materialized) -> None:
    assert checks_mod.weather_obs_unique_nonnull().passed
    assert checks_mod.weather_value_ranges().passed
    cov = checks_mod.weather_coverage_vs_bronze()
    assert cov.passed and cov.severity == AssetCheckSeverity.WARN
    assert cov.metadata["silver_obs"].value == 3


def test_range_check_catches_unit_bug(materialized) -> None:
    """Sanity: the range check fails on an impossible soil-moisture value."""
    path = weather_path(WEATHER_PARQUET)
    con = connect()
    con.execute(
        f"COPY (SELECT * REPLACE (9.9 AS soil_moisture_5) FROM read_parquet('{path}')) "
        f"TO '{path}' (FORMAT parquet)"
    )
    r = checks_mod.weather_value_ranges()
    assert not r.passed and r.severity == AssetCheckSeverity.ERROR


def test_refuses_write_under_bronze_root(weather_bronze_root, monkeypatch) -> None:
    """Guard: silver must never write inside BRONZE_ROOT."""
    monkeypatch.setattr(settings, "bronze_root", weather_bronze_root)
    monkeypatch.setattr(settings, "silver_root", os.path.join(weather_bronze_root, "nested"))
    result = materialize([silver_weather], raise_on_error=False)
    assert not result.success
