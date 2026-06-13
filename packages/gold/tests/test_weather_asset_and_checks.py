"""Asset materialization + asset-check tests for the gold daily weather mart."""

from __future__ import annotations

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_gold.config import settings
from grecohome_gold.dagster import weather_checks as checks_mod
from grecohome_gold.dagster.weather_assets import (
    WEATHER_PARQUET,
    gold_daily_weather,
    gold_weather_path,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def materialized(weather_silver_root, tmp_path, monkeypatch) -> str:
    monkeypatch.setattr(settings, "silver_root", weather_silver_root)
    monkeypatch.setattr(settings, "gold_root", str(tmp_path / "gold"))
    assert materialize([gold_daily_weather]).success
    return settings.gold_root


def test_materializes_to_parquet(materialized) -> None:
    n = int(connect().execute(
        f"SELECT count(*) FROM read_parquet('{gold_weather_path(WEATHER_PARQUET)}')"
    ).fetchone()[0])
    assert n == 3  # 2026-04-20, the 04-21 gap, 04-22


def test_rebuild_idempotent(weather_silver_root, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "silver_root", weather_silver_root)
    monkeypatch.setattr(settings, "gold_root", str(tmp_path / "gold"))
    path = gold_weather_path(WEATHER_PARQUET)
    assert materialize([gold_daily_weather]).success
    one = connect().execute(f"SELECT * FROM read_parquet('{path}') ORDER BY day").fetchall()
    assert materialize([gold_daily_weather]).success
    two = connect().execute(f"SELECT * FROM read_parquet('{path}') ORDER BY day").fetchall()
    assert one == two


def test_checks_pass(materialized) -> None:
    assert checks_mod.weather_day_unique_nonnull().passed
    assert checks_mod.weather_value_ranges().passed
    cov = checks_mod.weather_coverage()
    assert cov.passed and cov.severity == AssetCheckSeverity.WARN
    assert cov.metadata["days_with_weather"].value == 2  # 04-20 and 04-22
    assert cov.metadata["frost_days"].value == 1  # 04-20


def test_refuses_write_under_silver_root(weather_silver_root, monkeypatch) -> None:
    """Guard: gold must never write inside SILVER_ROOT."""
    monkeypatch.setattr(settings, "silver_root", weather_silver_root)
    monkeypatch.setattr(settings, "gold_root", weather_silver_root + "/nested")
    result = materialize([gold_daily_weather], raise_on_error=False)
    assert not result.success
