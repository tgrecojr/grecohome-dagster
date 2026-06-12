"""Asset materialization + asset-check tests for the gold mart."""

from __future__ import annotations

import pytest
from dagster import AssetCheckSeverity, materialize

from grecohome_core.silver import connect
from grecohome_gold.config import settings
from grecohome_gold.dagster import checks as checks_mod
from grecohome_gold.dagster.assets import WELLNESS_PARQUET, gold_daily_wellness, gold_path

pytestmark = pytest.mark.unit


@pytest.fixture
def materialized(silver_root, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "silver_root", silver_root)
    monkeypatch.setattr(settings, "gold_root", str(tmp_path / "gold"))
    assert materialize([gold_daily_wellness]).success
    return settings.gold_root


def test_materializes_to_parquet(materialized) -> None:
    n = int(connect().execute(
        f"SELECT count(*) FROM read_parquet('{gold_path(WELLNESS_PARQUET)}')"
    ).fetchone()[0])
    assert n == 5


def test_rebuild_idempotent(silver_root, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "silver_root", silver_root)
    monkeypatch.setattr(settings, "gold_root", str(tmp_path / "gold"))
    assert materialize([gold_daily_wellness]).success
    one = connect().execute(
        f"SELECT * FROM read_parquet('{gold_path(WELLNESS_PARQUET)}') ORDER BY day"
    ).fetchall()
    assert materialize([gold_daily_wellness]).success
    two = connect().execute(
        f"SELECT * FROM read_parquet('{gold_path(WELLNESS_PARQUET)}') ORDER BY day"
    ).fetchall()
    assert one == two


def test_checks_pass(materialized) -> None:
    assert checks_mod.wellness_day_unique_nonnull().passed
    assert checks_mod.wellness_value_ranges().passed
    cov = checks_mod.wellness_coverage()
    assert cov.passed and cov.severity == AssetCheckSeverity.WARN
    assert cov.metadata["days_with_glucose"].value == 2  # 2026-01-03 and -05


def test_refuses_write_under_silver_root(silver_root, monkeypatch) -> None:
    """Guard: gold must never write inside SILVER_ROOT."""
    monkeypatch.setattr(settings, "silver_root", silver_root)
    monkeypatch.setattr(settings, "gold_root", silver_root + "/nested")
    result = materialize([gold_daily_wellness], raise_on_error=False)
    assert not result.success
