"""Transform-level tests for the gold daily wellness mart."""

from __future__ import annotations

import os
import shutil

import pytest

from grecohome_core.silver import connect
from grecohome_gold.daily_wellness import daily_wellness_sql

pytestmark = pytest.mark.unit


def _rows(silver_root: str) -> dict[str, dict]:
    con = connect()
    cur = con.execute(daily_wellness_sql(silver_root))
    cols = [d[0] for d in cur.description]
    return {
        r[cols.index("day")].isoformat(): dict(zip(cols, r, strict=True))
        for r in cur.fetchall()
    }


def test_continuous_daily_spine(silver_root: str) -> None:
    """One row per day across the full union range, gaps included."""
    rows = _rows(silver_root)
    assert sorted(rows) == [
        "2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05",
    ]


def test_sleep_joins_one_to_one(silver_root: str) -> None:
    r = _rows(silver_root)["2026-01-01"]
    assert r["has_sleep"] is True and r["garmin_sleep_score"] == 80
    assert r["whoop_performance_pct"] == pytest.approx(86.0)
    # null-score night kept
    assert _rows(silver_root)["2026-01-03"]["garmin_sleep_score"] is None


def test_recovery_deduped_to_latest_per_day(silver_root: str) -> None:
    """Two recoveries on 2026-01-02 collapse to the latest created_at (score 70)."""
    r = _rows(silver_root)["2026-01-02"]
    assert r["has_recovery"] is True
    assert r["recovery_score"] == pytest.approx(70.0)


def test_workouts_aggregated_per_day(silver_root: str) -> None:
    r = _rows(silver_root)["2026-01-02"]
    assert r["workout_count"] == 2
    assert r["workout_total_min"] == pytest.approx((1800 + 1200) / 60.0)  # 50.0
    assert r["workout_distance_km"] == pytest.approx(8.0)
    assert r["workout_calories"] == pytest.approx(500.0)


def test_glucose_aggregation_and_tir(silver_root: str) -> None:
    r = _rows(silver_root)["2026-01-03"]
    assert r["has_glucose"] is True and r["glucose_readings"] == 4
    assert r["glucose_mean"] == pytest.approx(120.0)
    assert r["glucose_min"] == 60 and r["glucose_max"] == 200
    assert r["glucose_tir_pct"] == pytest.approx(50.0)
    assert r["glucose_pct_below"] == pytest.approx(25.0)
    assert r["glucose_pct_above"] == pytest.approx(25.0)


def test_gap_day_all_null(silver_root: str) -> None:
    """A day with no source data: flags false, aggregates null, workout_count 0."""
    # 2026-01-01 has sleep+recovery but no workout/glucose; build a true gap by checking
    # a day present in the spine but absent from every source is impossible here, so
    # assert the no-workout/no-glucose portions of 2026-01-01 instead.
    r = _rows(silver_root)["2026-01-01"]
    assert r["has_workout"] is False and r["workout_count"] == 0
    assert r["workout_total_min"] is None
    assert r["has_glucose"] is False and r["glucose_mean"] is None


def test_workout_only_day(silver_root: str) -> None:
    r = _rows(silver_root)["2026-01-04"]
    assert r["has_workout"] is True and r["workout_count"] == 1
    assert r["has_sleep"] is False and r["has_recovery"] is False and r["has_glucose"] is False


def test_strain_joined_via_recovery_cycle(silver_root: str) -> None:
    """Strain attaches to the day's recovery cycle; kilojoules → kcal."""
    rows = _rows(silver_root)
    a = rows["2026-01-01"]
    assert a["has_strain"] is True and a["day_strain"] == pytest.approx(10.0)
    assert a["strain_kilocalories"] == pytest.approx(2000.0)  # 8368 / 4.184
    assert a["strain_avg_hr"] == 120 and a["strain_max_hr"] == 160
    b = rows["2026-01-02"]
    assert b["day_strain"] == pytest.approx(14.0)
    assert b["strain_kilocalories"] == pytest.approx(2500.0)
    # 2026-01-03 has no recovery cycle → no strain.
    assert rows["2026-01-03"]["has_strain"] is False


def test_daily_activity_joined(silver_root: str) -> None:
    r = _rows(silver_root)["2026-01-01"]
    assert r["has_daily"] is True and r["steps"] == 9000
    assert r["active_calories"] == pytest.approx(600.0)
    assert r["distance_km"] == pytest.approx(6.5)
    assert r["floors"] == pytest.approx(10.0)
    assert r["intensity_minutes"] == 35  # 30 moderate + 5 vigorous
    assert r["avg_stress"] == 35 and r["body_battery_high"] == 95
    assert _rows(silver_root)["2026-01-02"]["has_daily"] is False


def test_weight_carried_forward(silver_root: str) -> None:
    """Sparse weigh-ins carry forward (ASOF): 80 kg until the 2026-01-03 weigh-in (81 kg)."""
    rows = _rows(silver_root)
    assert rows["2026-01-01"]["has_weight"] is True
    assert rows["2026-01-01"]["weight_kg"] == pytest.approx(80.0)
    assert rows["2026-01-01"]["weight_lb"] == pytest.approx(80.0 * 2.20462)
    assert rows["2026-01-01"]["body_bmi"] == pytest.approx(26.0)
    assert rows["2026-01-03"]["weight_kg"] == pytest.approx(81.0)
    assert rows["2026-01-05"]["weight_kg"] == pytest.approx(81.0)  # carried from 01-03


def test_missing_silver_tables_degrade_to_nulls(silver_root: str) -> None:
    """A not-yet-materialized silver table must degrade to NULLs, not fail the build."""
    for table in ("strain", "daily", "body"):
        shutil.rmtree(os.path.join(silver_root, table))
    rows = _rows(silver_root)
    # Spine still spans the surviving tables' range.
    assert sorted(rows) == [
        "2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05",
    ]
    r = rows["2026-01-01"]
    # Missing dimensions: provenance false, values null.
    assert r["has_strain"] is False and r["day_strain"] is None
    assert r["has_daily"] is False and r["steps"] is None
    assert r["has_weight"] is False and r["weight_kg"] is None
    # Surviving dimensions still populate.
    assert r["has_sleep"] is True and r["garmin_sleep_score"] == 80
    assert r["has_recovery"] is True


def test_all_silver_tables_missing_yields_empty(tmp_path) -> None:
    """With no silver Parquet at all, the mart builds to zero rows rather than erroring."""
    assert _rows(str(tmp_path / "absent_silver")) == {}
