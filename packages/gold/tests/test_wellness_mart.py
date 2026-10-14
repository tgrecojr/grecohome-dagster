"""Wellness mart behaviour added under LID: Whoop workouts, weigh-in ties, provenance."""

from __future__ import annotations

import os

import pytest

from grecohome_core.silver import connect
from grecohome_gold.daily_wellness import daily_wellness_sql

pytestmark = pytest.mark.unit


def _rows(silver_root: str) -> dict[str, dict]:
    cur = connect().execute(daily_wellness_sql(silver_root))
    cols = [d[0] for d in cur.description]
    return {r[0].isoformat(): dict(zip(cols, r, strict=True)) for r in cur.fetchall()}


def _replace(silver_root: str, parts: tuple[str, ...], select: str) -> None:
    path = os.path.join(silver_root, *parts)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    connect().execute(f"COPY ({select}) TO '{path}' (FORMAT parquet)")


# @spec GOLD-WELLNESS-MART-006, GOLD-WELLNESS-MART-009
def test_whoop_workouts_aggregated_separately(silver_root: str) -> None:
    rows = _rows(silver_root)
    r = rows["2026-01-02"]
    assert r["whoop_workout_count"] == 2
    assert r["whoop_workout_min"] == pytest.approx(75.0)
    assert r["whoop_workout_strain"] == pytest.approx(12.0)
    assert r["has_whoop_workout"] is True
    assert r["workout_count"] == 2  # Garmin stays its own column
    other = rows["2026-01-03"]
    assert other["whoop_workout_count"] == 0 and other["has_whoop_workout"] is False
    assert other["whoop_workout_min"] is None


# @spec GOLD-WELLNESS-MART-001
def test_whoop_workout_extends_the_spine(silver_root: str) -> None:
    _replace(silver_root, ("whoop_workouts", "silver_whoop_workouts.parquet"), """
        SELECT * FROM (VALUES (DATE '2026-01-07', TIMESTAMP '2026-01-07 14:00:00',
                               TIMESTAMP '2026-01-07 15:00:00', 9.0))
        AS t(workout_date, start_ts, end_ts, strain)""")
    rows = _rows(silver_root)
    assert max(rows) == "2026-01-07" and "2026-01-06" in rows
    assert rows["2026-01-07"]["has_whoop_workout"] is True


# @spec GOLD-WELLNESS-MART-002
def test_missing_whoop_workouts_degrade_to_zero_and_false(silver_root: str) -> None:
    os.remove(os.path.join(silver_root, "whoop_workouts", "silver_whoop_workouts.parquet"))
    r = _rows(silver_root)["2026-01-02"]
    assert r["whoop_workout_count"] == 0 and r["has_whoop_workout"] is False


# @spec GOLD-WELLNESS-MART-008
def test_two_weigh_ins_on_one_day_take_the_later(silver_root: str) -> None:
    _replace(silver_root, ("body", "silver_body.parquet"), """
        SELECT d AS measured_date, ts AS measured_ts_utc, w::DOUBLE AS weight_kg,
               b::DOUBLE AS bmi, f::DOUBLE AS body_fat_pct
        FROM (VALUES
            (DATE '2026-01-02', TIMESTAMP '2026-01-02 13:21:01', 82.5, 26.4, 22.4),
            (DATE '2026-01-02', TIMESTAMP '2026-01-02 13:21:02', 81.7, 26.1, 22.0)
        ) AS t(d, ts, w, b, f)""")
    rows = _rows(silver_root)
    assert rows["2026-01-02"]["weight_kg"] == pytest.approx(81.7)
    assert rows["2026-01-05"]["weight_kg"] == pytest.approx(81.7)  # carried forward
    assert rows["2026-01-01"]["has_weight"] is False  # nothing on or before 01-01


# @spec GOLD-WELLNESS-MART-009
def test_provenance_flags_mark_source_rows(silver_root: str) -> None:
    r = _rows(silver_root)["2026-01-03"]
    assert r["has_sleep"] is True and r["garmin_sleep_score"] is None  # row exists, value null
    assert r["has_glucose"] is True and r["has_daily"] is True
    assert r["has_recovery"] is False and r["has_strain"] is False
