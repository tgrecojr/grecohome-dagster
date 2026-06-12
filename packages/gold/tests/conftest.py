"""Synthetic silver-Parquet fixtures for the gold daily-wellness mart tests.

Writes tiny ``silver_sleep`` / ``silver_recovery`` / ``silver_workouts`` /
``silver_glucose`` Parquets covering the cases the mart must handle: a 1:1 sleep join,
two recoveries on one date (dedup to latest), multiple workouts per day (aggregate), a
glucose day with a known time-in-range split, and a day with no data (gap).
"""

from __future__ import annotations

# ruff: noqa: E501  (inline SQL VALUES rows read clearer on one line)
import os

import pytest

from grecohome_core.silver import connect


def _copy(con, sql: str, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")


@pytest.fixture
def silver_root(tmp_path) -> str:
    """A synthetic SILVER_ROOT with the four tables; spine spans 2026-01-01..2026-01-05."""
    root = str(tmp_path / "silver")
    con = connect()

    _copy(con, """
        SELECT * FROM (VALUES
            (DATE '2026-01-01', 80, 420.0, 55, 86.0, 88.0),
            (DATE '2026-01-02', 85, 400.0, 58, 70.0, 82.0),
            (DATE '2026-01-03', NULL, 390.0, 56, NULL, NULL)
        ) AS t(night_date, garmin_sleep_score, garmin_total_min, garmin_rhr,
               whoop_performance_pct, whoop_efficiency_pct)
    """, os.path.join(root, "sleep", "silver_sleep.parquet"))

    # 2026-01-02 has TWO recoveries (different created_at) -> dedup keeps score 70.
    _copy(con, """
        SELECT * FROM (VALUES
            (101::BIGINT, DATE '2026-01-01', TIMESTAMP '2026-01-01 12:00:00', 55.0, 60.0, 45.0, 96.0),
            (102::BIGINT, DATE '2026-01-02', TIMESTAMP '2026-01-02 08:00:00', 40.0, 62.0, 38.0, 95.0),
            (103::BIGINT, DATE '2026-01-02', TIMESTAMP '2026-01-02 09:00:00', 70.0, 58.0, 50.0, 97.0)
        ) AS t(cycle_id, recovery_date, created_at, recovery_score,
               resting_heart_rate, hrv_rmssd_milli, spo2_percentage)
    """, os.path.join(root, "recovery", "silver_recovery.parquet"))

    # 2026-01-02: two activities (aggregate); 2026-01-04: one.
    _copy(con, """
        SELECT * FROM (VALUES
            (DATE '2026-01-02', 1800.0, 5000.0, 300.0),
            (DATE '2026-01-02', 1200.0, 3000.0, 200.0),
            (DATE '2026-01-04', 3600.0, 10000.0, 600.0)
        ) AS t(activity_date, duration_sec, distance_m, calories)
    """, os.path.join(root, "workouts", "silver_workouts.parquet"))

    # 2026-01-03: [60,100,120,200] -> TIR(70-140)=50%, below=25%, above=25%; 2026-01-05: [90,95].
    _copy(con, """
        SELECT * FROM (VALUES
            (DATE '2026-01-03', 60), (DATE '2026-01-03', 100),
            (DATE '2026-01-03', 120), (DATE '2026-01-03', 200),
            (DATE '2026-01-05', 90), (DATE '2026-01-05', 95)
        ) AS t(reading_date, mgdl)
    """, os.path.join(root, "glucose", "silver_glucose.parquet"))

    return root
