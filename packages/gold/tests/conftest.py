"""Synthetic silver-Parquet fixtures for the gold daily-wellness mart tests.

Writes tiny silver Parquets (sleep, recovery, workouts, strain, daily, body, Whoop
workouts) covering the cases the mart must handle: a 1:1 sleep join, two recoveries on one
date (dedup to latest), multiple workouts per day (aggregate), sparse weigh-ins carried
forward, and days a source is missing from.
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
    """A synthetic SILVER_ROOT; the spine spans 2026-01-01..2026-01-05."""
    root = str(tmp_path / "silver")
    con = connect()

    _copy(con, """
        SELECT * FROM (VALUES
            (DATE '2026-01-01', 80, 420.0, 55, 86.0, 88.0),
            (DATE '2026-01-02', 85, 400.0, 58, 70.0, 82.0),
            (DATE '2026-01-03', NULL, 390.0, 56, NULL, NULL),
            (DATE '2026-01-05', 78, 410.0, 57, 75.0, 85.0)
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

    # Strain joins to the recovery cycle: 101 (2026-01-01) and 103 (2026-01-02, the kept
    # recovery). kilojoules / 4.184 -> 2000 / 2500 kcal.
    _copy(con, """
        SELECT * FROM (VALUES
            (101::BIGINT, DATE '2026-01-01', 10.0, 8368.0, 120, 160),
            (103::BIGINT, DATE '2026-01-02', 14.0, 10460.0, 130, 170)
        ) AS t(cycle_id, strain_date, day_strain, kilojoules, avg_heart_rate, max_heart_rate)
    """, os.path.join(root, "strain", "silver_strain.parquet"))

    # Daily activity on 2026-01-01 and 2026-01-03 (1:1 on the day).
    _copy(con, """
        SELECT * FROM (VALUES
            (DATE '2026-01-01', 9000, 600.0, 6500.0, 10.0, 30, 5, 35, 95, 20),
            (DATE '2026-01-03', 12000, 700.0, 8000.0, 12.0, 40, 10, 30, 90, 15)
        ) AS t(activity_date, total_steps, active_kilocalories, total_distance_m,
               floors_ascended, moderate_intensity_min, vigorous_intensity_min,
               avg_stress_level, body_battery_high, body_battery_low)
    """, os.path.join(root, "daily", "silver_daily.parquet"))

    # Sparse weigh-ins: 2025-12-20 (80 kg, before the spine -> carries to 01-01/02) and
    # 2026-01-03 (81 kg -> carries to 01-03/04/05).
    _copy(con, """
        SELECT * FROM (VALUES
            (DATE '2025-12-20', TIMESTAMP '2025-12-20 13:00:00', 80.0::DOUBLE, 26.0::DOUBLE, 22.0::DOUBLE),
            (DATE '2026-01-03', TIMESTAMP '2026-01-03 13:00:00', 81.0::DOUBLE, 26.3::DOUBLE, 22.5::DOUBLE)
        ) AS t(measured_date, measured_ts_utc, weight_kg, bmi, body_fat_pct)
    """, os.path.join(root, "body", "silver_body.parquet"))

    # Whoop workouts on 2026-01-02: two sessions (30 + 45 min, strain 8 and 12).
    _copy(con, """
        SELECT * FROM (VALUES
            (DATE '2026-01-02', TIMESTAMP '2026-01-02 14:00:00', TIMESTAMP '2026-01-02 14:30:00', 8.0),
            (DATE '2026-01-02', TIMESTAMP '2026-01-02 18:00:00', TIMESTAMP '2026-01-02 18:45:00', 12.0)
        ) AS t(workout_date, start_ts, end_ts, strain)
    """, os.path.join(root, "whoop_workouts", "silver_whoop_workouts.parquet"))

    return root
