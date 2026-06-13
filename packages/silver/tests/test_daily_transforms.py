"""Transform-level tests for silver daily-summary over a synthetic Garmin tree."""

from __future__ import annotations

import json
import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.daily import bronze_day_count_sql, daily_sql

pytestmark = pytest.mark.unit


def _summary(cal_date: str, **over) -> dict:
    obj = {
        "calendarDate": cal_date,
        "totalSteps": 9000,
        "totalDistanceMeters": 6500.0,
        "activeKilocalories": 600.0,
        "restingHeartRate": 52,
        "maxHeartRate": 150,
        "averageStressLevel": 35,
        "bodyBatteryHighestValue": 95,
        "averageSpo2": 96,
        "avgWakingRespirationValue": 14.5,
        "moderateIntensityMinutes": 30,
    }
    obj.update(over)
    return obj


def _write(root: str, dt: str, fetched_ms: int, obj: dict, short: str = "aa") -> None:
    pdir = os.path.join(root, "garmin", "user_summary", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"user_summary_{fetched_ms}_{short}.json"), "w") as fh:
        json.dump(obj, fh)
    with open(os.path.join(pdir, f"user_summary_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _rows(root: str) -> dict[str, dict]:
    files = list_payload_files(root, "garmin", "user_summary")
    con = connect()
    cur = con.execute(daily_sql(files))
    cols = [d[0] for d in cur.description]
    return {
        r[cols.index("activity_date")].isoformat(): dict(zip(cols, r, strict=True))
        for r in cur.fetchall()
    }


def test_typing_of_key_fields(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-10", 1_700_000_000000, _summary("2026-06-10"))
    r = _rows(root)["2026-06-10"]
    assert r["total_steps"] == 9000 and r["total_distance_m"] == 6500.0
    assert r["active_kilocalories"] == 600.0 and r["resting_heart_rate"] == 52
    assert r["avg_spo2"] == 96 and r["avg_waking_respiration"] == 14.5
    assert r["moderate_intensity_min"] == 30 and r["avg_stress_level"] == 35


def test_dedup_keeps_latest_fetch(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-10", 1_700_000_000000, _summary("2026-06-10", totalSteps=9000), short="a")
    _write(root, "2026-06-10", 1_700_000_999000, _summary("2026-06-10", totalSteps=12000),
           short="b")
    rows = _rows(root)
    assert len(rows) == 1 and rows["2026-06-10"]["total_steps"] == 12000


def test_stress_no_data_sentinels_become_null(tmp_path) -> None:
    """Garmin's -1/-2 'no data' stress levels map to NULL (other fields kept)."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-06-10", 1_700_000_000000,
           _summary("2026-06-10", averageStressLevel=-1, maxStressLevel=-2))
    r = _rows(root)["2026-06-10"]
    assert r["avg_stress_level"] is None and r["max_stress_level"] is None
    assert r["total_steps"] == 9000  # the rest of the day is kept


def test_bronze_count_and_empty(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    assert _rows(root) == {}
    _write(root, "2026-06-10", 1_700_000_000000, _summary("2026-06-10"))
    _write(root, "2026-06-11", 1_700_000_000000, _summary("2026-06-11"))
    files = list_payload_files(root, "garmin", "user_summary")
    assert int(connect().execute(bronze_day_count_sql(files)).fetchone()[0]) == 2
