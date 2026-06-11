"""Transform-level tests for silver workouts over a synthetic Garmin activities tree."""

from __future__ import annotations

import json
import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.workouts import bronze_activity_count_sql, workouts_sql

pytestmark = pytest.mark.unit


def _activity(activity_id: int, *, type_key="running", start_local="2026-01-15 06:30:00",
              start_gmt="2026-01-15 11:30:00", duration=1800.0, distance=5000.0,
              avg_hr=140, max_hr=165) -> dict:
    return {
        "activityId": activity_id,
        "activityName": "Morning Run",
        "activityType": {"typeId": 1, "typeKey": type_key, "parentTypeId": 17},
        "startTimeLocal": start_local,
        "startTimeGMT": start_gmt,
        "duration": duration,
        "movingDuration": duration,
        "elapsedDuration": duration,
        "distance": distance,
        "calories": 300.0,
        "averageHR": avg_hr,
        "maxHR": max_hr,
        "hrTimeInZone_1": 120.0,
        "deviceId": 99887766,
    }


def _write(root: str, dt: str, fetched_ms: int, activities: list[dict], short="aa") -> None:
    pdir = os.path.join(root, "garmin", "activities", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"activities_{fetched_ms}_{short}.json"), "w") as fh:
        json.dump(activities, fh)  # top-level JSON array
    with open(os.path.join(pdir, f"activities_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _rows(root: str) -> list[dict]:
    files = list_payload_files(root, "garmin", "activities")
    con = connect()
    cur = con.execute(workouts_sql(files))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def test_workouts_typing_and_local_date(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_activity(111)])
    (r,) = _rows(root)
    assert r["activity_id"] == 111
    assert r["activity_type"] == "running"
    assert r["activity_date"].isoformat() == "2026-01-15"  # local date, not GMT
    assert r["start_time_local"].isoformat() == "2026-01-15T06:30:00"
    assert r["duration_sec"] == pytest.approx(1800.0)
    assert r["distance_m"] == pytest.approx(5000.0)
    assert r["avg_hr"] == 140 and r["max_hr"] == 165


def test_workouts_dedup_by_activity_id(tmp_path) -> None:
    """The same activity re-captured across files collapses to one row (latest fetch)."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_activity(111, distance=5000.0)])
    _write(root, "2026-01-16", 1_700_000_999000, [_activity(111, distance=5050.0)], short="bb")
    rows = _rows(root)
    assert len(rows) == 1
    assert rows[0]["distance_m"] == pytest.approx(5050.0)  # latest fetch wins


def test_workouts_empty_array_and_null_distance_kept(tmp_path) -> None:
    """Empty-day arrays contribute nothing; a no-distance activity is kept (null)."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-14", 1_700_000_000000, [])  # empty day
    mobility = _activity(222, type_key="mobility")
    del mobility["distance"]
    _write(root, "2026-01-15", 1_700_000_100000, [mobility], short="cc")
    rows = _rows(root)
    assert len(rows) == 1
    assert rows[0]["activity_id"] == 222 and rows[0]["distance_m"] is None


def test_workouts_multiple_per_file(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_activity(1), _activity(2), _activity(3)])
    assert len(_rows(root)) == 3


def test_workouts_sidecars_excluded_and_count(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_000000, [_activity(1), _activity(2)])
    files = list_payload_files(root, "garmin", "activities")
    assert all(not f.endswith(".meta.json") for f in files)
    assert int(connect().execute(bronze_activity_count_sql(files)).fetchone()[0]) == 2


def test_workouts_empty_yields_no_rows(tmp_path) -> None:
    assert _rows(str(tmp_path / "bronze")) == []
