"""Transform-level tests for silver workout splits over a synthetic Garmin tree."""

from __future__ import annotations

import json
import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.workout_splits import bronze_lap_count_sql, splits_sql

pytestmark = pytest.mark.unit


def _lap(idx: int, *, distance=1000.0, avg_hr=150, dur=300.0) -> dict:
    return {
        "lapIndex": idx, "startTimeGMT": "2024-01-01T12:00:00.0",
        "duration": dur, "movingDuration": dur - 5, "distance": distance,
        "averageSpeed": 3.33, "maxSpeed": 4.0, "averageHR": avg_hr, "maxHR": avg_hr + 15,
        "calories": 50.0, "elevationGain": 5.0, "elevationLoss": 3.0,
    }


def _write(root: str, activity_id: int, fetched_ms: int, laps: list[dict],
           short: str = "aa") -> None:
    pdir = os.path.join(root, "garmin", "activity_splits", "dt=2024-01-01")
    os.makedirs(pdir, exist_ok=True)
    payload = {"activityId": activity_id, "lapDTOs": laps, "eventDTOs": []}
    with open(os.path.join(pdir, f"activity_splits_{fetched_ms}_{short}.json"), "w") as fh:
        json.dump(payload, fh)
    with open(os.path.join(pdir, f"activity_splits_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _rows(root: str) -> dict[tuple, dict]:
    files = list_payload_files(root, "garmin", "activity_splits")
    con = connect()
    cur = con.execute(splits_sql(files))
    cols = [d[0] for d in cur.description]
    out = {}
    for r in cur.fetchall():
        d = dict(zip(cols, r, strict=True))
        out[(d["activity_id"], d["lap_index"])] = d
    return out


def test_typing_one_row_per_lap(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, 555, 1_700_000_000000, [_lap(1, distance=1000.0), _lap(2, distance=1200.0)])
    rows = _rows(root)
    assert set(rows) == {(555, 1), (555, 2)}
    a = rows[(555, 1)]
    assert a["distance_m"] == 1000.0 and a["avg_hr"] == 150 and a["max_hr"] == 165
    assert a["duration_sec"] == 300.0 and a["avg_speed_mps"] == 3.33
    assert a["lap_start_gmt"].isoformat() == "2024-01-01T12:00:00"
    assert rows[(555, 2)]["distance_m"] == 1200.0


def test_dedup_keeps_latest_fetch(tmp_path) -> None:
    """The same activity re-pulled → dedup on (activity_id, lap_index)."""
    root = str(tmp_path / "bronze")
    _write(root, 555, 1_700_000_000000, [_lap(1, distance=1000.0)], short="early")
    _write(root, 555, 1_700_000_999000, [_lap(1, distance=1100.0)], short="late")
    rows = _rows(root)
    assert len(rows) == 1 and rows[(555, 1)]["distance_m"] == 1100.0


def test_multiple_activities(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, 555, 1_700_000_000000, [_lap(1)], short="a")
    _write(root, 777, 1_700_000_000000, [_lap(1), _lap(2)], short="b")
    rows = _rows(root)
    assert set(rows) == {(555, 1), (777, 1), (777, 2)}


def test_bronze_count_and_empty(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    assert _rows(root) == {}
    _write(root, 555, 1_700_000_000000, [_lap(1), _lap(2), _lap(3)])
    files = list_payload_files(root, "garmin", "activity_splits")
    assert int(connect().execute(bronze_lap_count_sql(files)).fetchone()[0]) == 3
