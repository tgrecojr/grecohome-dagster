"""Transform-level tests for silver fitness over a synthetic 3-collection Garmin tree."""

from __future__ import annotations

import json
import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.fitness import fitness_sql

pytestmark = pytest.mark.unit

WATCH, EDGE = "3381453277", "3454760518"  # Forerunner 945, Edge 1040 (primary training)


def _write(root: str, coll: str, dt: str, fetched_ms: int, obj, short: str = "aa") -> None:
    pdir = os.path.join(root, "garmin", coll, f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"{coll}_{fetched_ms}_{short}.json"), "w") as fh:
        json.dump(obj, fh)
    with open(os.path.join(pdir, f"{coll}_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _max_metrics(run=None, run_date=None, cyc=None, cyc_date=None) -> list:
    return [{"generic": {"vo2MaxValue": run, "calendarDate": run_date},
             "cycling": {"vo2MaxValue": cyc, "calendarDate": cyc_date}}]


def _entry(day: str, device: str, code: int, *, primary: bool, load: int = 250) -> dict:
    return {"calendarDate": day, "deviceId": int(device), "trainingStatus": code,
            "weeklyTrainingLoad": load, "trainingStatusFeedbackPhrase": f"P{code}",
            "primaryTrainingDevice": primary}


def _status(*entries: dict) -> dict:
    return {"mostRecentTrainingStatus": {"latestTrainingStatusData": {
        str(e["deviceId"]): e for e in entries} or None}}


def _race(t5k=1541, t10k=3200, half=7100, mara=15000) -> dict:
    return {"time5K": t5k, "time10K": t10k, "timeHalfMarathon": half, "timeMarathon": mara}


def _rows(root: str) -> dict[str, dict]:
    mm = list_payload_files(root, "garmin", "max_metrics")
    ts = list_payload_files(root, "garmin", "training_status")
    rp = list_payload_files(root, "garmin", "race_predictions")
    cur = connect().execute(fitness_sql(mm, ts, rp))
    cols = [d[0] for d in cur.description]
    return {
        r[cols.index("snapshot_date")].isoformat(): dict(zip(cols, r, strict=True))
        for r in cur.fetchall()
    }


# @spec SILVER-DAILY-FITNESS-002
def test_vo2max_dated_by_payload_not_folder(tmp_path) -> None:
    """A bulk pull wrote a 2023 value into a 2026 folder: it lands on 2023."""
    root = str(tmp_path / "bronze")
    _write(root, "max_metrics", "2026-06-06", 1_700_000_000000,
           _max_metrics(run=48.0, run_date="2023-05-01"))
    rows = _rows(root)
    assert set(rows) == {"2023-05-01"} and rows["2023-05-01"]["vo2max_running"] == 48.0


# @spec SILVER-DAILY-FITNESS-002
def test_vo2max_sports_dated_separately(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "max_metrics", "2026-06-10", 1_700_000_000000,
           _max_metrics(run=50.0, run_date="2026-06-09", cyc=55.0, cyc_date="2026-06-10"))
    rows = _rows(root)
    assert rows["2026-06-09"]["vo2max_running"] == 50.0
    assert rows["2026-06-09"]["vo2max_cycling"] is None
    assert rows["2026-06-10"]["vo2max_cycling"] == 55.0


# @spec SILVER-DAILY-FITNESS-002
def test_vo2max_latest_capture_wins_and_empty_never_erases(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    day = "2026-06-10"
    _write(root, "max_metrics", day, 1_700_000_000000, _max_metrics(run=48.0, run_date=day),
           short="early")
    _write(root, "max_metrics", day, 1_700_000_500000, _max_metrics(run=52.0, run_date=day),
           short="late")
    _write(root, "max_metrics", day, 1_700_000_900000, [], short="empty")
    assert _rows(root)[day]["vo2max_running"] == 52.0


# @spec SILVER-DAILY-FITNESS-003
def test_training_status_dated_by_entry_and_carried_value_collapses(tmp_path) -> None:
    """Later captures repeat a status set earlier; it belongs to the day it took effect."""
    root = str(tmp_path / "bronze")
    for i, dt in enumerate(("2026-08-10", "2026-08-11", "2026-08-12")):
        _write(root, "training_status", dt, 1_700_000_000000 + i,
               _status(_entry("2026-08-10", WATCH, 4, primary=False)), short=f"c{i}")
    rows = _rows(root)
    assert set(rows) == {"2026-08-10"}
    assert rows["2026-08-10"]["training_status_code"] == 4
    assert rows["2026-08-10"]["training_device_id"] == int(WATCH)


# @spec SILVER-DAILY-FITNESS-003
def test_primary_training_device_wins_over_a_later_watch_capture(tmp_path) -> None:
    """Edge (primary) on a ride day beats the watch's same-day entry, even captured later."""
    root = str(tmp_path / "bronze")
    day = "2026-08-13"
    _write(root, "training_status", day, 1_700_000_000000,
           _status(_entry(day, EDGE, 3, primary=True)), short="edge")
    _write(root, "training_status", day, 1_700_000_900000,
           _status(_entry(day, WATCH, 6, primary=False)), short="watch")
    r = _rows(root)[day]
    assert r["training_status_code"] == 3 and r["training_device_id"] == int(EDGE)


# @spec SILVER-DAILY-FITNESS-003
def test_two_device_map_picks_primary_and_watch_fills_other_days(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "training_status", "2026-06-07", 1_700_000_000000,
           _status(_entry("2026-06-07", WATCH, 6, primary=False),
                   _entry("2026-06-07", EDGE, 3, primary=True)))
    _write(root, "training_status", "2026-06-08", 1_700_000_100000,
           _status(_entry("2026-06-08", WATCH, 5, primary=False)), short="w")
    rows = _rows(root)
    assert rows["2026-06-07"]["training_device_id"] == int(EDGE)
    assert rows["2026-06-08"]["training_device_id"] == int(WATCH)


# @spec SILVER-DAILY-FITNESS-003
def test_empty_training_status_map_contributes_nothing(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    day = "2026-06-08"
    _write(root, "training_status", day, 1_700_000_000000,
           _status(_entry(day, WATCH, 5, primary=False)), short="real")
    _write(root, "training_status", day, 1_700_000_900000, _status(), short="empty")
    assert _rows(root)[day]["training_status_code"] == 5


# @spec SILVER-DAILY-FITNESS-004
def test_race_predictions_dated_by_folder_latest_capture(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "race_predictions", "2026-06-07", 1_700_000_000000, _race(t5k=1600), short="a")
    _write(root, "race_predictions", "2026-06-07", 1_700_000_900000, _race(t5k=1541), short="b")
    r = _rows(root)["2026-06-07"]
    assert r["race_5k_sec"] == 1541 and r["race_marathon_sec"] == 15000


# @spec SILVER-DAILY-FITNESS-001
def test_spine_union_across_collections(tmp_path) -> None:
    """A day present in only one relation still yields a row (others null)."""
    root = str(tmp_path / "bronze")
    _write(root, "max_metrics", "2026-06-06", 1_700_000_000000,
           _max_metrics(run=50.0, run_date="2026-06-06"))
    _write(root, "race_predictions", "2026-06-07", 1_700_000_000000, _race(t5k=1500))
    rows = _rows(root)
    assert set(rows) == {"2026-06-06", "2026-06-07"}
    assert rows["2026-06-06"]["race_5k_sec"] is None
    assert rows["2026-06-07"]["vo2max_running"] is None


# @spec SILVER-DAILY-FITNESS-001
def test_empty_yields_no_rows(tmp_path) -> None:
    assert _rows(str(tmp_path / "bronze")) == {}
