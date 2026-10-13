"""Transform-level tests for silver Whoop workouts over a synthetic tree."""

from __future__ import annotations

import json
import os

import duckdb
import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.whoop_workouts import bronze_workout_count_sql, whoop_workouts_sql

pytestmark = pytest.mark.unit


def _workout(wid, start, updated_at, *, offset="-05:00", sport="weightlifting", sport_id=45,
             strain=10.5, kj=900.0, avg_hr=120, distance=None) -> dict:
    score = {"strain": strain, "kilojoule": kj, "average_heart_rate": avg_hr,
             "max_heart_rate": avg_hr + 25}
    if distance is not None:
        score["distance_meter"] = distance
    return {
        "id": wid, "start": start, "end": start.replace("T1", "T2"),
        "timezone_offset": offset, "sport_name": sport, "sport_id": sport_id,
        "created_at": updated_at, "updated_at": updated_at, "score_state": "SCORED",
        "score": score,
    }


def _write(root: str, dt: str, fetched_ms: int, records: list[dict], short: str = "aa") -> None:
    pdir = os.path.join(root, "whoop", "workout", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"workout_{fetched_ms}_{short}.json"), "w") as fh:
        json.dump({"records": records}, fh)
    with open(os.path.join(pdir, f"workout_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _rows(root: str) -> dict[str, dict]:
    files = list_payload_files(root, "whoop", "workout")
    con = connect()
    cur = con.execute(whoop_workouts_sql(files))
    cols = [d[0] for d in cur.description]
    return {r[cols.index("workout_id")]: dict(zip(cols, r, strict=True)) for r in cur.fetchall()}


# @spec SILVER-WORKOUTS-WHOOP-001, SILVER-WORKOUTS-WHOOP-005
def test_typing_and_local_workout_date(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-02-09", 1_700_000_000000, [
        _workout("w-1", "2026-02-09T15:00:00.000Z", "2026-02-09T16:00:00.000Z",
                 sport="running", strain=8.4, distance=5000.0),
    ])
    r = _rows(root)["w-1"]
    assert r["sport_name"] == "running" and r["strain"] == 8.4
    assert r["distance_m"] == 5000.0 and r["avg_heart_rate"] == 120
    # 15:00Z − 5h = 10:00 local → 2026-02-09.
    assert r["workout_date"].isoformat() == "2026-02-09"


# @spec SILVER-WORKOUTS-WHOOP-003
def test_dedup_keeps_latest_rescore(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-02-09", 1_700_000_000000,
           [_workout("w-2", "2026-02-09T15:00:00.000Z", "2026-02-09T16:00:00.000Z", strain=9.0)],
           short="v1")
    _write(root, "2026-02-09", 1_700_000_999000,
           [_workout("w-2", "2026-02-09T15:00:00.000Z", "2026-02-09T20:00:00.000Z", strain=11.0)],
           short="v2")
    rows = _rows(root)
    assert len(rows) == 1 and rows["w-2"]["strain"] == 11.0


# @spec SILVER-WORKOUTS-WHOOP-001
def test_non_gps_workout_kept_with_null_distance(tmp_path) -> None:
    """Lifting/yard-work have no distance — kept, distance null."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-05-07", 1_700_000_000000, [
        _workout("w-3", "2026-05-07T15:00:00.000Z", "2026-05-07T16:00:00.000Z",
                 sport="weightlifting", distance=None),
    ])
    r = _rows(root)["w-3"]
    assert r["sport_name"] == "weightlifting" and r["distance_m"] is None


# @spec SILVER-WORKOUTS-WHOOP-001
def test_bronze_count_and_empty(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    assert _rows(root) == {}
    _write(root, "2026-02-09", 1_700_000_000000, [
        _workout("a", "2026-02-09T15:00:00.000Z", "2026-02-09T16:00:00.000Z"),
        _workout("b", "2026-02-10T15:00:00.000Z", "2026-02-10T16:00:00.000Z"),
    ])
    files = list_payload_files(root, "whoop", "workout")
    assert int(connect().execute(bronze_workout_count_sql(files)).fetchone()[0]) == 2


def _write_raw(root: str, payload) -> None:
    pdir = os.path.join(root, "whoop", "workout", "dt=2026-01-15")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, "workout_1700000000000_raw.json"), "w") as fh:
        json.dump(payload, fh)


# @spec SILVER-WORKOUTS-WHOOP-001
def test_payload_without_records_contributes_nothing(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_100000, [_workout("w1", "2026-01-15T15:00:00.000Z", "u")])
    _write_raw(root, {"next_token": None})
    assert list(_rows(root)) == ["w1"]


# @spec SILVER-WORKOUTS-WHOOP-002
@pytest.mark.parametrize("records", [{"id": "w"}, "oops", 5])
def test_non_array_records_fails_the_rebuild(tmp_path, records) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_100000, [_workout("w1", "2026-01-15T15:00:00.000Z", "u")])
    _write_raw(root, {"records": records})
    with pytest.raises(duckdb.Error):
        _rows(root)


# @spec SILVER-WORKOUTS-WHOOP-004
def test_record_without_id_dropped(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    base = _workout("w1", "2026-01-15T15:00:00.000Z", "u")
    no_id = {k: v for k, v in base.items() if k != "id"}
    _write(root, "2026-01-15", 1_700_000_100000, [base, no_id, {**base, "id": None}])
    assert list(_rows(root)) == ["w1"]


# @spec SILVER-WORKOUTS-WHOOP-005
@pytest.mark.parametrize("offset", [None, "garbage"])
def test_workout_date_falls_back_to_utc_date(tmp_path, offset) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-01-15", 1_700_000_100000,
           [_workout("w1", "2026-01-15T02:00:00.000Z", "u", offset=offset)])
    assert _rows(root)["w1"]["workout_date"].isoformat() == "2026-01-15"
