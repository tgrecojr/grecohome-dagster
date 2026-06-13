"""Transform-level tests for silver fitness over a synthetic 3-collection Garmin tree."""

from __future__ import annotations

import json
import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.fitness import fitness_sql

pytestmark = pytest.mark.unit


def _write(root: str, coll: str, dt: str, fetched_ms: int, obj, short: str = "aa") -> None:
    pdir = os.path.join(root, "garmin", coll, f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, f"{coll}_{fetched_ms}_{short}.json"), "w") as fh:
        json.dump(obj, fh)
    with open(os.path.join(pdir, f"{coll}_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _max_metrics(vo2_run=None, vo2_cyc=None) -> list:
    return [{"generic": {"vo2MaxValue": vo2_run}, "cycling": {"vo2MaxValue": vo2_cyc}}]


def _training_status(code=3, load=250, phrase="MAINTAINING_1") -> dict:
    return {"mostRecentTrainingStatus": {"latestTrainingStatusData": {
        "3381453277": {"trainingStatus": code, "weeklyTrainingLoad": load,
                       "trainingStatusFeedbackPhrase": phrase}}}}


def _race(t5k=1541, t10k=3200, half=7100, mara=15000) -> dict:
    return {"time5K": t5k, "time10K": t10k, "timeHalfMarathon": half, "timeMarathon": mara}


def _rows(root: str) -> dict[str, dict]:
    mm = list_payload_files(root, "garmin", "max_metrics")
    ts = list_payload_files(root, "garmin", "training_status")
    rp = list_payload_files(root, "garmin", "race_predictions")
    con = connect()
    cur = con.execute(fitness_sql(mm, ts, rp))
    cols = [d[0] for d in cur.description]
    return {
        r[cols.index("snapshot_date")].isoformat(): dict(zip(cols, r, strict=True))
        for r in cur.fetchall()
    }


def test_typing_and_snapshot_day_from_dt(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "max_metrics", "2026-06-06", 1_700_000_000000, _max_metrics(vo2_run=48.0))
    _write(root, "training_status", "2026-06-06", 1_700_000_000000, _training_status(3, 250))
    _write(root, "race_predictions", "2026-06-06", 1_700_000_000000, _race(t5k=1541))
    r = _rows(root)["2026-06-06"]
    assert r["vo2max_running"] == 48.0 and r["vo2max_cycling"] is None
    assert r["training_status_code"] == 3 and r["weekly_training_load"] == 250
    assert r["training_status_phrase"] == "MAINTAINING_1"
    assert r["race_5k_sec"] == 1541 and r["race_marathon_sec"] == 15000


def test_dedup_latest_capture_per_day(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "max_metrics", "2026-06-06", 1_700_000_000000, _max_metrics(vo2_run=48.0),
           short="early")
    _write(root, "max_metrics", "2026-06-06", 1_700_000_999000, _max_metrics(vo2_run=52.0),
           short="late")
    assert _rows(root)["2026-06-06"]["vo2max_running"] == 52.0


def test_spine_union_across_collections(tmp_path) -> None:
    """A day present in only one collection still yields a row (others null)."""
    root = str(tmp_path / "bronze")
    _write(root, "max_metrics", "2026-06-06", 1_700_000_000000, _max_metrics(vo2_run=50.0))
    _write(root, "race_predictions", "2026-06-07", 1_700_000_000000, _race(t5k=1500))
    rows = _rows(root)
    assert set(rows) == {"2026-06-06", "2026-06-07"}
    a, b = rows["2026-06-06"], rows["2026-06-07"]
    assert a["vo2max_running"] == 50.0 and a["race_5k_sec"] is None
    assert b["race_5k_sec"] == 1500 and b["vo2max_running"] is None


def test_empty_recapture_does_not_clobber_vo2max(tmp_path) -> None:
    """Most max_metrics captures are empty []; a later empty one must not null a real value."""
    root = str(tmp_path / "bronze")
    _write(root, "max_metrics", "2026-06-06", 1_700_000_000000, _max_metrics(vo2_run=48.0),
           short="valued")
    _write(root, "max_metrics", "2026-06-06", 1_700_000_999000, [], short="empty_later")
    assert _rows(root)["2026-06-06"]["vo2max_running"] == 48.0


def test_empty_yields_no_rows(tmp_path) -> None:
    assert _rows(str(tmp_path / "bronze")) == {}
