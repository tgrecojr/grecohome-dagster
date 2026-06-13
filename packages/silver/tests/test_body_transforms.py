"""Transform-level tests for silver body over a synthetic Garmin weigh-in tree."""

from __future__ import annotations

import json
import os

import pytest

from grecohome_core.silver import connect, list_payload_files
from grecohome_silver.body import body_sql, bronze_weighin_count_sql

pytestmark = pytest.mark.unit


def _weighin(pk: int, cal_date: str, *, weight_g=80000, bmi=27.0, body_fat=22.5,
             muscle_g=33000, ts_gmt=1700000000000) -> dict:
    return {
        "samplePk": pk, "calendarDate": cal_date, "date": ts_gmt, "timestampGMT": ts_gmt,
        "weight": weight_g, "bmi": bmi, "bodyFat": body_fat, "bodyWater": 55.0,
        "boneMass": 3200, "muscleMass": muscle_g, "physiqueRating": 5,
        "visceralFat": 8, "metabolicAge": 35, "sourceType": "INDEX_SCALE",
        "weightDelta": -500,
    }


def _write(root: str, dt: str, fetched_ms: int, weigh_ins: list[dict], short: str = "aa") -> None:
    pdir = os.path.join(root, "garmin", "daily_weigh_ins", f"dt={dt}")
    os.makedirs(pdir, exist_ok=True)
    payload = {"startDate": dt, "endDate": dt, "dateWeightList": weigh_ins, "totalAverage": {}}
    with open(os.path.join(pdir, f"daily_weigh_ins_{fetched_ms}_{short}.json"), "w") as fh:
        json.dump(payload, fh)
    with open(os.path.join(pdir, f"daily_weigh_ins_{fetched_ms}_{short}.meta.json"), "w") as fh:
        fh.write('{"sha256": "x"}')


def _rows(root: str) -> list[dict]:
    files = list_payload_files(root, "garmin", "daily_weigh_ins")
    con = connect()
    cur = con.execute(body_sql(files))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def test_typing_and_kg_units(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-03-30", 1_700_000_000000, [_weighin(111, "2026-03-30", weight_g=80000)])
    (r,) = _rows(root)
    assert r["sample_pk"] == 111
    assert r["measured_date"].isoformat() == "2026-03-30"
    assert r["weight_kg"] == 80.0  # 80000 g / 1000
    assert r["muscle_mass_kg"] == 33.0 and r["bone_mass_kg"] == 3.2
    assert r["bmi"] == 27.0 and r["body_fat_pct"] == 22.5
    assert r["source_type"] == "INDEX_SCALE" and r["measured_ts_utc"] is not None


def test_dedup_keeps_latest_fetch(tmp_path) -> None:
    """The same weigh-in recurs across the trailing window → dedup on sample_pk."""
    root = str(tmp_path / "bronze")
    _write(root, "2026-03-30", 1_700_000_000000, [_weighin(222, "2026-03-30", weight_g=80000)],
           short="a")
    _write(root, "2026-03-31", 1_700_000_999000, [_weighin(222, "2026-03-30", weight_g=81000)],
           short="b")
    rows = _rows(root)
    assert len(rows) == 1 and rows[0]["weight_kg"] == 81.0


def test_empty_list_and_empty_collection(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    assert _rows(root) == []  # not captured
    _write(root, "2026-03-30", 1_700_000_000000, [])  # a day with no weigh-in
    assert _rows(root) == []


def test_bronze_count(tmp_path) -> None:
    root = str(tmp_path / "bronze")
    _write(root, "2026-03-30", 1_700_000_000000,
           [_weighin(1, "2026-03-29"), _weighin(2, "2026-03-30")])
    files = list_payload_files(root, "garmin", "daily_weigh_ins")
    assert int(connect().execute(bronze_weighin_count_sql(files)).fetchone()[0]) == 2
