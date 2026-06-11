"""Transform-level tests: run the silver SQL directly over the synthetic bronze."""

from __future__ import annotations

import json
import os

import pytest

from grecohome_core.silver import connect, list_payload_files, payloads_relation_sql
from grecohome_silver.sleep import garmin_sleep_sql, unified_sleep_sql, whoop_sleep_sql

pytestmark = pytest.mark.unit


def _whoop_record(start: str, *, tz: str | None, sleep_id: str = "id-x") -> dict:
    rec: dict = {
        "id": sleep_id,
        "start": start,
        "end": start,
        "updated_at": "2026-01-16T12:00:00.000Z",
        "nap": False,
        "score": {"sleep_performance_percentage": 90.0},
    }
    if tz is not None:
        rec["timezone_offset"] = tz
    return rec


def _write_whoop(tmp_path, record: dict) -> str:
    """A one-file Whoop bronze tree; returns the bronze root."""
    root = os.path.join(str(tmp_path), "bronze")
    pdir = os.path.join(root, "whoop", "sleep", "dt=2026-01-16")
    os.makedirs(pdir, exist_ok=True)
    with open(os.path.join(pdir, "sleep_1700000000000_aa.json"), "w") as fh:
        json.dump({"records": [record]}, fh)
    return root


def _rows(sql: str) -> list[dict]:
    con = connect()
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _garmin_sql(bronze_root: str) -> str:
    files = list_payload_files(bronze_root, "garmin", "sleep")
    return garmin_sleep_sql(payloads_relation_sql(files))


def _whoop_sql(bronze_root: str) -> str:
    files = list_payload_files(bronze_root, "whoop", "sleep")
    return whoop_sleep_sql(payloads_relation_sql(files))


def test_sidecars_excluded(bronze_root: str) -> None:
    """list_payload_files never returns a .meta.json sidecar."""
    files = list_payload_files(bronze_root, "garmin", "sleep")
    assert files, "expected payloads"
    assert all(not f.endswith(".meta.json") for f in files)


def test_garmin_dedup_keeps_latest_fetch(bronze_root: str) -> None:
    """The same night captured twice collapses to one row — the latest fetch wins."""
    rows = {r["night_date"].isoformat(): r for r in _rows(_garmin_sql(bronze_root))}
    assert "2024-01-01" in rows
    # Two captures of 2024-01-01; the later fetch had total_s=28800 -> 480.0 min.
    assert rows["2024-01-01"]["garmin_total_min"] == pytest.approx(480.0)
    # One row per night.
    nights = [r["night_date"].isoformat() for r in _rows(_garmin_sql(bronze_root))]
    assert nights.count("2024-01-01") == 1


def test_garmin_old_night_null_score_kept(bronze_root: str) -> None:
    """An old night without an overall score is kept; the score is null."""
    rows = {r["night_date"].isoformat(): r for r in _rows(_garmin_sql(bronze_root))}
    assert "2022-06-06" in rows
    assert rows["2022-06-06"]["garmin_sleep_score"] is None


def test_garmin_units_and_timestamps(bronze_root: str) -> None:
    """Stage seconds normalize to minutes; epoch-millis GMT parses to a timestamp."""
    r = {x["night_date"].isoformat(): x for x in _rows(_garmin_sql(bronze_root))}["2025-12-20"]
    assert r["garmin_deep_min"] == pytest.approx(60.0)  # 3600s / 60
    assert r["garmin_rhr"] == 52
    assert r["garmin_start_gmt"] is not None  # epoch-ms parsed, not crashed


def test_whoop_rescore_keeps_latest(bronze_root: str) -> None:
    """One id with two updated_at collapses to one row — the latest rescore wins."""
    rows = {r["whoop_sleep_id"]: r for r in _rows(_whoop_sql(bronze_root))}
    assert rows["id-c"]["whoop_performance_pct"] == pytest.approx(95.0)
    assert sum(1 for r in _rows(_whoop_sql(bronze_root)) if r["whoop_sleep_id"] == "id-c") == 1


def test_whoop_nap_flagged_in_source(bronze_root: str) -> None:
    """A nap is kept in the source asset and flagged is_nap=True."""
    rows = {r["whoop_sleep_id"]: r for r in _rows(_whoop_sql(bronze_root))}
    assert rows["id-nap"]["is_nap"] is True
    assert rows["id-a"]["is_nap"] is False


def test_whoop_units(bronze_root: str) -> None:
    """Whoop millis normalize to minutes (5_400_000 ms -> 90 min)."""
    r = {x["whoop_sleep_id"]: x for x in _rows(_whoop_sql(bronze_root))}["id-a"]
    assert r["whoop_deep_min"] == pytest.approx(90.0)


def test_unified_join_grain_and_nap_exclusion(bronze_root: str) -> None:
    """FULL OUTER JOIN: one row per night; naps excluded; provenance correct."""
    sql = unified_sleep_sql(_garmin_sql(bronze_root), _whoop_sql(bronze_root))
    rows = {r["night_date"].isoformat(): r for r in _rows(sql)}
    # Garmin-only (2), both (1), whoop-only (1); the nap night is absent entirely.
    assert set(rows) == {"2022-06-06", "2024-01-01", "2025-12-20", "2025-12-22"}
    assert "2025-12-21" not in rows  # nap night never becomes a unified row

    both = rows["2025-12-20"]
    assert both["has_garmin"] is True and both["has_whoop"] is True
    assert both["garmin_rhr"] == 52 and both["whoop_performance_pct"] == pytest.approx(90.0)

    g_only = rows["2024-01-01"]
    assert g_only["has_garmin"] is True and g_only["has_whoop"] is False
    assert g_only["whoop_performance_pct"] is None  # nothing coalesced

    w_only = rows["2025-12-22"]
    assert w_only["has_garmin"] is False and w_only["has_whoop"] is True
    assert w_only["garmin_total_min"] is None


def test_unified_unique_nights(bronze_root: str) -> None:
    """No duplicate night_date survives the join (the dedup contract)."""
    sql = unified_sleep_sql(_garmin_sql(bronze_root), _whoop_sql(bronze_root))
    nights = [r["night_date"] for r in _rows(sql)]
    assert len(nights) == len(set(nights))


def test_empty_collection_yields_typed_empty(bronze_root: str) -> None:
    """A not-yet-captured collection produces zero rows, not an error."""
    files = list_payload_files(bronze_root, "garmin", "nonexistent")
    assert files == []
    rows = _rows(garmin_sleep_sql(payloads_relation_sql(files)))
    assert rows == []


def test_whoop_night_uses_local_timezone(tmp_path) -> None:
    """A bedtime that crosses midnight in UTC is dated by its LOCAL night.

    start 03:00 UTC with a -05:00 offset is 22:00 the previous local evening, so the
    night is the 15th — not the UTC 16th that a naive CAST(start AS DATE) would give.
    """
    root = _write_whoop(tmp_path, _whoop_record("2026-01-16T03:00:00.000Z", tz="-05:00"))
    files = list_payload_files(root, "whoop", "sleep")
    rows = _rows(whoop_sleep_sql(payloads_relation_sql(files)))
    assert rows[0]["night_date"].isoformat() == "2026-01-15"


def test_whoop_night_falls_back_to_utc_without_offset(tmp_path) -> None:
    """A record missing timezone_offset falls back to the UTC date (never dropped)."""
    root = _write_whoop(tmp_path, _whoop_record("2026-01-16T03:00:00.000Z", tz=None))
    files = list_payload_files(root, "whoop", "sleep")
    rows = _rows(whoop_sleep_sql(payloads_relation_sql(files)))
    assert len(rows) == 1
    assert rows[0]["night_date"].isoformat() == "2026-01-16"
