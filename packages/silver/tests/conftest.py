"""Synthetic bronze-tree fixtures for silver sleep tests.

Builds a tiny ``{root}/{source}/sleep/dt=.../`` tree covering every transform case
the spec calls out (§8): duplicate Garmin captures of one night, a Whoop rescore,
a nap, an old null-score Garmin night, a both-devices night, and ``.meta.json``
sidecars that must be ignored.
"""

from __future__ import annotations

import json
import os

import pytest


def _write_payload(part_dir: str, fetched_ms: int, obj: dict, short: str = "abc123") -> None:
    """Write a bronze payload + a sidecar that must never be read as a payload."""
    os.makedirs(part_dir, exist_ok=True)
    base = f"sleep_{fetched_ms}_{short}"
    with open(os.path.join(part_dir, base + ".json"), "w") as fh:
        json.dump(obj, fh)
    # A sidecar whose keys (sha256, dailySleepDTO-looking junk) would corrupt a
    # naive read; list_payload_files must exclude it.
    with open(os.path.join(part_dir, base + ".meta.json"), "w") as fh:
        json.dump({"sha256": "deadbeef", "fetched_at_unix_ms": fetched_ms}, fh)


def _garmin(cal_date: str, *, score: int | None = 85, total_s: int = 27000) -> dict:
    dto: dict = {
        "calendarDate": cal_date,
        "sleepTimeSeconds": total_s,
        "deepSleepSeconds": 3600,
        "lightSleepSeconds": 15000,
        "remSleepSeconds": 7200,
        "awakeSleepSeconds": 1200,
        "avgSleepStress": 18.5,
        "averageRespirationValue": 14.2,
        "averageSpO2Value": 95.0,
        "sleepStartTimestampGMT": 1703999400000,
        "sleepEndTimestampGMT": 1704027000000,
    }
    if score is not None:
        dto["sleepScores"] = {"overall": {"value": score}}
    return {"dailySleepDTO": dto, "restingHeartRate": 52}


def _whoop(sleep_id: str, start: str, updated_at: str, *, nap: bool = False, perf: float = 90.0,
           deep_ms: int = 5_400_000) -> dict:
    return {
        "id": sleep_id,
        "start": start,
        "end": start.replace("T0", "T1"),
        "updated_at": updated_at,
        "nap": nap,
        "cycle_id": 987654321,
        "score": {
            "sleep_performance_percentage": perf,
            "sleep_efficiency_percentage": 88.0,
            "sleep_consistency_percentage": 80.0,
            "respiratory_rate": 15.0,
            "stage_summary": {
                "total_slow_wave_sleep_time_milli": deep_ms,
                "total_rem_sleep_time_milli": 4_200_000,
                "total_light_sleep_time_milli": 9_000_000,
                "total_awake_time_milli": 1_200_000,
                "disturbance_count": 3,
            },
        },
    }


@pytest.fixture
def bronze_root(tmp_path) -> str:
    """A synthetic bronze tree exercising every silver sleep transform case."""
    root = str(tmp_path / "bronze")
    g = os.path.join(root, "garmin", "sleep")
    w = os.path.join(root, "whoop", "sleep")

    def gp(dt: str) -> str:
        return os.path.join(g, f"dt={dt}")

    def wp(*records: dict) -> dict:
        return {"records": list(records)}

    # Garmin: same night captured twice (dedup -> latest fetch wins, total_s=28800).
    _write_payload(gp("2024-01-01"), 1_704_067_200000, _garmin("2024-01-01", total_s=27000))
    later = _garmin("2024-01-01", total_s=28800)
    _write_payload(gp("2024-01-01"), 1_704_153_600000, later, short="def456")
    # Old night with no overall score (kept; score null).
    _write_payload(gp("2022-06-06"), 1_654_560_000000, _garmin("2022-06-06", score=None))
    # A both-devices night.
    _write_payload(gp("2025-12-20"), 1_734_700_000000, _garmin("2025-12-20"))

    # Whoop: real night (also Garmin -> "both").
    both = _whoop("id-a", "2025-12-20T05:30:00.000Z", "2025-12-20T12:00:00.000Z")
    _write_payload(os.path.join(w, "dt=2025-12-20"), 1_734_700_000000, wp(both))
    # Nap (flagged in source asset; excluded from the unified night).
    nap = _whoop("id-nap", "2025-12-21T14:00:00.000Z", "2025-12-21T15:00:00.000Z", nap=True)
    _write_payload(os.path.join(w, "dt=2025-12-21"), 1_734_786_000000, wp(nap))
    # Rescore: one id, two updated_at across two files (keep latest perf=95).
    v1 = _whoop("id-c", "2025-12-22T05:00:00.000Z", "2025-12-22T08:00:00.000Z", perf=70.0)
    v2 = _whoop("id-c", "2025-12-22T05:00:00.000Z", "2025-12-22T20:00:00.000Z", perf=95.0)
    _write_payload(os.path.join(w, "dt=2025-12-22"), 1_734_870_000000, wp(v1))
    _write_payload(os.path.join(w, "dt=2025-12-22"), 1_734_880_000000, wp(v2), short="ghi789")

    return root
