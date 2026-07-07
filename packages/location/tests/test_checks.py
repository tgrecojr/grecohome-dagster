"""Execution tests for the custom location checks (promote-lag, receipt-freshness).

Runs the real checks-only job in-process against a temp bronze/staging tree, so the
checks exercise their true read paths. Thresholds use the built-in defaults
(warn 24h / error 168h / lag 6h).
"""

import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dagster import AssetCheckSeverity
from grecohome_location.capture import capture_location
from grecohome_location.dagster.definitions import defs
from grecohome_location.promote import promote_stream

TODAY = datetime.now(UTC).strftime("%Y-%m-%d")


def _stage(capture_dir, stream, received_ms, shortid, body=b'{"_type":"location"}'):
    d = Path(capture_dir) / stream / f"dt={TODAY}"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{received_ms}_{shortid}.json").write_bytes(body)


def _run_checks() -> dict:
    job = defs.resolve_job_def("location_bronze_checks_job")
    result = job.execute_in_process(raise_on_error=False)
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


@pytest.mark.unit
class TestPromoteLag:
    def test_fires_for_old_unpromoted_file(self, capture_dir, bronze_root, state_dir):
        old_ms = int((time.time() - 10 * 3600) * 1000)  # 10h old > 6h threshold
        _stage(capture_dir, "overland", old_ms, "d4e5f6")
        evals = _run_checks()
        lag = evals["location_overland_promote_lag"]
        assert not lag.passed
        assert lag.severity == AssetCheckSeverity.ERROR

    def test_passes_when_caught_up(self, capture_dir, bronze_root, state_dir):
        old_ms = int((time.time() - 10 * 3600) * 1000)
        _stage(capture_dir, "overland", old_ms, "d4e5f6")
        promote_stream(
            capture_dir=capture_dir, bronze_root=bronze_root, state_dir=state_dir,
            stream="overland", now=datetime.now(UTC), window_days=3,
        )
        evals = _run_checks()
        assert evals["location_overland_promote_lag"].passed


@pytest.mark.unit
class TestReceiptFreshness:
    def test_unused_stream_passes_not_errors(self, capture_dir, bronze_root, state_dir):
        """A stream that has never captured is unused, not broken — pass (green), no page."""
        evals = _run_checks()
        fresh = evals["location_owntracks_receipt_freshness"]
        assert fresh.passed
        assert fresh.severity == AssetCheckSeverity.WARN

    def test_passes_with_recent_receipt(self, capture_dir, bronze_root, state_dir):
        recent_ms = int((time.time() - 1800) * 1000)  # 30 min ago
        capture_location(
            b'{"_type":"location"}', collection="owntracks", dt=TODAY,
            received_ms=recent_ms, staging_file=f"{recent_ms}_a1b2c3.json",
            bronze_root=bronze_root,
        )
        evals = _run_checks()
        assert evals["location_owntracks_receipt_freshness"].passed
