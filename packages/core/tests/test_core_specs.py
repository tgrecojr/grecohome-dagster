"""Tests pinning core bronze-writer and bronze-check behavior named in the core specs.

Behavior that already had a dedicated test is annotated where that test lives; this
file covers the remaining guarantees.
"""

import json
import os
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from dagster import AssetCheckSeverity, AssetKey, DefaultScheduleStatus

from grecohome_core.bronze import capture as capture_mod
from grecohome_core.bronze.capture import capture_bronze
from grecohome_core.checks import (
    build_bronze_checks_job,
    build_bronze_checks_schedule,
    build_checks_for,
    build_content_health_check,
    build_event_completeness_check,
    build_fetch_freshness_check,
    build_schema_drift_check,
)
from grecohome_core.checks.bronze_reads import trailing_partition_dirs
from grecohome_core.checks.config import CollectionCheckConfig


def _path(*args, **kwargs):
    """The written path (None when nothing was written): these tests predate outcomes."""
    return capture_bronze(*args, **kwargs).path


KEY = AssetKey("src_bronze_coll")


def _cfg(**kw) -> CollectionCheckConfig:
    base = dict(source="src", collection="coll", asset_key=KEY)
    base.update(kw)
    return CollectionCheckConfig(**base)


def _files(root) -> list[str]:
    return sorted(
        os.path.relpath(os.path.join(d, n), root) for d, _s, names in os.walk(root) for n in names
    )


@pytest.mark.unit
class TestBronzeWriter:
    # @spec CORE-BRONZE-002
    def test_missing_or_empty_dt_uses_fetch_date_utc(self, tmp_path):
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        for dt in (None, ""):
            path = _path(
                "s", "c", b"x", {}, bronze_root=str(tmp_path), dt=dt, dedupe=False
            )
            assert f"/dt={today}/" in path.replace(os.sep, "/")

    # @spec CORE-BRONZE-004
    def test_writer_forces_provenance_and_defaults_overridable_fields(self, tmp_path):
        meta = {
            "source": "spoofed",
            "sha256": "bogus",
            "byte_size": 999,
            "schema_version": "v2",
            "request_url": "https://x/y",
        }
        path = _path("s", "c", b"payload", meta, bronze_root=str(tmp_path))
        sidecar = json.load(open(path.rsplit(".", 1)[0] + ".meta.json"))
        assert sidecar["source"] == "s" and sidecar["byte_size"] == 7
        assert sidecar["sha256"] != "bogus"
        assert sidecar["schema_version"] == "v2"  # caller may override a defaulted field
        assert sidecar["stored_encoding"] == "identity"  # default when not given
        assert sidecar["request_url"] == "https://x/y"  # passed through

    # @spec CORE-BRONZE-005
    def test_payload_written_before_sidecar_with_no_temp_files_left(self, tmp_path):
        order: list[str] = []
        real = capture_mod._write_atomic

        def record(path, data):
            order.append("sidecar" if path.endswith(".meta.json") else "payload")
            real(path, data)

        with patch.object(capture_mod, "_write_atomic", side_effect=record):
            _path("s", "c", b"payload", {}, bronze_root=str(tmp_path), dt="2025-01-05")
        assert order == ["payload", "sidecar"]
        assert not [f for f in _files(tmp_path) if os.path.basename(f).startswith(".tmp_")]

    # @spec CORE-BRONZE-007
    def test_non_bytes_payload_is_a_logged_failure(self, tmp_path):
        with patch.object(capture_mod.logger, "warning") as warn:
            result = _path("s", "c", "decoded text", {}, bronze_root=str(tmp_path))
        assert result is None
        assert warn.call_args.args[0] == "bronze capture failed"
        assert _files(tmp_path) == []

    # @spec CORE-BRONZE-010
    def test_dedupe_writes_when_newest_sidecar_lacks_sha256(self, tmp_path):
        first = _path("s", "c", b"same", {}, bronze_root=str(tmp_path), dt="2025-01-05")
        sidecar = first.rsplit(".", 1)[0] + ".meta.json"
        data = json.load(open(sidecar))
        del data["sha256"]
        json.dump(data, open(sidecar, "w"))
        second = _path("s", "c", b"same", {}, bronze_root=str(tmp_path), dt="2025-01-05")
        assert second is not None


@pytest.mark.unit
class TestCheckConfigAndBuilding:
    # @spec CORE-CHECKS-001
    @pytest.mark.parametrize(
        "kw",
        [{"event_date_source": "payload"}, {"enabled_checks": frozenset({"freshness", "bogus"})}],
    )
    def test_invalid_config_is_rejected(self, kw):
        with pytest.raises(ValueError):
            _cfg(**kw)

    # @spec CORE-CHECKS-002
    def test_builds_enabled_families_and_skips_completeness_for_none(self, tmp_path):
        checks = build_checks_for(_cfg(event_date_source="none"), str(tmp_path), None)
        names = {spec.name for c in checks for spec in c.check_specs}
        assert names == {"src_coll_freshness", "src_coll_schema_drift", "src_coll_content_health"}
        assert all(spec.asset_key == KEY for c in checks for spec in c.check_specs)

    # @spec CORE-CHECKS-003
    def test_reads_the_most_recent_existing_folders(self, tmp_path):
        for dt in ("2025-01-01", "2025-01-03", "2025-01-09"):
            (tmp_path / f"dt={dt}").mkdir()
        assert [d for d, _ in trailing_partition_dirs(str(tmp_path), 2)] == [
            "2025-01-03",
            "2025-01-09",
        ]
        assert len(trailing_partition_dirs(str(tmp_path), 0)) == 3

    # @spec CORE-CHECKS-005
    def test_every_family_is_non_blocking(self, tmp_path):
        checks = build_checks_for(
            _cfg(event_date_source="partition"), str(tmp_path), str(tmp_path / "m")
        )
        assert checks and all(not spec.blocking for c in checks for spec in c.check_specs)


@pytest.mark.unit
class TestInternalErrors:
    # @spec CORE-CHECKS-006
    @pytest.mark.parametrize(
        ("builder", "target", "severity"),
        [
            (
                build_fetch_freshness_check,
                "grecohome_core.checks.freshness.newest_fetch",
                AssetCheckSeverity.ERROR,
            ),
            (
                build_content_health_check,
                "grecohome_core.checks.content_health.sample_payloads",
                AssetCheckSeverity.WARN,
            ),
            (
                build_event_completeness_check,
                "grecohome_core.checks.completeness.distinct_event_dates",
                AssetCheckSeverity.WARN,
            ),
        ],
    )
    def test_internal_error_returns_failed_result(self, tmp_path, builder, target, severity):
        check = builder(_cfg(event_date_source="partition"), str(tmp_path))
        with patch(target, side_effect=RuntimeError("boom")):
            result = check()
        assert result.passed is False and result.severity == severity

    def test_schema_drift_internal_error_is_error(self, tmp_path):
        check = build_schema_drift_check(_cfg(), str(tmp_path), str(tmp_path / "m"))
        with patch(
            "grecohome_core.checks.schema_drift.schema_signature", side_effect=RuntimeError("x")
        ):
            result = check()
        assert result.passed is False and result.severity == AssetCheckSeverity.ERROR


@pytest.mark.unit
class TestFamilyEdges:
    # @spec CORE-CHECKS-COMPLETE-003
    def test_fewer_than_two_event_dates_passes(self, capture, bronze_root):
        capture("src", "coll", {"a": 1}, dt="2025-01-05")
        check = build_event_completeness_check(_cfg(event_date_source="partition"), bronze_root)
        assert check().passed

    # @spec CORE-CHECKS-SCHEMA-005
    def test_no_non_empty_payload_passes_as_no_payload(self, capture, bronze_root, tmp_path):
        capture("src", "coll", [], dt="2025-01-05")
        check = build_schema_drift_check(_cfg(), bronze_root, str(tmp_path / "monitor"))
        result = check()
        assert result.passed and result.metadata["status"].value == "no_payload"

    # @spec CORE-CHECKS-CONTENT-002
    def test_zero_byte_payload_is_error_even_when_expected_empty(self, capture, bronze_root):
        capture("src", "coll", b"", dt="2025-01-05")
        check = build_content_health_check(_cfg(expected_empty=True), bronze_root)
        result = check()
        assert result.passed is False and result.severity == AssetCheckSeverity.ERROR


@pytest.mark.unit
class TestAlertingWrap:
    # @spec CORE-CHECKS-ALERT-003
    def test_freshness_schema_and_content_page_but_completeness_does_not(self, tmp_path):
        cfg = _cfg(event_date_source="partition")
        root = str(tmp_path / "bronze")  # empty: freshness fails ERROR
        with patch("grecohome_core.checks.alerting._logger.error") as err:
            build_fetch_freshness_check(cfg, root)()
        assert err.call_args.args[0] == "asset_check_failed"
        with (
            patch("grecohome_core.checks.schema_drift.schema_signature", side_effect=RuntimeError),
            patch("grecohome_core.checks.alerting._logger.error") as err,
        ):
            build_schema_drift_check(cfg, root, str(tmp_path / "m"))()
        assert err.called
        with (
            patch("grecohome_core.checks.content_health.sample_payloads", return_value=["x"]),
            patch(
                "grecohome_core.checks.content_health.verify_integrity", return_value=["INTEGRITY"]
            ),
            patch("grecohome_core.checks.content_health.classify_payload", return_value="DATA"),
            patch("grecohome_core.checks.content_health.read_sidecar", return_value={}),
            patch("grecohome_core.checks.alerting._logger.error") as err,
        ):
            build_content_health_check(cfg, root)()
        assert err.called
        with (
            patch(
                "grecohome_core.checks.completeness.distinct_event_dates", side_effect=RuntimeError
            ),
            patch("grecohome_core.checks.alerting._logger.error") as err,
        ):
            build_event_completeness_check(cfg, root)()
        assert not err.called


@pytest.mark.unit
class TestChecksSchedule:
    # @spec CORE-CHECKS-JOB-002
    def test_schedule_starts_running(self, tmp_path):
        job = build_bronze_checks_job(
            [build_fetch_freshness_check(_cfg(), str(tmp_path))], name="src_bronze_checks_job"
        )
        schedule = build_bronze_checks_schedule(job, cron_schedule="37 * * * *")
        assert schedule.default_status == DefaultScheduleStatus.RUNNING
