"""Tests for the event-completeness asset check (incl. the Lingo event-date case)."""

import pytest
from dagster import AssetCheckSeverity, AssetKey

from grecohome_core.checks import build_event_completeness_check
from grecohome_core.checks.config import CollectionCheckConfig

# The real Lingo CSV header for the reading timestamp.
LINGO_TS = "Time of Glucose Reading [T=(local time) +/- (time zone offset)]"


@pytest.mark.unit
class TestCompletenessPartition:
    def _cfg(self, **kw):
        base = dict(source="soil", collection="hourly",
                    asset_key=AssetKey("uscrn_bronze_hourly"),
                    reader="txt", event_date_source="partition", cadence_days=1)
        base.update(kw)
        return CollectionCheckConfig(**base)

    def test_dense_timeline_passes(self, capture, bronze_root):
        for d in ("2024-12-01", "2024-12-02", "2024-12-03"):
            capture("soil", "hourly", "row\n", dt=d, content_type="text/plain", ext="txt")
        res = build_event_completeness_check(self._cfg(), bronze_root)()
        assert res.passed
        assert res.severity == AssetCheckSeverity.WARN

    def test_interior_gap_warns(self, capture, bronze_root):
        for d in ("2024-12-01", "2024-12-05"):  # 3 missing days, cadence 1
            capture("soil", "hourly", "row\n", dt=d, content_type="text/plain", ext="txt")
        res = build_event_completeness_check(self._cfg(), bronze_root)()
        assert not res.passed
        assert res.severity == AssetCheckSeverity.WARN
        assert res.metadata["gaps_over_cadence"].value == 1
        assert res.metadata["biggest_gap_days"].value == 3


@pytest.mark.unit
class TestCompletenessSnapshotSkipped:
    def test_event_date_none_is_skipped(self, bronze_root):
        cfg = CollectionCheckConfig(
            source="whoop", collection="profile",
            asset_key=AssetKey("whoop_bronze_snapshots"),
            event_date_source="none", enabled_checks=frozenset({"completeness"}),
        )
        res = build_event_completeness_check(cfg, bronze_root)()
        assert res.passed
        assert res.metadata["status"].value == "skipped"


@pytest.mark.unit
class TestLingoEventDate:
    def _cfg(self, **kw):
        base = dict(source="lingo", collection="glucose",
                    asset_key=AssetKey("lingo_bronze_glucose"), reader="csv",
                    event_date_source="payload", event_date_field=LINGO_TS,
                    cadence_days=30)
        base.update(kw)
        return CollectionCheckConfig(**base)

    def test_event_span_reflects_csv_not_dt(self, capture, bronze_root):
        # dt = FETCH date (Dec); the readings inside are from November.
        header = f"{LINGO_TS},Glucose Value\n"
        capture("lingo", "glucose",
                header + "2024-11-01T08:00:00,100\n2024-11-02T09:00:00,110\n",
                dt="2024-12-10", content_type="text/csv", ext="csv")
        capture("lingo", "glucose",
                header + "2024-11-02T09:00:00,110\n2024-11-03T07:00:00,95\n",
                dt="2024-12-11", content_type="text/csv", ext="csv")
        res = build_event_completeness_check(self._cfg(), bronze_root)()
        # Distinct event dates span Nov 1-3 (deduped across overlapping files),
        # NOT the Dec fetch dates.
        assert res.metadata["event_days"].value == 3
        assert res.metadata["earliest"].value == "2024-11-01"
        assert res.metadata["latest"].value == "2024-11-03"
