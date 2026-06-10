"""Tests for the shared, read-only bronze helpers."""

import os
from datetime import date

import pytest

from grecohome_core.checks import bronze_reads as br


@pytest.mark.unit
class TestPayloadDiscovery:
    def test_iter_payloads_excludes_sidecars(self, capture, bronze_root):
        capture("whoop", "sleep", {"records": [{"start": "2024-12-01T08:00:00Z"}]},
                dt="2024-12-01")
        coll = br.collection_dir(bronze_root, "whoop", "sleep")
        parts = br.list_partition_dirs(coll)
        assert len(parts) == 1
        payloads = br.iter_payloads(parts[0][1])
        # Exactly one payload, and no .meta.json leaked in.
        assert len(payloads) == 1
        assert not any(p.endswith(".meta.json") for p in payloads)

    def test_sidecar_for_resolves_stem_layout(self, capture, bronze_root):
        path = capture("whoop", "sleep", {"records": []}, dt="2024-12-01")
        meta = br.sidecar_for(path)
        assert meta is not None and meta.endswith(".meta.json")
        sidecar = br.read_sidecar(path)
        assert sidecar["sha256"] and sidecar["source"] == "whoop"

    def test_sidecar_for_resolves_appended_layout(self, tmp_path):
        # Some writers use "<payload>.meta.json" rather than "<stem>.meta.json".
        payload = tmp_path / "p.json"
        payload.write_text("{}")
        (tmp_path / "p.json.meta.json").write_text('{"sha256": "x"}')
        assert br.sidecar_for(str(payload)).endswith("p.json.meta.json")


@pytest.mark.unit
class TestNewestFetch:
    def test_returns_newest_and_count(self, capture, bronze_root):
        capture("whoop", "sleep", {"records": [{"a": 1}]}, dt="2024-11-30",
                fetched="2024-11-30T10:00:00")
        capture("whoop", "sleep", {"records": [{"a": 2}]}, dt="2024-12-01",
                fetched="2024-12-01T10:00:00")
        coll = br.collection_dir(bronze_root, "whoop", "sleep")
        newest, count = br.newest_fetch(coll, recent_partitions=14)
        assert count == 2
        assert newest.date() == date(2024, 12, 1)

    def test_empty_collection(self, bronze_root):
        coll = br.collection_dir(bronze_root, "whoop", "sleep")
        newest, count = br.newest_fetch(coll, recent_partitions=14)
        assert newest is None and count == 0


@pytest.mark.unit
class TestEventDates:
    def test_partition_source_uses_dt(self, capture, bronze_root):
        for d in ("2024-12-01", "2024-12-03"):  # a one-day interior gap
            capture("soil", "hourly", "row\n", dt=d, content_type="text/plain", ext="txt")
        coll = br.collection_dir(bronze_root, "soil", "hourly")
        dates = br.distinct_event_dates(
            coll, event_date_source="partition", event_date_field=None,
            reader="txt", unnest_records=False, recent_partitions=14,
        )
        assert dates == [date(2024, 12, 1), date(2024, 12, 3)]

    def test_payload_source_unnest_records(self, capture, bronze_root):
        # dt is the FETCH date; the true event date lives in the record `start`.
        capture("whoop", "sleep",
                {"records": [{"start": "2024-10-05T23:00:00Z"}, {"start": "2024-10-06T01:00:00Z"}]},
                dt="2024-12-01")
        coll = br.collection_dir(bronze_root, "whoop", "sleep")
        dates = br.distinct_event_dates(
            coll, event_date_source="payload", event_date_field="start",
            reader="json", unnest_records=True, recent_partitions=14,
        )
        assert dates == [date(2024, 10, 5), date(2024, 10, 6)]

    def test_find_gaps(self):
        dates = [date(2024, 12, 1), date(2024, 12, 2), date(2024, 12, 10)]
        gaps = br.find_gaps(dates, cadence_days=2)
        assert len(gaps) == 1
        after, before, missing = gaps[0]
        assert (after, before) == (date(2024, 12, 2), date(2024, 12, 10))
        assert missing == 7

    def test_parse_event_date_formats(self):
        assert br.parse_event_date("2024-12-31T08:00:00Z") == date(2024, 12, 31)
        assert br.parse_event_date("2024-12-31 08:00:00") == date(2024, 12, 31)
        assert br.parse_event_date("12/31/2024") == date(2024, 12, 31)
        assert br.parse_event_date("") is None


@pytest.mark.unit
class TestSchemaSignature:
    def test_flat_json_signature_excludes_sidecar_fields(self, capture, bronze_root):
        # REGRESSION (§1.2): the signature must NOT contain sidecar field names.
        capture("garmin", "sleep",
                {"dailySleepDTO": {"id": 1}, "remSleepData": True, "sleepLevels": []},
                dt="2024-12-01")
        coll = br.collection_dir(bronze_root, "garmin", "sleep")
        sig = br.schema_signature(coll, reader="json", unnest_records=False,
                                  recent_partitions=14)
        assert sig == ["dailySleepDTO", "remSleepData", "sleepLevels"]
        for forbidden in ("sha256", "fetched_at", "stored_encoding", "byte_size",
                          "schema_version", "http_status"):
            assert forbidden not in sig

    def test_records_wrapper_signature_uses_record(self, capture, bronze_root):
        capture("whoop", "sleep",
                {"records": [{"id": 1, "start": "x", "score": {}}], "next_token": None},
                dt="2024-12-01")
        coll = br.collection_dir(bronze_root, "whoop", "sleep")
        sig = br.schema_signature(coll, reader="json", unnest_records=True,
                                  recent_partitions=14)
        assert sig == ["id", "score", "start"]

    def test_csv_signature_drops_dt(self, capture, bronze_root):
        capture("lingo", "glucose", "ts,value,dt\n2024-12-01T00:00:00,100,2024-12-02\n",
                dt="2024-12-02", content_type="text/csv", ext="csv")
        coll = br.collection_dir(bronze_root, "lingo", "glucose")
        sig = br.schema_signature(coll, reader="csv", unnest_records=False,
                                  recent_partitions=14)
        assert sig == ["ts", "value"]

    def test_txt_signature_is_field_count(self, capture, bronze_root):
        capture("soil", "hourly", "53878 20241201 0100 11.1 22.2\n", dt="2024-12-01",
                content_type="text/plain", ext="txt")
        coll = br.collection_dir(bronze_root, "soil", "hourly")
        sig = br.schema_signature(coll, reader="txt", unnest_records=False,
                                  recent_partitions=14)
        assert sig == ["fields=5"]

    def test_no_payload_returns_none(self, bronze_root):
        coll = br.collection_dir(bronze_root, "whoop", "sleep")
        assert br.schema_signature(coll, reader="json", unnest_records=True,
                                   recent_partitions=14) is None


@pytest.mark.unit
class TestClassification:
    @pytest.mark.parametrize("obj,expected", [
        ([{"a": 1}], "DATA"),
        ([], "EMPTY_LIST"),
        ({}, "EMPTY_OBJECT"),
        ({"records": [], "next_token": None}, "EMPTY_WRAPPER"),
        ({"records": [{"a": 1}]}, "DATA"),
        ({"error": "boom"}, "ERROR_LIKE"),
        ({"records": [{"a": 1}], "error": "x"}, "DATA"),  # data present beats error key
        ({"status": "ok"}, "DATA"),  # status alone is not an error
    ])
    def test_classify_json(self, obj, expected):
        assert br.classify_json(obj) == expected

    def test_classify_payload_http_error(self, tmp_path):
        p = tmp_path / "x.json"
        p.write_text("{}")
        assert br.classify_payload(str(p), {"http_status": 500}) == "HTTP_ERROR"

    def test_classify_payload_csv_and_txt(self, tmp_path):
        csv_p = tmp_path / "x.csv"
        csv_p.write_text("a,b\n1,2\n")
        assert br.classify_payload(str(csv_p), {"http_status": 200}) == "CSV_DATA"
        txt_p = tmp_path / "x.txt"
        txt_p.write_text("row\n")
        assert br.classify_payload(str(txt_p), {"http_status": 200}) == "TXT_DATA"


@pytest.mark.unit
class TestIntegrity:
    def test_clean_capture_has_no_issues(self, capture, bronze_root):
        path = capture("whoop", "sleep", {"records": [{"a": 1}]}, dt="2024-12-01")
        assert br.verify_integrity(path, br.read_sidecar(path)) == []

    def test_corruption_detected(self, capture, bronze_root):
        path = capture("whoop", "sleep", {"records": [{"a": 1}]}, dt="2024-12-01")
        sidecar = br.read_sidecar(path)
        with open(path, "ab") as fh:  # tamper after capture -> sha + size mismatch
            fh.write(b"junk")
        issues = br.verify_integrity(path, sidecar)
        assert any("sha256 mismatch" in i for i in issues)
        assert any("byte_size mismatch" in i for i in issues)

    def test_missing_sidecar(self, capture, bronze_root):
        path = capture("whoop", "sleep", {"records": []}, dt="2024-12-01")
        os.remove(br.sidecar_for(path))
        issues = br.verify_integrity(path, None)
        assert any("missing or unparseable sidecar" in i for i in issues)


@pytest.mark.unit
class TestSampling:
    def test_sample_spreads_and_caps(self, capture, bronze_root):
        for i in range(1, 11):
            capture("whoop", "sleep", {"records": [{"n": i}]}, dt=f"2024-12-{i:02d}",
                    fetched=f"2024-12-{i:02d}T10:00:00")
        coll = br.collection_dir(bronze_root, "whoop", "sleep")
        sampled = br.sample_payloads(coll, recent_partitions=14, sample=5)
        assert len(sampled) == 5
        assert len(set(sampled)) == 5  # distinct files
