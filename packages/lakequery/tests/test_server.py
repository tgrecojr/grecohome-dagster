"""Tests for the lakequery DuckDB HTTP service (pure pieces + a live request)."""

from __future__ import annotations

import json
import os
import threading
import urllib.request
from http.server import ThreadingHTTPServer

import duckdb
import pytest

from grecohome_lakequery.server import (
    LakeQueryHandler,
    _json_default,
    build_connection,
    is_read_only,
    run_sql,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "sql",
    ["SELECT 1", "  select * from t", "WITH x AS (SELECT 1) SELECT * FROM x", "SELECT 1;"],
)
def test_read_only_accepts_selects(sql):
    assert is_read_only(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "DROP TABLE t",
        "ATTACH 'x.db'",
        "COPY t TO 'x'",
        "SELECT 1; DROP TABLE t",  # multi-statement
        "",
        "   ",
    ],
)
def test_read_only_rejects_writes_and_multistatement(sql):
    assert is_read_only(sql) is False


def test_run_sql_returns_dicts():
    con = duckdb.connect(":memory:")
    rows = run_sql(con, "SELECT 1 AS a, 'x' AS b")
    assert rows == [{"a": 1, "b": "x"}]


def test_json_default_serializes_dates_and_decimals():
    import datetime
    import decimal

    assert _json_default(datetime.date(2026, 6, 13)) == "2026-06-13"
    assert _json_default(decimal.Decimal("1.5")) == 1.5


def _write_parquet(con, path: str, sql: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")


def test_build_connection_registers_views(tmp_path):
    silver = str(tmp_path / "silver")
    gold = str(tmp_path / "gold")
    seed = duckdb.connect(":memory:")
    _write_parquet(seed, f"{gold}/wellness/daily_wellness.parquet",
                   "SELECT DATE '2026-06-13' AS day, 95.0 AS glucose_tir_pct")
    _write_parquet(seed, f"{silver}/sleep/silver_sleep.parquet",
                   "SELECT DATE '2026-06-13' AS night_date, 82 AS garmin_sleep_score")
    con = build_connection(silver, gold)
    gold_rows = run_sql(con, "SELECT glucose_tir_pct FROM gold_daily_wellness")
    assert gold_rows[0]["glucose_tir_pct"] == 95.0
    sleep_rows = run_sql(con, "SELECT garmin_sleep_score FROM silver_sleep")
    assert sleep_rows[0]["garmin_sleep_score"] == 82


def test_http_query_endpoint_end_to_end(tmp_path):
    """A live request: SELECT returns JSON rows; a write is rejected 403; /healthz ok."""
    silver, gold = str(tmp_path / "s"), str(tmp_path / "g")
    seed = duckdb.connect(":memory:")
    _write_parquet(seed, f"{gold}/wellness/daily_wellness.parquet", "SELECT 1 AS day, 90.0 AS tir")
    LakeQueryHandler.con = build_connection(silver, gold)
    LakeQueryHandler.token = None
    server = ThreadingHTTPServer(("127.0.0.1", 0), LakeQueryHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        health = urllib.request.urlopen(f"{base}/healthz")
        assert json.loads(health.read())["status"] == "ok"

        req = urllib.request.Request(
            f"{base}/query",
            data=json.dumps({"sql": "SELECT tir FROM gold_daily_wellness"}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        rows = json.loads(urllib.request.urlopen(req).read())
        assert rows == [{"tir": 90.0}]

        bad = urllib.request.Request(
            f"{base}/query",
            data=json.dumps({"sql": "DROP TABLE gold_daily_wellness"}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(bad)
        assert exc.value.code == 403
    finally:
        server.shutdown()
