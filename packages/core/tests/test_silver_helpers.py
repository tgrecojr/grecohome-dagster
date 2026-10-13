"""Tests for the shared silver helpers (``grecohome_core.silver``)."""

from __future__ import annotations

import os

import duckdb
import pytest

from grecohome_core.silver import (
    connect,
    csv_relation_sql,
    dedup_latest_sql,
    json_date,
    json_num,
    json_str,
    list_payload_files,
    payloads_relation_sql,
    text_lines_relation_sql,
    write_parquet_atomic,
)

pytestmark = pytest.mark.unit


def _put(path: str, text: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(text)
    return path


def _rows(sql: str) -> list[tuple]:
    return connect().execute(sql).fetchall()


# @spec CORE-SILVER-001
def test_connect_returns_independent_in_memory_connections():
    a, b = connect(), connect()
    assert isinstance(a, duckdb.DuckDBPyConnection)
    a.execute("CREATE TABLE t AS SELECT 1 AS x")
    with pytest.raises(duckdb.CatalogException):
        b.execute("SELECT * FROM t")


# @spec CORE-SILVER-002
def test_list_payload_files_skips_sidecars_dirs_dotfiles_and_nesting(tmp_path):
    coll = tmp_path / "garmin" / "sleep"
    b = _put(str(coll / "dt=2026-01-02" / "sleep_2_b.json"), "{}")
    a = _put(str(coll / "dt=2026-01-01" / "sleep_1_a.json"), "{}")
    _put(str(coll / "dt=2026-01-01" / "sleep_1_a.meta.json"), "{}")
    _put(str(coll / "dt=2026-01-01" / ".sleep_9_tmp.json"), "{}")
    _put(str(coll / "dt=2026-01-01" / "nested" / "deep.json"), "{}")
    assert list_payload_files(str(tmp_path), "garmin", "sleep") == [a, b]


# @spec CORE-SILVER-002
def test_list_payload_files_uncaptured_collection_is_empty(tmp_path):
    assert list_payload_files(str(tmp_path), "garmin", "never") == []


# @spec CORE-SILVER-003
def test_payloads_relation_parses_each_file_and_types_empty(tmp_path):
    f = _put(str(tmp_path / "p.json"), '{"a": {"b": 7}}')
    rows = _rows(f"SELECT filename, j->>'$.a.b' FROM ({payloads_relation_sql([f])})")
    assert rows == [(f, "7")]
    con = connect()
    cur = con.execute(f"SELECT * FROM ({payloads_relation_sql([])})")
    assert cur.fetchall() == []
    assert [d[0] for d in cur.description] == ["filename", "j"]


# @spec CORE-SILVER-004
def test_csv_relation_maps_headers_by_name_and_types_empty(tmp_path):
    f1 = _put(str(tmp_path / "a.csv"), "Time [local],Value\n2026-01-01,5\n")
    f2 = _put(str(tmp_path / "b.csv"), "Value,Extra,Time [local]\n6,x,2026-01-02\n")
    cols = {"ts": "Time [local]", "v": "Value"}
    rows = _rows(f"SELECT ts, v FROM ({csv_relation_sql([f1, f2], cols)}) ORDER BY ts")
    assert rows == [("2026-01-01", "5"), ("2026-01-02", "6")]
    cur = connect().execute(f"SELECT * FROM ({csv_relation_sql([], cols)})")
    assert cur.fetchall() == []
    assert sorted(d[0] for d in cur.description) == ["filename", "ts", "v"]


# @spec CORE-SILVER-005
def test_text_lines_relation_one_row_per_nonblank_line(tmp_path):
    f = _put(str(tmp_path / "h.txt"), "23803 20260101 0100  1.2\n\n   \n23803 20260101 0200 -0.4\n")
    rows = _rows(f"SELECT line FROM ({text_lines_relation_sql([f])})")
    assert rows == [("23803 20260101 0100  1.2",), ("23803 20260101 0200 -0.4",)]
    assert _rows(f"SELECT * FROM ({text_lines_relation_sql([])})") == []


# @spec CORE-SILVER-006
def test_invalid_json_payload_raises(tmp_path):
    good = _put(str(tmp_path / "good.json"), "{}")
    bad = _put(str(tmp_path / "bad.json"), "{not json")
    with pytest.raises(duckdb.Error):
        _rows(f"SELECT j->>'$.a' FROM ({payloads_relation_sql([good, bad])})")


# @spec CORE-SILVER-007
def test_json_helpers_extract_and_null_safely():
    j = """'{"s": "x", "n": "4.5", "bad": "abc", "d": "2026-03-04T05:06:07Z"}'::JSON"""
    row = _rows(
        f"SELECT {json_str(j, 's')}, {json_str(j, 'missing')}, {json_num(j, 'n')}, "
        f"{json_num(j, 'bad')}, {json_date(j, 'd')}, {json_date(j, 'bad')}"
    )[0]
    assert row[0] == "x" and row[1] is None
    assert row[2] == pytest.approx(4.5) and row[3] is None
    assert row[4].isoformat() == "2026-03-04" and row[5] is None


# @spec CORE-SILVER-008
def test_dedup_latest_keeps_greatest_with_nulls_last():
    inner = "SELECT * FROM (VALUES ('a', 1, 'old'), ('a', 3, 'new'), ('a', NULL, 'null'), " \
            "('b', NULL, 'only')) t(k, ts, label)"
    con = connect()
    cur = con.execute(f"{dedup_latest_sql(inner, partition_key='k', order_by='ts')} ORDER BY k")
    assert [d[0] for d in cur.description] == ["k", "ts", "label"]
    assert cur.fetchall() == [("a", 3, "new"), ("b", None, "only")]


# @spec CORE-SILVER-009
def test_write_parquet_atomic_writes_zstd_and_returns_count(tmp_path):
    dest = str(tmp_path / "silver" / "fam" / "t.parquet")
    con = connect()
    n = write_parquet_atomic(con, "SELECT range AS x FROM range(5)", dest,
                             protected_root=str(tmp_path / "bronze"))
    assert n == 5
    assert os.listdir(os.path.dirname(dest)) == ["t.parquet"]
    codecs = {r[0] for r in con.execute(
        f"SELECT compression FROM parquet_metadata('{dest}')").fetchall()}
    assert codecs == {"ZSTD"}


# @spec CORE-SILVER-010
def test_failed_write_leaves_destination_and_no_temp_file(tmp_path):
    dest = str(tmp_path / "silver" / "t.parquet")
    con = connect()
    write_parquet_atomic(con, "SELECT 1 AS x", dest, protected_root=str(tmp_path / "bronze"))
    with pytest.raises(duckdb.Error):
        write_parquet_atomic(con, "SELECT * FROM no_such_table", dest,
                             protected_root=str(tmp_path / "bronze"))
    assert os.listdir(os.path.dirname(dest)) == ["t.parquet"]
    assert con.execute(f"SELECT x FROM read_parquet('{dest}')").fetchall() == [(1,)]


# @spec CORE-SILVER-011
@pytest.mark.parametrize("rel", ["bronze", "bronze/t.parquet", "bronze/garmin/t.parquet"])
def test_guard_refuses_destination_inside_protected_root(tmp_path, rel):
    (tmp_path / "bronze").mkdir()
    with pytest.raises(ValueError, match="protected_root"):
        write_parquet_atomic(connect(), "SELECT 1", str(tmp_path / rel),
                             protected_root=str(tmp_path / "bronze"))


# @spec CORE-SILVER-011
def test_guard_allows_sibling_with_shared_prefix(tmp_path):
    dest = str(tmp_path / "bronze-silver" / "t.parquet")
    assert write_parquet_atomic(connect(), "SELECT 1", dest,
                                protected_root=str(tmp_path / "bronze")) == 1


# @spec CORE-SILVER-011
def test_guard_refuses_symlinked_root_into_protected(tmp_path):
    bronze = tmp_path / "bronze"
    (bronze / "garmin").mkdir(parents=True)
    os.symlink(bronze / "garmin", tmp_path / "silver")
    with pytest.raises(ValueError, match="protected_root"):
        write_parquet_atomic(connect(), "SELECT 1", str(tmp_path / "silver" / "t.parquet"),
                             protected_root=str(bronze))
    assert os.listdir(bronze / "garmin") == []


# @spec CORE-SILVER-011
def test_guard_refuses_when_protected_root_is_a_symlink(tmp_path):
    real = tmp_path / "data" / "bronze"
    real.mkdir(parents=True)
    os.symlink(real, tmp_path / "bronze-link")
    with pytest.raises(ValueError, match="protected_root"):
        write_parquet_atomic(connect(), "SELECT 1", str(real / "t.parquet"),
                             protected_root=str(tmp_path / "bronze-link"))
