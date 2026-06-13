"""DuckDB plumbing for silver: read bronze payloads, write Parquet atomically.

Two invariants carried over from the bronze-check work (so no silver subject
re-hits them):

1. *Sidecars are never read as payloads.* ``*.meta.json`` sits beside every
   payload; :func:`list_payload_files` excludes them in Python, so the meta keys
   (``sha256``, ``fetched_at`` …) can never contaminate a parsed payload.
2. *Bronze is read, never written.* Everything here reads under ``bronze_root``
   and writes only under ``silver_root``; :func:`write_parquet_atomic` refuses a
   destination inside ``bronze_root``.

Payloads are read as **raw JSON text** (``read_text`` → ``json(content)``), not
``read_json_auto`` struct-typing. Heterogeneous bronze (older Garmin nights miss
fields, schemas drift) breaks struct typing; JSON-path extraction on a raw ``JSON``
value returns NULL for a missing/renamed key instead of crashing, which is exactly
the null-safe behavior silver needs.
"""

from __future__ import annotations

import glob
import os
import secrets

import duckdb

META_SUFFIX = ".meta.json"


def connect() -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB connection for a single silver materialization.

    Silver is a whole-table rebuild per run, so an ephemeral in-memory connection
    is all we need — no persistent database, no shared state between runs.
    """
    return duckdb.connect(database=":memory:")


def list_payload_files(bronze_root: str, source: str, collection: str) -> list[str]:
    """Every payload file for a collection across all ``dt=`` partitions, sorted.

    Globs ``{bronze_root}/{source}/{collection}/dt=*/*`` and excludes sidecars and
    directories. Returns an empty list when the collection has not been captured —
    a valid state the caller turns into an empty (but correctly-typed) result.
    """
    pattern = os.path.join(bronze_root, source, collection, "dt=*", "*")
    return sorted(
        f
        for f in glob.glob(pattern)
        if os.path.isfile(f) and not f.endswith(META_SUFFIX)
    )


def _sql_str_list(files: list[str]) -> str:
    """Render a Python list of paths as a DuckDB string-array literal."""
    return "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"


def payloads_relation_sql(files: list[str]) -> str:
    """A SQL subquery yielding ``(filename VARCHAR, j JSON)`` for the given files.

    With files present, reads them via ``read_text`` and parses each whole file to
    a single ``JSON`` value. With no files, returns a correctly-typed empty relation
    (``WHERE false``) so downstream extraction/typing still produces the right
    columns on a not-yet-captured collection rather than erroring on an empty glob.
    """
    if not files:
        return "SELECT NULL::VARCHAR AS filename, NULL::JSON AS j WHERE false"
    return (
        "SELECT filename, json(content) AS j "
        f"FROM read_text({_sql_str_list(files)})"
    )


def csv_relation_sql(files: list[str], columns: dict[str, str]) -> str:
    """A SQL subquery selecting named CSV columns (+ ``filename``) for the given files.

    ``columns`` maps a safe output alias to the exact source header (CSV headers can
    contain spaces/brackets, e.g. Lingo's reading-time column), so callers reference
    clean identifiers downstream. Files are read ``all_varchar`` with header detection
    and ``union_by_name`` (tolerating minor column drift across captures); typing is
    the caller's job. With no files, returns a correctly-typed empty relation so a
    not-yet-captured collection yields the right columns instead of erroring.
    """
    aliases = list(columns)
    if not files:
        cols = ", ".join(f"NULL::VARCHAR AS {a}" for a in aliases)
        return f"SELECT {cols}, NULL::VARCHAR AS filename WHERE false"
    sel = ", ".join(f'"{hdr}" AS {a}' for a, hdr in columns.items())
    return (
        f"SELECT {sel}, filename FROM read_csv({_sql_str_list(files)}, "
        "header=true, all_varchar=true, union_by_name=true, filename=true)"
    )


def text_lines_relation_sql(files: list[str]) -> str:
    """A SQL subquery yielding ``(filename VARCHAR, line VARCHAR)`` — one row per line.

    For line-oriented bronze with no header and no single field delimiter — fixed-width
    / whitespace-delimited text such as the NOAA USCRN hourly product. Each file is read
    with a delimiter that never occurs in the payload (ASCII Unit Separator, ``chr(31)``)
    so every physical line lands in one ``VARCHAR`` cell, plus ``filename`` for
    capture-recency tie-breaks. Blank lines are dropped; splitting and typing the line is
    the caller's job. With no files, returns a correctly-typed empty relation so a
    not-yet-captured collection yields the right columns instead of erroring on an empty
    glob (mirrors :func:`csv_relation_sql` / :func:`payloads_relation_sql`).
    """
    if not files:
        return "SELECT NULL::VARCHAR AS filename, NULL::VARCHAR AS line WHERE false"
    return (
        f"SELECT filename, line FROM read_csv({_sql_str_list(files)}, "
        "delim=chr(31), header=false, columns={'line': 'VARCHAR'}, filename=true) "
        "WHERE trim(line) <> ''"
    )


def write_parquet_atomic(
    con: duckdb.DuckDBPyConnection,
    select_sql: str,
    dest_path: str,
    *,
    protected_root: str,
) -> int:
    """Write ``select_sql`` to ``dest_path`` as Parquet, atomically and idempotently.

    A derived layer fully overwrites its output every run (last run wins, a pure
    projection of its source). We ``COPY`` to a temp file in the destination directory
    and ``os.replace`` it into place, so a crashed run never leaves a half-written
    Parquet where readers expect a complete one.

    Refuses to write anywhere under ``protected_root`` — the immutable source a layer
    must never write into (bronze for silver; silver for gold). Returns the row count,
    read back from the written Parquet so ``select_sql`` (often the whole multi-join
    transform) runs exactly once, not a second time just to count.
    """
    dest_abs = os.path.abspath(dest_path)
    protected_abs = os.path.abspath(protected_root)
    if dest_abs == protected_abs or dest_abs.startswith(protected_abs + os.sep):
        raise ValueError(
            f"refusing to write under protected_root: {dest_abs} is inside {protected_abs}"
        )

    os.makedirs(os.path.dirname(dest_abs), exist_ok=True)
    tmp_path = os.path.join(
        os.path.dirname(dest_abs), f".tmp_{secrets.token_hex(8)}.parquet"
    )
    escaped = tmp_path.replace("'", "''")
    try:
        con.execute(
            f"COPY ({select_sql}) TO '{escaped}' (FORMAT parquet, COMPRESSION zstd)"
        )
        os.replace(tmp_path, dest_abs)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    dest_lit = dest_abs.replace("'", "''")
    return int(con.execute(f"SELECT count(*) FROM read_parquet('{dest_lit}')").fetchone()[0])
