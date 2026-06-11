"""Shared, source-agnostic silver-layer helpers.

Silver reads raw immutable bronze payloads, extracts the true event date, types
and deduplicates them to one row per logical record, and writes typed columnar
Parquet that analysis runs against. Silver is *derived and rebuildable* — it can
always be dropped and regenerated from bronze; bronze stays the only source of
truth and is never touched here.

This package holds the generic machinery every silver asset copies:

* :mod:`grecohome_core.silver.duck` — a DuckDB connection, sidecar-safe payload
  reading (the analog of :mod:`grecohome_core.checks.bronze_reads`, but feeding
  DuckDB), and atomic Parquet overwrite under a silver root kept strictly outside
  ``BRONZE_ROOT``.
* :mod:`grecohome_core.silver.transform` — reusable SQL idioms (the
  ``row_number() … = 1`` dedup pattern, JSON-path helpers).

Subject-specific column mapping (which payload field becomes which typed column)
lives in the silver subject module, never here — mirroring the core-vs-subject
split the bronze checks already follow.
"""

from __future__ import annotations

from grecohome_core.silver.duck import (
    connect,
    csv_relation_sql,
    list_payload_files,
    payloads_relation_sql,
    write_parquet_atomic,
)
from grecohome_core.silver.transform import dedup_latest_sql, json_date, json_num, json_str

__all__ = [
    "connect",
    "csv_relation_sql",
    "list_payload_files",
    "payloads_relation_sql",
    "write_parquet_atomic",
    "dedup_latest_sql",
    "json_date",
    "json_num",
    "json_str",
]
