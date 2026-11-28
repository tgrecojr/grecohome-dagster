"""Shared, source-agnostic silver-layer helpers.

Silver reads raw immutable bronze payloads, extracts the true event date, types
and deduplicates them to one row per logical record, and writes typed columnar
Parquet that analysis runs against. Silver is *derived and rebuildable* — it can
always be dropped and regenerated from bronze; bronze stays the only source of
truth and is never touched here.

This package holds the generic machinery every silver asset copies:

* :mod:`grecohome_core.silver.duck` — a DuckDB connection, sidecar-safe payload
  reading, and atomic Parquet overwrite under a silver root kept strictly outside
  ``BRONZE_ROOT``.
* :mod:`grecohome_core.silver.transform` — reusable SQL idioms (the
  ``row_number() … = 1`` dedup pattern, JSON-path helpers).

Table-specific column mapping (which payload field becomes which typed column)
lives in the silver table module, never here.
"""

from __future__ import annotations

from grecohome_core.silver.duck import (
    connect,
    list_payload_files,
    payloads_relation_sql,
    write_parquet_atomic,
)
from grecohome_core.silver.transform import dedup_latest_sql, json_date, json_num, json_str

__all__ = [
    "connect",
    "list_payload_files",
    "payloads_relation_sql",
    "write_parquet_atomic",
    "dedup_latest_sql",
    "json_date",
    "json_num",
    "json_str",
]
