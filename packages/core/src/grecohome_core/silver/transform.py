"""Reusable SQL idioms every silver transform copies.

These are deliberately source-agnostic: the *which field maps to which column*
decision is subject-specific and lives in the silver subject module. Here we keep
only the patterns the spec calls out as shared — JSON-path extraction with safe
typing, and the dedup-to-latest projection.
"""

from __future__ import annotations


def json_str(expr: str, path: str) -> str:
    """Extract a JSON path as text (VARCHAR), NULL if the path is absent.

    ``expr`` is a SQL expression of DuckDB ``JSON`` type (e.g. ``j`` or
    ``records.value``); ``path`` is a dotted payload path *without* the leading
    ``$.`` (e.g. ``dailySleepDTO.calendarDate``). A missing/renamed key yields NULL
    rather than an error — the null-safety silver needs over heterogeneous bronze.
    """
    return f"({expr} ->> '$.{path}')"


def json_num(expr: str, path: str) -> str:
    """Extract a JSON path as DOUBLE, NULL if absent or non-numeric (TRY_CAST)."""
    return f"TRY_CAST({json_str(expr, path)} AS DOUBLE)"


def json_date(expr: str, path: str) -> str:
    """Extract a JSON path as DATE, NULL if absent or unparseable.

    Takes the leading 10 chars so an ISO datetime (``2024-12-31T08:00:00Z``) and a
    bare date (``2024-12-31``) both parse to the calendar date.
    """
    return f"TRY_CAST(substr({json_str(expr, path)}, 1, 10) AS DATE)"


def dedup_latest_sql(inner_sql: str, *, partition_key: str, order_by: str) -> str:
    """Keep one row per ``partition_key`` — the latest by ``order_by``.

    Wraps ``inner_sql`` with the canonical
    ``row_number() OVER (PARTITION BY key ORDER BY recency DESC) = 1`` pattern.
    Bronze is heavily re-captured (the same night appears in many files, and Whoop
    rescores), so dedup is mandatory, not optional. ``order_by`` is the recency
    expression (e.g. ``updated_at`` for Whoop rescores, ``fetched_at`` for Garmin
    re-pulls); NULLs sort last so a real value always wins a tie-break.
    """
    return (
        "SELECT * EXCLUDE (_rn) FROM ("
        f"  SELECT *, row_number() OVER ("
        f"    PARTITION BY {partition_key} ORDER BY {order_by} DESC NULLS LAST"
        f"  ) AS _rn FROM ({inner_sql})"
        ") WHERE _rn = 1"
    )
