"""Read-only DuckDB-over-Parquet HTTP query service for the data lake.

Exposes the silver/gold (and optionally bronze) Parquet under a SQL endpoint so Grafana
(Infinity / JSON datasource) builds dashboards directly off the lake — no Postgres, no
per-panel precomputed mart, the same DuckDB engine the pipeline already uses.

Endpoints:
- ``GET  /healthz``            -> ``{"status": "ok"}``
- ``GET  /query?q=<sql>``      -> JSON array of row objects (handy for quick checks)
- ``POST /query`` ``{"sql":...}`` -> JSON array of row objects (what Grafana uses)

Safety: only a **single SELECT/WITH** statement is accepted (no writes/DDL/ATTACH/COPY/
multiple statements). The real boundary is the deployment — the container mounts the lake
**read-only** and nothing else, so a query can only ever read those paths. An optional
``LAKEQUERY_TOKEN`` adds an ``X-API-Key`` check.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import duckdb

# Views registered at startup so dashboards query clean names, not file paths.
# Each is `SELECT * FROM read_parquet(path)`, re-read per query — so the daily
# silver/gold overwrites are picked up with no service restart.
LAKE_VIEWS: dict[str, str] = {
    "gold_daily_wellness": "{gold}/wellness/daily_wellness.parquet",
    "silver_sleep": "{silver}/sleep/silver_sleep.parquet",
    "silver_sleep_garmin": "{silver}/sleep/_garmin.parquet",
    "silver_sleep_whoop": "{silver}/sleep/_whoop.parquet",
    "silver_glucose": "{silver}/glucose/silver_glucose.parquet",
    "silver_workouts": "{silver}/workouts/silver_workouts.parquet",
    "silver_recovery": "{silver}/recovery/silver_recovery.parquet",
}

_READ_ONLY = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)


def is_read_only(sql: str) -> bool:
    """True only for a single read-only statement (SELECT/WITH, no extra statements)."""
    s = sql.strip().rstrip(";").strip()
    if not s or ";" in s:  # reject empty and multi-statement
        return False
    return bool(_READ_ONLY.match(s))


def _json_default(o: object) -> object:
    if isinstance(o, (_dt.date, _dt.datetime)):
        return o.isoformat()
    if isinstance(o, decimal.Decimal):
        return float(o)
    return str(o)


def run_sql(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    """Execute ``sql`` on a cursor and return rows as a list of column->value dicts."""
    cur = con.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def build_connection(
    silver_root: str, gold_root: str, bronze_root: str | None = None
) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB connection with the lake views registered."""
    con = duckdb.connect(database=":memory:")
    fmt = {"silver": silver_root, "gold": gold_root, "bronze": bronze_root or ""}
    for name, tmpl in LAKE_VIEWS.items():
        path = tmpl.format(**fmt).replace("'", "''")
        try:
            con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path}')")
        except duckdb.Error:
            # DuckDB binds read_parquet at view creation; a table not yet materialized
            # would crash startup. Skip its view (it just won't be queryable) rather
            # than fail — in a full deployment every table exists.
            pass
    return con


class LakeQueryHandler(BaseHTTPRequestHandler):
    con: duckdb.DuckDBPyConnection | None = None
    token: str | None = None
    lock = threading.Lock()

    def _send(self, code: int, payload: object) -> None:
        body = json.dumps(payload, default=_json_default).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authed(self) -> bool:
        return not self.token or self.headers.get("X-API-Key") == self.token

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self._send(200, {"status": "ok"})
            return
        if parsed.path == "/query":
            if not self._authed():
                self._send(401, {"error": "unauthorized"})
                return
            self._handle_query(parse_qs(parsed.query).get("q", [None])[0])
            return
        self._send(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        if urlparse(self.path).path != "/query":
            self._send(404, {"error": "not found"})
            return
        if not self._authed():
            self._send(401, {"error": "unauthorized"})
            return
        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self._send(400, {"error": "invalid json body"})
            return
        self._handle_query(body.get("sql"))

    def _handle_query(self, sql: str | None) -> None:
        if not sql:
            self._send(400, {"error": "missing sql"})
            return
        if not is_read_only(sql):
            self._send(403, {"error": "only a single SELECT/WITH query is allowed"})
            return
        try:
            with self.lock:
                rows = run_sql(self.con, sql)
            self._send(200, rows)
        except Exception as e:  # noqa: BLE001 - surface query errors to the caller
            self._send(400, {"error": str(e)})

    def log_message(self, *args: object) -> None:  # quiet access logs
        pass


def main() -> None:
    LakeQueryHandler.con = build_connection(
        os.environ["SILVER_ROOT"], os.environ["GOLD_ROOT"], os.environ.get("BRONZE_ROOT")
    )
    LakeQueryHandler.token = os.environ.get("LAKEQUERY_TOKEN")
    port = int(os.environ.get("LAKEQUERY_PORT", "9999"))
    ThreadingHTTPServer(("0.0.0.0", port), LakeQueryHandler).serve_forever()


if __name__ == "__main__":
    main()
