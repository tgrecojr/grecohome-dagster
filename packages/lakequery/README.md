# grecohome-lakequery

A tiny **read-only DuckDB-over-Parquet HTTP query service** — the lake-native serving
layer for dashboards. Grafana (Infinity / JSON datasource) sends SQL, gets JSON rows
back, querying the silver/gold Parquet directly. No Postgres, no per-panel precomputed
mart; the same DuckDB engine the pipeline already uses.

- **Endpoints:** `GET /healthz`; `GET /query?q=<sql>`; `POST /query {"sql": "..."}` →
  JSON array of row objects.
- **Views** registered at startup (queried by clean name, re-read per query so daily
  rebuilds are picked up): `gold_daily_wellness`, `silver_sleep`,
  `silver_sleep_garmin`, `silver_sleep_whoop`, `silver_glucose`, `silver_workouts`,
  `silver_recovery`.
- **Safety:** a single `SELECT`/`WITH` only (no writes/DDL/ATTACH/COPY/multi-statement).
  The real boundary is deployment — mount the lake **read-only** and nothing else, so a
  query can only read those paths. Optional `LAKEQUERY_TOKEN` → `X-API-Key` check.

## Run

```bash
SILVER_ROOT=/data/silver GOLD_ROOT=/data/gold python -m grecohome_lakequery.server
# GET http://host:9999/query?q=SELECT%20*%20FROM%20gold_daily_wellness%20LIMIT%205
```

Env: `SILVER_ROOT`, `GOLD_ROOT` (required); `BRONZE_ROOT`, `LAKEQUERY_PORT` (default
9999), `LAKEQUERY_TOKEN` (optional) — see `docs/DEPLOYMENT.md`.
