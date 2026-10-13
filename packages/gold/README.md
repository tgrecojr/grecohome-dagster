# grecohome-gold

The **gold layer**: analysis marts derived from silver. Where silver is one faithful
typed row per event, gold answers questions — per-day rollups and cross-source joins.
It reads `SILVER_ROOT` Parquet and writes marts under `GOLD_ROOT`; like silver it is
derived and fully rebuildable, and never writes under `SILVER_ROOT`.

- **Marts:** `gold_daily_wellness` (one row per local day: sleep, recovery, strain,
  daily activity, Garmin and Whoop workouts, glucose with time-in-range, weight) and
  `gold_daily_weather` (the gardening rollup). Designed in `docs/intent/gold/`.
- **Run monitors:** `run_failure_monitor` and `run_queue_monitor` watch the whole Dagster
  instance and log the lines Grafana pages on.
- Depends on its silver tables by `AssetKey` (cross-code-location lineage); reads
  are filesystem reads of `SILVER_ROOT` via DuckDB. Depends only on `grecohome-core` +
  `duckdb`. Daily schedule after the silver rebuilds; off the `*_api` pools.

Ships as its own gRPC **code-location image** registered with the host Dagster
daemon/webserver. See `docs/DEPLOYMENT.md`.
