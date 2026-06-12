# grecohome-gold

The **gold layer**: analysis marts derived from silver. Where silver is one faithful
typed row per event, gold answers questions — per-day rollups and cross-source joins.
It reads `SILVER_ROOT` Parquet and writes marts under `GOLD_ROOT`; like silver it is
derived and fully rebuildable, and never writes under `SILVER_ROOT`.

- **`gold_daily_wellness` (v1):** one row per local day, joining sleep + recovery +
  daily workout load + daily glucose summary (mean, min/max, variability, and
  time-in-range 70–140 mg/dL). The spine other analyses build on. See `docs/GOLD.md`.
- Depends on the four silver tables by `AssetKey` (cross-code-location lineage); reads
  are filesystem reads of `SILVER_ROOT` via DuckDB. Depends only on `grecohome-core` +
  `duckdb`. Daily schedule after the silver rebuilds; off the `*_api` pools.

Ships as its own gRPC **code-location image** registered with the host Dagster
daemon/webserver. See `docs/DEPLOYMENT.md`.
