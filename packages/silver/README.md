# grecohome-silver

The **silver layer**: a cross-subject Dagster code location that reads immutable
bronze payloads and writes typed, deduplicated, columnar **Parquet** that analysis
runs against. Silver is *derived and rebuildable* — it can always be dropped and
regenerated from bronze, so it is not precious; bronze stays the only source of
truth and is never touched here.

- **Sleep (v1):** three assets — `silver_sleep_garmin`, `silver_sleep_whoop`, and
  the unified `silver_sleep` (a FULL OUTER JOIN on the night, both devices' columns
  side by side and nullable, neither authoritative). See `docs/SILVER.md`.
- Reads bronze from the filesystem (DuckDB over `BRONZE_ROOT`, mounted read-only);
  declares its bronze upstreams by `AssetKey` so lineage renders across code
  locations. Writes Parquet under `SILVER_ROOT`, never under `BRONZE_ROOT`.
- Asset checks (uniqueness, ranges, coverage) and a daily rebuild schedule, all
  **off** the `*_api` concurrency pools — silver makes no source calls.

Ships as its own gRPC **code-location image** that registers with the host Dagster
daemon/webserver. See `docs/DEPLOYMENT.md`.
