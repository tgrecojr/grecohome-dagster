---
parent: high-level-design
prefix: GOLD
---

# Gold layer

## Context

Gold is **analysis**: marts that answer questions, built from silver. Silver keeps one faithful,
typed row per event; gold joins across sources and rolls up to a useful grain, usually the local
day, and adds everyday units (°F, inches, pounds, kilocalories). Gold is derived and
rebuildable: it can be dropped and regenerated from silver at any time. Silver, and bronze beneath
it, stay the sources of truth.

Gold is one code location (`grecohome_gold`, its own image). The same code location also hosts
the instance-wide **run monitors** (see [monitors](monitors/monitors-design.md)): it's the top of
the stack and the lightest image, and a sensor there can see every code location's runs.

## Layer rules

- **Rebuilt every run.** Each materialization fully overwrites its mart from current silver.
- **Outside silver.** Gold writes only under `GOLD_ROOT`. The shared writer refuses any
  destination inside `SILVER_ROOT` (`protected_root`), and silver is mounted read-only in the gold
  container. Gold never reads bronze.
- **Missing silver degrades, never fails.** A mart reading a silver table whose Parquet doesn't
  exist yet (a fresh deploy, a newly added table) substitutes a correctly typed empty relation, so
  that dimension comes out null rather than failing the whole mart. A missing table also stops
  contributing to the date spine's bounds; with every source missing, the mart is empty.
- **Per-event detail stays in silver.** Gold rows are per day; the events behind them are in
  silver, joinable by date, `sleep_id` or `cycle_id`.
- **Continuous date spines.** Daily marts cover every day between their first and last source
  day, so gaps are explicit rows for rolling and streak analysis, not missing rows.
- **Provenance flags.** Each source a mart joins has a `has_*` flag per row, so every null is
  explainable.
- **No source calls, no `*_api` pool;** gold runs after silver.

## Architecture

- **Silver upstreams are declared by `AssetKey`** for cross-code-location lineage; the reads are
  filesystem reads of `SILVER_ROOT` through DuckDB. The image depends on `grecohome-core` and
  DuckDB, never on a subject or silver package.
- **Shared machinery comes from core:** the DuckDB connection and the guarded atomic Parquet writer
  ([core silver helpers](../core/silver/silver-design.md)), and the checks-only job builders.
- **Settings** (`GoldSettings`) carry only what gold needs, not the bronze subject settings.

## Storage

Parquet (zstd), written atomically, under `{GOLD_ROOT}/<mart>/<table>.parquet`:
`wellness/daily_wellness.parquet` and `weather/daily_weather.parquet`. No partitioning.

## Asset checks

Every mart has checks: a unique, non-null key and plausible value ranges at **ERROR**, coverage at
**WARN** (fails only if the mart is empty, reporting per-source coverage), and **table
freshness** at ERROR, built with core's `table_freshness_check` (fails when the mart's Parquet is
more than 36 hours old). At the 08:00 check a mart is about half an hour old normally, about 24.5
hours after one missed rebuild and about 48.5 after two, so freshness pages on the second
consecutive miss, as in silver. Every check
is wrapped in `alerting_check`, and every check fails at ERROR if its mart's Parquet doesn't exist.
A checks-only job, `gold_checks_job`, runs every gold check daily at **08:00 UTC**
(`gold_checks_daily`, which starts running). Rebuild a mart on demand with its job (for example
`dagster job execute --job gold_wellness_job`).

## Schedules

Each mart has its own job and daily schedule, after silver's rebuilds (06:00–06:55) and silver's
checks (07:00): `gold_wellness_daily` at 07:30 and `gold_weather_daily` at 07:40 UTC. Rebuild
schedules start stopped (HLD).

## Marts and monitors

| Node | Doc | Contents |
|---|---|---|
| Wellness | [wellness/wellness-design.md](wellness/wellness-design.md) | `gold_daily_wellness` (`GOLD-WELLNESS`) |
| Weather | [weather/weather-design.md](weather/weather-design.md) | `gold_daily_weather` (`GOLD-WEATHER`) |
| Monitors | [monitors/monitors-design.md](monitors/monitors-design.md) | `run_queue_monitor` and the run-failure sensor (`GOLD-MONITORS`) |
| Layer checks | [checks/checks-design.md](checks/checks-design.md) | `gold_checks_job` and `gold_checks_daily` (`GOLD-CHECKS`) |

This parent doc owns no specs.

## Future marts

Not built; ideas to revisit when wanted:

- **Fitness** over `silver_fitness`: VO2max trend, training load.
- **Weekly rollups** (steps, stress, intensity minutes) derived from `silver_daily`, rather than
  projecting Garmin's weekly collections into silver.

## Configuration

| Variable | Meaning |
|---|---|
| `SILVER_ROOT` | Required; silver, read-only |
| `GOLD_ROOT` | Required; gold output, never under `SILVER_ROOT` |
| `GOLD_MONITOR_DIR` | Optional, reserved for future gold-check state, outside `GOLD_ROOT` |
| `GOLD_WEATHER_MIN_VALID_HOURS` | The weather mart's completeness gate (default 22) |

## References

- `docs/high-level-design.md`: layers are rebuildable; failures page through Loki and Grafana
- `docs/DEPLOYMENT.md`: the gold container and the schedules to enable
