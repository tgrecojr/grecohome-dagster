# Gold layer

The gold layer is **analysis** — derived marts that answer questions, built from silver. Where
silver is one faithful typed row per event, gold joins across sources and rolls up to a useful
grain (per day, per week, …). Gold reads `SILVER_ROOT` Parquet and writes marts under `GOLD_ROOT`;
like silver it is *derived and fully rebuildable*, and it never writes under `SILVER_ROOT`. Silver
(and bronze beneath it) remain the sources of truth.

Tables today:
- **`gold_daily_wellness`** — one row per local day joining sleep + recovery + daily workout load +
  daily glucose summary. The spine other analyses build on. See [Daily wellness](#daily-wellness)
  below.

## Invariants

- **Rebuildable, not precious.** Every materialization **fully overwrites** its output. Gold is a
  pure projection of *current* silver.
- **Outside silver.** Gold writes only under `GOLD_ROOT`; the atomic writer **refuses** any path
  inside `SILVER_ROOT` (the shared `write_parquet_atomic` guard, `protected_root=SILVER_ROOT`).
  Silver is mounted read-only in the gold container.
- **Own code location / image.** `packages/gold` (`grecohome_gold`) is its own gRPC code location,
  mirroring the layer-per-image pattern (bronze subjects, silver). Depends only on
  `grecohome-core` + DuckDB.
- **Cross-location lineage.** The mart declares its silver upstreams by `AssetKey`; reads are
  filesystem reads of `SILVER_ROOT`, not gRPC calls.
- **Off the API pools**, runs after silver.

## Storage & format

- **Parquet** (zstd), via DuckDB `COPY`, written atomically (temp + `os.replace`).
- **Layout** under `GOLD_ROOT`:

```
{GOLD_ROOT}/wellness/daily_wellness.parquet   # one row per local day
```

# Daily wellness

`gold_daily_wellness` is the foundational mart — **one row per local calendar day** over a
**continuous date spine** (so gaps are explicit for rolling/streak analysis), left-joining the four
silver tables:

| Source | Join | Aggregation |
|---|---|---|
| `silver_sleep` | `night_date = day` (1:1) | none (curated Garmin + Whoop columns) |
| `silver_recovery` | `recovery_date = day` | deduped to one/day (latest `created_at`) |
| `silver_workouts` | `activity_date = day` | per-day: count + total duration/distance/calories |
| `silver_glucose` | `reading_date = day` | per-day: mean / min / max / std + time-in-range |

Notes:
- **Recovery dedup.** Recovery's `recovery_date` is the UTC date of `created_at` and is occasionally
  2-per-date, so the mart keeps the latest `created_at` per day. (The precise per-sleep link lives
  in `silver_recovery` via `sleep_id`/`cycle_id`.)
- **Glucose time-in-range** uses **70–140 mg/dL** (non-diabetic / metabolic-health band):
  `glucose_tir_pct`, with `glucose_pct_below` (<70) and `glucose_pct_above` (>140).
- **Workouts** can be many per day → aggregated; `workout_count` is 0 (not null) on days with none.
- **Provenance:** `has_sleep` / `has_recovery` / `has_workout` / `has_glucose` make every null
  explainable.

### Schema (selected columns)
`day` (DATE, PK); sleep: `garmin_sleep_score`, `garmin_total_min`, `garmin_rhr`,
`whoop_performance_pct`, `whoop_efficiency_pct`; recovery: `recovery_score`,
`resting_heart_rate`, `hrv_rmssd_milli`, `spo2_percentage`; workouts: `workout_count`,
`workout_total_min`, `workout_distance_km`, `workout_calories`; glucose: `glucose_readings`,
`glucose_mean`, `glucose_min`, `glucose_max`, `glucose_std`, `glucose_tir_pct`,
`glucose_pct_below`, `glucose_pct_above`; `has_*` flags.

### Asset checks
| Check | Severity | What |
|---|---|---|
| `wellness_day_unique_nonnull` | ERROR | one row per `day`, never null |
| `wellness_value_ranges` | ERROR | TIR 0–100, glucose mean / recovery score / counts in bounds |
| `wellness_coverage` | WARN | per-source day coverage; fails only if the mart is empty |

# Operations

## Scheduling
- `gold_wellness_daily` (07:30 UTC) rebuilds the mart **after** silver's daily rebuilds (≤ 06:50)
  and silver checks (07:00).
- `gold_checks_daily` (08:00 UTC) runs the gold checks independently.

Both off by default; enable in the UI. Rebuild on demand with
`dagster job execute --job gold_wellness_job`.

## Deployment
See [DEPLOYMENT.md → Gold](DEPLOYMENT.md#gold-cross-layer-marts) and
[ENV_TEMPLATE.md](ENV_TEMPLATE.md): `SILVER_ROOT` mounted **read-only**, `GOLD_ROOT` writable on a
separate volume, reserved `GOLD_MONITOR_DIR`.
