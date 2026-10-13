---
parent: silver
prefix: SILVER-CHECKS
---

# Silver layer checks

## Context

Each silver family owns its tables' checks (uniqueness, ranges, coverage). Two pieces span every
family and live here:

- the **checks-only job and schedule** that run every silver check daily, independent of the
  rebuilds;
- the **table freshness check**, built the same way for every table.

## The checks-only job

`silver_checks_job` runs every silver table's asset checks (every family's checks list, freshness
included) without materializing anything. `silver_checks_daily` runs it daily at **07:00 UTC**,
after the 06:00–06:55 rebuilds, and **starts running**, as every checks schedule does (HLD:
checks schedules start running). It uses no concurrency pool.

A rebuild's own job also runs that table's checks. The daily checks job is what catches a table
whose rebuild has stopped or keeps failing: a per-run check never runs for an asset that doesn't
materialize.

It's built with core's checks-only job and schedule builders (`build_bronze_checks_job`,
`build_bronze_checks_schedule`), which set the running default status.

## Table freshness

`table_freshness_check(asset, path_fn)` builds an ERROR check named **`<asset>_freshness`** on a
table's Parquet:

- it **fails when the file was last modified more than 36 hours** before the check runs, and
  passes at 36 hours or less, reporting the age and the limit;
- it **fails at ERROR when the file doesn't exist**, like every silver check;
- it alerts through `alerting_check`;
- `path_fn` is called when the check runs, so the silver root is read at check time.

Freshness measures the table's last write, not the newest data in it: a day with nothing new still
rewrites the table. Why 36 hours, and why freshness rather than run-failure alerts, is the silver
layer's rule (see the [silver design](../silver-design.md), *Asset checks*).

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Home of the checks job | This leaf | Each family's schedule spec; the parent | The job spans every family, and the parent owns no specs. |
| Freshness construction | One builder, a check per table | Hand-written per table | Identical semantics everywhere; a family adds one line. |
| Freshness signal | Parquet modification time | Dagster materialization records | Readable from the check without the instance; the atomic writer's rename sets it at each successful rebuild. |

## References

- `packages/silver/src/grecohome_silver/dagster/schedules.py` (`silver_checks_job`,
  `silver_checks_daily`), `dagster/freshness.py`
