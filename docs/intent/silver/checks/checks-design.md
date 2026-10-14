---
parent: silver
prefix: SILVER-CHECKS
---

# Silver layer checks

## Context

Each silver family owns its tables' checks (uniqueness, ranges, coverage, freshness). What spans
every family lives here: the **checks-only job and schedule** that run every silver check daily,
independent of the rebuilds.

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

Every silver table's freshness check is built with core's `table_freshness_check` (specified in
the [core checks design](../../core/checks/checks-design.md), *Table freshness*), which gold
shares. Each family's specs require its tables' freshness checks; this leaf's job spec requires
that every silver table has one in the checks job. Why 36 hours, and why freshness rather than
run-failure alerts alone, is the silver layer's rule (see the [silver design](../silver-design.md),
*Asset checks*).

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Home of the checks job | This leaf | Each family's schedule spec; the parent | The job spans every family, and the parent owns no specs. |
| Freshness builder | Core's `table_freshness_check`, shared with gold | A silver-only builder | Silver and gold rebuild the same way; one builder keeps them identical. |

## References

- `packages/silver/src/grecohome_silver/dagster/schedules.py` (`silver_checks_job`,
  `silver_checks_daily`)
