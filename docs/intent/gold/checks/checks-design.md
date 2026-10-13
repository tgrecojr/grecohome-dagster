---
parent: gold
prefix: GOLD-CHECKS
---

# Gold layer checks

## Context

Each gold mart owns its checks (uniqueness, ranges, coverage, freshness). What spans the marts
lives here: the **checks-only job and schedule** that run every gold check daily, independent of
the rebuilds.

## The checks-only job

`gold_checks_job` runs every gold mart's asset checks, freshness included, without materializing
anything. `gold_checks_daily` runs it daily at **08:00 UTC**, after the 07:30 and 07:40 rebuilds,
and **starts running**, as every checks schedule does. It uses no concurrency pool, and it's built
with core's checks-only job and schedule builders.

A mart's own job also runs that mart's checks. The daily checks job is what catches a mart whose
rebuild has stopped or keeps failing.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Home of the checks job | This leaf | Each mart's schedule spec; the parent | The job spans every mart, and the parent owns no specs. |
| Time | 08:00 UTC | 07:00 with silver's | Gold rebuilds finish around 07:45; checking at 08:00 sees today's marts. |

## References

- `packages/gold/src/grecohome_gold/dagster/schedules.py`
