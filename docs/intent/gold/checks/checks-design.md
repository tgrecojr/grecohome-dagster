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
anything. `gold_checks_daily` runs it daily at **08:05 UTC**, after the 07:30 and 07:40 rebuilds,
and **starts running**, as every checks schedule does. It uses no concurrency pool and runs
in-process, as core's checks-only job and schedule builders make it.

A mart's own job also runs that mart's checks. The daily checks job is what catches a mart whose
rebuild has stopped or keeps failing.

## Decisions & Alternatives

| Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|
| Home of the checks job | This leaf | Each mart's schedule spec; the parent | The job spans every mart, and the parent owns no specs. |
| Time | 08:05 UTC | 07:05 with silver's; 08:00 | Gold rebuilds finish around 07:45; checking after 08:00 sees today's marts. Minute 5 keeps it off the host's cron minute (core checks design, *Checks-only job and schedule*). |

## References

- `packages/gold/src/grecohome_gold/dagster/schedules.py`
