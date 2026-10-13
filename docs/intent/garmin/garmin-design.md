---
parent: high-level-design
prefix: GARMIN
---

# Garmin bronze subject

## Context and Design Philosophy

`grecohome_garmin` is the bronze code location for Garmin Connect. It pulls every allowlisted
catalog endpoint through the `garminconnect` library and writes what Garmin returned to
`BRONZE_ROOT/garmin/<collection>/dt=<YYYY-MM-DD>/` through the shared core writer
(`grecohome_core.bronze.capture_bronze`). Silver reads these files. Nothing in this package
transforms or interprets the data.

Garmin data for a given day is **not final when the day ends**. The watch syncs late, Garmin
computes some daily aggregates hours or days later, and summaries are restated as more data
arrives. In bronze, the second capture of a day differs from the first for most of the
important daily collections (`user_summary`, `spo2`, `respiration`, `steps_intraday`,
`stress`, `body_battery`, `sleep`). Garmin also back-fills **whole batches of days** when a
device syncs after a gap: one sync fills in sleep, steps and heart rate for every day since the
last sync, and observed batches reach at least seven days back. The design therefore follows the HLD tenet *Completeness over economy*:
recent days are **re-captured every day** for a trailing window, and every capture that
differs from what bronze already holds is kept.

Storage is controlled by the core writer's **skip-identical** check rather than by capturing
less. This follows the HLD tenet *Shared core, source-specific overrides*: Garmin uses the
same re-capture-plus-skip-identical policy as Whoop and USCRN. The Garmin-specific override is
which captures may be skipped, described below.


## Components

| Component | Doc | Spec prefix | Owns |
|---|---|---|---|
| Capture | [capture/capture-design.md](capture/capture-design.md) | `GARMIN-CAPTURE` | Collections and partitions, the daily re-capture window, bronze writes and skip-identical, the silver contract, bronze-check guarantees, recovery, catalog and selection |
| Authentication | [auth/auth-design.md](auth/auth-design.md) | `GARMIN-AUTH` | Login, the token store and its guards, the token-store health check, the deployment rule for `GARMINTOKENS` |

The two meet at the Dagster resource: each run logs in once (owned by authentication) and
every capture op in the run uses that client.

## References

- `docs/high-level-design.md`: tenets and the bronze re-capture decision
- `docs/adr/0004-garmin-port.md`: the original Garmin port decision record
