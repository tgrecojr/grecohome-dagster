---
parent: high-level-design
prefix: CORE
---

# Shared core

## Context

`grecohome_core` is the shared, table-agnostic library the silver and gold code locations are
built from. Under the arrow are two parts:

| Component | Doc | Spec prefix | Owns |
|---|---|---|---|
| Check helpers | [checks/checks-design.md](checks/checks-design.md) | `CORE-CHECKS` | `alerting_check` (ERROR-check paging), the checks-only job and schedule builders, `table_freshness_check` |
| Silver helpers | [silver/silver-design.md](silver/silver-design.md) | `CORE-SILVER` | `grecohome_core.silver`: the DuckDB connection, the sidecar-safe bronze reader, the JSON-path and dedup SQL idioms, the `protected_root`-guarded atomic Parquet writer |

Each table decides **how** it uses these: which bronze collections it reads, its dedup key and
recency, which checks it carries and at what severity. Those choices are recorded in the
table's own design under `docs/intent/silver/` or `docs/intent/gold/`. The core docs define
only what the shared code guarantees.

The bronze files the silver helpers read are written outside this repo by WearStream, to the
layout the HLD records as a read contract (HLD: *Bronze is an external, read-only input*). A
change to that layout is a cross-system change: it reaches the silver reader before any table.

**Contracts flow one way.** Silver and gold depend on core; core never depends on a table. A
change to a core guarantee is a cross-segment change. Before it lands, every silver and gold
spec that relies on the guarantee must be reviewed. Examples: the reader's sidecar exclusion,
which every table relies on to parse only payloads, and the 36-hour table-freshness limit,
which the silver and gold checks schedules are timed against.

**Not under the arrow yet:** logging (`logging_config`, whose no-locals traceback guarantee is
the HLD decision *Logs never carry credentials*) and settings (`config`). Layer docs describe how
they use these.
