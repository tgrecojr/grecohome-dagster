---
parent: high-level-design
prefix: CORE
---

# Shared core

## Context

`grecohome_core` is the shared, source-agnostic library every code location uses. Under the
arrow are the two parts that define the **bronze contract**, which every bronze subject
relies on, and the silver helpers every silver and gold table is built from:

| Component | Doc | Spec prefix | Owns |
|---|---|---|---|
| Bronze writer | [bronze/bronze-design.md](bronze/bronze-design.md) | `CORE-BRONZE` | `capture_bronze`: what may be captured, the bytes/encoding rules, layout and naming, the sidecar, skip-identical, the non-2xx and type guards, atomic and non-fatal writes |
| Bronze checks | [checks/checks-design.md](checks/checks-design.md) | `CORE-CHECKS` | `CollectionCheckConfig`, the four check families, ERROR-check alerting, the checks-only job and schedule |
| Silver helpers | [silver/silver-design.md](silver/silver-design.md) | `CORE-SILVER` | `grecohome_core.silver`: the DuckDB connection, sidecar-safe bronze readers, the JSON-path and dedup SQL idioms, the `protected_root`-guarded atomic Parquet writer |

Each subject decides **how** it uses these: which collections, skip-identical on or off per
capture path, which checks per collection, and tolerances. Those choices are recorded in the
subject's own design (`docs/intent/garmin/`, `docs/intent/whoop/`). The core docs define
only what the shared code guarantees.

**Contracts flow one way.** Subjects depend on core; core never depends on a subject. A
change to a core guarantee is a cross-segment change. Before it lands, every subject spec
that relies on the guarantee must be reviewed. Examples: skip-identical's one-file
comparison (GARMIN-CAPTURE-004…008), freshness reading sidecar times (GARMIN-CAPTURE-CHECKS-002),
and the 2xx-only rule (WHOOP-CAPTURE-004).

**Not under the arrow yet:** logging
(`logging_config`, though its no-locals guarantee is specified as WHOOP-AUTH-LOG-005), the
token file store and lock (`tokens`), the HTTP rate limiter (`http`), the Dagster helpers
(`dagster`) and settings (`config`). Subject docs describe how they use these.
