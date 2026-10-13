# Architecture Decision Records

Short records of the load-bearing decisions behind `grecohome-dagster`.

Decisions about the bronze layer, Dagster deployment, Whoop, Garmin, Soil and Lingo are recorded in the LID
design docs instead: bronze-only capture, UTC partitions, Dagster pins and one image per subject
in the [HLD](../high-level-design.md); the plaintext token file in the
[Whoop token lifecycle](../intent/whoop/auth/auth-design.md); the Garmin port in the
[Garmin design](../intent/garmin/garmin-design.md); the Soil/USCRN port in the
[Soil design](../intent/soil/soil-design.md); the Lingo port in the
[Lingo design](../intent/lingo/lingo-design.md); the silver layer and silver sleep in the
[silver design](../intent/silver/silver-design.md), and the silver sleep, glucose and cycles
(recovery, strain) tables in their designs under `docs/intent/silver/`. The records below cover
the silver tables and gold layer not yet under LID.

- [0009 — Silver workouts: per-activity grain, dedup on activityId](0009-silver-workouts.md)
- [0011 — Gold layer: daily wellness mart as the spine](0011-gold-daily-wellness.md)
