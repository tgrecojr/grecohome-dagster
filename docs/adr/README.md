# Architecture Decision Records

Short records of the load-bearing decisions behind `grecohome-dagster`.

Decisions about the bronze layer, Dagster deployment, Whoop, Garmin, Soil and Lingo are recorded in the LID
design docs instead: bronze-only capture, UTC partitions, Dagster pins and one image per subject
in the [HLD](../high-level-design.md); the plaintext token file in the
[Whoop token lifecycle](../intent/whoop/auth/auth-design.md); the Garmin port in the
[Garmin design](../intent/garmin/garmin-design.md); the Soil/USCRN port in the
[Soil design](../intent/soil/soil-design.md); the Lingo port in the
[Lingo design](../intent/lingo/lingo-design.md); the silver layer and silver sleep in the
[silver design](../intent/silver/silver-design.md) and
[silver sleep design](../intent/silver/sleep/sleep-design.md). The records below cover the
silver tables and gold layer not yet under LID.

- [0008 — Silver glucose: per-reading grain, dedup on the UTC instant](0008-silver-glucose.md)
- [0009 — Silver workouts: per-activity grain, dedup on activityId](0009-silver-workouts.md)
- [0010 — Silver recovery: per-cycle grain, dedup on cycle_id, joins to sleep](0010-silver-recovery.md)
- [0011 — Gold layer: daily wellness mart as the spine](0011-gold-daily-wellness.md)
