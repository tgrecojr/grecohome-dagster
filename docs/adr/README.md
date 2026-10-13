# Architecture Decision Records

Short records of the load-bearing decisions behind `grecohome-dagster`.

Decisions about the bronze layer, Dagster deployment, Whoop and Garmin are recorded in the LID
design docs instead: bronze-only capture, UTC partitions, Dagster pins and one image per subject
in the [HLD](../high-level-design.md); the plaintext token file in the
[Whoop token lifecycle](../intent/whoop/auth/auth-design.md); the Garmin port in the
[Garmin design](../intent/garmin/garmin-design.md). The records below cover subjects and layers
not yet under LID.

- [0005 — Lingo port: sensor + dynamic partitions, service-account auth](0005-lingo-port.md)
- [0006 — Soil/USCRN port: daily row-slice + dedup over a growing year file](0006-soil-port.md)
- [0007 — Silver sleep: two co-equal sources, FULL OUTER JOIN, neither authoritative](0007-silver-sleep.md)
- [0008 — Silver glucose: per-reading grain, dedup on the UTC instant](0008-silver-glucose.md)
- [0009 — Silver workouts: per-activity grain, dedup on activityId](0009-silver-workouts.md)
- [0010 — Silver recovery: per-cycle grain, dedup on cycle_id, joins to sleep](0010-silver-recovery.md)
- [0011 — Gold layer: daily wellness mart as the spine](0011-gold-daily-wellness.md)
