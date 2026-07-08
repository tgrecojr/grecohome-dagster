# Architecture Decision Records

Short records of the load-bearing decisions behind `grecohome-dagster`.

- [0001 — Bronze-only, daily UTC partitions](0001-bronze-only.md)
- [0002 — Pin Dagster to the host; one code-location image per subject](0002-dagster-pins.md)
- [0003 — OAuth tokens in a plaintext-JSON file](0003-token-file.md)
- [0004 — Garmin port: per-collection assets, no dedup, delegated auth](0004-garmin-port.md)
- [0005 — Lingo port: sensor + dynamic partitions, service-account auth](0005-lingo-port.md)
- [0006 — Soil/USCRN port: daily row-slice + dedup over a growing year file](0006-soil-port.md)
- [0007 — Silver sleep: two co-equal sources, FULL OUTER JOIN, neither authoritative](0007-silver-sleep.md)
- [0008 — Silver glucose: per-reading grain, dedup on the UTC instant](0008-silver-glucose.md)
- [0009 — Silver workouts: per-activity grain, dedup on activityId](0009-silver-workouts.md)
- [0010 — Silver recovery: per-cycle grain, dedup on cycle_id, joins to sleep](0010-silver-recovery.md)
- [0011 — Gold layer: daily wellness mart as the spine](0011-gold-daily-wellness.md)
- [0012 — Reverse-geocode enrichment: a bronze Photon cache + silver_location](0012-geocode-cache-silver-location.md)
