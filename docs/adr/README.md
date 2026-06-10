# Architecture Decision Records

Short records of the load-bearing decisions behind `grecohome-dagster`.

- [0001 — Bronze-only, daily UTC partitions](0001-bronze-only.md)
- [0002 — Pin Dagster to the host; one code-location image per subject](0002-dagster-pins.md)
- [0003 — OAuth tokens in a plaintext-JSON file](0003-token-file.md)
- [0004 — Garmin port: per-collection assets, no dedup, delegated auth](0004-garmin-port.md)
- [0005 — Lingo port: sensor + dynamic partitions, service-account auth](0005-lingo-port.md)
- [0006 — Soil/USCRN port: daily row-slice + dedup over a growing year file](0006-soil-port.md)
