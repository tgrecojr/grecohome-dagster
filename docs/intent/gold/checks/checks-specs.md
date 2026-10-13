# Gold layer checks — EARS specs

Design: [checks-design.md](checks-design.md). Prefix `GOLD-CHECKS`. Facet: `JOB`.

## Checks-only job

- [x] **GOLD-CHECKS-JOB-001**: The system shall define `gold_checks_job` to run the asset checks of every gold mart without materializing any asset.
- [x] **GOLD-CHECKS-JOB-002**: The system shall define a `gold_checks_daily` schedule that runs `gold_checks_job` daily at 08:00 UTC with a default status of running.
