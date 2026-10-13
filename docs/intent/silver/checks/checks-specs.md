# Silver layer checks — EARS specs

Design: [checks-design.md](checks-design.md). Prefix `SILVER-CHECKS`. Facet: `JOB` (the
checks-only job and schedule). The freshness builder is specified in core (`CORE-CHECKS-TABLE`).

## Checks-only job

- [x] **SILVER-CHECKS-JOB-001**: The system shall define `silver_checks_job` to run the asset checks of every silver table, from every family's checks list, without materializing any asset.
- [x] **SILVER-CHECKS-JOB-002**: The system shall define a `silver_checks_daily` schedule that runs `silver_checks_job` daily at 07:00 UTC with a default status of running.
