# grecohome-core

Shared, table-agnostic library for the grecohome silver and gold code locations.

- **`checks/`** — `alerting_check` (ERROR checks page via Loki), the checks-only
  job/schedule builders, and `table_freshness_check`.
- **`silver/`** — DuckDB connection, sidecar-safe bronze reader, dedup idiom, atomic
  Parquet writer.
- **`config.py`** — `BaseSubjectSettings` (pydantic-settings base for bronze readers).
- **`logging_config.py`** — structlog/JSON logging setup.

The silver and gold layers depend on this package via the uv workspace. Bronze itself is
written outside this repo, by WearStream.
