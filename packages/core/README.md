# grecohome-core

Shared, source-agnostic framework for grecohome data-subject pipelines.

Houses the components every data subject reuses:

- **`bronze/capture.py`** — atomic, append-only raw-payload capture with content-hash dedup.
- **`checks/`** — the bronze asset-check families and the checks-only job/schedule.
- **`silver/`** — DuckDB connection, sidecar-safe bronze readers, dedup idiom, atomic Parquet writer.
- **`config.py`** — `BaseSubjectSettings` (pydantic-settings base).
- **`logging_config.py`** — structlog/JSON logging setup.
- **`dagster/helpers.py`** — partition + scheduling helpers.

Subjects (e.g. `grecohome-soil`) and the silver/gold layers depend on this package via the
uv workspace.
