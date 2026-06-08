# grecohome-core

Shared, source-agnostic framework for grecohome data-subject pipelines.

Houses the components every data subject reuses:

- **`bronze/capture.py`** — atomic, append-only raw-payload capture with content-hash dedup.
- **`http/rate_limiter.py`** — in-process sliding-window API rate limiter.
- **`tokens/file_store.py`** — atomic plaintext-JSON OAuth token store.
- **`config.py`** — `BaseSubjectSettings` (pydantic-settings base).
- **`logging_config.py`** — structlog/JSON logging setup.
- **`dagster/helpers.py`** — partition + scheduling + async-bridge helpers.

Subjects (e.g. `grecohome-whoop`) depend on this package via the uv workspace.
