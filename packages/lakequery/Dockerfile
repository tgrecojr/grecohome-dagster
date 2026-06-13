# syntax=docker/dockerfile:1.24@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89
# Read-only DuckDB-over-Parquet HTTP query service (Grafana backend for the lake).
# Build context is the repo root:
#   docker build -f packages/lakequery/Dockerfile -t grecohome-dagster-lakequery .

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM cgr.dev/chainguard/python:latest-dev@sha256:ddd3811dcbef56aa9f3882ae16fdc2920174ac6028c12e76cfb64c1d37b7abe2 AS builder

USER root
WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

COPY pyproject.toml uv.lock ./
COPY packages/lakequery/pyproject.toml packages/lakequery/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package grecohome-lakequery

COPY packages/lakequery packages/lakequery
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package grecohome-lakequery

# =============================================================================
# Stage 2: Runtime (distroless)
# =============================================================================
FROM cgr.dev/chainguard/python:latest@sha256:ffc3de748e31707704da3ec6566ebaf420a7d6a072ce3ac7fbb9491a6ac69ffd

LABEL org.opencontainers.image.title="grecohome-dagster-lakequery"
LABEL org.opencontainers.image.description="Read-only DuckDB-over-Parquet HTTP query service for the data lake"
LABEL org.opencontainers.image.source="https://github.com/tgrecojr/grecohome-dagster"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --chown=nonroot:nonroot --from=builder /app/.venv /app/.venv
COPY --chown=nonroot:nonroot --from=builder /app/packages /app/packages

USER nonroot

EXPOSE 9999

# HTTP health check against /healthz (no shell/curl in distroless — use the venv python).
HEALTHCHECK --interval=60s --timeout=10s --start-period=15s --retries=3 \
    CMD ["/app/.venv/bin/python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:9999/healthz').status==200 else 1)"]

# Use the venv interpreter explicitly: the base image's default ENTRYPOINT is the
# *system* /usr/bin/python (which lacks the workspace packages); PATH can't redirect
# an absolute entrypoint, so point at the venv python directly.
ENTRYPOINT ["/app/.venv/bin/python"]
CMD ["-m", "grecohome_lakequery.server"]
