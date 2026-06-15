# syntax=docker/dockerfile:1.24@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89
# Per-subject Dagster code-location image for the LINGO data subject.
# Build context is the repo root (the workspace uv.lock lives there):
#   docker build -f packages/lingo/Dockerfile -t grecohome-dagster-lingo .

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM cgr.dev/chainguard/python:latest-dev@sha256:fa7db0816e4dd581d4b4a76edda591804a71788b6a46e46af0b435dad4552642 AS builder

USER root
WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Dependency layer: lock + this subject's member manifests only (uv resolves the
# grecohome-lingo closure -- core + lingo -- from the frozen lock).
COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/pyproject.toml
COPY packages/lingo/pyproject.toml packages/lingo/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package grecohome-lingo

# Source layer: install the workspace members in grecohome-lingo's closure.
COPY packages/core packages/core
COPY packages/lingo packages/lingo
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package grecohome-lingo

# =============================================================================
# Stage 2: Runtime (distroless)
# =============================================================================
FROM cgr.dev/chainguard/python:latest@sha256:ffc3de748e31707704da3ec6566ebaf420a7d6a072ce3ac7fbb9491a6ac69ffd

LABEL org.opencontainers.image.title="grecohome-dagster-lingo"
LABEL org.opencontainers.image.description="Lingo CGM data subject: bronze-only Dagster code location"
LABEL org.opencontainers.image.source="https://github.com/tgrecojr/grecohome-dagster"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --chown=nonroot:nonroot --from=builder /app/.venv /app/.venv
COPY --chown=nonroot:nonroot --from=builder /app/packages /app/packages

USER nonroot

EXPOSE 4000

HEALTHCHECK --interval=60s --timeout=10s --start-period=20s --retries=3 \
    CMD ["dagster", "api", "grpc-health-check", "-p", "4000"]

# Chainguard's base ENTRYPOINT is `python`; override so CMD runs the dagster CLI.
ENTRYPOINT ["dagster"]

CMD ["code-server", "start", "-h", "0.0.0.0", "-p", "4000", \
     "-m", "grecohome_lingo.dagster.definitions"]
