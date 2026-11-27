# syntax=docker/dockerfile:1.29@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183
# Per-subject Dagster code-location image for the SOIL (NOAA USCRN) data subject.
# Build context is the repo root (the workspace uv.lock lives there):
#   docker build -f packages/soil/Dockerfile -t grecohome-dagster-soil .

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM cgr.dev/chainguard/python:latest-dev@sha256:69e90282a0f83fcd7a8f3e94e93755f6d3e340280639c2bf6440522085c6ae90 AS builder

USER root
WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Dependency layer: lock + this subject's member manifests only (uv resolves the
# grecohome-soil closure -- core + soil -- from the frozen lock).
COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/pyproject.toml
COPY packages/soil/pyproject.toml packages/soil/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package grecohome-soil

# Source layer: install the workspace members in grecohome-soil's closure.
COPY packages/core packages/core
COPY packages/soil packages/soil
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package grecohome-soil

# =============================================================================
# Stage 2: Runtime (distroless)
# =============================================================================
FROM cgr.dev/chainguard/python:latest@sha256:9c4bfa852765da1bfb86d4d500c3333db1c0929c4b9845a90ca7d199d857f90c

LABEL org.opencontainers.image.title="grecohome-dagster-soil"
LABEL org.opencontainers.image.description="Soil (NOAA USCRN) data subject: bronze-only Dagster code location"
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
     "-m", "grecohome_soil.dagster.definitions"]
