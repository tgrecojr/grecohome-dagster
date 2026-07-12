# syntax=docker/dockerfile:1.25@sha256:0adf442eae370b6087e08edc7c50b552d80ddf261576f4ebd6421006b2461f12
# Per-subject Dagster code-location image for the LOCATION data subject.
# Build context is the repo root (the workspace uv.lock lives there):
#   docker build -f packages/location/Dockerfile -t grecohome-dagster-location .
#
# Image built like every other code location (runs as `nonroot`). At RUNTIME set the
# user to uid 1000 (e.g. compose `user: "1000:998"`): the relay stages files mode 0600
# owned by uid 1000, so only uid 1000 can read them. Mount RELAY_CAPTURE_DIR read-only;
# the relay is its sole writer/cleaner. See docs/DEPLOYMENT.md.

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM cgr.dev/chainguard/python:latest-dev@sha256:727c118ee34aab194fa2b25c0116f669fc1459fd6bda6e2f570a15c75c9fda4e AS builder

USER root
WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Dependency layer: lock + this subject's member manifests only (uv resolves the
# grecohome-location closure -- core + location -- from the frozen lock).
COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/pyproject.toml
COPY packages/location/pyproject.toml packages/location/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package grecohome-location

# Source layer: install the workspace members in grecohome-location's closure.
COPY packages/core packages/core
COPY packages/location packages/location
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package grecohome-location

# =============================================================================
# Stage 2: Runtime (distroless)
# =============================================================================
FROM cgr.dev/chainguard/python:latest@sha256:4d908c6a44ba22460e34a2f6dd665b8fcb82bd3e6c887e749bd6fef243e10094

LABEL org.opencontainers.image.title="grecohome-dagster-location"
LABEL org.opencontainers.image.description="Location data subject: bronze-only Dagster code location"
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
     "-m", "grecohome_location.dagster.definitions"]
