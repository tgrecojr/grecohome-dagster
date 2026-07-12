# syntax=docker/dockerfile:1.25@sha256:0adf442eae370b6087e08edc7c50b552d80ddf261576f4ebd6421006b2461f12
# Per-subject Dagster code-location image for the WHOOP data subject.
# Build context is the repo root (the workspace uv.lock lives there):
#   docker build -f packages/whoop/Dockerfile -t grecohome-dagster-whoop .
#
# Stage 1: builder  - resolve + install the workspace with uv (Chainguard Wolfi dev)
# Stage 2: runtime  - Chainguard distroless Python; runs the gRPC code server

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

# Dependency layer: copy only the lock + member manifests so third-party deps
# cache independently of source changes. --no-install-workspace installs the
# external deps without building our packages (no source/README needed yet).
COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/pyproject.toml
COPY packages/whoop/pyproject.toml packages/whoop/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package grecohome-whoop

# Source layer: copy the packages and install the workspace members.
COPY packages/core packages/core
COPY packages/whoop packages/whoop
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package grecohome-whoop

# =============================================================================
# Stage 2: Runtime (distroless)
# =============================================================================
FROM cgr.dev/chainguard/python:latest@sha256:4d908c6a44ba22460e34a2f6dd665b8fcb82bd3e6c887e749bd6fef243e10094

LABEL org.opencontainers.image.title="grecohome-dagster-whoop"
LABEL org.opencontainers.image.description="Whoop data subject: bronze-only Dagster code location"
LABEL org.opencontainers.image.source="https://github.com/tgrecojr/grecohome-dagster"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# The venv installs the workspace members editable, so both the venv and the
# package sources are required at runtime.
COPY --chown=nonroot:nonroot --from=builder /app/.venv /app/.venv
COPY --chown=nonroot:nonroot --from=builder /app/packages /app/packages

USER nonroot

EXPOSE 4000

# gRPC health check (runs the binary directly; independent of ENTRYPOINT, no shell).
HEALTHCHECK --interval=60s --timeout=10s --start-period=20s --retries=3 \
    CMD ["dagster", "api", "grpc-health-check", "-p", "4000"]

# The Chainguard base image's ENTRYPOINT is `python`; override it so the CMD runs
# the dagster CLI rather than being appended to python.
ENTRYPOINT ["dagster"]

# Serve the Whoop code location; the host daemon/webserver connects to :4000.
CMD ["code-server", "start", "-h", "0.0.0.0", "-p", "4000", \
     "-m", "grecohome_whoop.dagster.definitions"]
