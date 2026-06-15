# syntax=docker/dockerfile:1.24@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89
# Per-subject Dagster code-location image for the WHOOP data subject.
# Build context is the repo root (the workspace uv.lock lives there):
#   docker build -f packages/whoop/Dockerfile -t grecohome-dagster-whoop .
#
# Stage 1: builder  - resolve + install the workspace with uv (Chainguard Wolfi dev)
# Stage 2: runtime  - Chainguard distroless Python; runs the gRPC code server

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
FROM cgr.dev/chainguard/python:latest@sha256:33334c2bf93fd99e6b3c42b518cc44bb5277a954826cdb4f9275ef95818d7eb7

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
