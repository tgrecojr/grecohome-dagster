# syntax=docker/dockerfile:1.27@sha256:bde3983e9c939224420ddaf6b784cc30e09b035a4dea01f581230c50809f372e
# Cross-layer Dagster code-location image for the GOLD layer.
# Build context is the repo root (the workspace uv.lock lives there):
#   docker build -f packages/gold/Dockerfile -t grecohome-dagster-gold .
#
# Stage 1: builder  - resolve + install the workspace with uv (Chainguard Wolfi dev)
# Stage 2: runtime  - Chainguard distroless Python; runs the gRPC code server

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM cgr.dev/chainguard/python:latest-dev@sha256:eb0d45dfc69fecb471d2eaee7a8eea281bf860578ef44cb85db1bfa8165c47fe AS builder

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
COPY packages/gold/pyproject.toml packages/gold/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package grecohome-gold

# Source layer: copy the packages and install the workspace members.
COPY packages/core packages/core
COPY packages/gold packages/gold
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package grecohome-gold

# =============================================================================
# Stage 2: Runtime (distroless)
# =============================================================================
FROM cgr.dev/chainguard/python:latest@sha256:565af762d7f3efedc4e60d7ac7815e41588211d3f5757be33d8303e915ee6c72

LABEL org.opencontainers.image.title="grecohome-dagster-gold"
LABEL org.opencontainers.image.description="Gold layer: analysis marts Dagster code location"
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

# Serve the gold code location; the host daemon/webserver connects to :4000.
CMD ["code-server", "start", "-h", "0.0.0.0", "-p", "4000", \
     "-m", "grecohome_gold.dagster.definitions"]
