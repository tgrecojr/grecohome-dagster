# syntax=docker/dockerfile:1.27@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# Per-subject Dagster code-location image for the GARMIN data subject.
# Build context is the repo root (the workspace uv.lock lives there):
#   docker build -f packages/garmin/Dockerfile -t grecohome-dagster-garmin .

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM cgr.dev/chainguard/python:latest-dev@sha256:c07959e43e3edece176d20c32428a53014379790169af4ea5de1dc47277eba75 AS builder

USER root
WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Dependency layer: lock + this subject's member manifests only (uv resolves the
# grecohome-garmin closure -- core + garmin -- from the frozen lock; other members
# aren't required to be present).
COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/pyproject.toml
COPY packages/garmin/pyproject.toml packages/garmin/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package grecohome-garmin

# Source layer: install the workspace members in grecohome-garmin's closure.
COPY packages/core packages/core
COPY packages/garmin packages/garmin
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package grecohome-garmin

# =============================================================================
# Stage 2: Runtime (distroless)
# =============================================================================
FROM cgr.dev/chainguard/python:latest@sha256:3f411b4aac1ed9cd6c9dec1486f92a8898737d9d6f95488dd727ca7d6f5254ee

LABEL org.opencontainers.image.title="grecohome-dagster-garmin"
LABEL org.opencontainers.image.description="Garmin data subject: bronze-only Dagster code location"
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
     "-m", "grecohome_garmin.dagster.definitions"]
