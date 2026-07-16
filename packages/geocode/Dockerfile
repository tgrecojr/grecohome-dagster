# syntax=docker/dockerfile:1.25@sha256:0adf442eae370b6087e08edc7c50b552d80ddf261576f4ebd6421006b2461f12
# Per-subject Dagster code-location image for the GEOCODE (Photon reverse-geocode cache)
# data subject. Build context is the repo root (the workspace uv.lock lives there):
#   docker build -f packages/geocode/Dockerfile -t grecohome-dagster-geocode .

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM cgr.dev/chainguard/python:latest-dev@sha256:bee63d1fd86c4b31dd2df85bb383be142e8067486e3d469265edb850af93e8e4 AS builder

USER root
WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Dependency layer: lock + this subject's member manifests only (uv resolves the
# grecohome-geocode closure -- core + geocode -- from the frozen lock).
COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/pyproject.toml
COPY packages/geocode/pyproject.toml packages/geocode/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package grecohome-geocode

# Source layer: install the workspace members in grecohome-geocode's closure.
COPY packages/core packages/core
COPY packages/geocode packages/geocode
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package grecohome-geocode

# =============================================================================
# Stage 2: Runtime (distroless)
# =============================================================================
FROM cgr.dev/chainguard/python:latest@sha256:ce9aaca1f826f7f963cd031e98f8c19f993b1843096d395ea919b646e72cb8de

LABEL org.opencontainers.image.title="grecohome-dagster-geocode"
LABEL org.opencontainers.image.description="Geocode (Photon reverse-geocode cache) data subject: bronze-only Dagster code location"
LABEL org.opencontainers.image.source="https://github.com/tgrecojr/grecohome-dagster"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --chown=nonroot:nonroot --from=builder /app/.venv /app/.venv
COPY --chown=nonroot:nonroot --from=builder /app/packages /app/packages

# Image builds as nonroot like the other subjects. At runtime the host runs it as
# uid 1000 (e.g. user: "1000:998") so it can read the location bronze the promoter
# (also uid 1000) wrote -- see docs/GEOCODE.md.
USER nonroot

EXPOSE 4000

HEALTHCHECK --interval=60s --timeout=10s --start-period=20s --retries=3 \
    CMD ["dagster", "api", "grpc-health-check", "-p", "4000"]

# Chainguard's base ENTRYPOINT is `python`; override so CMD runs the dagster CLI.
ENTRYPOINT ["dagster"]

CMD ["code-server", "start", "-h", "0.0.0.0", "-p", "4000", \
     "-m", "grecohome_geocode.dagster.definitions"]
