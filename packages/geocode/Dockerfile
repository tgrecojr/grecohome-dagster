# syntax=docker/dockerfile:1.26@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
# Per-subject Dagster code-location image for the GEOCODE (Photon reverse-geocode cache)
# data subject. Build context is the repo root (the workspace uv.lock lives there):
#   docker build -f packages/geocode/Dockerfile -t grecohome-dagster-geocode .

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM cgr.dev/chainguard/python:latest-dev@sha256:92b8a0af0e138d8f3d169ac3fc92fc691c9f66aa16a6e0fd8d429b645ec349f1 AS builder

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
FROM cgr.dev/chainguard/python:latest@sha256:5128c3823542cfb27fd83c42ef9ee3bbbf3fdd0eb9190802e5fc119974c5f776

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
