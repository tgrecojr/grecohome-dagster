# garmin (scaffold)

Placeholder for the **Garmin** data subject. Not yet implemented.

When built, this becomes a uv workspace member (`packages/garmin`) following the same
shape as `packages/whoop`: a bronze-only Dagster code location depending on
`grecohome-core`, shipped as its own per-subject gRPC code-location image.

To activate: add `"packages/garmin"` to `[tool.uv.workspace].members` in the root
`pyproject.toml` and add `garmin` to the CI build matrix.
