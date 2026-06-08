# lingo (scaffold)

Placeholder for the **Lingo** (Abbott Lingo CGM) data subject. Not yet implemented.

When built, this becomes a uv workspace member (`packages/lingo`) following the same
shape as `packages/whoop`: a bronze-only Dagster code location depending on
`grecohome-core`, shipped as its own per-subject gRPC code-location image.

To activate: add `"packages/lingo"` to `[tool.uv.workspace].members` in the root
`pyproject.toml` and add `lingo` to the CI build matrix.
