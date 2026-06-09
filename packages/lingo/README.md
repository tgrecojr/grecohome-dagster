# grecohome-lingo

The Lingo CGM data subject: a **bronze-only** Dagster code location that captures raw
glucose CSV files from a Google Drive folder. Ported from `glucose-loader` onto
`grecohome-core`.

Lingo has no API — glucose is exported from the Lingo iOS app and uploaded (sporadically)
to a Drive folder; **each file is a cumulative snapshot** (the full history to date). This
subject:

- Authenticates to Drive with a **service account** (mounted key JSON; the Drive folder is
  shared read-only with the SA's email — no interactive OAuth, no token refresh).
- Uses a **sensor + dynamic partitions** keyed by Drive `file_id`: each new file becomes a
  partition and is captured **exactly once** (Dagster's partition set replaces the old
  `ProcessedFile` table). One collection: `lingo/glucose`.
- Captures the raw CSV bytes to bronze (append-only); silver/parsing is downstream.

Ships as a per-subject gRPC **code-location image** (`grecohome-dagster-lingo`). See
`docs/DEPLOYMENT.md`.
