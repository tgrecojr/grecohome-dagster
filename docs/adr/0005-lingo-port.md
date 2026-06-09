# ADR 0005: Lingo port — sensor + dynamic partitions, service-account auth

## Status
Accepted.

## Context
Lingo (Abbott CGM) is the third data subject, ported from the standalone `glucose-loader`
service. Unlike Whoop and Garmin (the app pulls from a vendor API on a clock), Lingo has **no
API**. The user periodically and *sporadically* exports a CSV from the Lingo iOS app and uploads
it to a Google Drive folder; each export is a **cumulative** dump (all records to date, not a
delta). `glucose-loader` tracked which Drive files it had already processed in a Postgres
`ProcessedFile` table — incompatible with the bronze-only, Postgres-free direction.

## Decision
- **Reuse `grecohome-core`** for capture, settings, and logging. Lingo-specific logic
  (`drive`, `capture`) lives in `packages/lingo`. The source is **file bytes** (CSV), captured
  byte-for-byte into a single collection `lingo/glucose`.
- **Auth is a Google service account**, not OAuth. A mounted key JSON
  (`GDRIVE_SERVICE_ACCOUNT_PATH`) authenticates with the `drive.readonly` scope; the watched
  folder is shared read-only with the SA email, which is the *only* grant (no project IAM role).
  Core's `TokenFileStore` and any OAuth client are **not** used — there's no token to rotate.
- **File-arrival-driven via a sensor, not a schedule.** `lingo_drive_sensor` lists the folder
  every `GDRIVE_POLL_INTERVAL_MINUTES` and, for each Drive `file_id` not already a partition,
  adds a `DynamicPartitionsDefinition("lingo_files")` partition and requests a run
  (`run_key == file_id`). **The dynamic partition set is the "already captured" ledger** — it
  replaces the Postgres `ProcessedFile` table. The first tick captures the existing backlog;
  there is no `DailyPartitionsDefinition` grid and no `dagster backfill` path.
- **One asset** (`lingo_bronze_glucose`) partitioned on the Drive `file_id`: it fetches the file's
  metadata + bytes and captures once.
- **Content-hash dedup on** (`dedupe=True`). Capture-once-per-`file_id` is the first defense;
  content-hash dedup backstops a re-upload of an identical export under a new id. (Contrast Garmin,
  which is `dedupe=False` + capture-once on a date partition.)

## Consequences
- The bronze layout gains `lingo/glucose/dt=<fetch-date>/...csv`. `dt` is the *capture* date (each
  export is cumulative, so it belongs to "when it arrived"), not a per-record date.
- No clock dependency: data lands when the user uploads. The sensor must be **enabled** in the
  daemon (sensors are off by default) — documented in DEPLOYMENT.
- Two independent scope limits keep credentials tight: the Drive *share* bounds the SA to one
  folder, and the `drive.readonly` *scope* bounds it to read. Key rotation = new key + swap secret
  + delete old; instant revocation = un-share the folder.

## Related
[[0001-bronze-only]], [[0002-dagster-pins]], [[0004-garmin-port]].
