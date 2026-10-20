# Lingo (CGM glucose) bronze subject — EARS specs

Design: [lingo-design.md](lingo-design.md). Prefix `LINGO`. Facets: `SENSOR` (Drive-folder
sensor), `CAPTURE` (download and capture), `AUTH` (service account, settings), `CHECKS`.

## Sensor

- [x] **LINGO-SENSOR-001**: When the `lingo_drive_sensor` sensor ticks, the system shall list the files in `GDRIVE_FOLDER_ID` whose MIME type is `text/csv` and that are not trashed, ordered by creation time, following every page Drive returns.
- [x] **LINGO-SENSOR-002**: When the `lingo_drive_sensor` sensor finds listed files whose `file_id` is not yet a `lingo_files` partition, the system shall add each as a partition and request one `lingo_capture_job` run per file with `run_key` equal to the `file_id`.
- [x] **LINGO-SENSOR-003**: If the `lingo_drive_sensor` sensor finds no new `file_id`, then the system shall skip the tick with the reason "no new Drive files" and request no runs.
- [x] **LINGO-SENSOR-004**: The system shall limit `lingo_drive_sensor` evaluations to at most one per `GDRIVE_POLL_INTERVAL_MINUTES` (default 5).

## Capture

- [x] **LINGO-CAPTURE-001**: When `lingo_bronze_glucose` materializes a partition, the system shall download that Drive file's content and capture it byte-for-byte to `lingo/glucose/dt=<UTC capture date>` as `text/csv` (`ext=csv`) with skip-identical off.
- [x] **LINGO-CAPTURE-002**: The system shall record in each Lingo capture's sidecar `request_url` (the file's Drive media URL) and `request_params` holding `file_id`, `file_name`, `drive_folder_id`, `created_time` and `modified_time`, and no credentials.
- [x] **LINGO-CAPTURE-003**: When `lingo_bronze_glucose` captures a file, the system shall record `file_id`, `file_name`, `bytes` and `bronze_path` as materialization metadata.
- [x] **LINGO-CAPTURE-004**: If the bronze write for a Lingo file reports any outcome other than `written`, then the system shall raise and fail the run, so the partition isn't left looking captured.

## Authentication and settings

- [x] **LINGO-AUTH-001**: The system shall authenticate to Drive with the service-account key at `GDRIVE_SERVICE_ACCOUNT_PATH`, requesting only the `drive.readonly` scope.
- [x] **LINGO-AUTH-002**: If `GDRIVE_FOLDER_ID` or `GDRIVE_SERVICE_ACCOUNT_PATH` is not set, then the Lingo code location shall fail at startup with the standard missing-configuration error.

## Bronze checks

- [x] **LINGO-CHECKS-001**: The system shall configure the `lingo/glucose` bronze checks with the CSV reader, event dates from the `Time of Glucose Reading [T=(local time) +/- (time zone offset)]` column, a completeness gap tolerance of 14 days, and only the completeness, schema-drift and content-health families (no freshness), run hourly at minute 23 UTC by `lingo_bronze_checks_job`.
- [x] **LINGO-CHECKS-002**: When the `lingo_drive_folder_health` check runs, it shall fail at ERROR severity, and alert through `alerting_check`, if listing the watched folder raises or returns no CSV files, and pass otherwise.
- [x] **LINGO-CHECKS-003**: The `lingo_drive_folder_health` check shall report only the folder id, the number of files found, or the error's type name, never credentials or file contents.
