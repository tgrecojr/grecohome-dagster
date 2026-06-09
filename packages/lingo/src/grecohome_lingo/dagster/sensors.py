"""The Drive-watching sensor + capture job for Lingo.

The sensor runs in this code-location container (where the Drive creds live; the
host daemon just triggers evaluation over gRPC). Each tick it lists the watched
folder and, for every Drive file_id not already a dynamic partition, adds the
partition and requests a run to capture it. New/missed files are picked up; the
first tick captures the existing backlog. This replaces glucose-loader's Postgres
``ProcessedFile`` table — the partition set is the "already captured" ledger.
"""

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    define_asset_job,
    sensor,
)

from grecohome_lingo import drive
from grecohome_lingo.config import settings
from grecohome_lingo.dagster.assets import LINGO_FILES, lingo_bronze_glucose

lingo_capture_job = define_asset_job("lingo_capture_job", selection=[lingo_bronze_glucose])


@sensor(
    job=lingo_capture_job,
    minimum_interval_seconds=settings.gdrive_poll_interval_minutes * 60,
)
def lingo_drive_sensor(context: SensorEvaluationContext):
    """List the Drive folder; add a partition + run for each new file_id."""
    service = drive.get_drive_service()
    files = drive.list_csv_files(service)
    existing = set(context.instance.get_dynamic_partitions(LINGO_FILES.name))
    new_ids = [f["id"] for f in files if f["id"] not in existing]

    if not new_ids:
        return SkipReason("no new Drive files")

    return SensorResult(
        # run_key == file_id => each file is captured at most once.
        run_requests=[RunRequest(run_key=fid, partition_key=fid) for fid in new_ids],
        dynamic_partitions_requests=[LINGO_FILES.build_add_request(new_ids)],
    )
