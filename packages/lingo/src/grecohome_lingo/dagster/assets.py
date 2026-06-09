"""Bronze asset for Lingo glucose files.

One dynamic-partitioned asset: each Drive file is its own partition (keyed by
Drive ``file_id``), captured exactly once. The asset downloads that file from
Drive and captures the raw CSV bytes to bronze. The sensor (see schedules.py)
adds a partition per new file; the partition set is the "already captured" ledger.
"""

from dagster import AssetExecutionContext, DynamicPartitionsDefinition, asset

from grecohome_lingo import drive
from grecohome_lingo.capture import capture_glucose
from grecohome_lingo.config import settings

# One partition per Drive file_id (added by the sensor as files appear).
LINGO_FILES = DynamicPartitionsDefinition(name="lingo_files")

LINGO_POOL = "lingo_api"


@asset(partitions_def=LINGO_FILES, pool=LINGO_POOL, group_name="lingo")
def lingo_bronze_glucose(context: AssetExecutionContext) -> None:
    """Download one Drive glucose CSV (the partition's file_id) and capture it."""
    file_id = context.partition_key
    service = drive.get_drive_service()
    info = (
        service.files()
        .get(fileId=file_id, fields="id, name, createdTime, modifiedTime")
        .execute()
    )
    raw = drive.download_file_bytes(service, file_id)
    path = capture_glucose(
        raw,
        file_id=file_id,
        file_name=info.get("name"),
        folder_id=settings.gdrive_folder_id,
        bronze_root=settings.bronze_root,
        created_time=info.get("createdTime"),
        modified_time=info.get("modifiedTime"),
    )
    context.add_output_metadata(
        {
            "file_id": file_id,
            "file_name": info.get("name"),
            "bytes": len(raw),
            "captured": path is not None,  # False => deduped (identical content)
        }
    )
