"""Bronze promote assets for the location subject (one per stream).

Two unpartitioned, scheduled assets. Each scans the relay's trailing staging window
for its stream and promotes every new file into bronze; the promoted-set (in
``LOCATION_STATE_DIR``) plus the ``staging_file`` sidecar backstop are the
capture-once guard, so the asset itself carries no Dagster partitions. Both share a
single-slot pool so overlapping runs can't double-promote.
"""

from dagster import AssetExecutionContext, asset

from grecohome_location.capture import iso_from_ms
from grecohome_location.config import settings
from grecohome_location.promote import PromoteReport, promote_stream

#: Single-slot pool (limit enforced host-side) so two promote runs never overlap.
#: Off the source-API pools — this subject makes no API calls.
LOCATION_POOL = "location"


def _promote(stream: str) -> PromoteReport:
    return promote_stream(
        capture_dir=settings.relay_capture_dir,
        bronze_root=settings.bronze_root,
        state_dir=settings.location_state_dir,
        stream=stream,
        window_days=settings.location_promote_window_days,
    )


def _report_metadata(report: PromoteReport) -> dict:
    md: dict = {
        "stream": report.stream,
        "scanned": report.scanned,
        "promoted": report.promoted,
        "already_promoted": report.already,
        "bytes_promoted": report.bytes_promoted,
        "failed": report.failed,
    }
    if report.oldest_received_ms is not None:
        md["oldest_received_at"] = iso_from_ms(report.oldest_received_ms)
    if report.newest_received_ms is not None:
        md["newest_received_at"] = iso_from_ms(report.newest_received_ms)
    return md


@asset(pool=LOCATION_POOL, group_name="location")
def location_bronze_overland(context: AssetExecutionContext) -> None:
    """Promote new Overland staging files into ``location/overland`` bronze."""
    context.add_output_metadata(_report_metadata(_promote("overland")))


@asset(pool=LOCATION_POOL, group_name="location")
def location_bronze_owntracks(context: AssetExecutionContext) -> None:
    """Promote new OwnTracks staging files into ``location/owntracks`` bronze."""
    context.add_output_metadata(_report_metadata(_promote("owntracks")))
