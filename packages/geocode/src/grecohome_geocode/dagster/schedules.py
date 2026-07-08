"""Capture job + schedule for the geocode code location.

One job materializes the cache asset; a time-based schedule runs it every 30 minutes,
lagging the ``location`` promoter (which runs every ~5 min) so freshly-promoted points
get geocoded soon after they land. The single-slot pool makes the cadence safe regardless
of run duration, and the cache makes each run idempotent.
"""

from dagster import ScheduleDefinition, define_asset_job

from grecohome_geocode.dagster.assets import geocode_bronze_reverse

geocode_capture_job = define_asset_job(
    "geocode_capture_job", selection=[geocode_bronze_reverse]
)

geocode_capture_schedule = ScheduleDefinition(
    name="geocode_reverse_every_30m",
    job=geocode_capture_job,
    cron_schedule="*/30 * * * *",
    execution_timezone="UTC",
)
