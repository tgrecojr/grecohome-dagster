"""Promote job + schedule for the location code location.

One job materializes both stream assets; a time-based schedule runs it every few
minutes (location history isn't latency-critical). The assets share a single-slot
pool so an overlapping tick can't double-promote, and the promoted-set makes each
run idempotent regardless of cadence.
"""

from dagster import ScheduleDefinition, define_asset_job

from grecohome_location.dagster.assets import (
    location_bronze_overland,
    location_bronze_owntracks,
)

location_promote_job = define_asset_job(
    "location_promote_job",
    selection=[location_bronze_overland, location_bronze_owntracks],
)

location_promote_schedule = ScheduleDefinition(
    name="location_promote_every_5m",
    job=location_promote_job,
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
)
