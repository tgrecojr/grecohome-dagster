"""Dagster Definitions for the geocode code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_geocode.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``.
"""

from dagster import Definitions

from grecohome_geocode.dagster.assets import geocode_bronze_reverse
from grecohome_geocode.dagster.checks import (
    geocode_checks,
    geocode_checks_job,
    geocode_checks_schedule,
)
from grecohome_geocode.dagster.schedules import (
    geocode_capture_job,
    geocode_capture_schedule,
)

defs = Definitions(
    assets=[geocode_bronze_reverse],
    asset_checks=geocode_checks,
    jobs=[geocode_capture_job, geocode_checks_job],
    schedules=[geocode_capture_schedule, geocode_checks_schedule],
)
