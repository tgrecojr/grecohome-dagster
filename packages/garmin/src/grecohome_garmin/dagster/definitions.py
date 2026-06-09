"""Dagster Definitions for the Garmin code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_garmin.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``.
"""

from dagster import Definitions

from grecohome_garmin.dagster.assets import ALL_ASSETS
from grecohome_garmin.dagster.resources import garmin_client
from grecohome_garmin.dagster.schedules import (
    garmin_daily,
    garmin_daily_job,
    garmin_reference,
    garmin_reference_job,
)

defs = Definitions(
    assets=ALL_ASSETS,
    jobs=[garmin_daily_job, garmin_reference_job],
    schedules=[garmin_daily, garmin_reference],
    resources={"garmin": garmin_client},
)
