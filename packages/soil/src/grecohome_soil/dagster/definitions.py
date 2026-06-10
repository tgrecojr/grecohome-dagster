"""Dagster Definitions for the USCRN/soil code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_soil.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``.
"""

from dagster import Definitions

from grecohome_soil.dagster.assets import uscrn_bronze_hourly
from grecohome_soil.dagster.checks import (
    soil_checks,
    soil_checks_job,
    soil_checks_schedule,
)
from grecohome_soil.dagster.schedules import uscrn_capture_job, uscrn_schedule

defs = Definitions(
    assets=[uscrn_bronze_hourly],
    asset_checks=soil_checks,
    jobs=[uscrn_capture_job, soil_checks_job],
    schedules=[uscrn_schedule, soil_checks_schedule],
)
