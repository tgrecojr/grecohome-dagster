"""Dagster Definitions for the USCRN/soil code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_soil.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``.
"""

from dagster import Definitions

from grecohome_soil.dagster.assets import uscrn_bronze_hourly
from grecohome_soil.dagster.schedules import uscrn_capture_job, uscrn_schedule

defs = Definitions(
    assets=[uscrn_bronze_hourly],
    jobs=[uscrn_capture_job],
    schedules=[uscrn_schedule],
)
