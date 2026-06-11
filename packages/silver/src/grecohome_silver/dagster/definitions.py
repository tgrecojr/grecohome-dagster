"""Dagster Definitions for the silver code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_silver.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``.
"""

from dagster import Definitions

from grecohome_silver.dagster.assets import ALL_ASSETS
from grecohome_silver.dagster.checks import ALL_CHECKS
from grecohome_silver.dagster.schedules import (
    silver_checks_job,
    silver_checks_schedule,
    silver_sleep_daily,
    silver_sleep_job,
)

defs = Definitions(
    assets=ALL_ASSETS,
    asset_checks=ALL_CHECKS,
    jobs=[silver_sleep_job, silver_checks_job],
    schedules=[silver_sleep_daily, silver_checks_schedule],
)
