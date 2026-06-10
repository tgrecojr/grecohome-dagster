"""Dagster Definitions for the Whoop code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_whoop.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``.
"""

from dagster import Definitions

from grecohome_whoop.dagster.assets import RANGE_ASSETS, bronze_snapshots
from grecohome_whoop.dagster.checks import (
    whoop_checks,
    whoop_checks_job,
    whoop_checks_schedule,
)
from grecohome_whoop.dagster.schedules import (
    whoop_bronze_job,
    whoop_hourly,
    whoop_snapshots_hourly,
    whoop_snapshots_job,
)

defs = Definitions(
    assets=[*RANGE_ASSETS, bronze_snapshots],
    asset_checks=whoop_checks,
    jobs=[whoop_bronze_job, whoop_snapshots_job, whoop_checks_job],
    schedules=[whoop_hourly, whoop_snapshots_hourly, whoop_checks_schedule],
)
