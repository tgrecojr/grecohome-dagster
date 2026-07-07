"""Dagster Definitions for the location code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_location.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``. The
container must be run **at runtime** as uid 1000 (e.g. compose ``user: "1000:998"``)
with the relay staging dir mounted read-only (staging files are ``0600`` owned by uid
1000); see the subject README / docs/LOCATION.md.
"""

from dagster import Definitions

from grecohome_location.dagster.assets import (
    location_bronze_overland,
    location_bronze_owntracks,
)
from grecohome_location.dagster.checks import (
    location_checks,
    location_checks_job,
    location_checks_schedule,
)
from grecohome_location.dagster.schedules import (
    location_promote_job,
    location_promote_schedule,
)

defs = Definitions(
    assets=[location_bronze_overland, location_bronze_owntracks],
    asset_checks=location_checks,
    jobs=[location_promote_job, location_checks_job],
    schedules=[location_promote_schedule, location_checks_schedule],
)
