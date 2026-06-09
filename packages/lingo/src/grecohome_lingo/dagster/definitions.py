"""Dagster Definitions for the Lingo code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_lingo.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``. The
sensor must be enabled (sensors are off by default).
"""

from dagster import Definitions

from grecohome_lingo.dagster.assets import lingo_bronze_glucose
from grecohome_lingo.dagster.sensors import lingo_capture_job, lingo_drive_sensor

defs = Definitions(
    assets=[lingo_bronze_glucose],
    jobs=[lingo_capture_job],
    sensors=[lingo_drive_sensor],
)
