"""Dagster Definitions for the gold code location (the gRPC server target).

Served by ``dagster code-server start -m grecohome_gold.dagster.definitions`` and
registered with the host daemon/webserver via the host's ``workspace.yaml``.
"""

from dagster import Definitions

from grecohome_gold.dagster.assets import ALL_ASSETS
from grecohome_gold.dagster.checks import ALL_CHECKS
from grecohome_gold.dagster.monitoring import run_queue_monitor
from grecohome_gold.dagster.schedules import (
    gold_checks_job,
    gold_checks_schedule,
    gold_weather_daily,
    gold_weather_job,
    gold_wellness_daily,
    gold_wellness_job,
)
from grecohome_gold.dagster.weather_assets import WEATHER_ASSETS
from grecohome_gold.dagster.weather_checks import WEATHER_CHECKS

defs = Definitions(
    assets=ALL_ASSETS + WEATHER_ASSETS,
    asset_checks=ALL_CHECKS + WEATHER_CHECKS,
    jobs=[gold_wellness_job, gold_weather_job, gold_checks_job],
    schedules=[gold_wellness_daily, gold_weather_daily, gold_checks_schedule],
    sensors=[run_queue_monitor],
)
