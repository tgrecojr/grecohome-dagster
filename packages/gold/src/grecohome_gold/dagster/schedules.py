"""Jobs + schedules for the gold code location.

A daily rebuild of the marts plus a checks-only job, both off the ``*_api`` pools.
Gold runs **after** the silver rebuilds (silver finishes by ~06:50 + its 07:00 checks),
so the mart reads fresh silver.
"""

from __future__ import annotations

from dagster import ScheduleDefinition, define_asset_job

from grecohome_core.checks import build_bronze_checks_job, build_bronze_checks_schedule
from grecohome_gold.dagster.assets import ALL_ASSETS
from grecohome_gold.dagster.checks import ALL_CHECKS

gold_wellness_job = define_asset_job("gold_wellness_job", selection=ALL_ASSETS)

gold_wellness_daily = ScheduleDefinition(
    name="gold_wellness_daily",
    job=gold_wellness_job,
    cron_schedule="30 7 * * *",  # 07:30 UTC — after silver rebuilds + silver checks
    execution_timezone="UTC",
)

gold_checks_job = build_bronze_checks_job(ALL_CHECKS, name="gold_checks_job")
gold_checks_schedule = build_bronze_checks_schedule(
    gold_checks_job, name="gold_checks_daily", cron_schedule="0 8 * * *"
)
