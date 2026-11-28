"""Table-agnostic asset-check helpers shared by the silver and gold code locations.

* :func:`alerting_check` — a failing ERROR check (or one that raises) logs one
  ``asset_check_failed`` line, which Grafana pages on.
* :func:`build_checks_job` / :func:`build_checks_schedule` — a checks-only job and
  its UTC schedule, so a table whose rebuild stopped is still caught.
* :func:`~grecohome_core.checks.table_freshness.table_freshness_check` — the ERROR
  check that a table's Parquet was rebuilt recently.
"""

from grecohome_core.checks.alerting import alerting_check
from grecohome_core.checks.jobs import build_checks_job, build_checks_schedule

__all__ = [
    "alerting_check",
    "build_checks_job",
    "build_checks_schedule",
]
