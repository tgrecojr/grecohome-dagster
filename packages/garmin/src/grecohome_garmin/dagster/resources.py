"""Dagster resource: a single logged-in Garmin client per run.

``garminconnect`` login (resume-from-token-store + ``verify_login``) costs an API
call, so logging in once per *run* and sharing the client across all of that run's
ops is much cheaper than re-logging-in per asset. The garmin jobs use the
in-process executor, so this resource is initialized once and the same client is
handed to every op in the run.
"""

from dagster import resource

from grecohome_garmin.auth import login
from grecohome_garmin.config import settings


@resource
def garmin_client(_init_context):
    """Return a live, logged-in ``garminconnect.Garmin`` client (one per run)."""
    return login(settings)
