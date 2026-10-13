#!/usr/bin/env python3
"""One-time interactive MFA bootstrap for the Garmin token store.

Logs in via ``garminconnect`` -- prompting for the MFA code on first run -- and
persists the token store at ``GARMINTOKENS``. After this, the code-location
container resumes silently. Run once, interactively:

    docker run --rm -it \\
      -e GARMINCONNECT_EMAIL=... -e GARMINCONNECT_BASE64_PASSWORD=... \\
      -e GARMINTOKENS=/secrets/garmin -e BRONZE_ROOT=/data/bronze \\
      -v /opt/docker/dagster/garmin/secrets:/secrets/garmin \\
      --entrypoint python ghcr.io/tgrecojr/grecohome-dagster-garmin:latest \\
      -m grecohome_garmin.bootstrap
"""

import sys
import time

from grecohome_core.logging_config import get_logger
from grecohome_garmin.auth import login, session_saved
from grecohome_garmin.config import settings

log = get_logger(__name__)


# @spec GARMIN-AUTH-005
def main() -> int:
    print("\n" + "=" * 70)
    print("Garmin token bootstrap")
    print("=" * 70)
    print(f"\nToken store: {settings.garmintokens}")
    print("Logging in (you'll be prompted for the MFA code on first run)...\n")
    started = time.time()
    try:
        client = login(settings)
    except Exception as exc:  # noqa: BLE001 - surface auth failure clearly
        print(f"\nLogin failed: {exc}", file=sys.stderr)
        return 1
    if not session_saved(settings.garmintokens, client.client.dumps(), since=started):
        print(
            f"\nLogged in, but the session was NOT saved to {settings.garmintokens}.\n"
            "Check that GARMINTOKENS is the mounted, writable token directory.",
            file=sys.stderr,
        )
        return 1
    print(f"\n✓ Token store written to {settings.garmintokens}")
    print("The code location can now resume unattended.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
