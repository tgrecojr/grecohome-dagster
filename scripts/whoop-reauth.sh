#!/usr/bin/env bash
#
# Whoop OAuth re-auth shortcut (run on host `fridge`).
#
# When the Whoop grant is revoked (Loki: `whoop_token_invalid_grant status_code=400`),
# only a fresh headless OAuth flow recovers it — no client retry can. See
# docs/WHOOP_TOKEN_RUNBOOK.md for why the grant dies.
#
# This runs `grecohome_whoop.oauth_setup --headless` inside a throwaway container built
# from the *same image* and *same volumes/env* as the running `dagster_whoop` code
# location, so it writes the new token to the real `WHOOP_TOKEN_PATH` the pipeline reads.
# Authorize in a browser on any device and paste the callback URL back when prompted.
# No restart needed afterwards — the token file is re-read each run; the next hourly
# tick recovers. Re-run any failed partitions to backfill.
#
# Usage:
#   scripts/whoop-reauth.sh                 # defaults: container=dagster_whoop, user=1000:988
#   WHOOP_CONTAINER=dagster_whoop WHOOP_RUN_USER=1000:988 scripts/whoop-reauth.sh
#
set -euo pipefail

CONTAINER="${WHOOP_CONTAINER:-dagster_whoop}"
# The running container reads the 0600 token dir as uid 1000; group 988 is the host
# gid that owns the mounted secret. Override if either differs on your host.
RUN_USER="${WHOOP_RUN_USER:-1000:988}"

if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "error: container '$CONTAINER' not found (set WHOOP_CONTAINER)." >&2
  exit 1
fi

# The env carries the client id/secret and WHOOP_TOKEN_PATH. Keep it 0600 and delete it
# on exit (success, failure, or Ctrl-C) so the secret never lingers in /tmp.
ENV_FILE="$(mktemp)"
chmod 600 "$ENV_FILE"
trap 'rm -f "$ENV_FILE"' EXIT

docker inspect "$CONTAINER" --format '{{range .Config.Env}}{{println .}}{{end}}' >"$ENV_FILE"
IMAGE="$(docker inspect "$CONTAINER" --format '{{.Config.Image}}')"

echo "Re-authing Whoop via image $IMAGE (user $RUN_USER, volumes from $CONTAINER)..." >&2
# No `exec`: let the script resume after the container exits so the EXIT trap fires and
# the secret-bearing env file is removed.
docker run --rm -it \
  --entrypoint python \
  --user "$RUN_USER" \
  --env-file "$ENV_FILE" \
  --volumes-from "$CONTAINER" \
  "$IMAGE" \
  -m grecohome_whoop.oauth_setup --headless
