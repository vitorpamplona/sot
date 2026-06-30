#!/usr/bin/env bash
# Redeploy the local Vespa application package after editing
# vespa-app/schemas/doc.sd (or services.xml). Pushes the package straight to
# the config server's prepareandactivate endpoint — no container recreate, so
# iterating on ranking equations takes a few seconds.
#
#   tools/deploy.sh            # deploy ./vespa-app to localhost:19071
#   VESPA_CONFIG=host:19071 tools/deploy.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP="$ROOT/vespa-app"
CONFIG="${VESPA_CONFIG:-localhost:19071}"
QUERY="${VESPA_QUERY:-localhost:8080}"

tmp="$(mktemp /tmp/vespa-app.XXXXXX.tgz)"
trap 'rm -f "$tmp"' EXIT

echo "[deploy] packaging $APP ..."
tar -czf "$tmp" -C "$APP" .

echo "[deploy] prepareandactivate -> $CONFIG ..."
curl -fSs --data-binary "@$tmp" \
  -H 'Content-Type: application/x-gzip' \
  "http://$CONFIG/application/v2/tenant/default/prepareandactivate" \
  | (python3 -m json.tool 2>/dev/null || cat)
echo

echo "[deploy] waiting for $QUERY to serve the new generation ..."
for _ in $(seq 1 120); do
  if curl -fSs -o /dev/null "http://$QUERY/ApplicationStatus"; then
    echo "[deploy] vespa is serving — ready."
    exit 0
  fi
  sleep 2
done
echo "[deploy] timed out waiting for /ApplicationStatus" >&2
exit 1
