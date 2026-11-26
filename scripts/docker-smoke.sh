#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

#
# Smoke a published or locally-built Actae Docker image FULLY OFFLINE.
#
# The image ships the Alpine/musl artifacts, so its embedded PostgreSQL is
# self-contained. Running with `--network none` proves that: any startup
# that needed Maven (or any outbound network) fails fast. Also verifies
# record/replay and that data survives a container restart.
#
# Usage: scripts/docker-smoke.sh <image> [api-key]
#
# Notes (validated against Alpine 3.20's BusyBox):
#   * BusyBox wget has NO `--tries`/`--no-verbose` — use `-q -O-`.
#   * `localhost` resolves to `::1` in the container, but Actae binds IPv4
#     loopback — always probe `127.0.0.1`.
#   * The events/record endpoint requires `metadata` (400 without it).
set -euo pipefail

IMAGE="${1:?usage: docker-smoke.sh <image> [api-key]}"
KEY="${2:-sk-smoke-00000000000000000000000000000000}"
CID="actae-smoke-$$"

cleanup() {
    docker rm -f "$CID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() {
    echo "FAILED: $1"
    docker logs "$CID" 2>/dev/null || true
    exit 1
}

healthy() {
    docker exec "$CID" wget -q -O- http://127.0.0.1:8002/healthz 2>/dev/null \
        | grep -q '"status":"ok"'
}

docker run -d --name "$CID" --network none \
    -e ACTAE_INITIAL_API_KEY="$KEY" \
    "$IMAGE" >/dev/null

echo "  waiting for health (networking disabled)"
ok=""
for i in $(seq 1 90); do
    healthy && { ok=1; break; } || true
    sleep 1
done
[ -n "$ok" ] || fail "image never became healthy with networking disabled"
echo "  healthy offline"

docker exec "$CID" wget -q \
    --post-data='{"channel_id":"smoke","event_type":"smoke","payload":{"ok":true},"metadata":{"actor":"smoke"}}' \
    --header='Content-Type: application/json' --header="X-API-Key: $KEY" \
    -O- http://127.0.0.1:8002/api/v1/events/record >/dev/null \
    || fail "record"
docker exec "$CID" wget -q -O- \
    --header="X-API-Key: $KEY" \
    'http://127.0.0.1:8002/api/v1/events/replay/smoke?cursor=0' \
    | grep -q '"ok":true' || fail "replay did not contain the event"
echo "  record/replay OK"

docker restart "$CID" >/dev/null
ok=""
for i in $(seq 1 90); do
    healthy && { ok=1; break; } || true
    sleep 1
done
[ -n "$ok" ] || fail "image did not recover after restart"
docker exec "$CID" wget -q -O- \
    --header="X-API-Key: $KEY" \
    'http://127.0.0.1:8002/api/v1/events/replay/smoke?cursor=0' \
    | grep -q '"ok":true' || fail "event did not survive a container restart"
echo "  restart persistence OK"

echo "== Docker image smoke OK: ${IMAGE} =="
