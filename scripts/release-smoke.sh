#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

#
# Runtime smoke test for a release binary: starts `actae` (self-contained,
# managed PostgreSQL) with a fresh data dir and a known API key, waits for
# health, records an event, replays it, stops gracefully, then RESTARTS with
# the same data dir and verifies the event survived (restart persistence with
# the packaged binary). Proves static linkage, the embedded dashboard+managed
# PostgreSQL, graceful shutdown and crash-free restart on a real OS.
#
# Usage: release-smoke.sh <path-to-actae>
set -euo pipefail

BIN="${1:?usage: release-smoke.sh <path-to-actae>}"
DATA_DIR="$(mktemp -d)"
HTTP_PORT=18102
PG_PORT=18106
KEY="sk-smoke-00000000000000000000000000000000"
LOG="${DATA_DIR}/smoke.log"

cleanup() {
    pkill -TERM -f "${BIN}" 2>/dev/null || true
    sleep 2
    pkill -KILL -f "${BIN}" 2>/dev/null || true
    rm -rf "${DATA_DIR}"
}
trap cleanup EXIT

start_and_wait() {
    ACTAE_PORT="${HTTP_PORT}" \
    ACTAE_EMBEDDED_PG_PORT="${PG_PORT}" \
    ACTAE_DATA_DIR="${DATA_DIR}" \
    ACTAE_INITIAL_API_KEY="${KEY}" \
        "${BIN}" >> "${LOG}" 2>&1 &
    for i in $(seq 1 90); do
        if curl -sf "http://127.0.0.1:${HTTP_PORT}/healthz" > /dev/null 2>&1; then
            echo "  up after ~${i}s"
            return 0
        fi
        sleep 1
    done
    echo "  FAILED: server did not become healthy"
    tail -30 "${LOG}"
    return 1
}

echo "== smoke: ${BIN} =="

echo "  first start"
start_and_wait

echo "  recording + replaying an event"
curl -sf -X POST "http://127.0.0.1:${HTTP_PORT}/api/v1/events/record" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: ${KEY}" \
    -d '{"channel_id":"smoke","event_type":"smoke","payload":{"ok":true},"metadata":{"actor":"smoke"}}' \
    > /dev/null
curl -sf "http://127.0.0.1:${HTTP_PORT}/api/v1/events/replay/smoke?cursor=0" \
    -H "X-API-Key: ${KEY}" \
    | grep -q '"ok":true' || { echo "  FAILED: replay did not contain the event"; exit 1; }

echo "  dashboard served"
curl -sf "http://127.0.0.1:${HTTP_PORT}/" | grep -qi "<html" || {
    echo "  FAILED: dashboard not served"
    exit 1
}

echo "  graceful shutdown"
pkill -TERM -f "${BIN}" 2>/dev/null || true
sleep 5
if pgrep -f "${BIN}" > /dev/null 2>&1; then
    echo "  FAILED: process did not stop"
    exit 1
fi

echo "  restart (same data dir) — verifying persistence"
start_and_wait
curl -sf "http://127.0.0.1:${HTTP_PORT}/api/v1/events/replay/smoke?cursor=0" \
    -H "X-API-Key: ${KEY}" \
    | grep -q '"ok":true' || { echo "  FAILED: event did not survive a restart"; exit 1; }
pkill -TERM -f "${BIN}" 2>/dev/null || true
sleep 3

echo "  OK: ${BIN}"
