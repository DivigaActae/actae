#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

#
# Actae Test Suite Runner
#
# Usage:
#   ./tests/run_tests.sh                          # Run all tests (default, excluding slow)
#   ./tests/run_tests.sh --all                    # Run all tests including slow/performance
#   ./tests/run_tests.sh --fast                   # Run only core EventStore + validation
#   ./tests/run_tests.sh --smoke                  # Minimal smoke test
#   ./tests/run_tests.sh --ci                     # CI mode: junit output, failfast
#   ./tests/run_tests.sh --html                   # Run with HTML report
#   ./tests/run_tests.sh --embedded               # Embedded mode: no TLS, sk-dev-* API key
#   ./tests/run_tests.sh --embedded --fast        # Embedded mode + fast subset
#
# Environment variables:
#   ACTAE_API_URL     default: https://localhost:8002 (embedded: http://localhost:8002)
#   ACTAE_WS_URL      default: wss://localhost:8002 (embedded: ws://localhost:8002)
#   ACTAE_API_KEY     default: test-api-key-for-integration-tests (embedded: sk-dev-0000000000000000000000)
#   ACTAE_PG_DSN      default: postgresql://actae:actae_dev_password@localhost:5432/actae
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------

ensure_deps() {
    if ! python3 -c "import pytest" 2>/dev/null; then
        echo "Installing test dependencies..."
        pip install -r tests/requirements.txt -q
    fi
    # The docker stack (non-dev) needs the dev PKI (gitignored). Generate it
    # on demand so a fresh clone can run the suite without a manual step.
    if ! $DEV_MODE && [ ! -f certs/cert.pem ]; then
        echo "Generating dev PKI (certs/ is gitignored)..."
        ./scripts/rotate-certs.sh
    fi
}

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

wait_for_health() {
    local api_url="${ACTAE_API_URL:-https://localhost:8002}"
    local timeout=30
    local deadline=$(($(date +%s) + timeout))
    # The production stack enforces mTLS — the health check must present the
    # test client certificate or the handshake is rejected.
    local curl_args=(-sfk)
    if ! $DEV_MODE && [ -f certs/client/actae-test-client.crt ]; then
        curl_args+=(--cert certs/client/actae-test-client.crt --key certs/client/actae-test-client.key --cacert certs/ca.crt)
    fi
    echo "Waiting for Actae at $api_url/healthz..."
    while [ $(date +%s) -lt $deadline ]; do
        if curl "${curl_args[@]}" "$api_url/healthz" >/dev/null 2>&1; then
            echo "Server is healthy."
            return 0
        fi
        sleep 1
    done
    echo "ERROR: Server did not become healthy within ${timeout}s"
    exit 1
}

# ---------------------------------------------------------------------------
# Pytest args
# ---------------------------------------------------------------------------

PYTEST_ARGS=(
    -v
    --tb=short
    --strict-markers
)

MODE="${1:-normal}"
DEV_MODE=false

# Parse flags — allow --embedded combined with other flags
for arg in "$@"; do
    case "$arg" in
        --embedded)
            DEV_MODE=true
            ;;
    esac
done

if $DEV_MODE; then
    echo "=== Actae Test Suite: EMBEDDED MODE (no TLS) ==="
    export ACTAE_API_URL="${ACTAE_API_URL:-http://localhost:8002}"
    export ACTAE_WS_URL="${ACTAE_WS_URL:-ws://localhost:8002}"
    export ACTAE_API_KEY="${ACTAE_API_KEY:-sk-dev-0000000000000000000000}"
    export ACTAE_PG_DSN="${ACTAE_PG_DSN:-}"
    PYTEST_ARGS+=(--embedded)
fi

# Determine the mode from the first arg (or after --dev)
case "${1:-normal}" in
    --all|-a)
        echo "=== Actae Test Suite: FULL ==="
        PYTEST_ARGS+=(-m "")
        ;;
    --fast|-f)
        echo "=== Actae Test Suite: FAST (core only) ==="
        PYTEST_ARGS+=(
            -k "not slow and not performance"
            --ignore=tests/test_fanout.py
            --ignore=tests/test_resilience.py
        )
        ;;
    --smoke|-s)
        echo "=== Actae Test Suite: SMOKE ==="
        PYTEST_ARGS+=(
            -k "test_publish_returns_valid_event or test_cursor_monotonic or test_replay_from_cursor_zero or test_subscribe_returns_confirmation or test_welcome_on_connect"
            --ignore=tests/test_fanout.py
            --ignore=tests/test_resilience.py
            --ignore=tests/test_concurrency_fence.py
            --ignore=tests/test_performance.py
        )
        ;;
    --ci)
        echo "=== Actae Test Suite: CI ==="
        ensure_deps
        wait_for_health
        PYTEST_ARGS+=(
            -x                          # fail fast
            --junitxml=tests/results/junit.xml
            --reruns 2
            --reruns-delay 3
            -m "not slow"
            --ignore=tests/test_fanout.py
            --ignore=tests/test_resilience.py
        )
        mkdir -p tests/results
        ;;
    --html|-h)
        echo "=== Actae Test Suite: HTML Report ==="
        ensure_deps
        wait_for_health
        PYTEST_ARGS+=(
            --html=tests/results/report.html
            --self-contained-html
        )
        mkdir -p tests/results
        ;;
    --dev)
        # Already handled above
        PYTEST_ARGS+=(
            -k "not slow and not performance"
            --ignore=tests/test_fanout.py
            --ignore=tests/test_resilience.py
        )
        ;;
    *)
        echo "=== Actae Test Suite: NORMAL (default) ==="
        PYTEST_ARGS+=(
            -m "not slow and not performance"
            --ignore=tests/test_fanout.py
            --ignore=tests/test_resilience.py
        )
        ;;
esac

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

ensure_deps
if ! $DEV_MODE; then
    wait_for_health
fi

echo "Args: ${PYTEST_ARGS[*]}"
cd "$PROJECT_DIR"
exec python3 -m pytest "${PYTEST_ARGS[@]}"
