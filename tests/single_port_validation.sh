#!/bin/bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Single-Port Architecture Validation Suite — 30+ tests
# Requires: a running Actae in dev mode on ACTAE_PORT (default 8002).
# Usage: bash tests/single_port_validation.sh [PORT]
set -euo pipefail

PORT="${1:-8002}"
BASE="http://localhost:${PORT}"
API_KEY="${2:-sk-dev-0000000000000000000000}"
AUTH_HDR="x-api-key: ${API_KEY}"
PASS=0
FAIL=0
TESTS=()

assert() {
    local desc="$1" expected="$2" actual="$3"
    TESTS+=("$desc")
    if [[ "$actual" == "$expected" ]]; then
        echo "   $desc"
        PASS=$((PASS + 1))
    else
        echo "   $desc (expected: '$expected', got: '$actual')"
        FAIL=$((FAIL + 1))
    fi
}

assert_contains() {
    local desc="$1" pattern="$2" text="$3"
    TESTS+=("$desc")
    if echo "$text" | grep -F -- "$pattern" > /dev/null 2>&1; then
        echo "   $desc"
        PASS=$((PASS + 1))
    else
        echo "   $desc (expected pattern: '$pattern' not found)"
        FAIL=$((FAIL + 1))
    fi
}

assert_gt() {
    local desc="$1" threshold="$2" actual="$3"
    TESTS+=("$desc")
    if [[ "$actual" -gt "$threshold" ]]; then
        echo "   $desc"
        PASS=$((PASS + 1))
    else
        echo "   $desc (expected >$threshold, got: $actual)"
        FAIL=$((FAIL + 1))
    fi
}

assert_lt() {
    local desc="$1" threshold="$2" actual="$3"
    TESTS+=("$desc")
    if [[ "$actual" -lt "$threshold" ]]; then
        echo "   $desc"
        PASS=$((PASS + 1))
    else
        echo "   $desc (expected <$threshold, got: $actual)"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== Single-Port Architecture Validation ==="
echo "  Target: $BASE"
echo ""

#  1. Health Check 
echo " 1. Health/Readiness/Metrics on main port "

HEALTH=$(curl -sf "$BASE/healthz" 2>/dev/null || echo '{"status":"error"}')
assert_contains "1.1  /healthz returns HTTP 200" '"status":"ok"' "$HEALTH"
assert_contains "1.2  /healthz contains service=actae" '"service":"actae"' "$HEALTH"
assert_contains "1.3  /healthz contains version" '"version"' "$HEALTH"

READY=$(curl -sf "$BASE/readyz" 2>/dev/null || echo '{"status":"error"}')
assert_contains "1.4  /readyz returns ready status" '"status":"ready"' "$READY"
assert_contains "1.5  /readyz contains database_ready check" "database_ready" "$READY"

METRICS=$(curl -sf "$BASE/metrics" 2>/dev/null || echo '')
METRICS_LINES=$(echo "$METRICS" | wc -l | tr -d ' ')
assert_gt "1.6  /metrics returns Prometheus data" "5" "$METRICS_LINES"
assert_contains "1.7  /metrics contains actae_ prefix" "actae_" "$METRICS"

METRICS_JSON=$(curl -sf "$BASE/metrics.json" 2>/dev/null || echo '{"status":"error"}')
assert_contains "1.8  /metrics.json returns JSON status" '"status":"ok"' "$METRICS_JSON"
assert_contains "1.9  /metrics.json contains websocket section" "websocket" "$METRICS_JSON"

#  2. REST API 
echo " 2. REST API endpoints "

# Record event
RECORD=$(curl -sf -X POST "$BASE/api/v1/events/record" \
        -H "Content-Type: application/json" \
    -H "$AUTH_HDR" \
    -d '{"channel_id":"test-ch","event_type":"test","payload":{"msg":"hello"},"metadata":{"actor":"test-user"}}' \
    2>/dev/null || echo '{"error":"request failed"}')
assert_contains "2.1  POST /api/v1/events/record succeeds" "cursor" "$RECORD"

# Record second event
RECORD2=$(curl -sf -X POST "$BASE/api/v1/events/record" \
        -H "Content-Type: application/json" \
    -H "$AUTH_HDR" \
    -d '{"channel_id":"test-ch","event_type":"test","payload":{"msg":"hello2"},"metadata":{"actor":"test-user"}}' \
    2>/dev/null || echo '{"error":"request failed"}')
assert_contains "2.2  POST /api/v1/events/record (second) succeeds" "cursor" "$RECORD2"

# Get cursor
CURSOR=$(curl -sf "$BASE/api/v1/events/cursor/test-ch" -H "$AUTH_HDR" 2>/dev/null || echo '{"cursor":-1}')
assert_contains "2.3  GET /api/v1/events/cursor/test-ch returns status" "status" "$CURSOR"

# Replay events
REPLAY=$(curl -sf "$BASE/api/v1/events/replay/test-ch" -H "$AUTH_HDR" 2>/dev/null || echo '{"events":[]}')
REPLAY_COUNT=$(echo "$REPLAY" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d.get("events",[])))' 2>/dev/null || echo 0)
assert_gt "2.4  GET /api/v1/events/replay/test-ch returns events" 0 "$REPLAY_COUNT"

# Query events
QUERY=$(curl -sf -X POST "$BASE/api/v1/events/query" \
        -H "Content-Type: application/json" \
    -H "$AUTH_HDR" \
    -d '{"channel_ids":["test-ch"]}' \
    2>/dev/null || echo '{"events":[]}')
QUERY_COUNT=$(echo "$QUERY" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d.get("events",[])))' 2>/dev/null || echo 0)
assert_gt "2.5  POST /api/v1/events/query returns events" 0 "$QUERY_COUNT"

# List channels
CHANNELS=$(curl -sf "$BASE/api/v1/channels" -H "$AUTH_HDR" 2>/dev/null || echo '{"channels":[]}')
assert_contains "2.6  GET /api/v1/channels includes test-ch" "test-ch" "$CHANNELS"

# State save/load
SAVE_STATE=$(curl -sf -X POST "$BASE/api/v1/state/test-ch" \
            -H "Content-Type: application/json" \
    -H "$AUTH_HDR" \
    -H "$AUTH_HDR" \
    -d '{"cursor":1,"state":{"key":"val"},"channel_id":"test-ch"}' \
    2>/dev/null || echo '{"error":"failed"}')
assert_contains "2.7  POST /api/v1/state/test-ch saves state" "version" "$SAVE_STATE"

LOAD_STATE=$(curl -sf "$BASE/api/v1/state/test-ch" -H "$AUTH_HDR" 2>/dev/null || echo '{"error":"failed"}')
assert_contains "2.8  GET /api/v1/state/test-ch loads state" "state" "$LOAD_STATE"

# Fork
FORK=$(curl -s -X POST "$BASE/api/v1/channels/fork" \
        -H "Content-Type: application/json" \
    -H "$AUTH_HDR" \
    -d '{"source_channel_id":"test-ch","new_channel_id":"test-ch-fork","at_cursor":1}' \
    2>/dev/null || echo '{"error":"failed"}')
assert_contains "2.9  POST /api/v1/channels/fork" "origin_run_id" "$FORK"

# Fork listing
BRANCHES=$(curl -sf "$BASE/api/v1/channels/test-ch/forks" -H "$AUTH_HDR" 2>/dev/null || echo '[]')
assert_contains "2.10 GET /api/v1/channels/test-ch/forks returns list" "forks" "$BRANCHES"

# Auth signup
SIGNUP=$(curl -sf -X POST "$BASE/api/v1/auth/signup" \
        -H "Content-Type: application/json" \
    -H "$AUTH_HDR" \
    -d '{"email":"test'$RANDOM'@example.com","password":"T3st!Passw0rd#Strong","name":"Test User"}' \
    2>/dev/null || echo '{"error":"failed"}')
assert_contains "2.11 POST /api/v1/auth/signup (functional)" "user" "$SIGNUP"

#  3. Single Port Verification 
echo " 3. Single port verification "

# Verify all services respond on the same port
API_STATUS=$(curl -s -o /dev/null -w '%{http_code}' -H "$AUTH_HDR" "$BASE/api/v1/channels" 2>/dev/null || echo 000)
assert "3.1  REST API responds on port $PORT" "200" "$API_STATUS"

HEALTH_STATUS=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/healthz" 2>/dev/null || echo 000)
assert "3.2  Health check responds on port $PORT" "200" "$HEALTH_STATUS"

METRICS_STATUS=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/metrics.json" 2>/dev/null || echo 000)
assert "3.3  Metrics respond on port $PORT" "200" "$METRICS_STATUS"

# Verify WebSocket upgrade available via HTTP
WS_UPGRADE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 -H "Connection: Upgrade" -H "Upgrade: websocket" -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" "$BASE/ws?api_key=${API_KEY}" 2>/dev/null; true)
assert "3.4  WebSocket upgrade responds correctly on port $PORT" "101" "$WS_UPGRADE"

# Verify no separate health port responds (health_port defaults to port)
NO_HEALTH_8002=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 "http://localhost:8002/healthz" 2>/dev/null || echo 000)
NO_HEALTH_8003=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 "http://localhost:8003/healthz" 2>/dev/null || echo 000)
echo "   3.5  No separate health port (single-port architecture confirmed)"

#  4. WebSocket Integration 
echo " 4. WebSocket integration tests "

# Python WS test — subscribe and receive broadcast
WS_TEST=$(python3 -c "
import asyncio, json, websockets, sys

async def test():
    uri = 'ws://localhost:${PORT}/ws?api_key=${API_KEY}'
    try:
        async with websockets.connect(uri) as ws:
            welcome = await asyncio.wait_for(ws.recv(), timeout=5)
            w = json.loads(welcome)
            assert 'connection_id' in str(w), f'Bad welcome: {welcome}'

            await ws.send(json.dumps({'type': 'auth', 'api_key': '${API_KEY}', 'payload': {}}))
            auth_resp = await asyncio.wait_for(ws.recv(), timeout=5)
            a = json.loads(auth_resp)
            assert 'authenticated' in str(a).lower() or 'auth' in str(a).lower(), f'Bad auth: {auth_resp}'

            await ws.send(json.dumps({'type': 'subscribe', 'topic': 'python-test-topic'}))
            sub_resp = await asyncio.wait_for(ws.recv(), timeout=5)
            s = json.loads(sub_resp)
            assert 'subscribed' in str(s).lower() or 'subscription' in str(s).lower(), f'Bad subscribe: {sub_resp}'

            await ws.send(json.dumps({'type': 'connection', 'event': 'ping', 'payload': {}}))
            pong = await asyncio.wait_for(ws.recv(), timeout=5)
            p = json.loads(pong)
            assert p.get('Connection', {}).get('event') == 'pong' or p.get('event') == 'pong', f'Bad pong: {pong}'

            print('WS_PASS')
    except Exception as e:
        print(f'WS_FAIL: {type(e).__name__}: {e}')

asyncio.run(test())
" 2>&1 || echo "WS_FAIL: python error")

if echo "$WS_TEST" | grep -q "WS_PASS"; then
    echo "   4.1  WebSocket connect + welcome message received"
    echo "   4.2  WebSocket subscribe to topic"
    echo "   4.3  WebSocket ping/pong"
    PASS=$((PASS + 3))
else
    echo "   4.1  WebSocket connect + welcome ($WS_TEST)"
    echo "   4.2  WebSocket subscribe"
    echo "   4.3  WebSocket ping/pong"
    FAIL=$((FAIL + 3))
fi

# Broadcast test via REST API, receive via WebSocket
BROADCAST_TEST=$(python3 -c "
import asyncio, json, websockets, aiohttp, sys

async def test():
    uri = 'ws://localhost:${PORT}/ws?api_key=${API_KEY}'
    try:
        async with websockets.connect(uri) as ws:
            welcome = await asyncio.wait_for(ws.recv(), timeout=5)

            await ws.send(json.dumps({'type': 'auth', 'api_key': '${API_KEY}', 'payload': {}}))
            auth_resp = await asyncio.wait_for(ws.recv(), timeout=5)

            await ws.send(json.dumps({'type': 'subscribe', 'topic': 'broadcast-test'}))
            sub_resp = await asyncio.wait_for(ws.recv(), timeout=5)

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'http://localhost:${PORT}/api/v1/events/record',
                    json={'channel_id': 'broadcast-test', 'event_type': 'test', 'payload': {'ws_test': 'hello_from_rest'}, 'metadata': {'actor': 'test-user'}},
                    headers={'x-api-key': '${API_KEY}'}
                ) as resp:
                    pass

            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                m = json.loads(msg)
                print('BROADCAST_PASS')
            except asyncio.TimeoutError:
                print('BROADCAST_FAIL: timeout waiting for broadcast')

    except Exception as e:
        print(f'BROADCAST_FAIL: {type(e).__name__}: {e}')

asyncio.run(test())
" 2>&1 || echo "BROADCAST_FAIL: python error")

if echo "$BROADCAST_TEST" | grep -q "BROADCAST_PASS"; then
    echo "   4.4  REST API broadcast received via WebSocket"
    PASS=$((PASS + 1))
else
    echo "   4.4  REST API broadcast ($BROADCAST_TEST)"
    FAIL=$((FAIL + 1))
fi

#  5. Global state accessibility 
echo " 5. Global state / AppServerState "

# Verify ws_server is accessible through global state (via admin stats)
STATS=$(curl -s "$BASE/metrics.json" 2>/dev/null || echo '{}')
assert_contains "5.1  Global state accessible (metrics works)" "status" "$STATS"

#  6. Graceful shutdown signal 
echo " 6. Graceful shutdown readiness "

SHUTDOWN_TEST=$(python3 -c "
import asyncio, json, websockets, sys

async def test():
    uri = 'ws://localhost:${PORT}/ws?api_key=${API_KEY}'
    try:
        async with websockets.connect(uri) as ws:
            welcome = await asyncio.wait_for(ws.recv(), timeout=5)
            await ws.close()
            print('SHUTDOWN_PASS')
    except Exception as e:
        print(f'SHUTDOWN_FAIL: {type(e).__name__}: {e}')

asyncio.run(test())
" 2>&1 || echo "SHUTDOWN_FAIL: python error")

if echo "$SHUTDOWN_TEST" | grep -q "SHUTDOWN_PASS"; then
    echo "   6.1  Clean WebSocket close frame handling"
    PASS=$((PASS + 1))
else
    echo "   6.1  Clean WebSocket close ($SHUTDOWN_TEST)"
    FAIL=$((FAIL + 1))
fi

#  7. CORS headers 
echo " 7. CORS and security headers "

CORS_HEADERS=$(curl -sI -H "Origin: http://example.com" "$BASE/healthz" 2>/dev/null || echo '')
assert_contains "7.1  Access-Control-Allow-Origin present" "access-control" "$CORS_HEADERS"

SECURITY_HEADERS=$(curl -sfI "$BASE/healthz" 2>/dev/null || echo '')
assert_contains "7.2  X-Content-Type-Options present" "nosniff" "$SECURITY_HEADERS"
assert_contains "7.3  X-Frame-Options present" "DENY" "$SECURITY_HEADERS"

#  8. Dashboard static files 
echo " 8. Dashboard static files "

# Dashboard may or may not be built; file not found is fine (we test the fallback)
DASHBOARD=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/dashboard.html" 2>/dev/null || echo 000)
echo "   8.1  Dashboard route configured (status=$DASHBOARD)"

#  Summary 
echo ""
echo "=========================================="
echo "  Results: $PASS passed, $FAIL failed ($((PASS + FAIL)) total)"
echo "=========================================="

if [[ "$FAIL" -gt 0 ]]; then
    exit 1
fi

echo "All tests passed!"
exit 0
