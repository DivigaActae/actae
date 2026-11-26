#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Run the SaaS through the same production code path: PostgreSQL, static
# frontend served by FastAPI, signed control-plane data, Mailjet, and Polar
# Sandbox. It intentionally refuses production-like unsafe configuration.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${SAAS_LOCAL_ENV_FILE:-$root/saas/actae-saas.local.env}"
compose_file="$root/saas/ops/local-postgres.compose.yml"
runtime_dir="$root/saas/.local-runtime"
backend_log="$runtime_dir/backend.log"
worker_log="$runtime_dir/worker.log"
backend_pid=""
worker_pid=""

fail() { echo "error: $*" >&2; exit 1; }
value() { sed -n "s/^$1=//p" "$env_file" | tail -n 1; }

[[ -f "$env_file" ]] || fail "copy saas/actae-saas.local.env.example to saas/actae-saas.local.env first"
[[ -f "$compose_file" ]] || fail "local PostgreSQL compose file is missing"
command -v docker >/dev/null || fail "Docker is required for the isolated local PostgreSQL database"
docker compose version >/dev/null || fail "Docker Compose v2 is required"
command -v npm >/dev/null || fail "npm is required"
command -v python3 >/dev/null || fail "python3 is required"
command -v curl >/dev/null || fail "curl is required"
python3 -c 'import psycopg' >/dev/null 2>&1 || fail "install backend dependencies first: python3 -m pip install -r saas/backend/requirements.txt"

db_url="$(value SAAS_DATABASE_URL)"
base_url="$(value SAAS_PUBLIC_BASE_URL)"
polar_url="$(value SAAS_POLAR_BASE_URL)"
[[ "$db_url" == *"@127.0.0.1:"* || "$db_url" == *"@localhost:"* ]] || fail "local runner refuses a non-loopback SAAS_DATABASE_URL"
[[ "$base_url" == "http://127.0.0.1:8004" || "$base_url" == "http://localhost:8004" ]] || fail "local runner requires SAAS_PUBLIC_BASE_URL on local port 8004"
[[ "$polar_url" == "https://sandbox-api.polar.sh" ]] || fail "local runner permits Polar Sandbox only"
[[ -n "$(value SAAS_KEY_ENCRYPTION_KEY)" && -n "$(value SAAS_LICENSE_SIGNING_KEY)" ]] || fail "generate independent local Fernet and license-signing keys"
[[ -n "$(value SAAS_LOCAL_POSTGRES_PASSWORD)" ]] || fail "set SAAS_LOCAL_POSTGRES_PASSWORD"

cleanup() {
  trap - INT TERM EXIT
  [[ -n "$worker_pid" ]] && kill "$worker_pid" 2>/dev/null || true
  [[ -n "$backend_pid" ]] && kill "$backend_pid" 2>/dev/null || true
  [[ -n "$worker_pid" ]] && wait "$worker_pid" 2>/dev/null || true
  [[ -n "$backend_pid" ]] && wait "$backend_pid" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

mkdir -p "$runtime_dir"
echo "==> Starting isolated PostgreSQL"
docker compose --env-file "$env_file" -f "$compose_file" up -d --wait

echo "==> Building static frontend (the production serving model)"
(cd "$root/saas/frontend" && npm run build)

echo "==> Starting SaaS backend on $base_url"
(
  cd "$root"
  export PYTHONPATH="$root/saas/backend/src"
  export SAAS_STATIC_DIR="$root/saas/frontend/out"
  exec python3 -m actae_saas --env-file "$env_file" --host 127.0.0.1 --port 8004
) >"$backend_log" 2>&1 &
backend_pid=$!

for _ in {1..80}; do
  if curl --silent --fail "$base_url/healthz" >/dev/null; then break; fi
  kill -0 "$backend_pid" 2>/dev/null || { tail -n 80 "$backend_log" >&2; fail "backend exited during startup"; }
  sleep 0.25
done
curl --silent --fail "$base_url/healthz" >/dev/null || fail "backend did not become healthy; see $backend_log"

start_hosted_worker="${SAAS_LOCAL_START_HOSTED_WORKER:-$(value SAAS_LOCAL_START_HOSTED_WORKER)}"
if [[ "$start_hosted_worker" == "1" ]]; then
  [[ "$(value SAAS_HOSTED_ENABLED)" == "true" ]] || fail "SAAS_LOCAL_START_HOSTED_WORKER=1 requires SAAS_HOSTED_ENABLED=true"
  echo "==> Starting hosted worker (Docker runtime mutations enabled)"
  (
    cd "$root"
    export PYTHONPATH="$root/saas/backend/src"
    exec python3 -m actae_saas --env-file "$env_file" worker
  ) >"$worker_log" 2>&1 &
  worker_pid=$!
fi

echo
echo "Production-like local SaaS is ready: $base_url"
echo "Backend log: $backend_log"
[[ -n "$worker_pid" ]] && echo "Worker log:  $worker_log"
echo "This uses real Mailjet delivery and Polar Sandbox. Press Ctrl+C to stop the app; PostgreSQL persists in its named local Docker volume."
wait "$backend_pid"
