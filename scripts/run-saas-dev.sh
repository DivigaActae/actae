#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
backend_dir="$repo_root/saas/backend"
frontend_dir="$repo_root/saas/frontend"
runtime_dir="${ACTAE_SAAS_DEV_DIR:-/tmp/actae-saas-portal-dev}"
db_path="$runtime_dir/saas.db"
backend_log="$runtime_dir/backend.log"
frontend_log="$runtime_dir/frontend.log"
cookie_jar="$runtime_dir/demo.cookies"
backend_url="http://127.0.0.1:8004"
frontend_url="http://localhost:3000"
demo_email="alice@example.com"
demo_password="Correct-horse-42"
backend_pid=""
frontend_pid=""

mkdir -p "$runtime_dir"

cleanup() {
  trap - INT TERM EXIT
  if [[ -n "$frontend_pid" ]] && kill -0 "$frontend_pid" 2>/dev/null; then
    kill "$frontend_pid" 2>/dev/null || true
  fi
  if [[ -n "$backend_pid" ]] && kill -0 "$backend_pid" 2>/dev/null; then
    kill "$backend_pid" 2>/dev/null || true
  fi
  [[ -n "$frontend_pid" ]] && wait "$frontend_pid" 2>/dev/null || true
  [[ -n "$backend_pid" ]] && wait "$backend_pid" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

fail() {
  printf '\nSaaS development launcher failed: %s\n' "$1" >&2
  printf 'Backend log: %s\nFrontend log: %s\n' "$backend_log" "$frontend_log" >&2
  exit 1
}

command -v python3 >/dev/null 2>&1 || fail "python3 is required"
command -v npm >/dev/null 2>&1 || fail "npm is required"
command -v curl >/dev/null 2>&1 || fail "curl is required"
[[ -d "$frontend_dir/node_modules" ]] || fail "frontend dependencies are missing; run: cd saas/frontend && npm install"

if curl --silent --fail "$backend_url/healthz" >/dev/null 2>&1; then
  fail "port 8004 is already serving another backend"
fi
if curl --silent --fail "$frontend_url" >/dev/null 2>&1; then
  fail "port 3000 is already serving another frontend"
fi

fernet_key="$(PYTHONPATH="$backend_dir/src" python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')" \
  || fail "backend Python dependencies are missing; run: pip install -e ./saas/backend"
license_signing_key="$(PYTHONPATH="$backend_dir/src" python3 -c 'from actae_saas import crypto; print(crypto.generate_license_keypair()[0])')" \
  || fail "could not generate a local license signing key; check backend dependencies"

printf 'Starting Actae SaaS backend...\n'
(
  cd "$backend_dir"
  SAAS_DB_PATH="$db_path" \
  SAAS_KEY_ENCRYPTION_KEY="$fernet_key" \
  SAAS_LICENSE_SIGNING_KEY="$license_signing_key" \
  SAAS_PASSWORD_ITERATIONS=100000 \
  SAAS_PUBLIC_BASE_URL="$frontend_url" \
  SAAS_CORS_ORIGINS="$frontend_url" \
  SAAS_LOG_LEVEL=info \
  PYTHONPATH="$backend_dir/src" \
  exec python3 -m actae_saas --host 127.0.0.1 --port 8004
) >"$backend_log" 2>&1 &
backend_pid=$!

for _ in {1..80}; do
  if curl --silent --fail "$backend_url/healthz" >/dev/null 2>&1; then
    break
  fi
  kill -0 "$backend_pid" 2>/dev/null || fail "backend exited during startup"
  sleep 0.25
done
curl --silent --fail "$backend_url/healthz" >/dev/null 2>&1 || fail "backend did not become healthy"

signup_status="$(curl --silent --output "$runtime_dir/signup.json" --write-out '%{http_code}' \
  --request POST "$backend_url/api/auth/signup" \
  --header 'Content-Type: application/json' \
  --data "{\"email\":\"$demo_email\",\"password\":\"$demo_password\",\"name\":\"Alice\"}")"

if [[ "$signup_status" != "201" ]]; then
  SAAS_DB_PATH="$db_path" SAAS_PASSWORD_ITERATIONS=100000 PYTHONPATH="$backend_dir/src" \
    python3 -m actae_saas admin reset-password "$demo_email" --password "$demo_password" >/dev/null \
    || fail "could not create or reset the demo account"
fi

rm -f "$cookie_jar"
login_status="$(curl --silent --output "$runtime_dir/login.json" --write-out '%{http_code}' \
  --cookie-jar "$cookie_jar" \
  --request POST "$backend_url/api/auth/login" \
  --header 'Content-Type: application/json' \
  --data "{\"email\":\"$demo_email\",\"password\":\"$demo_password\"}")"
[[ "$login_status" == "200" ]] || fail "demo account login failed"

account_json="$(curl --silent --fail --cookie "$cookie_jar" "$backend_url/api/portal/account")" \
  || fail "could not load the demo account"
installation_count="$(printf '%s' "$account_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data_summary"]["installation_count"])')"
if [[ "$installation_count" == "0" ]]; then
  curl --silent --fail --cookie "$cookie_jar" \
    --request POST "$backend_url/api/portal/installations" \
    --header 'Content-Type: application/json' \
    --data '{"name":"demo-production","method":"env"}' \
    >"$runtime_dir/installation.json" \
    || fail "could not create the demo installation"
fi

printf 'Starting Actae SaaS frontend...\n'
(
  cd "$frontend_dir"
  exec npm run dev
) >"$frontend_log" 2>&1 &
frontend_pid=$!

for _ in {1..120}; do
  if curl --silent --fail "$frontend_url" >/dev/null 2>&1; then
    break
  fi
  kill -0 "$frontend_pid" 2>/dev/null || fail "frontend exited during startup"
  sleep 0.25
done
curl --silent --fail "$frontend_url" >/dev/null 2>&1 || fail "frontend did not become ready"

printf '\n'
printf '╭────────────────────────────────────────────────────────╮\n'
printf '│  Actae SaaS development site is ready                  │\n'
printf '├────────────────────────────────────────────────────────┤\n'
printf '│  Website:  %-43s│\n' "$frontend_url"
printf '│  Portal:   %-43s│\n' "$frontend_url/app/"
printf '│  Backend:  %-43s│\n' "$backend_url"
printf '│                                                        │\n'
printf '│  Email:    %-43s│\n' "$demo_email"
printf '│  Password: %-43s│\n' "$demo_password"
printf '├────────────────────────────────────────────────────────┤\n'
printf '│  Press Ctrl+C to stop both services.                   │\n'
printf '╰────────────────────────────────────────────────────────╯\n'
printf '\nLogs: %s and %s\n' "$frontend_log" "$backend_log"

wait "$backend_pid" "$frontend_pid"
