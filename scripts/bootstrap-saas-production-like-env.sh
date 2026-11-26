#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Create a safe local parity environment from an existing Sandbox configuration.
# It never accepts Polar production or a remote database, and regenerates the
# secrets that protect local control-plane data.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_file="${1:-$root/saas/actae-saas.env}"
target_file="${2:-$root/saas/actae-saas.local.env}"

fail() { echo "error: $*" >&2; exit 1; }
value() { sed -n "s/^$1=//p" "$source_file" | tail -n 1; }
replace() {
  local key="$1" replacement="$2" temporary
  temporary="$(mktemp "${target_file}.XXXXXX")"
  sed "/^${key}=/d" "$target_file" >"$temporary"
  printf '%s=%s\n' "$key" "$replacement" >>"$temporary"
  mv "$temporary" "$target_file"
}

[[ -f "$source_file" ]] || fail "source environment file not found: $source_file"
[[ ! -e "$target_file" ]] || fail "refusing to overwrite existing local file: $target_file"
command -v openssl >/dev/null || fail "openssl is required"
command -v python3 >/dev/null || fail "python3 is required"

db_url="$(value SAAS_DATABASE_URL)"
polar_url="$(value SAAS_POLAR_BASE_URL)"
[[ "$db_url" == *"@127.0.0.1:"* || "$db_url" == *"@localhost:"* ]] || fail "source SAAS_DATABASE_URL is not loopback"
[[ "$polar_url" == "https://sandbox-api.polar.sh" ]] || fail "source must use Polar Sandbox; production credentials are never copied"

umask 077
cp "$source_file" "$target_file"
local_db_password="$(openssl rand -hex 24)"
local_fernet="$(PYTHONPATH="$root/saas/backend/src" python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
local_license_key="$(PYTHONPATH="$root/saas/backend/src" python3 -c 'from actae_saas import crypto; print(crypto.generate_license_keypair()[0])')"

replace SAAS_LOCAL_POSTGRES_PASSWORD "$local_db_password"
replace SAAS_DATABASE_URL "postgresql://actae_saas:${local_db_password}@127.0.0.1:55439/actae_saas?sslmode=disable"
replace SAAS_PUBLIC_BASE_URL "http://127.0.0.1:8004"
replace SAAS_ALLOWED_HOSTS "127.0.0.1,localhost"
replace SAAS_CORS_ORIGINS "http://127.0.0.1:8004,http://localhost:8004"
replace SAAS_POLAR_CHECKOUT_SUCCESS_URL "http://127.0.0.1:8004/app?checkout=success&checkout_id={CHECKOUT_ID}"
replace SAAS_POLAR_CHECKOUT_RETURN_URL "http://127.0.0.1:8004/app/settings/"
replace SAAS_KEY_ENCRYPTION_KEY "$local_fernet"
replace SAAS_LICENSE_SIGNING_KEY "$local_license_key"
replace SAAS_HOSTED_ENABLED "false"

# Prevent a laptop from using a production-like R2 or hosted-runtime surface.
for key in SAAS_R2_ACCOUNT_ID SAAS_R2_ACCESS_KEY_ID SAAS_R2_SECRET_ACCESS_KEY SAAS_R2_BUCKET SAAS_R2_ENDPOINT SAAS_HOSTED_DOMAIN SAAS_HOSTED_RUNTIME_IMAGE SAAS_HOSTED_BACKUP_IMAGE SAAS_HOSTED_BACKUP_ENV_FILE; do
  replace "$key" ""
done

chmod 0600 "$target_file"
echo "Created isolated local parity configuration: $target_file"
echo "Mailjet and Polar Sandbox credentials were retained; database, URLs, hosting, R2, Fernet, and signing keys are local-only."
