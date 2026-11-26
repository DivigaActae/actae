#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Binaries-only release: build the host's platform(s), GPG-sign the checksums,
# and upload the archives + manifest + SHA256SUMS(.asc) to R2. No CI, no SaaS
# deploy. Windows is not built here (only a Windows host or the tagged CI job
# can); macOS + Linux are.
#
# For the full hosted-runtime + SaaS + tenant roll, use
# saas/ops/release-and-deploy.sh (which also calls scripts/release-r2.sh).
#
# Usage:
#   scripts/release.sh [--version X.Y.Z] [--targets "..."] [--tag] [--dry-run]
#     --version V   bump actae/Cargo.toml to V before building
#     --tag         create a local git tag vVERSION (not pushed)
#     --dry-run     build + assemble the manifest, skip the upload
#   Env:
#     ACTAE_LICENSE_PUBKEY  production license pubkey baked into the binary
#                           (derived from the VPS when SAAS_HOST is set)
#     ACTAE_GPG_KEY         GPG key id for SHA256SUMS.asc (see
#                           scripts/init-release-signing-key.sh)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RELEASE_ENV="${SAAS_RELEASE_ENV_FILE:-$ROOT/saas/actae-saas.release.env}"
VERSION=""
TARGETS=""
TAG=0
DRY_RUN=0
ALLOW_DEV_KEY=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) VERSION="$2"; shift 2 ;;
    --targets) TARGETS="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --tag) TAG=1; shift ;;
    --allow-dev-key) ALLOW_DEV_KEY=1; shift ;;
    *) echo "usage: $0 [--version X.Y.Z] [--targets \"...\"] [--tag] [--dry-run] [--allow-dev-key]" >&2; exit 2 ;;
  esac
done

[[ -f "$RELEASE_ENV" ]] || { echo "error: release env file not found: $RELEASE_ENV" >&2; exit 1; }
command -v aws >/dev/null 2>&1 || { echo "error: the aws CLI is required" >&2; exit 1; }

# 1. Optionally bump the crate version.
if [[ -n "$VERSION" ]]; then
  echo "==> Bumping actae/Cargo.toml to $VERSION"
  python3 - "$ROOT/actae/Cargo.toml" "$VERSION" <<'PY'
import re, sys
path, version = sys.argv[1], sys.argv[2]
s = open(path).read()
s2, n = re.subn(r'(?m)^version = "[^"]+"', 'version = "%s"' % version, s, count=1)
assert n == 1, "version line not found"
open(path, "w").write(s2)
PY
fi
CRATE_VERSION="$(grep -m1 '^version' "$ROOT/actae/Cargo.toml" | sed -E 's/.*= *"([^"]+)".*/\1/')"
echo "==> Releasing Actae $CRATE_VERSION"

# 2. Resolve the production license pubkey (baked into the binary). A release
#    built with the dev/test key cannot verify production licenses, so this
#    fails closed unless --allow-dev-key is given.
if [[ -z "${ACTAE_LICENSE_PUBKEY:-}" && -n "${SAAS_HOST:-}" ]]; then
  echo "==> Deriving production license public key from $SAAS_HOST"
  SSH_OPTS=(-o ControlMaster=auto -o "ControlPath=$HOME/.ssh/cm-actae-%C" -o ControlPersist=600 -o ConnectTimeout=15)
  PUBKEY_STAGE="$(ssh -n "${SSH_OPTS[@]}" "$SAAS_HOST" "mktemp /tmp/actae-rel-pubkey.XXXXXX")"
  ssh -tt "${SSH_OPTS[@]}" "$SAAS_HOST" "sudo /opt/actae-saas/venv/bin/python -c \"from actae_saas.config import load_env_file; import os; from actae_saas.crypto import license_signing_key, license_public_key_b64; load_env_file('/etc/actae-saas.env'); print(license_public_key_b64(license_signing_key(os.environ['SAAS_LICENSE_SIGNING_KEY'])))\" > '$PUBKEY_STAGE'"
  ACTAE_LICENSE_PUBKEY="$(ssh -n "${SSH_OPTS[@]}" "$SAAS_HOST" "cat '$PUBKEY_STAGE'")"
  ssh -n "${SSH_OPTS[@]}" "$SAAS_HOST" "rm -f '$PUBKEY_STAGE'" || true
  export ACTAE_LICENSE_PUBKEY
fi
if [[ -n "${ACTAE_LICENSE_PUBKEY:-}" && ! "$ACTAE_LICENSE_PUBKEY" =~ ^[A-Za-z0-9_-]{43}$ ]]; then
  echo "error: ACTAE_LICENSE_PUBKEY is not a valid base64url Ed25519 public key" >&2
  exit 1
fi
if [[ -z "${ACTAE_LICENSE_PUBKEY:-}" ]]; then
  if [[ "$ALLOW_DEV_KEY" == "1" ]]; then
    echo "WARNING: building with the DEV/TEST license key (--allow-dev-key); not for production" >&2
  else
    echo "error: no production license public key to bake into the binary." >&2
    echo "  export ACTAE_LICENSE_PUBKEY=<43-char base64url>, or set SAAS_HOST=... to derive it," >&2
    echo "  or pass --allow-dev-key for a throwaway/dev build." >&2
    exit 1
  fi
fi

# 3. Build the embedded dashboard (page-origin, hermetic) for the binary.
echo "==> Building the embedded dashboard"
( cd "$ROOT/dashboard" && NEXT_PUBLIC_API_BASE= NEXT_PUBLIC_MOCK_MODE=false npm run build )

# 4. Default targets + cross toolchain.
if [[ -z "$TARGETS" ]]; then
  TARGETS="aarch64-apple-darwin x86_64-apple-darwin x86_64-unknown-linux-musl aarch64-unknown-linux-musl"
fi
BUILD_FLAG=()
if command -v cargo-zigbuild >/dev/null 2>&1 && command -v zig >/dev/null 2>&1; then
  BUILD_FLAG=(--zigbuild)
elif command -v cross >/dev/null 2>&1; then
  BUILD_FLAG=(--cross)
fi
if [[ "$TARGETS" == *"linux"* && ${#BUILD_FLAG[@]} -eq 0 ]]; then
  echo "error: Linux targets need cargo-zigbuild+zig or cross" >&2
  exit 1
fi
if [[ -n "${ACTAE_GPG_KEY:-}" ]]; then
  echo "==> GPG signing enabled (key ${ACTAE_GPG_KEY})"
else
  echo "WARNING: ACTAE_GPG_KEY unset; SHA256SUMS will ship unsigned" >&2
fi

# 5. Build + package + (optionally sign) + upload.
R2_ARGS=(--env-file "$RELEASE_ENV" --targets "$TARGETS")
[[ "$DRY_RUN" == "1" ]] && R2_ARGS+=(--dry-run)
( cd "$ROOT" && scripts/release-r2.sh "${R2_ARGS[@]}" "${BUILD_FLAG[@]}" )

if [[ "$TAG" == "1" && "$DRY_RUN" != "1" ]]; then
  echo "==> Creating local tag v$CRATE_VERSION (not pushed)"
  git -C "$ROOT" tag -f "v$CRATE_VERSION" >/dev/null
  echo "    push with: git push origin v$CRATE_VERSION"
fi

echo "Release $CRATE_VERSION complete."
