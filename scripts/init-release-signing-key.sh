#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Create the Actae release-signing GPG key (Ed25519, sign-only) and export its
# public key next to the site's `.well-known` path. The key is passphrase-less
# so `release-r2.sh` can sign non-interactively; protect the machine/keyring.
#
# Usage: scripts/init-release-signing-key.sh [output.asc]
#   ACTAE_GPG_NAME / ACTAE_GPG_EMAIL override the identity
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${ACTAE_GPG_NAME:-Actae Releases}"
EMAIL="${ACTAE_GPG_EMAIL:-viganotti.exe@gmail.com}"
KEY_UID="$NAME <$EMAIL>"
OUT="${1:-$ROOT/saas/frontend/public/.well-known/actae-release-signing-key.asc}"

command -v gpg >/dev/null 2>&1 || { echo "error: gpg is required" >&2; exit 1; }

FPR="$( { gpg --list-secret-keys --with-colons "$EMAIL" 2>/dev/null || true; } | awk -F: '/^fpr:/{print $10; exit}')"
if [[ -z "$FPR" ]]; then
  echo "==> Generating Ed25519 signing key for $KEY_UID (no passphrase)"
  gpg --batch --passphrase '' --quick-generate-key "$KEY_UID" ed25519 sign 0
  FPR="$(gpg --list-secret-keys --with-colons "$EMAIL" | awk -F: '/^fpr:/{print $10; exit}')"
else
  echo "==> Reusing existing signing key for $EMAIL"
fi

mkdir -p "$(dirname "$OUT")"
gpg --armor --export "$FPR" > "$OUT"

echo "fingerprint: $FPR"
echo "public key:  $OUT"
echo
echo "Sign releases with:"
echo "  export ACTAE_GPG_KEY=$FPR"
echo "  scripts/release.sh            # or scripts/release-r2.sh ..."
