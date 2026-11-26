#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Replicate scripts/publish-hosted-runtime.sh but push to the local
# registry (127.0.0.1:5000) — the script itself rejects the `127.0.0.1:5000/`
# prefix in ACTAE_RUNTIME_IMAGE because of the embedded colon.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
IMAGE="127.0.0.1:5000/actae-runtime"
PUBKEY="${ACTAE_LICENSE_PUBKEY:?set ACTAE_LICENSE_PUBKEY}"
TARGET="x86_64-unknown-linux-musl"
PG_PLATFORM="linux-amd64-alpine"
TAG="candidate-e2e-$(git -C "$REPO_ROOT" rev-parse --short=12 HEAD)"
REFERENCE="$IMAGE:$TAG"

WORK="$(mktemp -d)"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT
JAR="$WORK/embedded-postgres.jar"
CONTEXT="$WORK/context"

echo "==> Building embedded dashboard"
(
  cd "$REPO_ROOT/dashboard"
  NEXT_PUBLIC_API_BASE= NEXT_PUBLIC_MOCK_MODE=false npm ci
  NEXT_PUBLIC_API_BASE= NEXT_PUBLIC_MOCK_MODE=false npm run build
)

echo "==> Fetching and verifying embedded PostgreSQL ($PG_PLATFORM)"
"$REPO_ROOT/scripts/fetch-pg-jar.sh" "$PG_PLATFORM" "$JAR"

echo "==> Building production Linux runtime"
(
  cd "$REPO_ROOT/actae"
  ACTAE_PG_EMBED_JAR="$JAR" \
  ACTAE_PG_EMBED_PLATFORM="$PG_PLATFORM" \
  ACTAE_LICENSE_PUBKEY="$PUBKEY" \
  cargo zigbuild --release --features production --target "$TARGET"
)

mkdir -p "$CONTEXT/bins/amd64"
install -m 0755 "$REPO_ROOT/actae/target/$TARGET/release/actae" "$CONTEXT/bins/amd64/actae"
install -m 0644 "$REPO_ROOT/docker/actae/Dockerfile.release" "$CONTEXT/Dockerfile"

echo "==> Building $REFERENCE"
docker buildx build --platform linux/amd64 --load \
  --file "$CONTEXT/Dockerfile" --tag "$REFERENCE" "$CONTEXT"

echo "==> Pushing $REFERENCE"
docker push "$REFERENCE"

DIGEST="$(docker buildx imagetools inspect "$REFERENCE" 2>/dev/null | awk '/^Digest:[[:space:]]*/ {print $2; exit}')"
[[ "$DIGEST" =~ ^sha256:[a-f0-9]{64}$ ]] || {
  echo "error: could not resolve an OCI digest after push" >&2
  exit 1
}

echo
echo "Published and verified runtime image:"
echo "SAAS_HOSTED_RUNTIME_IMAGE=$IMAGE@$DIGEST"