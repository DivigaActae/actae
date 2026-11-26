#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

#
# Build, verify, and publish the one-architecture Actae runtime image used by
# the initial hosted beta.  This deliberately produces an immutable digest for
# SAAS_HOSTED_RUNTIME_IMAGE; the hosted worker must never consume a mutable
# tag.
#
# The final image is Alpine, so this builds the musl Actae binary with the
# matching Alpine PostgreSQL payload baked in.  It mirrors the Docker portion
# of .github/workflows/release.yml without requiring GitHub Actions.
#
# Prerequisites on the trusted release workstation:
#   Docker Desktop/Engine (with buildx), Rust/rustup, Zig, cargo-zigbuild,
#   Node/npm.  Log in to the target private registry before running.
#
# Usage:
#   ACTAE_RUNTIME_IMAGE=n4ughtydiviga/actae-runtime \
#   ACTAE_LICENSE_PUBKEY=<public-key-only> \
#   scripts/publish-hosted-runtime.sh [--tag candidate-20260817] [--no-smoke]
#
# The signing seed is never needed here and must never be supplied.  Obtain
# the public key from the configured SaaS signing key, then provide only that
# public value as ACTAE_LICENSE_PUBKEY.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
IMAGE="${ACTAE_RUNTIME_IMAGE:-}"
TAG=""
RUN_SMOKE=1
TARGET="x86_64-unknown-linux-musl"
PG_PLATFORM="linux-amd64-alpine"
TEST_LICENSE_PUBKEY="Zve_tDxUYX8mvCePbIGY26-7zzJVMy5N-3GsQxV-eds"

usage() {
  cat <<'EOF'
usage: ACTAE_RUNTIME_IMAGE=<namespace>/actae-runtime \
       ACTAE_LICENSE_PUBKEY=<public-key> scripts/publish-hosted-runtime.sh [--tag TAG] [--no-smoke]

Builds and publishes a linux/amd64 hosted-runtime image, then prints the
digest-pinned SAAS_HOSTED_RUNTIME_IMAGE value for /etc/actae-saas.env.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag) TAG="${2:-}"; [[ -n "$TAG" ]] || { usage >&2; exit 2; }; shift 2 ;;
    --no-smoke) RUN_SMOKE=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

[[ -n "$IMAGE" ]] || { echo "error: ACTAE_RUNTIME_IMAGE is required" >&2; exit 1; }
[[ "$IMAGE" != *"@"* && "$IMAGE" != *":"* ]] || {
  echo "error: ACTAE_RUNTIME_IMAGE must be an untagged repository, e.g. n4ughtydiviga/actae-runtime" >&2
  exit 1
}
[[ -n "${ACTAE_LICENSE_PUBKEY:-}" ]] || {
  echo "error: ACTAE_LICENSE_PUBKEY is required (public key only)" >&2
  exit 1
}
[[ "$ACTAE_LICENSE_PUBKEY" != "$TEST_LICENSE_PUBKEY" ]] || {
  echo "error: refusing to publish a hosted runtime with the shared test license key" >&2
  exit 1
}
[[ -z "${NEXT_PUBLIC_API_BASE:-}" && "${NEXT_PUBLIC_MOCK_MODE:-}" != "true" ]] || {
  echo "error: NEXT_PUBLIC_API_BASE/NEXT_PUBLIC_MOCK_MODE must not be set when publishing the runtime image (the release dashboard must use page-origin)" >&2
  exit 1
}

for command in docker cargo rustup zig npm; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "error: required command is unavailable: $command" >&2
    exit 1
  }
done
command -v cargo-zigbuild >/dev/null 2>&1 || {
  echo "error: cargo-zigbuild is required; install it with: cargo install cargo-zigbuild --locked" >&2
  exit 1
}
docker buildx version >/dev/null 2>&1 || {
  echo "error: Docker buildx is required" >&2
  exit 1
}

if [[ -z "$TAG" ]]; then
  TAG="candidate-$(git -C "$REPO_ROOT" rev-parse --short=12 HEAD)"
fi
[[ "$TAG" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || {
  echo "error: invalid image tag: $TAG" >&2
  exit 1
}
REFERENCE="$IMAGE:$TAG"

if ! rustup target list --installed | grep -qx "$TARGET"; then
  rustup target add "$TARGET"
fi

WORK="$(mktemp -d)"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT
JAR="$WORK/embedded-postgres.jar"
CONTEXT="$WORK/context"

echo "==> Building embedded dashboard"
# Hermetic build: the release dashboard must never carry a baked API base.
# A developer's dashboard/.env.local (NEXT_PUBLIC_API_BASE=http://localhost:8002
# is the dev default) or an exported shell variable would otherwise be inlined
# into the image, sending every tenant-dashboard request to the wrong origin.
# Explicit empty assignments override both sources deterministically; the
# runtime then falls back to page-origin (dashboard and API share the tenant
# endpoint).
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
  ACTAE_LICENSE_PUBKEY="$ACTAE_LICENSE_PUBKEY" \
  cargo zigbuild --release --features production --target "$TARGET"
)

mkdir -p "$CONTEXT/bins/amd64"
install -m 0755 "$REPO_ROOT/actae/target/$TARGET/release/actae" "$CONTEXT/bins/amd64/actae"
install -m 0644 "$REPO_ROOT/docker/actae/Dockerfile.release" "$CONTEXT/Dockerfile"

echo "==> Building $REFERENCE"
docker buildx build --platform linux/amd64 --load \
  --file "$CONTEXT/Dockerfile" --tag "$REFERENCE" "$CONTEXT"

if [[ "$RUN_SMOKE" == "1" ]]; then
  echo "==> Offline runtime smoke test"
  "$REPO_ROOT/scripts/docker-smoke.sh" "$REFERENCE"
fi

echo "==> Pushing $REFERENCE"
docker push "$REFERENCE"

# buildx obtains the digest from the registry, rather than trusting a local
# image ID.  The hosted worker receives this immutable reference only.
DIGEST="$(docker buildx imagetools inspect "$REFERENCE" 2>/dev/null | awk '/^Digest:[[:space:]]*/ {print $2; exit}')"
[[ "$DIGEST" =~ ^sha256:[a-f0-9]{64}$ ]] || {
  echo "error: could not resolve an OCI digest after push; run: docker buildx imagetools inspect $REFERENCE" >&2
  exit 1
}

echo
echo "Published and verified runtime image:"
echo "SAAS_HOSTED_RUNTIME_IMAGE=$IMAGE@$DIGEST"
