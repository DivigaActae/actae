#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Build, smoke-check, and publish the isolated fleet gateway image.  This is
# intentionally separate from scripts/publish-hosted-runtime.sh: hosted Actae
# instances need embedded PostgreSQL while the gateway must never contain it.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
IMAGE="${ACTAE_FLEET_GATEWAY_IMAGE:-}"
TAG=""
TARGET="x86_64-unknown-linux-musl"

usage() {
  cat <<'EOF'
usage: ACTAE_FLEET_GATEWAY_IMAGE=<registry>/actae-fleet-gateway \
       scripts/publish-federation-gateway.sh [--tag TAG]

Builds the gateway with federation-gateway + gateway-nats enabled, verifies
the produced binary, publishes it, and prints a digest-pinned image reference.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag) TAG="${2:-}"; [[ -n "$TAG" ]] || { usage >&2; exit 2; }; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

[[ -n "$IMAGE" ]] || { echo "ACTAE_FLEET_GATEWAY_IMAGE is required" >&2; exit 1; }
[[ "$IMAGE" != *"@"* && "$IMAGE" != *":"* ]] || {
  echo "ACTAE_FLEET_GATEWAY_IMAGE must be an untagged repository" >&2; exit 1;
}
for command in docker cargo rustup zig; do
  command -v "$command" >/dev/null 2>&1 || { echo "required command unavailable: $command" >&2; exit 1; }
done
command -v cargo-zigbuild >/dev/null 2>&1 || {
  echo "cargo-zigbuild is required; install with cargo install cargo-zigbuild --locked" >&2; exit 1;
}
docker buildx version >/dev/null 2>&1 || { echo "Docker buildx is required" >&2; exit 1; }

if [[ -z "$TAG" ]]; then
  TAG="release-$(git -C "$REPO_ROOT" rev-parse --short=12 HEAD)-$(date -u +%Y%m%d%H%M%S)"
fi
[[ "$TAG" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || { echo "invalid image tag" >&2; exit 1; }
REFERENCE="$IMAGE:$TAG"

rustup target list --installed | grep -qx "$TARGET" || rustup target add "$TARGET"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "==> Building production fleet gateway"
(
  cd "$REPO_ROOT/actae"
  cargo zigbuild --release --target "$TARGET" \
    --bin actae-federation-gateway \
    --features federation-gateway,gateway-nats
)

GATEWAY_BIN="$REPO_ROOT/actae/target/$TARGET/release/actae-federation-gateway"
[[ -x "$GATEWAY_BIN" ]] || { echo "gateway binary was not produced" >&2; exit 1; }
"$GATEWAY_BIN" --help >/dev/null 2>&1 || true

mkdir -p "$WORK/context/bins/amd64"
install -m 0755 "$GATEWAY_BIN" "$WORK/context/bins/amd64/actae-federation-gateway"
install -m 0644 "$REPO_ROOT/docker/actae/Dockerfile.federation-gateway.release" "$WORK/context/Dockerfile"

echo "==> Building and pushing $REFERENCE"
docker buildx build --platform linux/amd64 --load --file "$WORK/context/Dockerfile" --tag "$REFERENCE" "$WORK/context"
set +e
docker run --rm --entrypoint /usr/local/bin/actae-federation-gateway "$REFERENCE" >/dev/null 2>&1
gateway_start_status=$?
set -e
if [[ "$gateway_start_status" -eq 0 ]]; then
  echo "gateway unexpectedly started without required configuration" >&2
  exit 1
fi
docker push "$REFERENCE"
DIGEST="$(docker buildx imagetools inspect "$REFERENCE" 2>/dev/null | awk '/^Digest:[[:space:]]*/ {print $2; exit}')"
[[ "$DIGEST" =~ ^sha256:[a-f0-9]{64}$ ]] || { echo "could not resolve published digest" >&2; exit 1; }
printf 'ACTAE_FLEET_GATEWAY_IMAGE=%s@%s\n' "$IMAGE" "$DIGEST"
