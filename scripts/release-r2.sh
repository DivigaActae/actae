#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

#
# Publish the Actae self-contained binary to the private Cloudflare R2
# distribution bucket (manual path; the GitHub Actions release workflow
# automates the same steps on `v*` tags).
#
# Builds production binaries (dashboard + embedded PostgreSQL baked in),
# packages them into `actae-<ver>-<platform>.{tar.gz,zip}`, uploads each to
#   actae/<version>/<filename>
# and rewrites actae/manifest.json in the shared format the SaaS
# downloads endpoints and `actae upgrade` consume:
#
#   {"v": 1, "latest": "1.0.0", "platforms": {
#     "linux-x86_64": {"version": "...", "filename": "...", "sha256": "...", "size": N}, ...}}
#
# Requirements: cargo, the `aws` CLI (S3-compatible endpoint), and the zonky
# PostgreSQL jar for each target (ACTAE_PG_EMBED_JAR, or fetched via
# scripts/fetch-pg-jar.sh). Cross-compilation uses `cargo-zigbuild` when
# --zigbuild is given (the Linux path, matching the release workflow), or
# `cross` when --cross is given.
#
# Credentials (from the environment): SAAS_R2_ACCOUNT_ID /
# SAAS_R2_ACCESS_KEY_ID / SAAS_R2_SECRET_ACCESS_KEY / SAAS_R2_BUCKET
# (R2_* spellings also accepted). ACTAE_LICENSE_PUBKEY should be set for a
# production release build.
#
# Usage:
#   scripts/release-r2.sh [--env-file FILE] [--version 0.2.0] [--targets "..."]
#   [--zigbuild] [--cross] [--dry-run] [--output-dir DIR]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
ACTAE_CRATE="$REPO_ROOT/actae"

DEFAULT_VERSION="$(grep -m1 '^version' "$ACTAE_CRATE/Cargo.toml" | sed -E 's/.*= *"([^"]+)".*/\1/')"
VERSION="${DEFAULT_VERSION:-0.1.0}"
DRY_RUN=0
USE_CROSS=0
USE_ZIGBUILD=0
TARGETS=""
ENV_FILE=""
# Local-friendly layout: <OUTPUT_DIR>/<version>/<os>/actae (e.g.
# dist/0.1.0/macos/actae, dist/0.1.0/linux/actae) so a build can be handed
# off directly. The remote R2 upload still uses the versioned tarballs.
OUTPUT_DIR="${REPO_ROOT}/dist"

# Build triple → (platform_key, pg_platform, ext). ACTAE_ALPINE=1 selects the
# musl/Alpine artifacts (PostgreSQL built against musl) — the `-alpine`
# release keys; glibc hosts must use the plain keys. The manual path mirrors
# the release workflow's matrix.
triple_to_platform() {
  case "$1" in
    x86_64-unknown-linux-musl)
      if [[ "${ACTAE_ALPINE:-0}" == "1" ]]; then echo "linux-x86_64-alpine linux-amd64-alpine tar.gz"
      else echo "linux-x86_64 linux-amd64 tar.gz"; fi ;;
    aarch64-unknown-linux-musl)
      if [[ "${ACTAE_ALPINE:-0}" == "1" ]]; then echo "linux-aarch64-alpine linux-arm64v8-alpine tar.gz"
      else echo "linux-aarch64 linux-arm64v8 tar.gz"; fi ;;
    x86_64-unknown-linux-gnu)   echo "linux-x86_64 linux-amd64 tar.gz" ;;
    aarch64-unknown-linux-gnu)  echo "linux-aarch64 linux-arm64v8 tar.gz" ;;
    aarch64-apple-darwin)       echo "macos-arm64 darwin-arm64v8 tar.gz" ;;
    x86_64-apple-darwin)        echo "macos-x86_64 darwin-amd64 tar.gz" ;;
    x86_64-pc-windows-msvc)     echo "windows-x86_64 windows-amd64 zip" ;;
    *) echo "" ;;
  esac
}

# macOS code signing (see docs/RELEASE_SIGNING.md §2): a free ad-hoc
# signature, shared with the CI release workflow so both paths sign
# identically. Must run BEFORE packaging so the signed binary is what goes
# into the archive (the manifest sha256 then covers it).
sign_macos_binary() {
  local bin="$1"
  "$REPO_ROOT/scripts/sign-macos.sh" "$bin"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) ENV_FILE="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --zigbuild) USE_ZIGBUILD=1; shift ;;
    --cross) USE_CROSS=1; shift ;;
    --targets) TARGETS="$2"; shift 2 ;;
    *) echo "usage: $0 [--env-file FILE] [    --version V] [--targets \"...\"] [--zigbuild|--cross] [--dry-run] [--output-dir DIR]" >&2; exit 2 ;;
  esac
done

if [[ -n "$ENV_FILE" ]]; then
  [[ -f "$ENV_FILE" ]] || { echo "error: env file not found: $ENV_FILE" >&2; exit 1; }
  # Parse literal KEY=value lines; do not source the file as shell code.
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "${line#\#}" != "$line" ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    case "$key" in
      SAAS_R2_ACCOUNT_ID|SAAS_R2_ACCESS_KEY_ID|SAAS_R2_SECRET_ACCESS_KEY|SAAS_R2_BUCKET|R2_ACCOUNT_ID|R2_ACCESS_KEY_ID|R2_SECRET_ACCESS_KEY|R2_BUCKET)
        export "$key=$value" ;;
    esac
  done < "$ENV_FILE"
fi

account_id="${SAAS_R2_ACCOUNT_ID:-${R2_ACCOUNT_ID:-}}"
access_key="${SAAS_R2_ACCESS_KEY_ID:-${R2_ACCESS_KEY_ID:-}}"
secret_key="${SAAS_R2_SECRET_ACCESS_KEY:-${R2_SECRET_ACCESS_KEY:-}}"
bucket="${SAAS_R2_BUCKET:-${R2_BUCKET:-}}"

for var in account_id access_key secret_key bucket; do
  if [[ -z "${!var}" ]]; then
    echo "error: SAAS_R2_$([ "$var" = bucket ] && echo BUCKET || echo "${var^^}") is not set" >&2
    exit 1
  fi
done

# AWS CLI uses the standard names even when the application uses SAAS_R2_*.
# Bind these explicitly so a developer's unrelated AWS profile cannot receive
# the upload or silently override the configured R2 credentials.
export AWS_ACCESS_KEY_ID="$access_key"
export AWS_SECRET_ACCESS_KEY="$secret_key"
export AWS_DEFAULT_REGION="${SAAS_R2_REGION:-auto}"

endpoint="${SAAS_R2_ENDPOINT:-https://${account_id}.r2.cloudflarestorage.com}"

if [[ -z "$TARGETS" ]]; then
  host_triple="$(rustc -vV | sed -n 's/^host: //p')"
  case "$host_triple" in
    aarch64-apple-darwin | x86_64-apple-darwin | x86_64-pc-windows-msvc) TARGETS="$host_triple" ;;
    x86_64-unknown-linux-gnu | aarch64-unknown-linux-gnu) TARGETS="$host_triple" ;;
    *) echo "error: unsupported host triple $host_triple; pass --targets" >&2; exit 1 ;;
  esac
  USE_CROSS=0
fi

read -ra triples <<< "$TARGETS"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# --------------------------------------------------------------------- #
# 1. Build + package each target
# --------------------------------------------------------------------- #
declare -A PKG_PATHS
for triple in "${triples[@]}"; do
  platform="$(triple_to_platform "$triple")"
  if [[ -z "$platform" ]]; then
    echo "error: unsupported target triple '$triple'" >&2
    exit 1
  fi
  read -r plat_key pg_platform ext <<< "$platform"
  echo "==> building $triple ($plat_key)"

  if [[ "$USE_CROSS" == "1" ]]; then
    (cd "$ACTAE_CRATE" && cross build --release --features production --target "$triple")
  else
    if command -v rustup >/dev/null 2>&1 && ! rustup target list --installed | grep -qx "$triple"; then
      rustup target add "$triple" >/dev/null
    fi
    jar="${ACTAE_PG_EMBED_JAR:-}"
    if [[ -z "$jar" ]]; then
      jar="$WORK/pg-${pg_platform}.jar"
      "$REPO_ROOT/scripts/fetch-pg-jar.sh" "$pg_platform" "$jar"
    fi
    if [[ "$USE_ZIGBUILD" == "1" ]]; then
      # Static musl Linux binaries via cargo-zigbuild (matching the release
      # workflow); darwin/windows targets fall through to a native build.
      if [[ "$triple" == *"-linux-"* ]]; then
        (cd "$ACTAE_CRATE" && ACTAE_PG_EMBED_JAR="$jar" ACTAE_PG_EMBED_PLATFORM="$pg_platform" cargo zigbuild --release --features production --target "$triple")
      else
        (cd "$ACTAE_CRATE" && ACTAE_PG_EMBED_JAR="$jar" ACTAE_PG_EMBED_PLATFORM="$pg_platform" cargo build --release --features production --target "$triple")
      fi
    else
      (cd "$ACTAE_CRATE" && ACTAE_PG_EMBED_JAR="$jar" ACTAE_PG_EMBED_PLATFORM="$pg_platform" cargo build --release --features production --target "$triple")
    fi
  fi

  bin_dir="$(cd "$ACTAE_CRATE" && pwd)/target/$triple/release"
  pkg_dir="$WORK/$plat_key"
  mkdir -p "$pkg_dir"
  if [[ "$triple" == *"-apple-darwin" ]]; then
    sign_macos_binary "$bin_dir/actae"
  fi
  if [[ "$ext" == "zip" ]]; then
    cp "$bin_dir/actae.exe" "$pkg_dir/actae.exe"
    (cd "$pkg_dir" && zip -q "$WORK/actae-${VERSION}-${plat_key}.zip" actae.exe)
  else
    cp "$bin_dir/actae" "$pkg_dir/actae"
    (cd "$pkg_dir" && tar -czf "$WORK/actae-${VERSION}-${plat_key}.tar.gz" actae)
  fi
  PKG_PATHS["$plat_key"]="$WORK/actae-${VERSION}-${plat_key}.${ext}"

  # Local-friendly hand-off layout: <OUTPUT_DIR>/<version>/<os>/actae, where
  # <os> is macos / linux / windows. The recipient runs `./macos/actae …`.
  os="${plat_key%%-*}"   # macos-arm64 → macos, linux-x86_64 → linux, …
  friendly_dir="$OUTPUT_DIR/$VERSION/$os"
  mkdir -p "$friendly_dir"
  if [[ "$ext" == "zip" ]]; then
    cp "$bin_dir/actae.exe" "$friendly_dir/actae.exe"
    chmod +x "$friendly_dir/actae.exe"
  else
    cp "$bin_dir/actae" "$friendly_dir/actae"
    chmod +x "$friendly_dir/actae"
  fi
  echo "==> local layout: $friendly_dir/actae"
done

# --------------------------------------------------------------------- #
# 2. Assemble the shared manifest (MERGED with the existing one, so a
#    host-limited local publish never drops platforms it did not build —
#    e.g. Windows, which only the tagged CI workflow can build).
# --------------------------------------------------------------------- #
built_manifest="$WORK/built-manifest.json"
PLATFORM_LIST=("${!PKG_PATHS[@]}")
{
  echo "{"
  first=1
  for plat in "${PLATFORM_LIST[@]}"; do
    path="${PKG_PATHS[$plat]}"
    sha="$(shasum -a 256 "$path" | cut -d' ' -f1)"
    size="$(wc -c < "$path" | tr -d ' ')"
    [[ $first -ne 1 ]] && echo ","
    printf '  "%s": {"version": "%s", "filename": "%s", "sha256": "%s", "size": %s}' \
      "$plat" "$VERSION" "$(basename "$path")" "$sha" "$size"
    first=0
  done
  echo
  echo "}"
} > "$built_manifest"

existing_manifest="$WORK/existing-manifest.json"
if [[ "$DRY_RUN" != "1" ]]; then
  aws s3 cp --endpoint-url "$endpoint" "s3://${bucket}/actae/manifest.json" "$existing_manifest" \
    --only-show-errors >/dev/null 2>&1 || true
fi
new_manifest="$WORK/manifest.json"
python3 - "$VERSION" "$built_manifest" "$existing_manifest" "$new_manifest" <<'PY'
import json, sys
version, built_path, existing_path, out_path = sys.argv[1:5]
with open(built_path) as fh:
    built = json.load(fh)
platforms = {}
try:
    with open(existing_path) as fh:
        platforms = json.load(fh).get("platforms") or {}
except (OSError, ValueError):
    platforms = {}
platforms.update(built)
with open(out_path, "w") as fh:
    json.dump({"v": 1, "latest": version, "platforms": platforms}, fh, indent=2)
sys.stderr.write("manifest platforms: %s\n" % ", ".join(sorted(platforms)))
PY

if [[ "$DRY_RUN" == "1" ]]; then
  echo "[dry-run] would upload to ${bucket} at ${endpoint}"
  cat "$new_manifest"
  exit 0
fi

# --------------------------------------------------------------------- #
# 2b. Checksums + optional GPG signature (docs/RELEASE_SIGNING.md)
# --------------------------------------------------------------------- #
checksums="$WORK/SHA256SUMS"
( cd "$WORK" && shasum -a 256 actae-"$VERSION"-* > SHA256SUMS )
signed_checksums=""
if [[ -n "${ACTAE_GPG_KEY:-}" ]]; then
  echo "==> signing SHA256SUMS with GPG key ${ACTAE_GPG_KEY}"
  gpg --batch --yes --armor --detach-sign --local-user "$ACTAE_GPG_KEY" "$checksums"
  signed_checksums="$checksums.asc"
fi

# --------------------------------------------------------------------- #
# 3. Upload
# --------------------------------------------------------------------- #
for plat in "${PLATFORM_LIST[@]}"; do
  key="actae/${VERSION}/$(basename "${PKG_PATHS[$plat]}")"
  echo "==> uploading $key"
  aws s3 cp --endpoint-url "$endpoint" \
    --content-type application/octet-stream \
    --content-disposition "attachment; filename=\"$(basename "${PKG_PATHS[$plat]}")\"" \
    "${PKG_PATHS[$plat]}" "s3://${bucket}/${key}"
done

echo "==> uploading manifest"
aws s3 cp --endpoint-url "$endpoint" \
  --content-type application/json \
  "$new_manifest" "s3://${bucket}/actae/manifest.json"

echo "==> uploading checksums"
aws s3 cp --endpoint-url "$endpoint" \
  --content-type text/plain \
  "$checksums" "s3://${bucket}/actae/SHA256SUMS"
if [[ -n "$signed_checksums" ]]; then
  aws s3 cp --endpoint-url "$endpoint" \
    --content-type application/pgp-signature \
    "$signed_checksums" "s3://${bucket}/actae/SHA256SUMS.asc"
fi

echo
echo "Release ${VERSION} published. Download from the customer portal"
echo "(/api/portal/download), or verify:"
echo "  aws s3 ls --endpoint-url $endpoint s3://$bucket/actae/$VERSION/"
