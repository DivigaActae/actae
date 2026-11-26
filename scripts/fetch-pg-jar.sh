#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

#
# Fetch the zonky PostgreSQL jar for a platform (the same artifact the
# managed-storage runtime extracts). Used by the release workflow to bake the
# PG binaries into the `actae` binary at build time (ACTAE_PG_EMBED_JAR).
#
# The SHA-256 of every supported artifact is source-controlled in
# actae/pg-checksums.json; the download is verified against it before the
# jar is used. A mismatch aborts the build — a tampered/compromised
# package must never reach a customer machine.
#
# Platforms: linux-amd64 | linux-arm64v8 | linux-amd64-alpine |
#            linux-arm64v8-alpine | darwin-amd64 | darwin-arm64v8 |
#            windows-amd64
#
# Usage: scripts/fetch-pg-jar.sh <platform> [output-path]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
PG_VERSION="${ACTAE_PG_VERSION:-18.2.0}"
PLATFORM="${1:?usage: fetch-pg-jar.sh <platform> [output-path]}"
OUT="${2:-/tmp/embedded-postgres-binaries-${PLATFORM}-${PG_VERSION}.jar}"

MANIFEST="$REPO_ROOT/actae/pg-checksums.json"
EXPECTED="$(python3 -c "
import json, sys
m = json.load(open(sys.argv[1]))
if m['postgresql_version'] != sys.argv[2]:
    sys.exit('manifest targets PostgreSQL %s, but this build pins %s' % (m['postgresql_version'], sys.argv[2]))
sys.stdout.write(m['zonky_jars'].get(sys.argv[3], ''))
" "$MANIFEST" "$PG_VERSION" "$PLATFORM")"
if [[ -z "$EXPECTED" ]]; then
  echo "error: no pinned SHA-256 for zonky platform '$PLATFORM' in $MANIFEST" >&2
  exit 1
fi

URL="https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-${PLATFORM}/${PG_VERSION}/embedded-postgres-binaries-${PLATFORM}-${PG_VERSION}.jar"

echo "fetching ${URL}"
curl -fsSL -o "${OUT}" "${URL}"

ACTUAL="$(python3 -c "
import hashlib, sys
print(hashlib.sha256(open(sys.argv[1], 'rb').read()).hexdigest())
" "$OUT")"
if [[ "$ACTUAL" != "$EXPECTED" ]]; then
  echo "error: SHA-256 mismatch for ${PLATFORM}-${PG_VERSION}: expected ${EXPECTED}, got ${ACTUAL}" >&2
  rm -f "$OUT"
  exit 1
fi

ls -lh "${OUT}"
echo "OK: ${OUT} (sha256 ${ACTUAL})"
