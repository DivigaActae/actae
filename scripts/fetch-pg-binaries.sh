#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

#
# Download the zonky PostgreSQL package for a platform and extract the FULL
# tree (bin/lib/share) into <dest> — the ACTAE_PG_BIN_DIR layout used by
# managed storage and the conformance/lifecycle test suites. The zonky binaries
# resolve their shared libraries via rpath (`$ORIGIN/../lib`), so the whole
# tree is required; ACTAE_PG_BIN_DIR must point at a tree root containing
# bin/initdb (a plain bin dir is also accepted by Actae).
#
# The SHA-256 of the downloaded artifact is verified against the
# source-controlled actae/pg-checksums.json manifest before extraction, so a
# compromised repository or proxy can never become executable code on a
# customer machine.
#
# Platforms: linux-amd64 | linux-arm64v8 | linux-amd64-alpine |
#            linux-arm64v8-alpine | darwin-amd64 | darwin-arm64v8 |
#            windows-amd64
#
# Usage: scripts/fetch-pg-binaries.sh <platform> <dest-tree-root>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
PLATFORM="${1:?usage: fetch-pg-binaries.sh <platform> <dest-tree-root>}"
DEST="${2:?usage: fetch-pg-binaries.sh <platform> <dest-tree-root>}"
PG_VERSION="${ACTAE_PG_VERSION:-18.2.0}"

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

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

URL="https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-${PLATFORM}/${PG_VERSION}/embedded-postgres-binaries-${PLATFORM}-${PG_VERSION}.jar"
echo "fetching ${URL}"
curl -fsSL -o "$TMP/pkg.jar" "$URL"

ACTUAL="$(python3 -c "
import hashlib, sys
print(hashlib.sha256(open(sys.argv[1], 'rb').read()).hexdigest())
" "$TMP/pkg.jar")"
if [[ "$ACTUAL" != "$EXPECTED" ]]; then
  echo "error: SHA-256 mismatch for ${PLATFORM}-${PG_VERSION}: expected ${EXPECTED}, got ${ACTUAL}" >&2
  exit 1
fi

# The jar is a ZIP containing an XZ-compressed tar of bin/lib/share.
python3 - "$TMP/pkg.jar" "$TMP/extracted" <<'PY'
import io
import lzma
import sys
import tarfile
import zipfile

jar, dest = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(jar) as z:
    name = next(n for n in z.namelist() if n.endswith((".txz", ".xz")))
    data = lzma.decompress(z.read(name))
with tarfile.open(fileobj=io.BytesIO(data)) as tf:
    tf.extractall(dest)
PY

rm -rf "$DEST"
mkdir -p "$DEST"
cp -R "$TMP/extracted/." "$DEST/"
chmod +x "$DEST/bin/initdb" "$DEST/bin/postgres" "$DEST/bin/pg_ctl"
ls -lh "$DEST"
echo "OK: PostgreSQL binaries extracted to ${DEST} (sha256 ${ACTUAL}); set ACTAE_PG_BIN_DIR=${DEST}"
