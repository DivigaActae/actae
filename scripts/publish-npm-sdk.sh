#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Publish @actae/sdk to npm (open-core: the SDK is MIT; the server is not).
# Requires an authenticated npm session: `npm login`, or an automation token
# in NODE_AUTH_TOKEN / NPM_TOKEN.
#
# Usage: scripts/publish-npm-sdk.sh [--dry-run]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
SDK="$ROOT/sdks/typescript"
DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

command -v npm >/dev/null 2>&1 || { echo "error: npm is required" >&2; exit 1; }
[[ -f "$SDK/package.json" ]] || { echo "error: $SDK/package.json not found" >&2; exit 1; }

# A configured token can still be expired; verify before doing a full build.
if ! npm whoami >/dev/null 2>&1; then
  echo "error: not authenticated to npm (registry: $(npm config get registry))." >&2
  echo "  fix:  npm login" >&2
  echo "  or:   export NODE_AUTH_TOKEN=<npm automation token>" >&2
  exit 1
fi
PUBLISHER="$(npm whoami)"

echo "==> Building + testing @actae/sdk ($PUBLISHER)"
cd "$SDK"
if [[ -f package-lock.json ]]; then npm ci; else npm install; fi
npm test
npm run typecheck
npm run build

VERSION="$(node -p "require('./package.json').version")"
if [[ "$DRY_RUN" == "1" ]]; then
  echo "==> Dry run: would publish @actae/sdk@$VERSION to $(npm config get registry)"
  npm publish --access public --dry-run
else
  echo "==> Publishing @actae/sdk@$VERSION"
  npm publish --access public
  echo "Published @actae/sdk@$VERSION"
fi
