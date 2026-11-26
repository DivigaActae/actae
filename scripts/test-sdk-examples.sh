#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Run the monorepo-only tests that couple the SDKs to repo fixtures/examples.
# They deliberately live outside `sdks/python` and `sdks/typescript` so those
# directories stay verbatim-copyable into their public repos.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> Python example-coupled tests"
python3 -m pytest "$ROOT/sdks/example-tests/python" -q

echo "==> TypeScript example-coupled tests"
cd "$ROOT/sdks/example-tests/typescript"
if [[ ! -d node_modules ]]; then
  npm install
fi
npm test
