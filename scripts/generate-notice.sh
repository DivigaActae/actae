#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Regenerates THIRD_PARTY.md. Review the output by hand; "UNKNOWN" is blocking.
set -euo pipefail

out="THIRD_PARTY.md"

{
  echo "# Third-party components"
  echo
  echo "Generated $(date -u +%Y-%m-%dT%H:%M:%SZ). Do not edit by hand."
  echo
  echo "## Rust crates"
  echo '```'
  (cd actae && cargo metadata --format-version 1 --no-deps >/dev/null 2>&1 || true)
  (cd actae && cargo metadata --format-version 1 2>/dev/null \
    | jq -r '.packages[] | select(.source != null) | "\(.name) \(.version) \(.license // "UNKNOWN")"')
  echo '```'
  echo
  echo "## Go modules"
  echo '```'
  (cd sdks/go && go list -m -json all 2>/dev/null | jq -r 'select(.Main != true) | "\(.Path) \(.Version)"' || true)
  echo '```'
  echo
  echo "## Embedded binaries"
  echo
  echo "| Component | Version | License | Source |"
  echo "|---|---|---|---|"
  echo "| PostgreSQL | 16.x (bundled) | PostgreSQL License | https://www.postgresql.org/about/licence/ |"
} > "$out"

echo "wrote $out"
