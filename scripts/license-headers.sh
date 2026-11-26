#!/usr/bin/env bash
# Adds an SPDX header to source files that lack one. Idempotent.
set -euo pipefail

HOLDER="Diviga"
YEAR="2026"

header_for() {
  case "$1" in
    *.rs|*.go|*.ts|*.tsx|*.js)
      printf '// Copyright %s %s\n// SPDX-License-Identifier: Apache-2.0\n\n' "$YEAR" "$HOLDER" ;;
    *.py)
      printf '# Copyright %s %s\n# SPDX-License-Identifier: Apache-2.0\n\n' "$YEAR" "$HOLDER" ;;
    *.sh)
      printf '# Copyright %s %s\n# SPDX-License-Identifier: Apache-2.0\n\n' "$YEAR" "$HOLDER" ;;
  esac
}

find . -type f \( -name '*.rs' -o -name '*.go' -o -name '*.ts' -o -name '*.tsx' \
  -o -name '*.py' -o -name '*.sh' \) \
  -not -path './.git/*' -not -path '*/node_modules/*' -not -path '*/target/*' \
  -not -path '*/dist/*' -not -path '*/.next/*' -not -path '*/out/*' \
  -not -path '*/coverage/*' -not -path '*/.venv*' \
  | while read -r f; do
      grep -q "SPDX-License-Identifier" "$f" && continue
      tmp="$(mktemp)"
      if head -1 "$f" | grep -q '^#!'; then
        { head -1 "$f"; header_for "$f"; tail -n +2 "$f"; } > "$tmp"
      else
        { header_for "$f"; cat "$f"; } > "$tmp"
      fi
      mv "$tmp" "$f"
    done
