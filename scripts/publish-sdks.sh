#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Mirror the three open-source SDKs (and the public benchmark repo) to their
# public repos and tag releases. Registry uploads are opt-in (`--publish`)
# because they need credentials.
#
# Every mirror is attempted even if an earlier one fails, and the exit status is
# non-zero only if at least one failed — so a transient GitHub hiccup on one SDK
# is visible but never hides the others' outcome. The per-SDK push is itself
# idempotent + retrying (scripts/lib-sdk-mirror.sh).
#
# Usage: scripts/publish-sdks.sh [--publish] [--dry-run]
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARGS=()
for arg in "$@"; do
  case "$arg" in
    --publish|--dry-run|--no-tag) ARGS+=("$arg") ;;
    *) echo "usage: $0 [--publish] [--dry-run] [--no-tag]" >&2; exit 2 ;;
  esac
done

failed=()
for entry in \
  "go:publish-go-sdk.sh" \
  "python:publish-python-sdk.sh" \
  "ts:publish-ts-sdk.sh" \
  "benchmarks:publish-benchmarks-repo.sh"; do
  label="${entry%%:*}"
  script="$SCRIPT_DIR/${entry#*:}"
  echo "==> Mirror: $label"
  if ! "$script" "${ARGS[@]+"${ARGS[@]}"}"; then
    echo "!! Mirror FAILED: $label" >&2
    failed+=("$label")
  fi
done

if (( ${#failed[@]} > 0 )); then
  echo >&2
  echo "SDK mirrors failed: ${failed[*]}" >&2
  echo "Re-run when GitHub recovers:  $SCRIPT_DIR/publish-sdks.sh ${ARGS[*]-}" >&2
  exit 1
fi

echo "All SDK mirrors published."
