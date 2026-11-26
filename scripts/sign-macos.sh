#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

#
# Code-sign a macOS `actae` release binary. The ONLY macOS signing path:
# used by scripts/release-r2.sh (which release.sh and
# saas/ops/release-and-deploy.sh call) and by the tagged CI release workflow,
# so every published macOS archive comes out of the same signer.
#
# What it does: a free **ad-hoc** signature. No Apple Developer Program
# membership, no certificates, no secrets — see docs/RELEASE_SIGNING.md §2 for
# why that is sufficient and why the paid Developer ID + notarization tier is
# deliberately not implemented.
#
# Why sign at all: on Apple Silicon every arm64 executable must carry a
# *valid* code signature or macOS kills it outright ("Bad CPU type executable"
# / killed by SIGKILL). The Rust linker emits one, but `strip` (which the
# release paths run) invalidates it, so signing is always done AFTER the last
# binary mutation. It also gives `codesign --verify` something to check.
#
# What it does NOT do: silence Gatekeeper. That prompt is driven by the
# com.apple.quarantine xattr that browsers/Mail set on downloaded files, not
# by the signature tier. curl/wget and `actae upgrade` never set it. See
# docs/RELEASE_SIGNING.md §2.1 for the user-facing fixes.
#
# Usage: scripts/sign-macos.sh <binary>
#
# Exit codes: 0 signed (or skipped where codesign is unavailable), 1 failed.
set -euo pipefail

BIN="${1:?usage: sign-macos.sh <binary>}"

if [[ ! -f "$BIN" ]]; then
  echo "error: sign-macos.sh: binary not found: $BIN" >&2
  exit 1
fi

# Not macOS (or no Xcode CLT): cross-built archives cannot be signed here.
if ! command -v codesign >/dev/null 2>&1; then
  echo "==> macos signing: skipped (no codesign toolchain on this host)" >&2
  exit 0
fi

echo "==> macos signing: ad-hoc (free, no Apple account)"
codesign --force --sign - "$BIN"

codesign --verify --strict --verbose=2 "$BIN"
codesign -dv "$BIN" 2>&1 | sed 's/^/    /'
