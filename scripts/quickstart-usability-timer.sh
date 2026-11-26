#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

output_path="${1:-quickstart-usability.csv}"
participant="${PARTICIPANT_ID:-anonymous}"
started_at="$(date +%s)"

mark() {
  local label="$1"
  local now
  now="$(date +%s)"
  printf '%s,%s,%s,%s\n' "$participant" "$label" "$((now - started_at))" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$output_path"
  printf 'Recorded %s at %ss\n' "$label" "$((now - started_at))"
}

if [[ ! -f "$output_path" ]]; then
  printf 'participant,milestone,elapsed_seconds,recorded_at\n' > "$output_path"
fi

printf 'Actae quickstart timing — participant: %s\n' "$participant"
printf 'Press Enter when each milestone is genuinely complete.\n'
read -r -p 'Actae started and ready: '
mark "server_ready"
read -r -p 'First event visible in the dashboard: '
mark "first_visible_event"
read -r -p 'Killed process and resumed without rerunning inherited work: '
mark "recovery_complete"
read -r -p 'Forked saved state and understood the ownership boundary: '
mark "fork_complete"
printf 'Saved results to %s\n' "$output_path"
