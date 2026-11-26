#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Shared git push helper for the public SDK mirror repos.
#
# GitHub intermittently rejects a pack with e.g.
#   remote: error: unable to write file .../pack-*.pack: No such file or directory
#   remote: fatal: unable to rename temporary '*.pack' file to ...
#   ! [remote rejected] vX.Y.Z -> vX.Y.Z (missing necessary objects)
# It is a server-side hiccup, and force-updating an EXISTING tag (which happens
# whenever a release is re-run) is what tends to trigger it. This helper makes
# the mirror push idempotent and self-healing:
#
#   * a tag that already points at the same commit is left alone (no push);
#   * a tag that must move is deleted and re-pushed rather than force-updated
#     in place (in-place force-update of an existing tag is the failing path);
#   * every network push is retried with backoff, and a rejected tag push gets
#     a remote-tag delete before the next attempt.
#
# Usage (from a staged SDK checkout with `origin` configured to the mirror):
#   source lib-sdk-mirror.sh
#   mirror_push <repo_slug> <tag>
set -euo pipefail

mirror_retry() {           # mirror_retry <attempts> <cmd...>
  local attempts="$1"; shift
  local n=1 delay=3
  while true; do
    if "$@"; then return 0; fi
    if (( n >= attempts )); then
      echo "error: '$*' failed after $attempts attempts" >&2
      return 1
    fi
    echo "    transient failure; retry $((n + 1))/$attempts in ${delay}s: $*" >&2
    sleep "$delay"
    n=$((n + 1))
    delay=$((delay * 2))
  done
}

mirror_remote_tag_commit() {   # mirror_remote_tag_commit <tag> -> commit (or empty)
  local tag="$1" ref out
  for ref in "refs/tags/$tag^{}" "refs/tags/$tag"; do
    out="$(git ls-remote origin "$ref" 2>/dev/null | awk 'NR==1{print $1}')" || true
    if [[ -n "$out" ]]; then
      echo "$out"
      return 0
    fi
  done
  return 0
}

mirror_push() {            # mirror_push <repo_slug> [tag]
  local slug="$1" tag="${2:-}"
  local head remote_tag attempt

  head="$(git rev-parse HEAD)"

  echo "==> Pushing main to $slug"
  mirror_retry 4 git push --force --quiet origin main

  # An empty tag means "main only": used for docs/README refreshes that must
  # not move an immutable release tag (notably the Go module version).
  if [[ -z "$tag" ]]; then
    echo "==> No tag requested; updated main only"
    return 0
  fi

  remote_tag="$(mirror_remote_tag_commit "$tag")"
  if [[ "$remote_tag" == "$head" ]]; then
    echo "==> Tag $tag already points at $head; nothing to push"
    return 0
  fi

  if [[ -n "$remote_tag" ]]; then
    echo "==> Moving tag $tag ($remote_tag -> $head)"
    # Delete first: an in-place force-update of an existing tag is the case
    # GitHub rejects with "missing necessary objects".
    mirror_retry 3 git push --quiet origin ":refs/tags/$tag" || true
  else
    echo "==> Pushing tag $tag"
  fi

  attempt=1
  while true; do
    if git push --force --quiet origin "refs/tags/$tag"; then
      return 0
    fi
    if (( attempt >= 4 )); then
      echo "error: pushing tag $tag to $slug failed after $attempt attempts" >&2
      echo "       (a GitHub 'missing necessary objects' outage clears once the" >&2
      echo "        remote tag is deleted — re-run this script when it passes)" >&2
      return 1
    fi
    echo "    tag push rejected; deleting the remote tag and retrying ($attempt/4)" >&2
    git push --quiet origin ":refs/tags/$tag" >/dev/null 2>&1 || true
    sleep $((attempt * 5))
    attempt=$((attempt + 1))
  done
}
