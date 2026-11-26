// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import type { StateDiff, StateDiffEntry } from '@/lib/api'

export interface DiffTreeNode {
  /** Leaf path segment or branch key. */
  key: string
  /** Present only on leaf entries. */
  entry?: StateDiffEntry
  /** Child keys for nested paths. */
  children: DiffTreeNode[]
}

/**
 * Build a nested tree from the flat path-keyed diff entries, so the UI can
 * render "config → retries: 3 → 5" as a hierarchy instead of a flat path.
 *
 * Entries with deeper paths are attached under their ancestor segments.
 * Order is preserved per level (stable).
 */
export function buildDiffTree(entries: StateDiffEntry[]): DiffTreeNode[] {
  const roots: DiffTreeNode[] = []

  for (const entry of entries) {
    const path = entry.path
    if (path.length === 0) {
      // Top-level scalar mismatch — render as a bare leaf.
      roots.push({ key: '(root)', entry, children: [] })
      continue
    }
    // Walk (or create) nodes for each path segment. The parent chain is
    // threaded manually to avoid recursive type-inference issues.
    let level: DiffTreeNode[] = roots
    let node: DiffTreeNode | undefined
    for (let i = 0; i < path.length; i++) {
      const seg = path[i]
      const isLeaf = i === path.length - 1
      node = level.find(n => n.key === seg)
      if (!node) {
        node = { key: seg, children: [] }
        level.push(node)
      }
      if (isLeaf) node.entry = entry
      level = node.children
    }
  }
  return roots
}

/** Summary counts for the diff header ("3 added · 1 removed · 2 changed"). */
export function summarizeDiff(entries: StateDiffEntry[]): {
  added: number
  removed: number
  changed: number
} {
  let added = 0
  let removed = 0
  let changed = 0
  for (const e of entries) {
    if (e.kind === 'added') added++
    else if (e.kind === 'removed') removed++
    else changed++
  }
  return { added, removed, changed }
}

/**
 * State-diff path roots that belong to the *framework adapter envelope*, not
 * the agent's business state.
 *
 * A LangGraph run stores its graph state inside an opaque serde envelope, so
 * the server's structural diff surfaces paths like `_actae_adapter.channel_id`,
 * `checkpoint_id`, `metadata.b` — identity/plumbing noise, not "what actually
 * changed". Hiding them keeps the State tab honest and readable; the business
 * change shows up in the Output / Config / Metrics tabs.
 */
const OPAQUE_STATE_ROOTS = new Set([
  '_actae_adapter',
  'checkpoint',
  'checkpoint_id',
  'parent_checkpoint_id',
  'metadata',
  'pending_writes',
  'event_cursor',
  'event_id',
  'thread_id',
  'checkpoint_ns',
])

export function isOpaqueStateEntry(entry: StateDiffEntry): boolean {
  const root = entry.path[0]
  return root !== undefined && OPAQUE_STATE_ROOTS.has(root)
}

/**
 * The business-level rows of a state diff, with framework-adapter envelope
 * noise removed. An empty result is meaningful: the two runs differ only in
 * opaque checkpoint bytes (callers should explain that rather than showing
 * cryptic paths).
 */
export function meaningfulStateEntries(
  entries: StateDiffEntry[] | undefined | null,
): StateDiffEntry[] {
  if (!entries) return []
  return entries.filter((entry) => !isOpaqueStateEntry(entry))
}

/** Human label for a diff entry kind. */
export function kindLabel(kind: StateDiffEntry['kind']): string {
  switch (kind) {
    case 'added':
      return 'added'
    case 'removed':
      return 'removed'
    default:
      return 'changed'
  }
}
