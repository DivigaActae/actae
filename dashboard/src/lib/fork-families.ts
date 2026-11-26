// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import type { ExperimentGroup } from '@/lib/api'

type ForkEvent = { channel_id?: string; timestamp?: string; payload?: Record<string, unknown> | null }

/** Reconstruct experiment-like families from durable fork.started events.
 *
 * `nameFor` (optional) resolves a channel id to its human display name (channel
 * metadata). Without it, ids are shown — so pass a resolver from the page that
 * has the channel list, and forks read as "Fork A · +fact-check", not a hash.
 */
export function deriveForkGroups(
  events: ForkEvent[],
  nameFor?: (channelId: string) => string | null | undefined,
): ExperimentGroup[] {
  const parent = new Map<string, string>()
  const byChild = new Map<string, ForkEvent>()
  for (const event of events) {
    const child = event.channel_id
    const source = event.payload?.source_channel_id
    if (child && typeof source === 'string' && child !== source) {
      parent.set(child, source)
      byChild.set(child, event)
    }
  }
  const rootOf = (channel: string) => {
    let cursor = channel
    const seen = new Set<string>()
    while (parent.has(cursor) && !seen.has(cursor)) {
      seen.add(cursor)
      cursor = parent.get(cursor)!
    }
    return cursor
  }
  const families = new Map<string, string[]>()
  for (const child of Array.from(parent.keys())) {
    const root = rootOf(child)
    families.set(root, [...(families.get(root) || []), child])
  }
  return Array.from(families.entries()).map(([root, children]) => {
    const members: ExperimentGroup['members'] = [{
      channel_id: root, role: 'baseline', declared_delta: null,
      display_name: nameFor?.(root) ?? null,
      created_at: byChild.get(children[0])?.timestamp || new Date(0).toISOString(),
    }, ...children.map(child => {
      const event = byChild.get(child)!
      const payload = event.payload || {}
      const delta: Record<string, unknown> = {}
      if (payload.reason) delta.reason = payload.reason
      if (payload.reproducibility) delta.reproducibility = payload.reproducibility
      if (payload.resolved_cursor != null) delta.fork_cursor = payload.resolved_cursor
      return {
        channel_id: child,
        role: 'variant' as const,
        declared_delta: delta,
        display_name: nameFor?.(child) ?? (typeof payload.display_name === 'string' ? payload.display_name : null),
        created_at: event.timestamp || new Date(0).toISOString(),
      }
    })]
    return {
      group_id: `lineage:${root}`,
      name: `Fork family · ${nameFor?.(root) || shortName(root)}`,
      description: `${children.length} fork${children.length === 1 ? '' : 's'} derived from recorded fork lineage.`,
      baseline_channel_id: root,
      status: 'running' as const,
      created_at: members[0].created_at,
      updated_at: members.reduce((latest, member) => member.created_at > latest ? member.created_at : latest, members[0].created_at),
      members,
      derived: true,
    }
  }).sort((a, b) => b.updated_at.localeCompare(a.updated_at))
}

function shortName(id: string) { return id.length > 28 ? `…${id.slice(-27)}` : id }
