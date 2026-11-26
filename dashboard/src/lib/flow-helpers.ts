// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import type { ChannelMetadata } from './api'
import type { DashboardEvent } from './flow-store'

export interface AncestryLink {
  channelId: string
  forkCursor: number
}

export interface PaginationOptions {
  /** Fetch events with cursor > fromCursor. Defaults to 0. */
  fromCursor?: number
  /** Stop fetching once an event exceeds this cursor (inclusive events are kept). */
  upToCursor?: number | null
  /** Page size. Defaults to 1000 (backend max). */
  limit?: number
}

export type ReplayFn = (
  channelId: string,
  cursor?: number,
  limit?: number
) => Promise<{ data?: any; error?: string; status: number }>

export type MetadataFn = (
  channelId: string
) => Promise<{ data?: ChannelMetadata | null; status?: number }>

/**
 * Walk the channel_metadata parent chain from a channel up to the root run.
 *
 * Returns an ordered list where the first element is the immediate parent of
 * `channelId` and the last element is the root/origin channel. This ordering
 * makes it easy to prepend inherited events from oldest to newest.
 *
 * Cycles are detected and broken via a visited set.
 */
export async function resolveAncestry(
  channelId: string,
  getMetadata: MetadataFn
): Promise<AncestryLink[]> {
  const chain: AncestryLink[] = []
  let current = channelId
  const visited = new Set<string>()

  while (!visited.has(current)) {
    visited.add(current)
    let res = await getMetadata(current)
    // Some backends may need a moment before metadata is queryable after fork.
    if (!res.data && res.status === 404) {
      await new Promise((r) => setTimeout(r, 500))
      res = await getMetadata(current)
    }
    const meta = res.data
    if (!meta?.parent_channel_id) break
    // Break cycles before adding a parent that is already part of the chain.
    if (visited.has(meta.parent_channel_id)) break
    chain.push({ channelId: meta.parent_channel_id, forkCursor: meta.forked_at_cursor })
    current = meta.parent_channel_id
  }

  return chain
}

/**
 * Replay all events for a channel, paginating past the backend's default
 * 100-event limit until the requested range is exhausted.
 *
 * The returned events are de-duplicated by id (or by channel/cursor/type when
 * id is absent) and ordered by cursor ascending.
 */
export async function paginateReplay(
  channelId: string,
  replay: ReplayFn,
  options: PaginationOptions = {}
): Promise<DashboardEvent[]> {
  const { fromCursor = 0, upToCursor = null, limit = 1000 } = options
  const events: DashboardEvent[] = []
  const seen = new Set<string>()
  let cursor = fromCursor

  while (true) {
    const { data, error, status } = await replay(channelId, cursor, limit)
    if (error) {
      throw new Error(error)
    }
    if (status >= 400) {
      throw new Error(`Replay failed with status ${status}`)
    }

    const rawList = data?.events ?? data
    const batch = Array.isArray(rawList) ? rawList : []
    if (batch.length === 0) break

    let exceeded = false
    for (const e of batch) {
      const eventCursor = e.cursor ?? 0
      if (upToCursor !== null && eventCursor > upToCursor) {
        exceeded = true
        break
      }
      const dedupeKey = e.id
        ? `id:${e.id}`
        : `${channelId}:${eventCursor}:${e.event_type || e.type || 'unknown'}`
      if (seen.has(dedupeKey)) continue
      seen.add(dedupeKey)
      events.push({
        id: e.id,
        cursor: eventCursor,
        type: e.event_type || e.type || 'unknown',
        channel: channelId,
        timestamp: new Date(e.timestamp || Date.now()),
        payload: e.payload || {},
        isState: e.isState || false,
        state: e.state,
      })
    }

    if (exceeded) break

    const last = batch[batch.length - 1]
    if (!last || last.cursor === cursor || batch.length < limit) break
    cursor = last.cursor
  }

  events.sort((a, b) => a.cursor - b.cursor)
  return events
}

/**
 * Build the full inherited timeline for an ancestry chain in CHRONOLOGICAL
 * (causal) order: root inherited segment → intermediate fork segments →
 * immediate parent segment.
 *
 * The chain is already fully resolved by the caller (immediate → root) and
 * is NOT re-resolved here (each channel is replayed exactly once, for the
 * segment it contributes). This fixes two historical bugs:
 *  - the old recursive version appended parent events BEFORE grandparent
 *    events (reverse chronology for a nested fork), and
 *  - it re-resolved ancestry + replayed every ancestor for every element
 *    (quadratic metadata + replay work).
 *
 * Segment rule: each channel `X` contributes its own events in cursor range
 * `[1, forkCursor]` where `forkCursor` is the cursor in `X`'s stream at
 * which the next-deeper channel split from it (AncestryLink.forkCursor). The
 * deepest/root channel with no recorded boundary (forkCursor 0) contributes
 * its full history — the best available when no fork provenance exists.
 */
export async function loadInheritedTimeline(
  ancestry: AncestryLink[],
  replay: ReplayFn,
  _getMetadata: MetadataFn
): Promise<DashboardEvent[]> {
  const events: DashboardEvent[] = []
  const seen = new Set<string>()

  const push = (e: DashboardEvent) => {
    const key = e.id ? `id:${e.id}` : `${e.channel}:${e.cursor}:${e.type}`
    if (seen.has(key)) return
    seen.add(key)
    events.push({ ...e, isInherited: true })
  }

  // Reverse to root → leaf so segments are appended oldest-first.
  const chain = [...ancestry].reverse()
  for (const hop of chain) {
    const own = await paginateReplay(hop.channelId, replay, {
      fromCursor: 0,
      upToCursor: hop.forkCursor || null,
      limit: 1000,
    })
    for (const e of own) push(e)
  }

  return events
}

/**
 * Merge inherited events with a channel's own events, de-duplicating by id.
 *
 * Ordering: the inherited timeline is preserved as-is (it is already in
 * chronological order from {@link loadInheritedTimeline}). Own events are sorted
 * by cursor ascending and appended. This produces the expected fork view:
 *
 *   parent events 1..N  ->  fork events 1..M
 */
export function mergeEvents(
  inherited: DashboardEvent[],
  own: DashboardEvent[]
): DashboardEvent[] {
  const seen = new Set<string>()
  const result: DashboardEvent[] = []

  const key = (e: DashboardEvent) =>
    e.id ? `id:${e.id}` : `${e.channel}:${e.cursor}:${e.type}`

  for (const e of inherited) {
    const k = key(e)
    if (seen.has(k)) continue
    seen.add(k)
    result.push(e)
  }

  const sortedOwn = [...own].sort((a, b) => a.cursor - b.cursor)
  for (const e of sortedOwn) {
    const k = key(e)
    if (seen.has(k)) continue
    seen.add(k)
    result.push(e)
  }

  return result
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function deepMergeContext(target: Record<string, unknown>, source: Record<string, unknown>): Record<string, unknown> {
  for (const key of Object.keys(source)) {
    const srcVal = source[key]
    const tgtVal = target[key]

    if (isPlainObject(srcVal) && isPlainObject(tgtVal)) {
      target[key] = deepMergeContext({ ...tgtVal }, srcVal)
    } else if (Array.isArray(srcVal) && Array.isArray(tgtVal)) {
      target[key] = [...tgtVal, ...srcVal]
    } else if (typeof srcVal === 'string' && typeof tgtVal === 'string') {
      target[key] = tgtVal + '\n' + srcVal
    } else {
      target[key] = srcVal
    }
  }
  return target
}

export function reconstructContext(
  events: DashboardEvent[],
  upToCursor: number,
): Record<string, unknown> {
  const sorted = events
    .filter(e => !e.isState)
    .sort((a, b) => a.cursor - b.cursor)

  const cumulative: Record<string, unknown> = {}
  for (const evt of sorted) {
    if (evt.cursor > upToCursor) break
    const ctx = evt.payload?.context as Record<string, unknown> | undefined
    if (ctx && isPlainObject(ctx)) {
      deepMergeContext(cumulative, ctx)
    }
  }
  return cumulative
}

/**
 * Count events grouped by filter key, sorted by count descending.
 *
 * Uses the same keying as `nodeTypeKey` in flow-layout (state snapshots are
 * grouped under the pseudo-type `"state"`) so the breakdown always matches
 * what a node-type filter would show. State events carry their own payload
 * type (`"state.transition"` etc.) — but for the flow view we group them as
 * `"state"` only when `isState` is set, mirroring buildFlowNodes.
 */
export function computeTypeCounts(
  events: DashboardEvent[]
): { type: string; count: number }[] {
  const counts = new Map<string, number>()
  for (const evt of events) {
    const key = evt.isState ? 'state' : evt.type || 'unknown'
    counts.set(key, (counts.get(key) || 0) + 1)
  }
  return Array.from(counts.entries())
    .map(([type, count]) => ({ type, count }))
    .sort((a, b) => b.count - a.count)
}
