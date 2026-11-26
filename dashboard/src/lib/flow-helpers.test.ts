// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest'
import {
  resolveAncestry,
  paginateReplay,
  loadInheritedTimeline,
  mergeEvents,
  type ReplayFn,
  type MetadataFn,
} from './flow-helpers'
import type { ChannelMetadata } from './api'

function meta(
  channelId: string,
  parent: string | null = null,
  forkCursor: number = 0
): ChannelMetadata {
  return {
    channel_id: channelId,
    parent_channel_id: parent,
    origin_run_id: parent ?? channelId,
    forked_at_cursor: forkCursor,
    forked_at: new Date().toISOString(),
    display_name: channelId,
    reason: null,
    experiment_metadata: null,
  }
}

function mockReplay(events: any[]): ReplayFn {
  return vi.fn(async (_channelId, cursor = 0, limit = 1000) => {
    const from = cursor ?? 0
    const filtered = events.filter((e) => (e.cursor ?? 0) > from)
    const paginated = filtered.slice(0, limit)
    return { data: { events: paginated }, status: 200 }
  })
}

function mockMetadata(mapping: Record<string, ChannelMetadata>): MetadataFn {
  return vi.fn(async (id) => {
    const m = mapping[id]
    return m ? { data: m, status: 200 } : { data: null, status: 404 }
  })
}

describe('resolveAncestry', () => {
  it('walks the parent chain up to the root', async () => {
    const getMetadata = mockMetadata({
      child: meta('child', 'parent', 4),
      parent: meta('parent', 'grandparent', 7),
      grandparent: meta('grandparent', null, 0),
    })

    const chain = await resolveAncestry('child', getMetadata)

    expect(chain).toEqual([
      { channelId: 'parent', forkCursor: 4 },
      { channelId: 'grandparent', forkCursor: 7 },
    ])
  })

  it('returns an empty chain for a root channel', async () => {
    const getMetadata = mockMetadata({
      root: meta('root', null, 0),
    })

    const chain = await resolveAncestry('root', getMetadata)
    expect(chain).toEqual([])
  })

  it('breaks cycles', async () => {
    const getMetadata = mockMetadata({
      a: meta('a', 'b', 1),
      b: meta('b', 'c', 1),
      c: meta('c', 'a', 1),
    })

    const chain = await resolveAncestry('a', getMetadata)
    expect(chain.length).toBeLessThan(4)
    expect(chain.map((c) => c.channelId)).toEqual(['b', 'c'])
  })

  it('retries once on 404 before giving up', async () => {
    const getMetadata = vi.fn()
    getMetadata
      .mockResolvedValueOnce({ data: null, status: 404 })
      .mockResolvedValueOnce({ data: meta('child', 'parent', 4), status: 200 })
    getMetadata
      .mockResolvedValueOnce({ data: meta('parent', null, 0), status: 200 })

    const chain = await resolveAncestry('child', getMetadata)
    expect(chain).toEqual([{ channelId: 'parent', forkCursor: 4 }])
    expect(getMetadata).toHaveBeenCalledTimes(3)
  })
})

describe('paginateReplay', () => {
  it('returns all events across multiple pages', async () => {
    const events = Array.from({ length: 5 }, (_, i) => ({
      id: `evt-${i + 1}`,
      cursor: i + 1,
      event_type: 'step',
    }))
    const replay = mockReplay(events)

    const result = await paginateReplay('ch', replay, { limit: 2 })

    expect(result).toHaveLength(5)
    expect(replay).toHaveBeenCalledTimes(3)
    expect(replay).toHaveBeenNthCalledWith(1, 'ch', 0, 2)
    expect(replay).toHaveBeenNthCalledWith(2, 'ch', 2, 2)
    expect(replay).toHaveBeenNthCalledWith(3, 'ch', 4, 2)
  })

  it('stops at upToCursor', async () => {
    const events = [
      { id: 'evt-1', cursor: 1, event_type: 'step' },
      { id: 'evt-2', cursor: 2, event_type: 'step' },
      { id: 'evt-3', cursor: 3, event_type: 'step' },
      { id: 'evt-4', cursor: 4, event_type: 'step' },
    ]
    const replay = mockReplay(events)

    const result = await paginateReplay('ch', replay, { upToCursor: 2 })

    expect(result).toHaveLength(2)
    expect(result.map((e) => e.cursor)).toEqual([1, 2])
  })

  it('deduplicates by event id', async () => {
    const events = [
      { id: 'evt-1', cursor: 1, event_type: 'step' },
      { id: 'evt-1', cursor: 2, event_type: 'step' },
    ]
    const replay = mockReplay(events)

    const result = await paginateReplay('ch', replay, {})
    expect(result).toHaveLength(1)
  })

  it('returns empty for an empty channel', async () => {
    const replay = mockReplay([])
    const result = await paginateReplay('ch', replay, {})
    expect(result).toEqual([])
  })
})

describe('loadInheritedTimeline', () => {
  it('builds the inherited timeline in chronological order (root → parent)', async () => {
    const grandparent = [
      { id: 'g1', cursor: 1, event_type: 'step' },
      { id: 'g2', cursor: 2, event_type: 'step' },
    ]
    const parent = [
      { id: 'p1', cursor: 1, event_type: 'step' },
      { id: 'p2', cursor: 2, event_type: 'step' },
      { id: 'p3', cursor: 3, event_type: 'step' },
    ]

    const allEvents: Record<string, any[]> = {
      grandparent,
      parent,
    }

    const replay = vi.fn(async (channelId, cursor = 0, limit = 1000) => {
      const from = cursor ?? 0
      const filtered = (allEvents[channelId] ?? []).filter((e) => (e.cursor ?? 0) > from)
      return { data: { events: filtered.slice(0, limit) }, status: 200 }
    })
    const getMetadata: MetadataFn = vi.fn()

    // The caller passes the FULLY resolved chain (immediate → root). Each
    // channel contributes its own events up to its fork boundary:
    // grandparent [1..3] → g1,g2 ; parent [1..2] → p1,p2 (p3 happened AFTER
    // the child split, so it is NOT inherited).
    const inherited = await loadInheritedTimeline(
      [
        { channelId: 'parent', forkCursor: 2 },
        { channelId: 'grandparent', forkCursor: 3 },
      ],
      replay,
      getMetadata
    )

    expect(inherited.map((e) => e.id)).toEqual(['g1', 'g2', 'p1', 'p2'])
    expect(inherited.every((e) => e.isInherited)).toBe(true)
    // Each channel is replayed exactly once, and ancestry is never
    // re-resolved (the caller owns the chain).
    expect(replay).toHaveBeenCalledTimes(2)
    expect(replay).toHaveBeenNthCalledWith(1, 'grandparent', 0, 1000)
    expect(replay).toHaveBeenNthCalledWith(2, 'parent', 0, 1000)
    expect(getMetadata).not.toHaveBeenCalled()
  })

  it('includes the immediate parent segment up to the active fork cursor', async () => {
    const parent = [
      { id: 'p1', cursor: 1, event_type: 'step' },
      { id: 'p2', cursor: 2, event_type: 'step' },
      { id: 'p3', cursor: 3, event_type: 'step' },
    ]
    const replay: ReplayFn = vi.fn(async (channelId, cursor = 0, limit = 1000) => {
      const from = cursor ?? 0
      const filtered = (parent as any[]).filter((e) => (e.cursor ?? 0) > from)
      return { data: { events: filtered.slice(0, limit) }, status: 200 }
    })

    const inherited = await loadInheritedTimeline(
      [{ channelId: 'parent', forkCursor: 2 }],
      replay,
      vi.fn()
    )

    expect(inherited.map((e) => e.id)).toEqual(['p1', 'p2'])
  })
})

describe('mergeEvents', () => {
  it('preserves inherited order and appends own events', () => {
    const inherited = [
      { cursor: 1, type: 'step', channel: 'parent', id: 'p1', isInherited: true },
      { cursor: 2, type: 'step', channel: 'parent', id: 'p2', isInherited: true },
    ]
    const own = [
      { cursor: 1, type: 'step', channel: 'child', id: 'c1' },
      { cursor: 2, type: 'step', channel: 'child', id: 'c2' },
    ]

    const merged = mergeEvents(inherited as any, own as any)

    expect(merged.map((e) => e.id)).toEqual(['p1', 'p2', 'c1', 'c2'])
  })

  it('deduplicates shared events by id', () => {
    const shared = { cursor: 1, type: 'step', channel: 'parent', id: 'shared', isInherited: true }
    const inherited = [shared]
    const own = [{ cursor: 1, type: 'step', channel: 'child', id: 'shared' }]

    const merged = mergeEvents(inherited as any, own as any)
    expect(merged).toHaveLength(1)
  })
})
