// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest'
import {
  buildFlowNodes,
  makeNodeId,
  nodeMatchesSelection,
  gridColumnsForCount,
  START_X,
} from './flow-layout'
import type { DashboardEvent } from './flow-store'

function event(
  channel: string,
  cursor: number,
  id: string | undefined,
  overrides: Partial<DashboardEvent> = {}
): DashboardEvent {
  return {
    id,
    cursor,
    type: 'step',
    channel,
    timestamp: new Date().toISOString(),
    payload: {},
    ...overrides,
  }
}

describe('makeNodeId', () => {
  it('uses event id when available', () => {
    const id = makeNodeId(event('ch', 1, 'abc-123'), 0)
    expect(id).toBe('evt-ch-abc-123')
  })

  it('falls back to channel + cursor + index when id is missing', () => {
    const id = makeNodeId(event('ch', 1, undefined), 0)
    expect(id).toBe('event-ch-1-0')
  })

  it('keeps ids unique across channels for the same cursor', () => {
    const a = makeNodeId(event('ch-a', 5, undefined), 0)
    const b = makeNodeId(event('ch-b', 5, undefined), 0)
    expect(a).not.toBe(b)
  })

  it('produces unique ids for inherited and fork-owned events with the same cursor', () => {
    const inherited = event('parent', 3, undefined, { isInherited: true, forkedFrom: 'parent' })
    const own = event('child', 3, undefined, { isInherited: false })

    const inheritedId = makeNodeId(inherited, 2)
    const ownId = makeNodeId(own, 3)
    expect(inheritedId).not.toBe(ownId)
  })
})

describe('buildFlowNodes', () => {
  it('creates unique nodes for overlapping inherited and own cursors', () => {
    const events: DashboardEvent[] = [
      event('parent', 1, 'p1', { isInherited: true, forkedFrom: 'parent' }),
      event('parent', 2, 'p2', { isInherited: true, forkedFrom: 'parent' }),
      event('parent', 3, 'p3', { isInherited: true, forkedFrom: 'parent' }),
      event('child', 1, 'c1'),
      event('child', 2, 'c2'),
      event('child', 3, 'c3'),
    ]

    const { nodes, edges } = buildFlowNodes(events, 'horizontal')

    const ids = nodes.map((n) => n.id)
    expect(new Set(ids).size).toBe(ids.length)
    expect(ids.length).toBe(6)
    expect(edges.length).toBe(5)
  })

  it('connects edges with the generated node ids', () => {
    const events: DashboardEvent[] = [
      event('ch', 1, 'a'),
      event('ch', 2, 'b'),
    ]

    const { nodes, edges } = buildFlowNodes(events, 'horizontal')
    expect(edges[0].source).toBe(nodes[0].id)
    expect(edges[0].target).toBe(nodes[1].id)
  })

  it('handles state nodes separately from event nodes', () => {
    const events: DashboardEvent[] = [
      event('ch', 1, 'a'),
      { cursor: 1, type: 'state', channel: 'ch', timestamp: new Date().toISOString(), payload: {}, isState: true, state: {} },
    ]

    const { nodes } = buildFlowNodes(events, 'horizontal')
    const ids = nodes.map((n) => n.id)
    expect(new Set(ids).size).toBe(ids.length)
    expect(ids.some((id) => id.startsWith('state-'))).toBe(true)
    expect(ids.some((id) => id.startsWith('evt-'))).toBe(true)
  })

  it('renders everything when visibleTypes is null or omitted', () => {
    const events: DashboardEvent[] = [
      event('ch', 1, 'a', { type: 'agent.started' }),
      event('ch', 2, 'b', { type: 'tool.llm' }),
      { cursor: 3, type: 'state', channel: 'ch', timestamp: new Date().toISOString(), payload: {}, isState: true, state: {} },
    ]
    const all = buildFlowNodes(events, 'horizontal')
    const withNull = buildFlowNodes(events, 'horizontal', undefined, null)
    expect(all.nodes.length).toBe(3)
    expect(withNull.nodes.length).toBe(3)
  })

  it('filters events by type and re-chains the remaining nodes in order', () => {
    const events: DashboardEvent[] = [
      event('ch', 1, 'a', { type: 'agent.started' }),
      event('ch', 2, 'b', { type: 'tool.llm' }),
      event('ch', 3, 'c', { type: 'agent.completed' }),
    ]
    const { nodes, edges } = buildFlowNodes(
      events,
      'horizontal',
      undefined,
      new Set(['agent.started', 'agent.completed'])
    )
    expect(nodes.map((n) => n.id)).toEqual(['evt-ch-a', 'evt-ch-c'])
    expect(edges).toHaveLength(1)
    expect(edges[0].source).toBe('evt-ch-a')
    expect(edges[0].target).toBe('evt-ch-c')
  })

  it('filters state snapshots under the pseudo-type "state"', () => {
    const events: DashboardEvent[] = [
      event('ch', 1, 'a', { type: 'agent.started' }),
      { cursor: 2, type: 'state', channel: 'ch', timestamp: new Date().toISOString(), payload: {}, isState: true, state: {} },
      event('ch', 3, 'c', { type: 'tool.llm' }),
    ]
    const { nodes } = buildFlowNodes(events, 'horizontal', undefined, new Set(['state']))
    expect(nodes).toHaveLength(1)
    expect(nodes[0].id.startsWith('state-')).toBe(true)

    const none = buildFlowNodes(events, 'horizontal', undefined, new Set(['agent.started']))
    expect(none.nodes.some((n) => n.id.startsWith('state-'))).toBe(false)
  })

  it('returns no nodes when the filter matches nothing', () => {
    const events: DashboardEvent[] = [
      event('ch', 1, 'a', { type: 'agent.started' }),
      event('ch', 2, 'b', { type: 'tool.llm' }),
    ]
    const { nodes, edges } = buildFlowNodes(events, 'horizontal', undefined, new Set(['nope']))
    expect(nodes).toHaveLength(0)
    expect(edges).toHaveLength(0)
  })
})

describe('nodeMatchesSelection', () => {
  const events: DashboardEvent[] = [
    event('parent', 5, 'p5', { type: 'tool.llm', isInherited: true, forkedFrom: 'root' }),
    event('child', 5, 'c5', { type: 'tool.llm' }),
    event('child', 6, 'c6', { type: 'agent.completed' }),
  ]
  const { nodes } = buildFlowNodes(events, 'horizontal')
  const byData = (type: string, cursor: number, channel: string) =>
    nodes.find(n => {
      const d = n.data as any
      return d.type === type && d.cursor === cursor && d.channel === channel
    })!

  it('matches a node by type + cursor', () => {
    const node = byData('agent.completed', 6, 'child')
    expect(nodeMatchesSelection(node, { type: 'agent.completed', cursor: 6 })).toBe(true)
  })

  it('does not match when type or cursor differ', () => {
    const node = byData('agent.completed', 6, 'child')
    expect(nodeMatchesSelection(node, { type: 'tool.llm', cursor: 6 })).toBe(false)
    expect(nodeMatchesSelection(node, { type: 'agent.completed', cursor: 5 })).toBe(false)
  })

  it('disambiguates same-cursor same-type nodes across channels', () => {
    const inherited = byData('tool.llm', 5, 'parent')
    const own = byData('tool.llm', 5, 'child')
    expect(nodeMatchesSelection(inherited, { type: 'tool.llm', cursor: 5, channel: 'parent' })).toBe(true)
    expect(nodeMatchesSelection(own, { type: 'tool.llm', cursor: 5, channel: 'parent' })).toBe(false)
    expect(nodeMatchesSelection(own, { type: 'tool.llm', cursor: 5, channel: 'child' })).toBe(true)
  })

  it('a selection without a channel matches any channel', () => {
    const inherited = byData('tool.llm', 5, 'parent')
    expect(nodeMatchesSelection(inherited, { type: 'tool.llm', cursor: 5 })).toBe(true)
  })

  it('returns false for null selections', () => {
    expect(nodeMatchesSelection(byData('tool.llm', 5, 'parent'), null)).toBe(false)
  })
})

describe('gridColumnsForCount', () => {
  it('uses the classic 4 columns for small timelines', () => {
    expect(gridColumnsForCount(0)).toBe(4)
    expect(gridColumnsForCount(1)).toBe(4)
    expect(gridColumnsForCount(12)).toBe(4)
  })

  it('scales columns with the node count', () => {
    expect(gridColumnsForCount(13)).toBe(5)
    expect(gridColumnsForCount(30)).toBe(5)
    expect(gridColumnsForCount(31)).toBe(6)
    expect(gridColumnsForCount(60)).toBe(6)
    expect(gridColumnsForCount(61)).toBe(8)
    expect(gridColumnsForCount(100)).toBe(8)
    expect(gridColumnsForCount(121)).toBe(10)
    expect(gridColumnsForCount(251)).toBe(12)
    expect(gridColumnsForCount(501)).toBe(16)
    expect(gridColumnsForCount(1000)).toBe(16)
  })
})

describe('adaptive grid layout', () => {
  const events = (n: number) =>
    Array.from({ length: n }, (_, i) =>
      event('ch', i + 1, `e${i}`, { type: 'agent.started' })
    )

  it('lays 100 nodes out in rows of 8', () => {
    const { nodes } = buildFlowNodes(events(100), 'grid')
    expect(nodes).toHaveLength(100)
    const xs = new Set(nodes.map((n) => n.position.x))
    // Columns 0..7 → 8 distinct x positions.
    expect(xs.size).toBe(8)
    expect(Math.max(...Array.from(xs))).toBeLessThan(START_X + 8 * 260)
  })

  it('keeps 4 columns for a small timeline', () => {
    const { nodes } = buildFlowNodes(events(10), 'grid')
    const xs = new Set(nodes.map((n) => n.position.x))
    expect(xs.size).toBe(4)
  })

  it('recomputes columns from the filtered node count', () => {
    const mixed = Array.from({ length: 100 }, (_, i) =>
      event('ch', i + 1, `e${i}`, { type: i < 40 ? 'agent.started' : 'tool.llm' })
    )
    // Unfiltered: 100 nodes → 8 columns.
    const all = buildFlowNodes(mixed, 'grid')
    expect(new Set(all.nodes.map((n) => n.position.x)).size).toBe(8)
    // Filtered to 60 tool.llm nodes → 6 columns.
    const subset = buildFlowNodes(mixed, 'grid', undefined, new Set(['tool.llm']))
    expect(subset.nodes).toHaveLength(60)
    expect(new Set(subset.nodes.map((n) => n.position.x)).size).toBe(6)
  })
})
