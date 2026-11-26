// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest'
import { diffEvents, computeComparisonMetrics } from './diff'
import type { DashboardEvent } from './flow-store'

function evt(cursor: number, type: string, payload: Record<string, unknown> = {}): DashboardEvent {
  return { id: `e${cursor}`, cursor, type, channel: 'ch', timestamp: new Date(), payload }
}

describe('diffEvents', () => {
  it('reports fully shared runs', () => {
    const a = [evt(1, 'a'), evt(2, 'b')]
    const b = [evt(1, 'a'), evt(2, 'b')]
    expect(diffEvents(a, b)).toEqual({ shared: 2, divergedAt: null, leftOnly: [], rightOnly: [] })
  })

  it('finds the divergence point and returns divergent tails', () => {
    const a = [evt(1, 'a'), evt(2, 'b'), evt(3, 'c')]
    const b = [evt(1, 'a'), evt(2, 'x'), evt(3, 'y')]
    const res = diffEvents(a, b)
    expect(res.shared).toBe(1)
    expect(res.divergedAt).toBe(2)
    expect(res.leftOnly.map(e => e.type)).toEqual(['b', 'c'])
    expect(res.rightOnly.map(e => e.type)).toEqual(['x', 'y'])
  })

  it('divergedAt is null when the left run ends exactly at the divergence', () => {
    const a = [evt(1, 'a')]
    const b = [evt(1, 'a'), evt(2, 'b')]
    const res = diffEvents(a, b)
    expect(res.shared).toBe(1)
    expect(res.divergedAt).toBeNull()
    expect(res.leftOnly).toEqual([])
    expect(res.rightOnly).toHaveLength(1)
  })

  it('compares by position, not cursor value', () => {
    const a = [evt(1, 'a'), evt(9, 'z')]
    const b = [evt(1, 'a'), evt(5, 'z')]
    expect(diffEvents(a, b).shared).toBe(2)
  })

  it('handles empty inputs', () => {
    expect(diffEvents([], [])).toEqual({ shared: 0, divergedAt: null, leftOnly: [], rightOnly: [] })
    const res = diffEvents([], [evt(1, 'a')])
    expect(res.shared).toBe(0)
    expect(res.rightOnly).toHaveLength(1)
    expect(res.divergedAt).toBeNull()
  })
})

describe('computeComparisonMetrics', () => {
  it('sums latency, tokens and cost across events', () => {
    const events = [
      evt(1, 'a', { latency_ms: 100, tokens: 50, cost: 0.01 }),
      evt(2, 'b', { latency_ms: 250, token_count: 30, cost: 0.02 }),
    ]
    expect(computeComparisonMetrics(events)).toEqual({
      totalLatencyMs: 350,
      tokenCount: 80,
      costEstimate: 0.03,
      eventCount: 2,
    })
  })

  it('treats missing fields as zero', () => {
    expect(computeComparisonMetrics([evt(1, 'a', {})])).toEqual({
      totalLatencyMs: 0,
      tokenCount: 0,
      costEstimate: 0,
      eventCount: 1,
    })
  })

  it('ignores non-numeric payload values', () => {
    const events = [
      evt(1, 'a', { latency_ms: 'fast', tokens: 'many', cost: undefined }),
      evt(2, 'b', { latency_ms: 10, tokens: 5, cost: 0.5 }),
    ]
    expect(computeComparisonMetrics(events)).toEqual({
      totalLatencyMs: 10,
      tokenCount: 5,
      costEstimate: 0.5,
      eventCount: 2,
    })
  })
})
