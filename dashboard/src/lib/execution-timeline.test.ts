// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest'
import { classifyTimelineEvent, formatTimelineElapsed, isFrameworkInternalType, timelineEventKey, timelineTitle } from './execution-timeline'
import type { DashboardEvent } from './flow-store'

const event = (type: string, payload: Record<string, unknown> = {}): DashboardEvent => ({ cursor: 4, channel: 'run', timestamp: '2026-01-01T00:00:01.250Z', type, payload })

describe('execution timeline semantics', () => {
  it.each([
    ['agent.crashed', 'failure'], ['execution.resumed', 'recovery'], ['fork.started', 'fork'],
    ['tool.claimed', 'tool'], ['agent.completed', 'success'], ['agent.started', 'active'], ['custom.fact', 'default'],
  ])('classifies %s as %s', (type, expected) => expect(classifyTimelineEvent(event(type))).toBe(expected))

  it('always treats state snapshots as state', () => expect(classifyTimelineEvent({ ...event('agent.failed'), isState: true })).toBe('state'))
  it('prefers concise payload labels and humanizes the event type', () => {
    expect(timelineTitle(event('tool.completed', { title: 'Refund verified' }))).toBe('Refund verified')
    expect(timelineTitle(event('customer_context.loaded'))).toBe('Customer Context Loaded')
  })
  it('formats elapsed time and clamps negative values', () => {
    expect(formatTimelineElapsed('2026-01-01T00:00:00Z', '2026-01-01T00:01:02.340Z')).toBe('01:02.3')
    expect(formatTimelineElapsed('2026-01-01T00:00:02Z', '2026-01-01T00:00:01Z')).toBe('00:00.0')
  })
  it('uses durable ids and a collision-resistant fallback key', () => {
    expect(timelineEventKey({ ...event('x'), id: 'evt-1' }, 0)).toBe('evt-1')
    expect(timelineEventKey(event('x'), 2)).toBe('run:4:x:2')
  })
  it('flags framework-checkpointer events as internal (hidden by default)', () => {
    expect(isFrameworkInternalType('langgraph.checkpoint')).toBe(true)
    expect(isFrameworkInternalType('langgraph.pending_writes')).toBe(true)
    expect(isFrameworkInternalType('checkpointer.saved')).toBe(true)
    expect(isFrameworkInternalType('filter_source')).toBe(false)
    expect(isFrameworkInternalType('fork.started')).toBe(false)
    expect(isFrameworkInternalType(undefined)).toBe(false)
  })
})
