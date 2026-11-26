// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest'
import {
  MOCK_CHANNELS,
  MOCK_EVENTS,
  MOCK_STATES,
  generateMockLiveEvent,
} from './mock-data'

describe('MOCK_CHANNELS', () => {
  it('exposes the 10 configured mock channels', () => {
    expect(MOCK_CHANNELS).toHaveLength(10)
    expect(MOCK_CHANNELS).toContain('agent-research-run')
    expect(MOCK_CHANNELS).toContain('agent-research-run-exp-a-sub')
    expect(MOCK_CHANNELS).toContain('data-pipeline-orchestrator')
  })
})

describe('MOCK_EVENTS', () => {
  it('has event lists for every configured channel', () => {
    for (const channel of MOCK_CHANNELS) {
      expect(Array.isArray(MOCK_EVENTS[channel]), `missing events for ${channel}`).toBe(true)
      expect(MOCK_EVENTS[channel].length).toBeGreaterThan(0)
    }
  })

  it('cursors are non-decreasing within each channel (state events share the last cursor)', () => {
    for (const [channel, events] of Object.entries(MOCK_EVENTS)) {
      let last = 0
      for (const e of events) {
        const cursor = e.cursor as number
        expect(cursor).toBeGreaterThanOrEqual(last)
        last = cursor
      }
    }
  })

  it('every non-state event has event_type, channel_id, timestamp and payload', () => {
    for (const events of Object.values(MOCK_EVENTS)) {
      for (const e of events) {
        if (e.isState) continue
        expect(typeof e.event_type).toBe('string')
        expect(typeof e.channel_id).toBe('string')
        expect(typeof e.timestamp).toBe('string')
        expect(e.payload).toBeTruthy()
      }
    }
  })

  it('agent-research-run includes a state event at the end', () => {
    const root = MOCK_EVENTS['agent-research-run']
    const stateEvent = root[root.length - 1]
    expect(stateEvent.isState).toBe(true)
    expect(stateEvent.state).toHaveProperty('status', 'completed')
  })

  it('fork experiments record their fork origin in the first event payload', () => {
    const expA = MOCK_EVENTS['agent-research-run-exp-a'][0].payload as any
    expect(expA.forked_from).toBe('agent-research-run')
    expect(expA.at_cursor).toBe(3)
  })

  it('real-time channels contain node lifecycle events', () => {
    const types = MOCK_EVENTS['langgraph-agent-prod'].map(e => e.event_type)
    expect(types.some((t: any) => String(t).startsWith('node.'))).toBe(true)
    expect(types.some((t: any) => String(t).includes('tool'))).toBe(true)
  })
})

describe('MOCK_STATES', () => {
  it('has a state entry for every channel (may be null)', () => {
    for (const channel of MOCK_CHANNELS) {
      expect(channel in MOCK_STATES, `missing state entry for ${channel}`).toBe(true)
    }
  })

  it('root channel snapshot carries run summary fields', () => {
    const s = MOCK_STATES['agent-research-run']
    expect(s?.cursor).toBe(5)
    expect(s?.state).toMatchObject({ status: 'completed', agent: 'research-v1' })
  })

  it('oncall-automation has no state yet', () => {
    expect(MOCK_STATES['oncall-automation']).toBeNull()
  })

  it('data pipeline state is rich and nested', () => {
    const s = MOCK_STATES['data-pipeline-orchestrator']
    const state = s?.state as any
    expect(state.stages.extract.rows).toBeGreaterThan(0)
    expect(state.metrics.total_rows).toBe(2483921)
  })
})

describe('generateMockLiveEvent', () => {
  it('returns a well-formed DashboardEvent', () => {
    const e = generateMockLiveEvent()
    expect(e.id).toMatch(/^mock-live-/)
    expect(e.type).toBeTruthy()
    expect(e.channel).toContain('-')
    expect(e.cursor).toBeGreaterThanOrEqual(500)
    expect(e.timestamp).toBeInstanceOf(Date)
    expect(e.payload).toBeTruthy()
    expect(e.isState).toBe(false)
  })

  it('generates a valid event type from the template set', () => {
    const validTypes = ['node.heartbeat', 'tool.llm.called', 'tool.api_response', 'agent.token_used', 'metric.recorded', 'cache.hit', 'node.progress', 'log.info']
    for (let i = 0; i < 50; i++) {
      expect(validTypes).toContain(generateMockLiveEvent().type)
    }
  })

  it('produces unique ids across calls', () => {
    const ids = new Set(Array.from({ length: 100 }, () => generateMockLiveEvent().id))
    expect(ids.size).toBe(100)
  })
})
