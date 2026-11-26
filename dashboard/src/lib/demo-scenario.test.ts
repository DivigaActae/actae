// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest'
import { DEMO_CHANNEL, DEMO_FORK_CHANNEL, DEMO_SCENARIO, DEMO_TOTAL_MS, demoOffsets, demoStepToEvent, demoTimestampAt } from './demo-scenario'

describe('demo scenario (deterministic replay)', () => {
  it('is a fixed, well-formed timeline', () => {
    expect(DEMO_SCENARIO.length).toBeGreaterThan(10)
    for (const step of DEMO_SCENARIO) {
      expect(step.delayMs).toBeGreaterThan(0)
      expect(step.cursor).toBeGreaterThan(0)
      expect(step.type).toBeTruthy()
      expect(step.actor).toBeTruthy()
      expect([DEMO_CHANNEL, DEMO_FORK_CHANNEL]).toContain(step.channel)
    }
  })

  it('records a fork from the saved boundary (cursor 3)', () => {
    const fork = DEMO_SCENARIO.find(step => step.type === 'fork.started')
    expect(fork).toBeDefined()
    expect(fork!.channel).toBe(DEMO_FORK_CHANNEL)
    expect(fork!.payload.source_channel_id).toBe(DEMO_CHANNEL)
    expect(fork!.payload.resolved_cursor).toBe(3)
  })

  it('exposes stable cumulative offsets that sum to the total', () => {
    const offsets = demoOffsets()
    expect(offsets).toEqual(demoOffsets())
    expect(offsets[offsets.length - 1]).toBe(DEMO_TOTAL_MS)
    for (let i = 1; i < offsets.length; i += 1) expect(offsets[i]).toBeGreaterThan(offsets[i - 1]!)
  })

  it('builds store events with stable, unique ids', () => {
    const events = DEMO_SCENARIO.map((step, index) => demoStepToEvent(step, index))
    const ids = events.map(event => event.id)
    expect(new Set(ids).size).toBe(ids.length)
    expect(events[0]!.channel).toBe(DEMO_CHANNEL)
    expect(events[0]!.isState).toBe(false)
    // Same step → same id (idempotent re-runs).
    expect(demoStepToEvent(DEMO_SCENARIO[0]!, 0).id).toBe(events[0]!.id)
  })

  it('uses deterministic, strictly-increasing timestamps', () => {
    const stamps = DEMO_SCENARIO.map((_, index) => demoTimestampAt(index))
    expect(stamps).toEqual(DEMO_SCENARIO.map((_, index) => demoTimestampAt(index)))
    for (let i = 1; i < stamps.length; i += 1) {
      expect(new Date(stamps[i]!).getTime()).toBeGreaterThan(new Date(stamps[i - 1]!).getTime())
    }
  })

  it('replays into dedicated, otherwise-empty channels', () => {
    // The demo channels must not exist in the recorded mock dataset, so the
    // replay builds them from empty (no duplication with history).
    const channels = new Set(DEMO_SCENARIO.map(step => step.channel))
    expect(channels).toEqual(new Set([DEMO_CHANNEL, DEMO_FORK_CHANNEL]))
    expect(DEMO_CHANNEL).toBe('demo-live-run')
  })
})
