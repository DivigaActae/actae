// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import type { DashboardEvent } from '@/lib/flow-store'

export interface DiffResult {
  shared: number
  divergedAt: number | null
  leftOnly: DashboardEvent[]
  rightOnly: DashboardEvent[]
}

export function diffEvents(
  eventsLeft: DashboardEvent[],
  eventsRight: DashboardEvent[],
): DiffResult {
  const max = Math.min(eventsLeft.length, eventsRight.length)
  let shared = 0
  for (let i = 0; i < max; i++) {
    if (eventsLeft[i].type === eventsRight[i].type) {
      shared++
    } else {
      break
    }
  }
  return {
    shared,
    divergedAt: shared < eventsLeft.length ? eventsLeft[shared]?.cursor ?? null : null,
    leftOnly: eventsLeft.slice(shared),
    rightOnly: eventsRight.slice(shared),
  }
}

export function computeComparisonMetrics(events: DashboardEvent[]) {
  let totalLatencyMs = 0
  let tokenCount = 0
  let costEstimate = 0

  for (const e of events) {
    if (e.payload) {
      if (typeof e.payload.latency_ms === 'number') totalLatencyMs += e.payload.latency_ms
      if (typeof e.payload.tokens === 'number') tokenCount += e.payload.tokens
      if (typeof e.payload.token_count === 'number') tokenCount += e.payload.token_count
      if (typeof e.payload.cost === 'number') costEstimate += e.payload.cost
    }
  }

  return {
    totalLatencyMs,
    tokenCount,
    costEstimate,
    eventCount: events.length,
  }
}
