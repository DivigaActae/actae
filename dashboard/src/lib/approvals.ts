// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import type { WakeupView } from './api'

export interface QueryEvent {
  id?: string
  event_type?: string
  channel_id?: string
  payload?: Record<string, unknown> | null
  cursor?: number
  timestamp?: string
}

export interface ApprovalView {
  request_id: string
  channel_id: string
  summary: string
  details?: unknown
  status: 'pending' | 'approved' | 'rejected' | 'expired'
  requested_at: string
  decided_at?: string
  decided_by?: string
  reason?: string
  deadline?: string
  timeout_seconds?: number
}

function asString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined
}

function requestIdOf(event: QueryEvent): string | undefined {
  const payload = event.payload || {}
  return asString(payload.request_id) || event.id
}

/** Join the approval.requested / decided / expired event streams (+ pending
 * wake-up deadlines) into one row per request. Pure and unit-testable. */
export function buildApprovals(
  requested: QueryEvent[],
  decided: QueryEvent[],
  expired: QueryEvent[],
  wakeups: WakeupView[]
): ApprovalView[] {
  const decisions = new Map<string, QueryEvent>()
  for (const event of decided) {
    const id = requestIdOf(event)
    if (id) decisions.set(id, event)
  }
  const expirations = new Set<string>()
  for (const event of expired) {
    const id = requestIdOf(event)
    if (id) expirations.add(id)
  }
  const deadlines = new Map<string, string>()
  for (const wakeup of wakeups) {
    if (wakeup.status !== 'pending') continue
    const requestId = asString(wakeup.payload?.approval_request_id)
    if (requestId) deadlines.set(requestId, wakeup.run_at)
  }

  const rows: ApprovalView[] = []
  for (const event of requested) {
    const payload = event.payload || {}
    const requestId = requestIdOf(event)
    if (!requestId) continue
    const decision = decisions.get(requestId)
    const status: ApprovalView['status'] = decision
      ? (asString(decision.payload?.decision) === 'rejected' ? 'rejected' : 'approved')
      : expirations.has(requestId)
        ? 'expired'
        : 'pending'
    rows.push({
      request_id: requestId,
      channel_id: event.channel_id || decision?.channel_id || '',
      summary: asString(payload.summary) || '(no summary)',
      details: payload.details,
      status,
      requested_at: event.timestamp || '',
      decided_at: decision?.timestamp,
      decided_by: asString(decision?.payload?.actor) || asString(decision?.payload?.decided_by),
      reason: asString(decision?.payload?.reason),
      deadline: deadlines.get(requestId),
      timeout_seconds: typeof payload.timeout_seconds === 'number' ? payload.timeout_seconds : undefined,
    })
  }
  // Pending first (attention), then most recent.
  rows.sort((a, b) => {
    if ((a.status === 'pending') !== (b.status === 'pending')) return a.status === 'pending' ? -1 : 1
    return (b.requested_at || '').localeCompare(a.requested_at || '')
  })
  return rows
}
