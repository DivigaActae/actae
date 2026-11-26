// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import ApprovalsPage from './page'
import { buildApprovals } from '@/lib/approvals'

const queryEvents = vi.hoisted(() => vi.fn())
const listWakeups = vi.hoisted(() => vi.fn())
const recordEvent = vi.hoisted(() => vi.fn())

vi.mock('@/components/ProtectedLayout', () => ({ default: ({ children }: { children: React.ReactNode }) => <>{children}</> }))
vi.mock('@/lib/auth', () => ({ useAuth: () => ({ user: { email: 'alice@example.com', name: 'Alice' } }) }))
vi.mock('@/lib/api', () => ({ queryEvents, listWakeups, recordEvent }))

const requested = {
  id: 'ev-1',
  event_type: 'approval.requested',
  channel_id: 'run-1',
  timestamp: '2026-09-21T08:00:00Z',
  payload: { request_id: 'req-1', summary: 'Send the contract to legal', timeout_seconds: 3600 },
}
const decided = {
  id: 'ev-2',
  event_type: 'approval.decided',
  channel_id: 'run-1',
  timestamp: '2026-09-21T08:05:00Z',
  payload: { request_id: 'req-1', decision: 'approved', reason: 'looks good' },
}

function mockEvents(byType: Record<string, unknown[]>) {
  queryEvents.mockImplementation((params: { event_type?: string }) =>
    Promise.resolve({ data: { events: byType[params.event_type || ''] || [] }, status: 200 })
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  mockEvents({})
  listWakeups.mockResolvedValue({ data: { wakeups: [] }, status: 200 })
  recordEvent.mockResolvedValue({ data: {}, status: 200 })
})

describe('buildApprovals', () => {
  it('joins requested/expired rows and pending wake-up deadlines', () => {
    const rows = buildApprovals(
      [requested],
      [],
      [{ payload: { request_id: 'req-1' } }],
      [{ id: 'w1', status: 'pending', payload: { approval_request_id: 'req-1' }, run_at: '2026-09-21T09:00:00Z' } as never]
    )
    expect(rows).toHaveLength(1)
    expect(rows[0].status).toBe('expired')
    expect(rows[0].deadline).toBe('2026-09-21T09:00:00Z')
  })

  it('marks a request approved and carries the decider', () => {
    const rows = buildApprovals([requested], [{ ...decided, payload: { ...decided.payload, actor: 'alice' } }], [], [])
    expect(rows[0].status).toBe('approved')
    expect(rows[0].decided_by).toBe('alice')
    expect(rows[0].reason).toBe('looks good')
  })

  it('marks an expired request', () => {
    const rows = buildApprovals([requested], [], [{ payload: { request_id: 'req-1' } }], [])
    expect(rows[0].status).toBe('expired')
  })
})

describe('ApprovalsPage', () => {
  it('renders a pending approval and records an approve decision as the signed-in user', async () => {
    mockEvents({ 'approval.requested': [requested] })
    const user = userEvent.setup()
    render(<ApprovalsPage />)
    expect((await screen.findAllByText('Send the contract to legal')).length).toBeGreaterThan(0)
    await user.click(screen.getByRole('button', { name: 'Approve' }))
    await waitFor(() =>
      expect(recordEvent).toHaveBeenCalledWith(
        'run-1',
        'approval.decided',
        expect.objectContaining({ request_id: 'req-1', decision: 'approved' }),
        'alice@example.com'
      )
    )
  })

  it('shows resolved statuses and pending wake-ups', async () => {
    mockEvents({ 'approval.requested': [requested], 'approval.decided': [decided] })
    listWakeups.mockResolvedValue({
      data: { wakeups: [{ id: 'w1', channel_id: 'run-1', run_at: '2026-09-21T09:00:00Z', status: 'pending', payload: { kind: 'approval_timeout' }, created_at: '', executed_at: null, attempts: 0, error: null }] },
      status: 200,
    })
    render(<ApprovalsPage />)
    expect(await screen.findByText('approved')).toBeInTheDocument()
    expect(screen.getByText('approval_timeout')).toBeInTheDocument()
    expect(screen.getByText('Awaiting decision')).toBeInTheDocument()
  })

  it('shows empty states with no approvals or wake-ups', async () => {
    render(<ApprovalsPage />)
    expect(await screen.findByText(/No pending approvals/)).toBeInTheDocument()
    expect(screen.getByText('No approval events yet.')).toBeInTheDocument()
    expect(screen.getByText('No scheduled wake-ups.')).toBeInTheDocument()
  })
})
