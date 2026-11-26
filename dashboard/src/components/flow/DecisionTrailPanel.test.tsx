// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { DecisionTrailPanel } from './DecisionTrailPanel'

const getDecisionTrail = vi.hoisted(() => vi.fn())

vi.mock('@/lib/api', () => ({ getDecisionTrail }))

const baseTrail = {
  channel_id: 'fork-c',
  origin_run_id: 'root',
  ancestry: [
    { channel_id: 'fork-c', parent_channel_id: 'fork-a', forked_at_cursor: 5, display_name: 'C', reason: null },
    { channel_id: 'fork-a', parent_channel_id: 'root', forked_at_cursor: 3, display_name: 'A', reason: 'first fork' },
    { channel_id: 'root', parent_channel_id: null, forked_at_cursor: 0, display_name: null, reason: null },
  ],
  boundary: { channel_id: 'fork-a', cursor: 3, state: { step: 3 } },
  executions: [
    {
      id: 'ex-1', channel_id: 'fork-c', key_name: 'calc', tool_name: 'calculator',
      status: 'completed', params: null, result: { ok: true }, error: null,
      started_cursor: 6, completed_cursor: 7,
    },
    {
      id: 'ex-2', channel_id: 'fork-c', key_name: 'risky', tool_name: 'http.request',
      status: 'failed', params: null, result: null, error: { msg: 'timeout' },
      started_cursor: 8, completed_cursor: null,
    },
  ],
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('DecisionTrailPanel', () => {
  it('renders lineage, origin, boundary, and executions', async () => {
    getDecisionTrail.mockResolvedValue({ data: baseTrail, status: 200 })
    render(<DecisionTrailPanel channelId="fork-c" />)
    expect(await screen.findByText('Lineage')).toBeInTheDocument()
    // ancestry chain order
    const chain = screen.getAllByText(/fork-c|fork-a|root/).length
    expect(chain).toBeGreaterThanOrEqual(3)
    expect(screen.getByText(/origin run:/)).toBeInTheDocument()
    expect(screen.getByText('Fork boundary state')).toBeInTheDocument()
    expect(screen.getByText('Tool executions (2)')).toBeInTheDocument()
    expect(screen.getByText('calculator')).toBeInTheDocument()
    expect(screen.getByText('completed')).toBeInTheDocument()
    expect(screen.getByText('failed')).toBeInTheDocument()
  })

  it('calls the api with the channel id', async () => {
    getDecisionTrail.mockResolvedValue({ data: baseTrail, status: 200 })
    render(<DecisionTrailPanel channelId="fork-c" />)
    await screen.findByText('Lineage')
    expect(getDecisionTrail).toHaveBeenCalledWith('fork-c')
  })

  it('shows empty executions state', async () => {
    getDecisionTrail.mockResolvedValue({
      data: { ...baseTrail, executions: [], boundary: null },
      status: 200,
    })
    render(<DecisionTrailPanel channelId="fork-c" />)
    expect(await screen.findByText('No tool executions on this fork')).toBeInTheDocument()
  })

  it('shows the error when the api fails', async () => {
    getDecisionTrail.mockResolvedValue({ status: 500, error: 'nope' })
    render(<DecisionTrailPanel channelId="fork-c" />)
    expect(await screen.findByText('nope')).toBeInTheDocument()
  })
})
