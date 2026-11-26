// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StateDiffPanel } from './StateDiffPanel'

const diffChannelStates = vi.hoisted(() => vi.fn())

vi.mock('@/lib/api', () => ({ diffChannelStates }))

const baseDiff = {
  left_channel_id: 'fix-a',
  right_channel_id: 'fix-b',
  left: { channel_id: 'fix-a', cursor: 1, state: { step: 2 } },
  right: { channel_id: 'fix-b', cursor: 1, state: { step: 3 } },
  common: { channel_id: 'root', cursor: 1, state: { step: 1 } },
  left_diverged_at_cursor: 1,
  right_diverged_at_cursor: 1,
  truncated: false,
  entry_count_total: 3,
  max_entries: 500,
  entries: [
    { path: ['step'], kind: 'changed', left: 2, right: 3 },
    { path: ['extra'], kind: 'added', left: null, right: true },
    { path: ['old'], kind: 'removed', left: 'gone', right: null },
  ],
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('StateDiffPanel', () => {
  it('renders divergence point, ancestor, and summary', async () => {
    diffChannelStates.mockResolvedValue({ data: baseDiff, status: 200 })
    render(<StateDiffPanel activeChannel="fix-a" compareChannel="fix-b" />)
    expect(await screen.findByText(/divergence \(left · right\)/i)).toBeInTheDocument()
    expect(screen.getByText(/Shared ancestor/)).toBeInTheDocument()
    expect(screen.getByText('1 added')).toBeInTheDocument()
    expect(screen.getByText('1 removed')).toBeInTheDocument()
    expect(screen.getByText('1 changed')).toBeInTheDocument()
  })

  it('calls the api with both channels', async () => {
    diffChannelStates.mockResolvedValue({ data: baseDiff, status: 200 })
    render(<StateDiffPanel activeChannel="fix-a" compareChannel="fix-b" />)
    await screen.findByText(/Shared ancestor/)
    expect(diffChannelStates).toHaveBeenCalledWith('fix-a', 'fix-b')
  })

  it('renders entry kinds', async () => {
    diffChannelStates.mockResolvedValue({ data: baseDiff, status: 200 })
    render(<StateDiffPanel activeChannel="fix-a" compareChannel="fix-b" />)
    expect(await screen.findByText('changed')).toBeInTheDocument()
    expect(screen.getByText('added')).toBeInTheDocument()
    expect(screen.getByText('removed')).toBeInTheDocument()
    expect(screen.getByText('step:')).toBeInTheDocument()
  })

  it('shows empty state when no differences', async () => {
    diffChannelStates.mockResolvedValue({
      data: { ...baseDiff, entries: [] },
      status: 200,
    })
    render(<StateDiffPanel activeChannel="fix-a" compareChannel="fix-b" />)
    expect(await screen.findByText('No structural differences.')).toBeInTheDocument()
  })

  it('shows the error when the api fails', async () => {
    diffChannelStates.mockResolvedValue({ status: 500, error: 'boom' })
    render(<StateDiffPanel activeChannel="fix-a" compareChannel="fix-b" />)
    expect(await screen.findByText('boom')).toBeInTheDocument()
  })
})
