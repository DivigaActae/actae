// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import RunsPage from './page'

const listChannels = vi.hoisted(() => vi.fn())
const getChannelMetadata = vi.hoisted(() => vi.fn())

vi.mock('@/lib/api', () => ({ listChannels, getChannelMetadata }))
vi.mock('@/components/ProtectedLayout', () => ({
  default: ({ children }: any) => <div>{children}</div>,
}))

beforeEach(() => {
  vi.clearAllMocks()
  getChannelMetadata.mockResolvedValue({ data: null, status: 404 })
})

describe('RunsPage', () => {
  it('lists runs with stats and links to the run detail', async () => {
    listChannels.mockResolvedValue({
      status: 200,
      data: {
        channels: ['ch-a', 'ch-b'],
        stats: {
          'ch-a': { event_count: 1200, latest_cursor: 42, last_event_at: new Date().toISOString() },
          'ch-b': { event_count: 7, latest_cursor: 7, last_event_at: '2026-07-25T10:00:00Z' },
        },
      },
    })
    render(<RunsPage />)
    await waitFor(() => {
      expect(screen.getByText('ch-a')).toBeInTheDocument()
    })
    expect(screen.getByText('ch-b')).toBeInTheDocument()
    expect(screen.getByText('1,200')).toBeInTheDocument()
    const openLinks = screen.getAllByRole('link', { name: 'Open run' })
    expect(openLinks[0]).toHaveAttribute('href', '/dashboard/flow/?channel=ch-a')
    const eventLinks = screen.getAllByRole('link', { name: 'Events' })
    expect(eventLinks[0]).toHaveAttribute('href', '/dashboard/events/?channel=ch-a')
  })

  it('shows a fork badge and parent for forked runs', async () => {
    listChannels.mockResolvedValue({ status: 200, data: { channels: ['root', 'root-exp-1'], stats: {} } })
    getChannelMetadata.mockImplementation(async (id: string) => id === 'root-exp-1'
      ? { data: { channel_id: id, display_name: 'Exp 1', parent_channel_id: 'root' }, status: 200 }
      : { data: null, status: 404 })
    render(<RunsPage />)
    await waitFor(() => {
      expect(screen.getByText('Exp 1')).toBeInTheDocument()
    })
    expect(screen.getByText('fork')).toBeInTheDocument()
    expect(screen.getByText('from root')).toBeInTheDocument()
  })

  it('filters runs by name or id', async () => {
    const user = userEvent.setup()
    listChannels.mockResolvedValue({ status: 200, data: { channels: ['alpha-run', 'beta-run'], stats: {} } })
    render(<RunsPage />)
    await waitFor(() => {
      expect(screen.getByText('alpha-run')).toBeInTheDocument()
    })
    await user.type(screen.getByLabelText('Filter runs'), 'beta')
    expect(screen.queryByText('alpha-run')).not.toBeInTheDocument()
    expect(screen.getByText('beta-run')).toBeInTheDocument()
  })

  it('shows the empty state when there are no runs', async () => {
    listChannels.mockResolvedValue({ status: 200, data: { channels: [], stats: {} } })
    render(<RunsPage />)
    await waitFor(() => {
      expect(screen.getByText('No runs yet')).toBeInTheDocument()
    })
  })

  it('uses server-provided names and lineage from stats without metadata lookups', async () => {
    const now = new Date().toISOString()
    listChannels.mockResolvedValue({
      status: 200,
      data: {
        channels: ['alpha-run', 'beta-run'],
        stats: {
          'alpha-run': { event_count: 1, latest_cursor: 1, last_event_at: now, display_name: 'Alpha Run', parent_channel_id: null },
          'beta-run': { event_count: 2, latest_cursor: 2, last_event_at: now, display_name: 'Beta Run', parent_channel_id: 'alpha-run' },
        },
      },
    })
    render(<RunsPage />)
    await waitFor(() => {
      expect(screen.getByText('Alpha Run')).toBeInTheDocument()
    })
    expect(screen.getByText('Beta Run')).toBeInTheDocument()
    expect(screen.getByText('fork', { exact: true })).toBeInTheDocument()
    expect(screen.getByText('from alpha-run')).toBeInTheDocument()
    // No per-channel metadata requests when the server supplies the fields.
    expect(getChannelMetadata).not.toHaveBeenCalled()
  })

  it('shows an error banner when the channel list fails', async () => {
    listChannels.mockResolvedValue({ status: 500, error: 'channels down' })
    render(<RunsPage />)
    await waitFor(() => {
      expect(screen.getByRole('alert')).toHaveTextContent('channels down')
    })
  })

  it('tolerates metadata enrichment failures', async () => {
    listChannels.mockResolvedValue({ status: 200, data: { channels: ['ch-a'], stats: {} } })
    getChannelMetadata.mockRejectedValue(new Error('boom'))
    render(<RunsPage />)
    await waitFor(() => {
      expect(screen.getByText('ch-a')).toBeInTheDocument()
    })
    // The run still links even though its friendly name could not be resolved.
    expect(screen.getByRole('link', { name: 'Open run' })).toHaveAttribute('href', '/dashboard/flow/?channel=ch-a')
  })
})
