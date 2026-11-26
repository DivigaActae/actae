// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { CompareView } from './CompareView'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const compareChannels = vi.hoisted(() => vi.fn())
const getChannelMetadata = vi.hoisted(() => vi.fn())
vi.mock('@/lib/api', () => ({ compareChannels, getChannelMetadata, getPreferences: async () => ({ status: 200, data: { prefs: {} } }), putPreferences: async () => ({ status: 200 }) }))
beforeEach(() => {
  resetFlowStore()
  vi.clearAllMocks()
  getChannelMetadata.mockResolvedValue({ data: { channel_id: 'root', display_name: 'Baseline run' }, status: 200 })
  compareChannels.mockResolvedValue({ data: {
    state_diff: { entries: [], common: { channel_id: 'root', cursor: 3 }, left_diverged_at_cursor: null, right_diverged_at_cursor: 3 },
    manifest_diff: [], tool_diff: [], metrics_delta: [], output_changed: false,
  }, status: 200 })
})

const forks = [
  { channel_id: 'root-exp-a', display_name: 'Exp A', parent_channel_id: 'root', forked_at_cursor: 3, forked_at: '', origin_run_id: '', reason: null, event_count: 0, latest_cursor: 0, experiment_metadata: null },
  { channel_id: 'root-exp-b', display_name: null, parent_channel_id: 'root', forked_at_cursor: 3, forked_at: '', origin_run_id: '', reason: null, event_count: 0, latest_cursor: 0, experiment_metadata: null },
]

describe('CompareView', () => {
  it('shows a no-forks message when the store has none', () => {
    useFlowStore.setState({ activeChannel: 'root', forks: [] })
    render(<CompareView />)
    expect(screen.getByText('No other forks yet')).toBeInTheDocument()
  })

  it('lists fork options with display names', () => {
    useFlowStore.setState({ activeChannel: 'root', forks })
    render(<CompareView />)
    expect(screen.getByRole('option', { name: 'Exp A' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'root-exp-b' })).toBeInTheDocument()
  })

  it('runs the authoritative server comparison and shows lineage divergence', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'root', forks })
    render(<CompareView />)
    await user.click(screen.getByRole('button', { name: 'Compare' }))
    await waitFor(() => {
      expect(screen.getByText('Authoritative server diff')).toBeInTheDocument()
    })
    expect(compareChannels).toHaveBeenCalledWith('root', 'root-exp-a')
    expect(screen.getByText('@3')).toBeInTheDocument()
  })

  it('surfaces comparison errors', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'root', forks })
    compareChannels.mockRejectedValue(new Error('server down'))
    render(<CompareView />)
    await user.click(screen.getByRole('button', { name: 'Compare' }))
    await waitFor(() => {
      expect(screen.getByText('server down')).toBeInTheDocument()
    })
  })

  it('disables the compare button when there is no candidate', () => {
    useFlowStore.setState({ activeChannel: 'root', forks: [] })
    render(<CompareView />)
    expect(screen.queryByRole('button', { name: 'Compare' })).not.toBeInTheDocument()
  })
})
