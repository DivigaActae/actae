// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MetadataEditor } from './MetadataEditor'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const getChannelMetadata = vi.hoisted(() => vi.fn())
const updateChannelMetadata = vi.hoisted(() => vi.fn())
vi.mock('@/lib/api', () => ({ getChannelMetadata, updateChannelMetadata, getPreferences: async () => ({ status: 200, data: { prefs: {} } }), putPreferences: async () => ({ status: 200 }) }))
beforeEach(() => {
  resetFlowStore()
  vi.clearAllMocks()
  getChannelMetadata.mockResolvedValue({ data: null, status: 404 })
  updateChannelMetadata.mockResolvedValue({ data: {}, status: 200 })
})

describe('MetadataEditor', () => {
  it('renders nothing without an active channel', () => {
    const { container } = render(<MetadataEditor />)
    expect(container).toBeEmptyDOMElement()
  })

  it('renders nothing when metadata is missing', async () => {
    useFlowStore.setState({ activeChannel: 'ch' })
    const { container } = render(<MetadataEditor />)
    await waitFor(() => {
      expect(container).toBeEmptyDOMElement()
    })
  })

  it('pre-fills the form from metadata', async () => {
    useFlowStore.setState({ activeChannel: 'ch' })
    getChannelMetadata.mockResolvedValue({
      data: {
        channel_id: 'ch', display_name: 'Exp 1', reason: 'testing', parent_channel_id: 'root',
        forked_at_cursor: 3, forked_at: '', origin_run_id: '', event_count: 0, latest_cursor: 0, experiment_metadata: null,
      },
      status: 200,
    })
    render(<MetadataEditor />)
    await waitFor(() => {
      expect(screen.getByPlaceholderText('Experiment name...')).toHaveValue('Exp 1')
    })
    expect(screen.getByPlaceholderText('Why this experiment...')).toHaveValue('testing')
    expect(screen.getByText(/Forked at resolved cursor/)).toBeInTheDocument()
    expect(screen.getByText('@3')).toBeInTheDocument()
  })

  it('saves edits via updateChannelMetadata', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'ch' })
    getChannelMetadata.mockResolvedValue({
      data: {
        channel_id: 'ch', display_name: null, reason: null, parent_channel_id: null,
        forked_at_cursor: 0, forked_at: '', origin_run_id: '', event_count: 0, latest_cursor: 0, experiment_metadata: null,
      },
      status: 200,
    })
    render(<MetadataEditor />)
    await waitFor(() => {
      expect(screen.getByPlaceholderText('Experiment name...')).toBeInTheDocument()
    })
    await user.type(screen.getByPlaceholderText('Experiment name...'), 'Renamed')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => {
      expect(updateChannelMetadata).toHaveBeenCalledWith({ channel_id: 'ch', display_name: 'Renamed', reason: undefined })
    })
    expect(screen.getByText('Saved')).toBeInTheDocument()
  })

  it('shows the server error on failure', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'ch' })
    getChannelMetadata.mockResolvedValue({
      data: {
        channel_id: 'ch', display_name: null, reason: null, parent_channel_id: null,
        forked_at_cursor: 0, forked_at: '', origin_run_id: '', event_count: 0, latest_cursor: 0, experiment_metadata: null,
      },
      status: 200,
    })
    updateChannelMetadata.mockResolvedValue({ data: null, error: 'nope', status: 400 })
    render(<MetadataEditor />)
    await waitFor(() => {
      expect(screen.getByPlaceholderText('Experiment name...')).toBeInTheDocument()
    })
    await user.type(screen.getByPlaceholderText('Experiment name...'), 'X')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => {
      expect(screen.getByText('nope')).toBeInTheDocument()
    })
  })
})
