// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import ChannelSelector from './ChannelSelector'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const api = vi.hoisted(() => ({
  listChannels: vi.fn(),
  replayEvents: vi.fn(),
  loadState: vi.fn(),
  listForks: vi.fn(),
  getChannelMetadata: vi.fn(),
  getPreferences: vi.fn(),
  putPreferences: vi.fn(),
}))

vi.mock('@/lib/api', () => ({
  listChannels: api.listChannels,
  replayEvents: api.replayEvents,
  loadState: api.loadState,
  listForks: api.listForks,
  getChannelMetadata: api.getChannelMetadata,
  getPreferences: api.getPreferences,
  putPreferences: api.putPreferences,
}))

beforeEach(() => {
  resetFlowStore()
  vi.clearAllMocks()
  api.listChannels.mockResolvedValue({ data: { channels: ['ch-a', 'ch-b'] }, status: 200 })
  api.replayEvents.mockResolvedValue({ data: { events: [] }, status: 200 })
  api.loadState.mockResolvedValue({ data: null, status: 404 })
  api.listForks.mockResolvedValue({ data: { forks: [] }, status: 200 })
  api.getChannelMetadata.mockResolvedValue({ data: null, status: 404 })
  api.getPreferences.mockResolvedValue({ data: { prefs: {} }, status: 200 })
  api.putPreferences.mockResolvedValue({ status: 200 })
})

describe('flow ChannelSelector', () => {
  it('renders the store channels as options', async () => {
    useFlowStore.setState({ channels: ['ch-a', 'ch-b'], activeChannel: 'ch-a' })
    render(<ChannelSelector />)
    const select = await screen.findByRole('combobox')
    const options = Array.from(select.querySelectorAll('option')).map((o) => o.value)
    expect(options).toEqual(['', 'ch-a', 'ch-b'])
    expect(select).toHaveValue('ch-a')
  })

  it('does not render its own "Channel" label (the header pill provides it)', async () => {
    useFlowStore.setState({ channels: ['ch-a'], activeChannel: 'ch-a' })
    render(<ChannelSelector />)
    expect(screen.queryByText('Channel')).not.toBeInTheDocument()
  })

  it('gives long channel names substantially more toolbar space', async () => {
    useFlowStore.setState({ channels: ['chchchchchchchchchchchchchchchchchchchch'], activeChannel: 'chchchchchchchchchchchchchchchchchchchch' })
    render(<ChannelSelector />)
    const select = await screen.findByRole('combobox')
    expect(select.className).toContain('w-full')
    expect(select.className).toContain('min-w-0')
    expect(select.parentElement?.className).toContain('520px')
  })

  it('exposes the full channel id as a tooltip', async () => {
    useFlowStore.setState({ channels: ['a-very-long-channel-id'], activeChannel: 'a-very-long-channel-id' })
    render(<ChannelSelector />)
    const select = await screen.findByRole('combobox')
    expect(select).toHaveAttribute('title', 'a-very-long-channel-id')
  })

  it('switches the active channel on change', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ channels: ['ch-a', 'ch-b'], activeChannel: 'ch-a' })
    render(<ChannelSelector />)
    const select = await screen.findByRole('combobox')
    await user.selectOptions(select, 'ch-b')
    expect(useFlowStore.getState().activeChannel).toBe('ch-b')
  })
})
