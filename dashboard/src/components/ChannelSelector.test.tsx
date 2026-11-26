// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import ChannelSelector from './ChannelSelector'

const listChannels = vi.hoisted(() => vi.fn())

vi.mock('@/lib/api', () => ({
  listChannels,
  getPreferences: async () => ({ status: 200, data: { prefs: {} } }),
  putPreferences: async () => ({ status: 200 }),
}))

beforeEach(() => {
  vi.clearAllMocks()
})

describe('ChannelSelector', () => {
  it('renders the placeholder option first', () => {
    listChannels.mockResolvedValue({ data: { channels: [] }, status: 200 })
    render(<ChannelSelector value="" onChange={() => {}} />)
    expect(screen.getByText('— select —')).toBeInTheDocument()
  })

  it('loads and lists channels from the API', async () => {
    listChannels.mockResolvedValue({ data: { channels: ['a', 'b'] }, status: 200 })
    render(<ChannelSelector value="a" onChange={() => {}} />)
    await waitFor(() => {
      expect(screen.getByText('a')).toBeInTheDocument()
    })
    expect(screen.getByText('b')).toBeInTheDocument()
  })

  it('handles raw-array responses', async () => {
    listChannels.mockResolvedValue({ data: ['x', 'y'], status: 200 })
    render(<ChannelSelector value="" onChange={() => {}} />)
    await waitFor(() => {
      expect(screen.getByText('x')).toBeInTheDocument()
    })
    expect(screen.getByText('y')).toBeInTheDocument()
  })

  it('maps object channel entries by id', async () => {
    listChannels.mockResolvedValue({ data: [{ channel_id: 'c1' }, { id: 'c2' }], status: 200 })
    render(<ChannelSelector value="" onChange={() => {}} />)
    await waitFor(() => {
      expect(screen.getByText('c1')).toBeInTheDocument()
    })
    expect(screen.getByText('c2')).toBeInTheDocument()
  })

  it('fires onChange when a channel is selected', async () => {
    listChannels.mockResolvedValue({ data: { channels: ['ch1', 'ch2'] }, status: 200 })
    const onChange = vi.fn()
    const user = userEvent.setup()
    render(<ChannelSelector value="" onChange={onChange} />)
    await waitFor(() => {
      expect(screen.getByText('ch1')).toBeInTheDocument()
    })
    await user.selectOptions(screen.getByRole('combobox'), 'ch2')
    expect(onChange).toHaveBeenCalledWith('ch2')
  })

  it('is disabled while loading', () => {
    listChannels.mockReturnValue(new Promise(() => {}))
    render(<ChannelSelector value="" onChange={() => {}} />)
    expect(screen.getByRole('combobox')).toBeDisabled()
  })
})
