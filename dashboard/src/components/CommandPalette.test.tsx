// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import CommandPalette from './CommandPalette'
import { useUiStore } from '@/lib/ui-store'

const push = vi.hoisted(() => vi.fn())
const listChannels = vi.hoisted(() => vi.fn())

vi.mock('next/navigation', () => ({ useRouter: () => ({ push }) }))
vi.mock('@/lib/api', () => ({ listChannels }))

beforeEach(() => {
  vi.clearAllMocks()
  useUiStore.setState({ commandOpen: false, helpOpen: false })
  listChannels.mockResolvedValue({ status: 200, data: { channels: ['alpha-run', 'beta-run'] } })
})

describe('CommandPalette', () => {
  it('renders nothing while closed', () => {
    const { container } = render(<CommandPalette />)
    expect(container).toBeEmptyDOMElement()
  })

  it('lists runs once open and navigates to a run detail', async () => {
    const user = userEvent.setup()
    render(<CommandPalette />)
    act(() => useUiStore.getState().openCommand())
    await waitFor(() => {
      expect(screen.getByText('alpha-run')).toBeInTheDocument()
    })
    await user.click(screen.getByText('alpha-run'))
    expect(push).toHaveBeenCalledWith('/dashboard/flow/?channel=alpha-run')
    expect(useUiStore.getState().commandOpen).toBe(false)
  })

  it('offers an event search for the typed query', async () => {
    const user = userEvent.setup()
    render(<CommandPalette />)
    act(() => useUiStore.getState().openCommand())
    await user.type(screen.getByLabelText('Search runs and events'), 'timeout')
    await user.click(screen.getByText(/Search events for/))
    expect(push).toHaveBeenCalledWith('/dashboard/events/?q=timeout')
  })
})
