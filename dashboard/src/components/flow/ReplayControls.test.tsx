// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import ReplayControls from './ReplayControls'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

beforeEach(() => {
  resetFlowStore()
})

const seedEvents = (count: number, startCursor = 1000) =>
  useFlowStore.setState({
    events: Array.from({ length: count }, (_, i) => ({
      id: `e${i + 1}`,
      cursor: startCursor + i,
      type: 'agent.started',
      channel: 'ch',
      payload: {},
      timestamp: '2026-01-01T00:00:00Z',
    })),
  })

describe('ReplayControls', () => {
  it('renders an input and Apply button', () => {
    render(<ReplayControls />)
    expect(screen.getByRole('spinbutton')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Apply' })).toBeInTheDocument()
  })

  it('disables the Apply button without an active channel', () => {
    render(<ReplayControls />)
    expect(screen.getByRole('button', { name: 'Apply' })).toBeDisabled()
  })

  it('maps a position to the global cursor of the Nth event', async () => {
    const user = userEvent.setup()
    useFlowStore.getState().setActiveChannel('ch')
    seedEvents(50, 1000)
    render(<ReplayControls />)
    await user.type(screen.getByRole('spinbutton'), '42')
    await user.click(screen.getByRole('button', { name: 'Apply' }))
    expect(useFlowStore.getState().replayFromCursor).toBe(1041)
  })

  it('skips the first N events even with sparse global cursors', async () => {
    const user = userEvent.setup()
    useFlowStore.getState().setActiveChannel('ch')
    seedEvents(50, 5579)
    render(<ReplayControls />)
    await user.type(screen.getByRole('spinbutton'), '25')
    await user.click(screen.getByRole('button', { name: 'Apply' }))
    expect(useFlowStore.getState().replayFromCursor).toBe(5603)
  })

  it('clamps beyond the timeline to the last event', async () => {
    const user = userEvent.setup()
    useFlowStore.getState().setActiveChannel('ch')
    seedEvents(50, 1000)
    render(<ReplayControls />)
    await user.type(screen.getByRole('spinbutton'), '60')
    await user.click(screen.getByRole('button', { name: 'Apply' }))
    expect(useFlowStore.getState().replayFromCursor).toBe(1049)
  })

  it('ignores invalid input', async () => {
    const user = userEvent.setup()
    useFlowStore.getState().setActiveChannel('ch')
    render(<ReplayControls />)
    await user.type(screen.getByRole('spinbutton'), '-5')
    await user.click(screen.getByRole('button', { name: 'Apply' }))
    expect(useFlowStore.getState().replayFromCursor).toBeNull()
  })

  it('fires Apply on Enter', async () => {
    const user = userEvent.setup()
    useFlowStore.getState().setActiveChannel('ch')
    seedEvents(50, 1000)
    render(<ReplayControls />)
    await user.type(screen.getByRole('spinbutton'), '9{Enter}')
    expect(useFlowStore.getState().replayFromCursor).toBe(1008)
  })

  it('treats 0 as the full timeline', async () => {
    const user = userEvent.setup()
    useFlowStore.getState().setActiveChannel('ch')
    seedEvents(50, 1000)
    render(<ReplayControls />)
    await user.type(screen.getByRole('spinbutton'), '0')
    await user.click(screen.getByRole('button', { name: 'Apply' }))
    expect(useFlowStore.getState().replayFromCursor).toBeNull()
  })

  it('shows Show all and restores when a replay cursor is active', async () => {
    const user = userEvent.setup()
    useFlowStore.getState().setActiveChannel('ch')
    useFlowStore.getState().setReplayCursor(5)
    render(<ReplayControls />)
    expect(screen.getByRole('button', { name: 'Show all' })).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Show all' }))
    expect(useFlowStore.getState().replayFromCursor).toBeNull()
    expect(screen.queryByRole('button', { name: 'Show all' })).not.toBeInTheDocument()
  })

  it('shows a loading placeholder on Go', () => {
    useFlowStore.getState().setActiveChannel('ch')
    useFlowStore.setState({ loading: true })
    render(<ReplayControls />)
    expect(screen.getByRole('button', { name: '...' })).toBeInTheDocument()
  })
})
