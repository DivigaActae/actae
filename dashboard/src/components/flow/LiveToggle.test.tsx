// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import LiveToggle from './LiveToggle'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const useWebSocket = vi.hoisted(() => vi.fn())
vi.mock('@/hooks/useWebSocket', () => ({ useWebSocket }))

beforeEach(() => {
  resetFlowStore()
  vi.clearAllMocks()
})

describe('LiveToggle', () => {
  it('renders Historical by default', () => {
    useFlowStore.getState().setActiveChannel?.('ch')
    render(<LiveToggle />)
    expect(screen.getByText('Historical')).toBeInTheDocument()
  })

  it('is disabled without an active channel', () => {
    render(<LiveToggle />)
    expect(screen.getByRole('button')).toBeDisabled()
  })

  it('toggles to Offline until a live transport is established', async () => {
    const user = userEvent.setup()
    useFlowStore.getState().setActiveChannel?.('ch')
    render(<LiveToggle />)
    await user.click(screen.getByRole('button'))
    expect(useFlowStore.getState().isLive).toBe(true)
    expect(screen.getByText('Offline')).toBeInTheDocument()
    expect(useWebSocket).toHaveBeenCalledWith('ch', true)
  })

  it('toggles back to Historical', async () => {
    const user = userEvent.setup()
    useFlowStore.getState().setActiveChannel?.('ch')
    useFlowStore.getState().setIsLive(true)
    useFlowStore.getState().setConnectionStatus({ connected: true, authenticated: true })
    render(<LiveToggle />)
    expect(screen.getByText('Live')).toBeInTheDocument()
    await user.click(screen.getByRole('button'))
    expect(useFlowStore.getState().isLive).toBe(false)
    expect(screen.getByText('Historical')).toBeInTheDocument()
  })

  it('shows Offline while a live connection has not opened', () => {
    useFlowStore.getState().setActiveChannel?.('ch')
    useFlowStore.getState().setIsLive(true)
    useFlowStore.getState().setConnectionStatus({ connected: false })
    render(<LiveToggle />)
    expect(screen.getByText('Offline')).toBeInTheDocument()
    expect(screen.queryByText('connecting...')).not.toBeInTheDocument()
  })

  it('shows the connecting indicator only while authenticating an open transport', () => {
    useFlowStore.getState().setActiveChannel?.('ch')
    useFlowStore.getState().setIsLive(true)
    useFlowStore.getState().setConnectionStatus({ connected: true, authenticated: false })
    render(<LiveToggle />)
    expect(screen.getByText('Connecting')).toBeInTheDocument()
    expect(screen.getByText('connecting...')).toBeInTheDocument()
  })

  it('does not claim to be live when the connection is unavailable', () => {
    useFlowStore.getState().setActiveChannel?.('ch')
    useFlowStore.getState().setIsLive(true)
    useFlowStore.getState().setConnectionStatus({ connected: false, authenticated: false, error: 'Disconnected' })
    render(<LiveToggle />)
    expect(screen.getByText('Offline')).toBeInTheDocument()
    expect(screen.getByText('Disconnected')).toBeInTheDocument()
    expect(screen.queryByText('Live')).not.toBeInTheDocument()
  })
})
