// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import ExecutionTimeline from './ExecutionTimeline'
import { useFlowStore, type DashboardEvent } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const events: DashboardEvent[] = [
  { id: 'e1', cursor: 1, channel: 'support', timestamp: '2026-01-01T10:00:00.000Z', type: 'customer_context.loaded', payload: { title: 'Customer context loaded', model: 'gpt-5' } },
  { id: 'e2', cursor: 2, channel: 'support', timestamp: '2026-01-01T10:00:01.200Z', type: 'tool.completed', payload: { tool: 'refund_policy', status: 'verified' } },
  { id: 'e3', cursor: 3, channel: 'support', timestamp: '2026-01-01T10:00:04.100Z', type: 'agent.crashed', payload: { error: 'connection_lost' } },
  { id: 'e4', cursor: 4, channel: 'support', timestamp: '2026-01-01T10:00:04.500Z', type: 'execution.resumed', payload: { from_cursor: 3 } },
]

beforeEach(() => {
  resetFlowStore()
  Object.defineProperty(HTMLElement.prototype, 'scrollTo', { configurable: true, value: vi.fn() })
})

describe('ExecutionTimeline', () => {
  it('renders real events chronologically with elapsed time and cursor metadata', () => {
    useFlowStore.setState({ activeChannel: 'support', events })
    render(<ExecutionTimeline />)
    expect(screen.getByText('Customer context loaded')).toBeInTheDocument()
    expect(screen.getByText('00:01.2')).toBeInTheDocument()
    expect(screen.getByText('CURSOR 0004')).toBeInTheDocument()
    expect(screen.getAllByRole('listitem')).toHaveLength(4)
  })

  it('turns failures and recoveries into explicit execution boundaries', () => {
    useFlowStore.setState({ activeChannel: 'support', events })
    render(<ExecutionTimeline />)
    expect(screen.getByText('PROCESS INTERRUPTED')).toBeInTheDocument()
    expect(screen.getAllByText('connection_lost')).toHaveLength(2)
    expect(screen.getByText('ACTAE RECOVERY')).toBeInTheDocument()
    expect(screen.getByText('No committed steps rerun.')).toBeInTheDocument()
  })

  it('selects an event and opens the existing inspector contract', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'support', events })
    render(<ExecutionTimeline />)
    await user.click(screen.getByRole('button', { name: /Customer context loaded/i }))
    expect(useFlowStore.getState().selectedNode).toMatchObject({ cursor: 1, channel: 'support', type: 'customer_context.loaded' })
    expect(useFlowStore.getState().inspectorOpen).toBe(true)
  })

  it('applies event-type and payload-field filters', () => {
    useFlowStore.setState({ activeChannel: 'support', events, visibleEventTypes: new Set(['tool.completed']), visibleFields: new Set(['tool']) })
    render(<ExecutionTimeline />)
    expect(screen.getAllByRole('listitem')).toHaveLength(1)
    expect(screen.getAllByText('refund_policy').length).toBeGreaterThan(0)
    expect(screen.queryByText('verified')).not.toBeInTheDocument()
  })

  it('marks inherited history and newly streamed events', () => {
    const inherited = { ...events[0], isInherited: true, forkedFrom: 'root' }
    const live = { ...events[1], id: 'live-event' }
    useFlowStore.setState({ activeChannel: 'support', events: [inherited, live], liveEvents: [live], isLive: true, connection: { connected: true, authenticated: true, error: null } })
    render(<ExecutionTimeline />)
    expect(screen.getByText('INHERITED')).toBeInTheDocument()
    expect(screen.getByText('JUST NOW')).toBeInTheDocument()
    expect(screen.getByText('Listening for the next event')).toBeInTheDocument()
  })

  it('reports an unavailable live connection as offline instead of connecting', () => {
    useFlowStore.setState({ activeChannel: 'support', events, isLive: true, connection: { connected: false, authenticated: false, error: null } })
    render(<ExecutionTimeline />)
    expect(screen.getByText('OFFLINE')).toBeInTheDocument()
    expect(screen.getByText('Live connection unavailable')).toBeInTheDocument()
    expect(screen.queryByText('Connecting to live events')).not.toBeInTheDocument()
  })

  it('provides useful empty, filtered-empty, loading, and error recovery states', async () => {
    const { rerender } = render(<ExecutionTimeline />)
    expect(screen.getByText('No execution selected')).toBeInTheDocument()
    act(() => useFlowStore.setState({ activeChannel: 'support', loading: true }))
    rerender(<ExecutionTimeline />)
    expect(screen.getByText('Loading execution history…')).toBeInTheDocument()
    act(() => useFlowStore.setState({ loading: false, error: 'network unavailable' }))
    rerender(<ExecutionTimeline />)
    expect(screen.getByText('Timeline unavailable')).toBeInTheDocument()
    act(() => useFlowStore.setState({ error: null, events, visibleEventTypes: new Set(['missing']) }))
    rerender(<ExecutionTimeline />)
    expect(screen.getByText('No events match the current filter')).toBeInTheDocument()
  })
})
