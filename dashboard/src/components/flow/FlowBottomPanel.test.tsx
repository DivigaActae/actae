// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import FlowBottomPanel from './FlowBottomPanel'
import { useFlowStore, type DashboardEvent } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const push = vi.hoisted(() => vi.fn())
vi.mock('next/navigation', () => ({ useRouter: () => ({ push }) }))
vi.mock('@/components/flow/CompareView', () => ({ CompareView: () => <div>Fork comparison workspace</div> }))

beforeEach(() => {
  resetFlowStore()
  vi.clearAllMocks()
})

function evt(cursor: number, type: string, payload: Record<string, unknown> = {}, extra: Partial<DashboardEvent> = {}): DashboardEvent {
  return {
    id: `e${cursor}`,
    cursor,
    type,
    channel: 'root',
    timestamp: '2026-07-25T09:00:00Z',
    payload,
    ...extra,
  }
}

function withEvents(events: DashboardEvent[]) {
  useFlowStore.setState({ events, activeChannel: 'root' })
}

describe('FlowBottomPanel', () => {
  it('shows the four tabs with timeline active by default', () => {
    render(<FlowBottomPanel />)
    expect(screen.getByRole('button', { name: 'Timeline' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Event Log' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Metadata' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Compare' })).toBeInTheDocument()
  })

  it('opens the fork comparison workspace', async () => {
    const user = userEvent.setup()
    render(<FlowBottomPanel />)
    await user.click(screen.getByRole('button', { name: 'Compare' }))
    expect(screen.getByText('Fork comparison workspace')).toBeInTheDocument()
  })

  it('shows an empty state with no events', () => {
    render(<FlowBottomPanel />)
    expect(screen.getByText('No events recorded yet')).toBeInTheDocument()
  })

  it('lists events in the timeline with derived step, agent and duration', () => {
    withEvents([
      evt(1, 'tool.web_search', { elapsed_ms: 250 }),
      evt(2, 'agent.completed', { input: { topic: 'AI' } }),
    ])
    render(<FlowBottomPanel />)
    expect(screen.getAllByText('tool.web_search').length).toBeGreaterThanOrEqual(1)
    expect(screen.getByText('250ms')).toBeInTheDocument()
    expect(screen.getAllByText('agent.completed').length).toBeGreaterThanOrEqual(1)
    expect(screen.getByText(/topic.*AI/)).toBeInTheDocument()
  })

  it('derives agent names from event types', () => {
    withEvents([evt(1, 'writer.draft', {})])
    render(<FlowBottomPanel />)
    expect(screen.getByText('writer')).toBeInTheDocument()
  })

  it('excludes state events from the timeline', () => {
    withEvents([evt(1, 'step.a', {}), evt(1, 'state.saved', {}, { isState: true })])
    render(<FlowBottomPanel />)
    expect(screen.queryByText('state.saved')).not.toBeInTheDocument()
  })

  it('selects an event on row click', async () => {
    const user = userEvent.setup()
    withEvents([evt(1, 'step.a', { x: 1 })])
    render(<FlowBottomPanel />)
    await user.click(screen.getAllByText('step.a')[0])
    expect(useFlowStore.getState().selectedNode).toMatchObject({ type: 'step.a', cursor: 1 })
  })

  it('deep-links to the events page from the event log', async () => {
    const user = userEvent.setup()
    withEvents([evt(5, 'step.b', {})])
    render(<FlowBottomPanel />)
    await user.click(screen.getByRole('button', { name: 'Event Log' }))
    await user.click(screen.getByTitle('Open in Events page'))
    expect(push).toHaveBeenCalledWith('/dashboard/events/?channel=root&cursor=5')
  })

  it('shows the metadata tab with the selected event payload', async () => {
    const user = userEvent.setup()
    withEvents([evt(1, 'step.a', { model: 'gpt-4' })])
    render(<FlowBottomPanel />)
    await user.click(screen.getByRole('button', { name: 'Metadata' }))
    expect(screen.getByText(/Select an event to view its metadata/)).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Timeline' }))
    await user.click(screen.getAllByText('step.a')[0])
    await user.click(screen.getByRole('button', { name: 'Metadata' }))
    expect(screen.getByText('"model":')).toBeInTheDocument()
  })

  it('shows an empty log state', async () => {
    const user = userEvent.setup()
    render(<FlowBottomPanel />)
    await user.click(screen.getByRole('button', { name: 'Event Log' }))
    expect(screen.getByText('No events to display')).toBeInTheDocument()
  })

  it('highlights the row matching a canvas node click (selection carries channel)', () => {
    withEvents([
      evt(1, 'agent.started'),
      evt(2, 'tool.llm'),
      evt(3, 'agent.completed'),
    ])
    // Simulate a canvas node click: store selection with channel.
    useFlowStore.getState().setSelectedNode({ type: 'tool.llm', cursor: 2, channel: 'root' })
    render(<FlowBottomPanel />)
    const rows = screen.getAllByRole('row')
    const selected = rows.filter(r => r.className.includes('bg-actae-950'))
    expect(selected).toHaveLength(1)
    expect(selected[0]).toHaveTextContent('tool.llm')
    expect(selected[0]).toHaveTextContent('2')
  })

  it('disambiguates same-cursor same-type events across channels', () => {
    withEvents([
      evt(5, 'tool.llm', { input: 'parent-payload' }, { channel: 'parent', isInherited: true, forkedFrom: 'root' }),
      evt(5, 'tool.llm', { input: 'child-payload' }, { channel: 'child' }),
    ])
    useFlowStore.getState().setSelectedNode({ type: 'tool.llm', cursor: 5, channel: 'parent' })
    render(<FlowBottomPanel />)
    const rows = screen.getAllByRole('row')
    const selected = rows.filter(r => r.className.includes('bg-actae-950'))
    expect(selected).toHaveLength(1)
    expect(selected[0]).toHaveTextContent('parent-payload')
    expect(selected[0]).not.toHaveTextContent('child-payload')
  })

  it('scrolls the selected row into view when the selection changes', () => {
    const scrollIntoView = vi.fn()
    Element.prototype.scrollIntoView = scrollIntoView
    withEvents(Array.from({ length: 50 }, (_, i) => evt(i + 1, 'agent.started')))
    render(<FlowBottomPanel />)
    // A canvas node click selects a row far down the list.
    act(() => {
      useFlowStore.getState().setSelectedNode({ type: 'agent.started', cursor: 40, channel: 'root' })
    })
    expect(scrollIntoView).toHaveBeenCalled()
  })

  it('row clicks forward the channel so the canvas can highlight the node', async () => {
    const user = userEvent.setup()
    withEvents([evt(7, 'step.a', { x: 1 })])
    render(<FlowBottomPanel />)
    await user.click(screen.getAllByText('step.a')[0])
    expect(useFlowStore.getState().selectedNode).toMatchObject({ type: 'step.a', cursor: 7, channel: 'root' })
  })

  it('hides non-selected payload fields in the event log', async () => {
    const user = userEvent.setup()
    withEvents([evt(1, 'step.a', { model: 'gpt-4', tokens: 42 })])
    useFlowStore.getState().setVisibleFields(new Set(['tokens']))
    render(<FlowBottomPanel />)
    await user.click(screen.getByRole('button', { name: 'Event Log' }))
    expect(screen.getByText(/"tokens": 42/)).toBeInTheDocument()
    expect(screen.queryByText(/"model":/)).not.toBeInTheDocument()
  })

  it('filters the timeline input/output and duration cells', async () => {
    const user = userEvent.setup()
    withEvents([
      evt(1, 'tool.web_search', { elapsed_ms: 250, input: { topic: 'AI' } }),
    ])
    useFlowStore.getState().setVisibleFields(new Set(['elapsed_ms']))
    render(<FlowBottomPanel />)
    expect(screen.getByText('250ms')).toBeInTheDocument()
    expect(screen.queryByText(/topic.*AI/)).not.toBeInTheDocument()
    expect(screen.getByText(/elapsed_ms/)).toBeInTheDocument()
  })

  it('filters the metadata tab payload by visible fields', async () => {
    const user = userEvent.setup()
    withEvents([evt(1, 'step.a', { model: 'gpt-4', status: 'ok' })])
    useFlowStore.getState().setVisibleFields(new Set(['status']))
    render(<FlowBottomPanel />)
    await user.click(screen.getByRole('button', { name: 'Timeline' }))
    await user.click(screen.getAllByText('step.a')[0])
    await user.click(screen.getByRole('button', { name: 'Metadata' }))
    expect(screen.getByText('"status":')).toBeInTheDocument()
    expect(screen.queryByText('"model":')).not.toBeInTheDocument()
  })

  it('leaves state snapshots unfiltered in the metadata tab', async () => {
    const user = userEvent.setup()
    withEvents([evt(1, 'step.a', {}, { state: { saved: true } })])
    useFlowStore.getState().setVisibleFields(new Set(['model']))
    render(<FlowBottomPanel />)
    await user.click(screen.getByRole('button', { name: 'Timeline' }))
    await user.click(screen.getAllByText('step.a')[0])
    await user.click(screen.getByRole('button', { name: 'Metadata' }))
    expect(screen.getByText('"saved":')).toBeInTheDocument()
  })
})
