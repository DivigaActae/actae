// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, act } from '@testing-library/react'
import InspectorPanel from './InspectorPanel'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const diffChannelStates = vi.hoisted(() => vi.fn())
const getDecisionTrail = vi.hoisted(() => vi.fn())

vi.mock('@/lib/api', () => ({
  diffChannelStates,
  getDecisionTrail,
}))

beforeEach(() => {
  resetFlowStore()
  vi.clearAllMocks()
  diffChannelStates.mockResolvedValue({
    data: {
      left_channel_id: 'root', right_channel_id: 'root-exp-1',
      left: { channel_id: 'root', cursor: 4, state: {} },
      right: { channel_id: 'root-exp-1', cursor: 4, state: {} },
      common: null, left_diverged_at_cursor: null, right_diverged_at_cursor: null, truncated: false, entry_count_total: 0, max_entries: 500, entries: [],
    },
    status: 200,
  })
  getDecisionTrail.mockResolvedValue({
    data: {
      channel_id: 'root', origin_run_id: 'root',
      ancestry: [{ channel_id: 'root', parent_channel_id: null, forked_at_cursor: 0, display_name: null, reason: null }],
      boundary: null, executions: [],
    },
    status: 200,
  })
})

function renderInspector(props: Partial<React.ComponentProps<typeof InspectorPanel>> = {}) {
  const onClose = props.onClose ?? vi.fn()
  const onResize = props.onResize ?? vi.fn()
  const utils = render(
    <InspectorPanel width={320} onClose={onClose} onResize={onResize} liveSlot={props.liveSlot} />
  )
  return { onClose, onResize, ...utils }
}

describe('InspectorPanel', () => {
  it('renders stats with no events', () => {
    renderInspector()
    expect(screen.getByText('Inspector')).toBeInTheDocument()
    expect(screen.getAllByText('0')).toHaveLength(1)
    expect(screen.getAllByText('—')).toHaveLength(3)
    expect(screen.getByText('No')).toBeInTheDocument()
    expect(screen.getByText('No forks from this fork')).toBeInTheDocument()
  })

  it('counts events and computes the latest cursor', () => {
    useFlowStore.setState({
      events: [
        { id: '1', cursor: 1, type: 'a', channel: 'c', timestamp: new Date(), payload: {} },
        { id: '2', cursor: 5, type: 'b', channel: 'c', timestamp: new Date(), payload: {} },
        { id: '3', cursor: 5, type: 'a', channel: 'c', timestamp: new Date(), payload: {}, isState: true },
      ],
    })
    renderInspector()
    expect(screen.getByText('events loaded')).toBeInTheDocument()
    expect(screen.getByText('latest cursor')).toBeInTheDocument()
    expect(screen.getByText('5')).toBeInTheDocument()
  })

  it('shows the state snapshot section when present', () => {
    useFlowStore.setState({ stateSnapshot: { cursor: 7, state: { agent: 'x' } } })
    renderInspector()
    expect(screen.getByText('Latest State @7')).toBeInTheDocument()
    expect(screen.getByText('Yes')).toBeInTheDocument()
  })

  it('lists event type breakdown sorted by count', () => {
    useFlowStore.setState({
      events: [
        { id: '1', cursor: 1, type: 'tool.a', channel: 'c', timestamp: new Date(), payload: {} },
        { id: '2', cursor: 2, type: 'tool.a', channel: 'c', timestamp: new Date(), payload: {} },
        { id: '3', cursor: 3, type: 'agent.b', channel: 'c', timestamp: new Date(), payload: {} },
      ],
    })
    renderInspector()
    expect(screen.getByText('tool.a')).toBeInTheDocument()
    expect(screen.getByText('agent.b')).toBeInTheDocument()
  })

  it('renders the live slot', () => {
    renderInspector({ liveSlot: <span>live-slot</span> })
    expect(screen.getByText('live-slot')).toBeInTheDocument()
  })

  it('closes via the close button', () => {
    const { onClose } = renderInspector()
    fireEvent.click(screen.getByRole('button', { name: 'Close inspector' }))
    expect(onClose).toHaveBeenCalled()
  })

  it('resizes by dragging the handle', () => {
    const { onResize, container } = renderInspector()
    const handle = container.querySelector('.cursor-col-resize')!
    fireEvent.mouseDown(handle, { clientX: 400 })
    fireEvent.mouseMove(document, { clientX: 350 })
    expect(onResize).toHaveBeenCalledWith(370)
    fireEvent.mouseUp(document)
    fireEvent.mouseMove(document, { clientX: 100 })
    expect(onResize).toHaveBeenCalledTimes(1)
  })

  it('shows forked-at info when forkedAtCursor is set', () => {
    useFlowStore.setState({ activeChannel: 'fork', forkedAtCursor: 3 })
    renderInspector()
    expect(screen.getByText('forked at @3')).toBeInTheDocument()
  })

  it('lists forks of the active channel', () => {
    useFlowStore.setState({
      activeChannel: 'root',
      forks: [
        {
          channel_id: 'root-exp-1', display_name: 'Exp 1', parent_channel_id: 'root',
          forked_at_cursor: 4, forked_at: '', origin_run_id: '', reason: null, experiment_metadata: null,
        },
      ],
    })
    renderInspector()
    // "Exp 1" appears in both the Forks card and the State Diff dropdown.
    expect(screen.getAllByText('Exp 1').length).toBeGreaterThanOrEqual(1)
    expect(screen.getByText('Forked at @4')).toBeInTheDocument()
    // The new State Diff + Decision Trail sections render for a channel with forks.
    expect(screen.getByText('State Diff')).toBeInTheDocument()
    expect(screen.getByText('Decision Trail')).toBeInTheDocument()
  })

  it('renders the selected event detail and switches views', () => {
    useFlowStore.setState({
      selectedNode: {
        type: 'tool.llm',
        cursor: 2,
        payload: { input: { q: 'hi' }, output: { a: 1 }, model: 'gpt-4' },
      },
    })
    renderInspector()
    expect(screen.getByText('Selected Event @2')).toBeInTheDocument()
    expect(screen.getByText('Input')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'JSON' }))
    expect(screen.getByText('"q":')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Text' }))
    expect(screen.getByText('Input')).toBeInTheDocument()
  })

  it('renders scalar input and output as text instead of character indexes', () => {
    useFlowStore.setState({
      selectedNode: {
        type: 'step.3.gtm',
        cursor: 4,
        payload: { input: 'Here are 3 concrete needs', output: '## Launch Plan\nOffline mode' },
      },
    })
    renderInspector()
    expect(screen.getByText('Here are 3 concrete needs')).toBeInTheDocument()
    expect(screen.getByText(/## Launch Plan/)).toBeInTheDocument()
    expect(screen.queryByText('0:')).not.toBeInTheDocument()
  })

  it('shows context delta and cumulative context', () => {
    useFlowStore.setState({
      selectedNode: {
        type: 'step',
        cursor: 2,
        payload: { context: { topic: 'AI' } },
        cumulativeContext: { topic: 'AI', depth: 3 },
      },
    })
    renderInspector()
    expect(screen.getByText('Context Delta')).toBeInTheDocument()
    expect(screen.getByText('Full Context @2')).toBeInTheDocument()
  })

  it('filters the selected event payload by the visible fields selection', () => {
    useFlowStore.setState({
      visibleFields: new Set(['input']),
      selectedNode: {
        type: 'tool.llm',
        cursor: 2,
        payload: { input: { q: 'hi' }, output: { a: 1 }, model: 'gpt-4' },
      },
    })
    renderInspector()
    expect(screen.getByText('Input')).toBeInTheDocument()
    expect(screen.queryByText('Output')).not.toBeInTheDocument()
    expect(screen.queryByText('gpt-4')).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'JSON' }))
    expect(screen.getByText('"q":')).toBeInTheDocument()
    expect(screen.queryByText('"model":')).not.toBeInTheDocument()
  })

  it('keeps context sections visible even when the payload filter is active', () => {
    useFlowStore.setState({
      visibleFields: new Set(['context']),
      selectedNode: {
        type: 'step',
        cursor: 2,
        payload: { context: { topic: 'AI' } },
        cumulativeContext: { topic: 'AI', depth: 3 },
      },
    })
    renderInspector()
    expect(screen.getByText('Context Delta')).toBeInTheDocument()
    expect(screen.getByText('Full Context @2')).toBeInTheDocument()
  })
})
