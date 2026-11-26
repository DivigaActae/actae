// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, waitFor } from '@testing-library/react'
import FlowPage from './page'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const params = vi.hoisted(() => ({ current: new URLSearchParams() }))

vi.mock('next/navigation', () => ({
  useSearchParams: () => params.current,
}))

vi.mock('@/components/ProtectedLayout', () => ({
  default: ({ children }: any) => <div>{children}</div>,
}))

vi.mock('@/components/flow/EventFlowCanvas', () => ({ default: () => <div data-testid="canvas" /> }))
vi.mock('@/components/flow/ExecutionTimeline', () => ({ default: () => <div data-testid="execution-timeline" /> }))
vi.mock('@/components/flow/ChannelSelector', () => ({ default: () => <div data-testid="channel-sel" /> }))
vi.mock('@/components/flow/LiveToggle', () => ({ default: () => <div data-testid="live-toggle" /> }))
vi.mock('@/components/flow/InspectorPanel', () => ({ default: () => <div data-testid="inspector" /> }))
vi.mock('@/components/flow/FlowHeader', () => ({ default: () => <div data-testid="flow-header" /> }))
vi.mock('@/components/flow/FlowBottomPanel', () => ({ default: () => <div data-testid="bottom-panel" /> }))

const loadChannels = vi.hoisted(() => vi.fn().mockResolvedValue(undefined))
const setActiveChannel = vi.hoisted(() => vi.fn().mockResolvedValue(undefined))

beforeEach(() => {
  resetFlowStore()
  vi.clearAllMocks()
  params.current = new URLSearchParams()
  useFlowStore.setState({ loadChannels, setActiveChannel })
})

describe('FlowPage', () => {
  it('deep-links to the channel from the URL', async () => {
    params.current = new URLSearchParams('?channel=ch-x')
    render(<FlowPage />)
    await waitFor(() => {
      expect(setActiveChannel).toHaveBeenCalledWith('ch-x')
    })
    expect(loadChannels).toHaveBeenCalled()
  })

  it('opens with only the timeline — inspector and activity panel are on demand', () => {
    params.current = new URLSearchParams('?channel=ch-x')
    useFlowStore.setState({ activeChannel: 'ch-x' })
    const { getByTestId, queryByTestId } = render(<FlowPage />)
    expect(getByTestId('execution-timeline')).toBeInTheDocument()
    expect(getByTestId('flow-header')).toBeInTheDocument()
    // Progressive disclosure: the inspector and activity panel start closed.
    expect(queryByTestId('inspector')).not.toBeInTheDocument()
    expect(queryByTestId('bottom-panel')).not.toBeInTheDocument()
  })

  it('opens the inspector only once a step is selected', async () => {
    params.current = new URLSearchParams('?channel=ch-x')
    useFlowStore.setState({ activeChannel: 'ch-x' })
    const { queryByTestId } = render(<FlowPage />)
    expect(queryByTestId('inspector')).not.toBeInTheDocument()
    useFlowStore.getState().setSelectedNode({ type: 'agent.started', cursor: 1, channel: 'ch-x', payload: {} })
    await waitFor(() => {
      expect(queryByTestId('inspector')).toBeInTheDocument()
    })
  })

  it('renders the graph canvas when graph visualization is selected', () => {
    useFlowStore.setState({ activeChannel: 'ch-x', visualizationMode: 'graph' })
    const { getByTestId, queryByTestId } = render(<FlowPage />)
    expect(getByTestId('canvas')).toBeInTheDocument()
    expect(queryByTestId('execution-timeline')).not.toBeInTheDocument()
  })

  it('shows the live feed bar only when live with events', async () => {
    params.current = new URLSearchParams('?channel=ch-x')
    useFlowStore.setState({ isLive: true, liveEvents: [
      { id: 'l1', cursor: 1, type: 'agent.started', channel: 'ch-x', timestamp: new Date(), payload: {} },
    ] })
    const { queryByText } = render(<FlowPage />)
    await waitFor(() => {
      expect(queryByText('click any step to inspect it · new events stream in live')).toBeInTheDocument()
    })
    expect(queryByText(/@1 · agent\.started/)).toBeInTheDocument()
  })

  it('does not show the live bar when not live', () => {
    params.current = new URLSearchParams('?channel=ch-x')
    const { queryByText } = render(<FlowPage />)
    expect(queryByText('click any step to inspect it · new events stream in live')).not.toBeInTheDocument()
  })
})
