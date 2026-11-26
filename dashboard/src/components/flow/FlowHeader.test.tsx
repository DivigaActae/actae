// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import FlowHeader from './FlowHeader'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const getChannelMetadata = vi.hoisted(() => vi.fn())
const buildChannelExport = vi.hoisted(() => vi.fn())
const downloadJsonFile = vi.hoisted(() => vi.fn())
const copyToClipboard = vi.hoisted(() => vi.fn())
const updateChannelMetadata = vi.hoisted(() => vi.fn())
vi.mock('@/lib/api', () => ({
  getChannelMetadata,
  buildChannelExport,
  downloadJsonFile,
  copyToClipboard,
  updateChannelMetadata,
  getPreferences: async () => ({ status: 200, data: { prefs: {} } }),
  putPreferences: async () => ({ status: 200 }),
}))
beforeEach(() => {
  resetFlowStore()
  useFlowStore.setState({ visualizationMode: 'graph' })
  vi.clearAllMocks()
  getChannelMetadata.mockResolvedValue({ data: null, status: 404 })
  buildChannelExport.mockResolvedValue({ status: 200, data: { schema: 'actae.channel_export.v1', channel_id: 'ch', event_count: 0, events: [] } })
  copyToClipboard.mockResolvedValue(undefined)
  updateChannelMetadata.mockResolvedValue({ status: 200, data: {} })
})

describe('FlowHeader', () => {
  it('shows Untitled execution when no channel is active', () => {
    render(<FlowHeader />)
    expect(screen.getByText('Untitled execution')).toBeInTheDocument()
    expect(screen.getByText('Idle')).toBeInTheDocument()
  })

  it('uses the channel id as the title', () => {
    useFlowStore.setState({ activeChannel: 'agent-research-run' })
    render(<FlowHeader />)
    expect(screen.getByText('agent-research-run')).toBeInTheDocument()
  })

  it('prefers the display name from metadata', async () => {
    useFlowStore.setState({ activeChannel: 'ch' })
    getChannelMetadata.mockResolvedValue({
      data: { channel_id: 'ch', display_name: 'My Experiment', reason: null, forked_at_cursor: 0 },
      status: 200,
    })
    render(<FlowHeader />)
    await waitFor(() => {
      expect(screen.getByText('My Experiment')).toBeInTheDocument()
    })
  })

  it('shows a completed badge when the last event completes', () => {
    useFlowStore.setState({
      activeChannel: 'ch',
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} },
        { cursor: 2, type: 'agent.completed', channel: 'ch', timestamp: new Date(Date.now() + 5000), payload: {} },
      ],
    })
    render(<FlowHeader />)
    expect(screen.getByText('Completed')).toBeInTheDocument()
  })

  it('shows a failed badge when the last event errors', () => {
    useFlowStore.setState({
      activeChannel: 'ch',
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} },
        { cursor: 2, type: 'agent.crashed', channel: 'ch', timestamp: new Date(), payload: {} },
      ],
    })
    render(<FlowHeader />)
    expect(screen.getByText('Failed')).toBeInTheDocument()
  })

  it('shows an active badge only for a recent event on an authenticated live connection', () => {
    useFlowStore.setState({
      activeChannel: 'ch',
      isLive: true,
      connection: { connected: true, authenticated: true, error: null },
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} },
        { cursor: 2, type: 'tool.llm_call', channel: 'ch', timestamp: new Date(), payload: {} },
      ],
    })
    render(<FlowHeader />)
    expect(screen.getByText('Active')).toBeInTheDocument()
  })

  it('shows offline rather than running when the live transport is unavailable', () => {
    useFlowStore.setState({
      activeChannel: 'ch',
      isLive: true,
      connection: { connected: false, authenticated: false, error: 'Disconnected' },
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} },
      ],
    })
    render(<FlowHeader />)
    expect(screen.getByText('Offline')).toBeInTheDocument()
    expect(screen.queryByText('Running')).not.toBeInTheDocument()
  })

  it('computes duration from first and last event timestamps', () => {
    useFlowStore.setState({
      activeChannel: 'ch',
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: '2026-07-25T09:00:00Z', payload: {} },
        { cursor: 2, type: 'agent.completed', channel: 'ch', timestamp: '2026-07-25T09:01:30Z', payload: {} },
      ],
    })
    render(<FlowHeader />)
    expect(screen.getByText('1m 30s')).toBeInTheDocument()
  })

  it('renders a channel selector node when provided', () => {
    useFlowStore.setState({ activeChannel: 'ch' })
    render(<FlowHeader channelSelector={<span>selector</span>} />)
    expect(screen.getByText('selector')).toBeInTheDocument()
  })

  it('renders a real breadcrumb with the parent run', async () => {
    useFlowStore.setState({ activeChannel: 'ch' })
    getChannelMetadata.mockResolvedValue({
      data: { channel_id: 'ch', display_name: 'Child run', parent_channel_id: 'parent-1', reason: null, forked_at_cursor: 1 },
      status: 200,
    })
    render(<FlowHeader />)
    await waitFor(() => {
      expect(screen.getByText('Child run')).toBeInTheDocument()
    })
    expect(screen.getByRole('link', { name: 'Runs' })).toHaveAttribute('href', '/dashboard/runs/')
    expect(screen.getByRole('link', { name: 'parent-1' })).toHaveAttribute('href', '/dashboard/flow/?channel=parent-1')
  })

  it('renames the run via updateChannelMetadata', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'ch' })
    getChannelMetadata.mockResolvedValue({
      data: { channel_id: 'ch', display_name: 'Old name', reason: null, forked_at_cursor: 0 },
      status: 200,
    })
    render(<FlowHeader />)
    await waitFor(() => {
      expect(screen.getByText('Old name')).toBeInTheDocument()
    })
    await user.click(screen.getByRole('button', { name: 'Rename run' }))
    const input = screen.getByLabelText('Run name')
    await user.clear(input)
    await user.type(input, 'New name{Enter}')
    await waitFor(() => {
      expect(updateChannelMetadata).toHaveBeenCalledWith({ channel_id: 'ch', display_name: 'New name' })
    })
    expect(screen.getByText('New name')).toBeInTheDocument()
  })

  it('surfaces a rename failure instead of failing silently', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'ch' })
    updateChannelMetadata.mockResolvedValue({ status: 400, error: 'name taken' })
    render(<FlowHeader />)
    await user.click(screen.getByRole('button', { name: 'Rename run' }))
    const input = screen.getByLabelText('Run name')
    await user.clear(input)
    await user.type(input, 'Taken{Enter}')
    await waitFor(() => {
      expect(screen.getByText(/Rename failed: name taken/)).toBeInTheDocument()
    })
  })

  it('calls the canvas controller from the overflow zoom controls', async () => {
    const user = userEvent.setup()
    const zoomIn = vi.fn()
    const zoomOut = vi.fn()
    const fitView = vi.fn()
    useFlowStore.getState().setCanvasController({ zoomIn, zoomOut, fitView })
    render(<FlowHeader />)
    await user.click(screen.getByTitle('More options'))
    await user.click(screen.getByTitle('Zoom out'))
    expect(zoomOut).toHaveBeenCalled()
    await user.click(screen.getByTitle('Zoom in'))
    expect(zoomIn).toHaveBeenCalled()
    await user.click(screen.getByTitle('Fit view'))
    expect(fitView).toHaveBeenCalled()
  })

  it('renders the type filter with counts derived from the events', () => {
    useFlowStore.setState({
      activeChannel: 'ch',
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} },
        { cursor: 2, type: 'tool.llm', channel: 'ch', timestamp: new Date(), payload: {} },
        { cursor: 3, type: 'tool.llm', channel: 'ch', timestamp: new Date(), payload: {} },
      ],
    })
    render(<FlowHeader />)
    expect(screen.getByText('Types: all 2')).toBeInTheDocument()
  })

  it('renders the fields filter with payload-key counts', () => {
    useFlowStore.setState({
      activeChannel: 'ch',
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: { model: 'gpt-4', tokens: 1 } },
        { cursor: 2, type: 'tool.llm', channel: 'ch', timestamp: new Date(), payload: { model: 'gpt-4' } },
      ],
    })
    render(<FlowHeader />)
    expect(screen.getByText('Fields: all 2')).toBeInTheDocument()
  })

  it('toggles the store field filter from the Fields dropdown', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({
      activeChannel: 'ch',
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: { model: 'gpt-4', tokens: 1 } },
        { cursor: 2, type: 'tool.llm', channel: 'ch', timestamp: new Date(), payload: { model: 'gpt-4' } },
      ],
    })
    render(<FlowHeader />)
    await user.click(screen.getByText('Fields: all 2'))
    const listbox = screen.getByRole('listbox')
    const checkbox = Array.from(listbox.querySelectorAll('input[type="checkbox"]'))
      .find((el) => el.parentElement?.textContent?.includes('tokens')) as HTMLInputElement
    await user.click(checkbox)
    expect(useFlowStore.getState().visibleFields).toEqual(new Set(['model']))
    expect(screen.getByText('Fields: 1/2')).toBeInTheDocument()
  })

  it('toggles the store filter from the type dropdown', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({
      activeChannel: 'ch',
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} },
        { cursor: 2, type: 'tool.llm', channel: 'ch', timestamp: new Date(), payload: {} },
      ],
    })
    render(<FlowHeader />)
    await user.click(screen.getByText('Types: all 2'))
    const listbox = screen.getByRole('listbox')
    const checkbox = Array.from(listbox.querySelectorAll('input[type="checkbox"]'))
      .find((el) => el.parentElement?.textContent?.includes('tool.llm')) as HTMLInputElement
    await user.click(checkbox)
    expect(useFlowStore.getState().visibleEventTypes).toEqual(new Set(['agent.started']))
    expect(screen.getByText('Types: 1/2')).toBeInTheDocument()
  })

  it('shows the filter as active-styled when a subset is selected', () => {
    useFlowStore.setState({
      activeChannel: 'ch',
      visibleEventTypes: new Set(['agent.started']),
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} },
        { cursor: 2, type: 'tool.llm', channel: 'ch', timestamp: new Date(), payload: {} },
      ],
    })
    render(<FlowHeader />)
    expect(screen.getByText('Types: 1/2')).toBeInTheDocument()
    expect(screen.getByTitle('Filter is active')).toBeInTheDocument()
  })

  it('keeps the header above the flow canvas so the filter popover stays clickable', () => {
    const { container } = render(<FlowHeader />)
    // The canvas is positioned after the header in DOM order; the header must
    // create its own stacking context above it (z-40) or the dropdown is
    // painted under the canvas and pointer events never reach it.
    const root = container.firstElementChild as HTMLElement
    expect(root.className).toContain('relative')
    expect(root.className).toContain('z-40')
  })

  it('shows the adaptive grid column count in the layout menu', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({
      activeChannel: 'ch',
      layoutMode: 'grid',
      events: Array.from({ length: 100 }, (_, i) => ({
        cursor: i + 1,
        type: 'agent.started',
        channel: 'ch',
        timestamp: new Date(),
        payload: {},
      })),
    })
    render(<FlowHeader />)
    await user.click(screen.getByTitle('More options'))
    expect(screen.getByText('8 cols')).toBeInTheDocument()
  })

  it('updates the column count when the type filter narrows the grid', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({
      activeChannel: 'ch',
      layoutMode: 'grid',
      visibleEventTypes: new Set(['agent.started']),
      events: Array.from({ length: 100 }, (_, i) => ({
        cursor: i + 1,
        type: i < 20 ? 'agent.started' : 'tool.llm',
        channel: 'ch',
        timestamp: new Date(),
        payload: {},
      })),
    })
    render(<FlowHeader />)
    await user.click(screen.getByTitle('More options'))
    // 20 visible nodes → 5 columns.
    expect(screen.getByText('5 cols')).toBeInTheDocument()
  })

  it('renders the Export JSON and Copy JSON buttons between the replay controls and the more-options button', () => {
    useFlowStore.setState({ activeChannel: 'ch' })
    render(<FlowHeader />)
    const exportButton = screen.getByRole('button', { name: /export json/i })
    const copyButton = screen.getByRole('button', { name: /copy json/i })
    const replayGroup = screen.getByText('Show from event')
    const moreButton = screen.getByTitle('More options')
    expect(replayGroup.compareDocumentPosition(exportButton) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(exportButton.compareDocumentPosition(copyButton) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(copyButton.compareDocumentPosition(moreButton) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  })

  it('exports the channel as a JSON file on click, applying the active filters', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({
      activeChannel: 'ch',
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: { model: 'gpt-4', tokens: 1 } },
        { cursor: 2, type: 'tool.llm', channel: 'ch', timestamp: new Date(), payload: { model: 'gpt-4' } },
      ],
      visibleEventTypes: new Set(['tool.llm']),
      visibleFields: new Set(['model']),
    })
    buildChannelExport.mockResolvedValue({
      status: 200,
      data: { schema: 'actae.channel_export.v1', channel_id: 'ch', event_count: 3, events: [] },
    })
    render(<FlowHeader />)
    await user.click(screen.getByRole('button', { name: /export json/i }))
    expect(buildChannelExport).toHaveBeenCalledWith(
      'ch',
      expect.objectContaining({
        visibleEventTypes: new Set(['tool.llm']),
        visibleFields: new Set(['model']),
        events: expect.any(Array),
      })
    )
    await waitFor(() => {
      expect(downloadJsonFile).toHaveBeenCalledWith('ch.json', expect.objectContaining({ channel_id: 'ch', event_count: 3 }))
    })
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /exported/i })).toBeInTheDocument()
    })
  })

  it('copies the filtered export to the clipboard on click', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'ch', visibleEventTypes: new Set(['tool.llm']) })
    buildChannelExport.mockResolvedValue({
      status: 200,
      data: { schema: 'actae.channel_export.v1', channel_id: 'ch', event_count: 2, events: [] },
    })
    render(<FlowHeader />)
    await user.click(screen.getByRole('button', { name: /copy json/i }))
    await waitFor(() => {
      expect(copyToClipboard).toHaveBeenCalledWith(expect.stringContaining('"schema": "actae.channel_export.v1"'))
    })
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /copied/i })).toBeInTheDocument()
    })
    expect(downloadJsonFile).not.toHaveBeenCalled()
  })

  it('surfaces the error when the export fails', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'ch' })
    buildChannelExport.mockResolvedValue({ status: 0, error: 'pg is down' })
    render(<FlowHeader />)
    await user.click(screen.getByRole('button', { name: /export json/i }))
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /export failed/i })).toBeInTheDocument()
    })
    expect(screen.getByTitle('pg is down')).toBeInTheDocument()
  })

  it('surfaces the error when the clipboard write fails', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'ch' })
    copyToClipboard.mockRejectedValue(new Error('clipboard blocked'))
    render(<FlowHeader />)
    await user.click(screen.getByRole('button', { name: /copy json/i }))
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /copy failed/i })).toBeInTheDocument()
    })
    expect(screen.getByTitle('clipboard blocked')).toBeInTheDocument()
  })

  it('disables the export buttons when no channel is active', () => {
    render(<FlowHeader />)
    expect(screen.getByRole('button', { name: /export json/i })).toBeDisabled()
    expect(screen.getByRole('button', { name: /copy json/i })).toBeDisabled()
  })
})
