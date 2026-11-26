// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { ForkTree } from './ForkTree'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const getForkTree = vi.hoisted(() => vi.fn())
const loadForks = vi.hoisted(() => vi.fn())
vi.mock('@/lib/api', () => ({ getForkTree, getPreferences: async () => ({ status: 200, data: { prefs: {} } }), putPreferences: async () => ({ status: 200 }) }))
beforeEach(() => {
  resetFlowStore()
  vi.clearAllMocks()
  useFlowStore.setState({ loadForks })
  getForkTree.mockResolvedValue({ data: null, status: 200 })
  loadForks.mockResolvedValue(undefined)
})

const tree = {
  channel_id: 'root',
  display_name: 'Root run',
  reason: null,
  event_count: 10,
  latest_cursor: 10,
  forked_at_cursor: 0,
  children: [
    {
      channel_id: 'root-exp-a',
      display_name: 'Exp A',
      reason: 'prompt test',
      event_count: 4,
      latest_cursor: 4,
      forked_at_cursor: 3,
      children: [],
    },
  ],
}

describe('ForkTree', () => {
  it('renders nothing without an active channel', () => {
    const { container } = render(<ForkTree />)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows the loading state', () => {
    useFlowStore.setState({ activeChannel: 'root' })
    getForkTree.mockReturnValue(new Promise(() => {}))
    render(<ForkTree />)
    expect(screen.getByText('Loading execution tree...')).toBeInTheDocument()
  })

  it('shows a no-tree message when the API returns nothing', async () => {
    useFlowStore.setState({ activeChannel: 'root' })
    render(<ForkTree />)
    await waitFor(() => {
      expect(screen.getByText('No execution tree available')).toBeInTheDocument()
    })
  })

  it('renders the tree with root and children', async () => {
    useFlowStore.setState({ activeChannel: 'root-exp-a' })
    getForkTree.mockResolvedValue({ data: tree, status: 200 })
    render(<ForkTree />)
    await waitFor(() => {
      expect(screen.getByText('Root run')).toBeInTheDocument()
    })
    expect(screen.getByText('Exp A')).toBeInTheDocument()
    expect(screen.getByText('prompt test')).toBeInTheDocument()
    expect(screen.getByText(/10 events/)).toBeInTheDocument()
    expect(screen.getByText(/forked @3/)).toBeInTheDocument()
  })

  it('auto-expands the path to the active channel', async () => {
    useFlowStore.setState({ activeChannel: 'root-exp-a' })
    getForkTree.mockResolvedValue({ data: tree, status: 200 })
    render(<ForkTree />)
    await waitFor(() => {
      expect(screen.getByText('Exp A')).toBeInTheDocument()
    })
    // Child is visible, meaning the tree auto-expanded along the active path.
    expect(screen.getByText('prompt test')).toBeInTheDocument()
  })

  it('selects a channel when a node is clicked', async () => {
    const user = userEvent.setup()
    const setActiveChannel = vi.fn()
    useFlowStore.setState({ activeChannel: 'root', setActiveChannel })
    getForkTree.mockResolvedValue({ data: tree, status: 200 })
    render(<ForkTree />)
    await waitFor(() => {
      expect(screen.getByText('Root run')).toBeInTheDocument()
    })
    await user.click(screen.getByText('Root run'))
    expect(setActiveChannel).toHaveBeenCalledWith('root')
  })

  it('toggles children via the node chevron', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'root' })
    getForkTree.mockResolvedValue({ data: tree, status: 200 })
    const { container } = render(<ForkTree />)
    await waitFor(() => {
      expect(screen.getByText('Exp A')).toBeInTheDocument()
    })
    const chevron = container.querySelector('button.rotate-90')!
    await user.click(chevron)
    expect(screen.queryByText('Exp A')).not.toBeInTheDocument()
    await user.click(chevron)
    expect(screen.getByText('Exp A')).toBeInTheDocument()
  })

  it('expand all reveals nested grandchildren; collapse all hides them', async () => {
    const user = userEvent.setup()
    useFlowStore.setState({ activeChannel: 'root' })
    const deep = {
      ...tree,
      children: [
        {
          ...tree.children[0],
          children: [{ channel_id: 'root-exp-a-sub', display_name: 'Sub', reason: null, event_count: 2, latest_cursor: 2, forked_at_cursor: 1, children: [] }],
        },
      ],
    }
    getForkTree.mockResolvedValue({ data: deep, status: 200 })
    render(<ForkTree />)
    await waitFor(() => {
      expect(screen.getByText('Exp A')).toBeInTheDocument()
    })
    expect(screen.queryByText('Sub')).not.toBeInTheDocument()
    await user.click(screen.getByTitle('Expand all'))
    expect(screen.getByText('Sub')).toBeInTheDocument()
    await user.click(screen.getByTitle('Collapse all'))
    expect(screen.queryByText('Sub')).not.toBeInTheDocument()
    // The root node stays expanded by default, so its direct children remain.
    expect(screen.getByText('Exp A')).toBeInTheDocument()
  })
})
