// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import EventNode from './EventNode'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

vi.mock('@xyflow/react', () => ({
  Handle: () => null,
  Position: { Left: 'left', Right: 'right', Top: 'top', Bottom: 'bottom' },
}))

beforeEach(() => {
  resetFlowStore()
})

function nodeData(overrides: Record<string, unknown> = {}) {
  return {
    type: 'agent.started',
    cursor: 7,
    payload: { model: 'gpt-4' },
    timestamp: '2026-07-25T09:00:00Z',
    onClick: vi.fn(),
    ...overrides,
  }
}

describe('EventNode', () => {
  it('renders type, cursor and payload summary rows', () => {
    render(<EventNode data={nodeData({ payload: { model: 'gpt-4', tokens: 900 } })} />)
    expect(screen.getByText('agent.started')).toBeInTheDocument()
    expect(screen.getByText('@7')).toBeInTheDocument()
    expect(screen.getByText('gpt-4')).toBeInTheDocument()
    expect(screen.getByText('900')).toBeInTheDocument()
  })

  it('renders fallback key=value rows for unknown scalar payloads', () => {
    render(<EventNode data={nodeData({ payload: { region: 'eu-west' } })} />)
    expect(screen.getByText('region=eu-west')).toBeInTheDocument()
  })

  it('shows a no-details placeholder for empty payloads', () => {
    render(<EventNode data={nodeData({ payload: {} })} />)
    expect(screen.getByText('no details')).toBeInTheDocument()
  })

  it('fires onClick when clicked', async () => {
    const data = nodeData()
    const user = userEvent.setup()
    render(<EventNode data={data} />)
    await user.click(screen.getByText('agent.started'))
    expect(data.onClick).toHaveBeenCalled()
  })

  it('marks inherited nodes with the fork source', () => {
    render(<EventNode data={nodeData({ isInherited: true, forkedFrom: 'agent-research-run' })} />)
    expect(screen.getByText(/forked from agent-research/)).toBeInTheDocument()
  })

  it('uses muted gray border styling for inherited nodes', () => {
    const { container } = render(<EventNode data={nodeData({ isInherited: true })} />)
    expect(container.firstChild).toHaveClass('border-gray-700')
    expect(container.firstChild).toHaveClass('opacity-70')
  })

  it('renders time from a string timestamp', () => {
    render(<EventNode data={nodeData({ timestamp: '2026-07-25T12:34:56Z' })} />)
    expect(screen.getByText('12:34:56')).toBeInTheDocument()
  })

  it('shows the context indicator when payload has context', () => {
    const { container } = render(<EventNode data={nodeData({ payload: { context: { topic: 'AI' } } })} />)
    expect(container.querySelector('[title="Has context"]')).toBeInTheDocument()
  })

  it('maps error types to red styling', () => {
    const { container } = render(<EventNode data={nodeData({ type: 'agent.crashed' })} />)
    const badge = screen.getByText('agent.crashed')
    expect(badge).toHaveClass('bg-red-500/15')
    expect(container.firstChild).toHaveClass('border-red-500/30')
  })

  it('maps completed types to emerald styling', () => {
    render(<EventNode data={nodeData({ type: 'pipeline.complete' })} />)
    expect(screen.getByText('pipeline.complete')).toHaveClass('bg-emerald-500/15')
  })

  it('maps started types to blue styling', () => {
    render(<EventNode data={nodeData({ type: 'agent.started' })} />)
    expect(screen.getByText('agent.started')).toHaveClass('bg-blue-500/15')
  })

  it('maps state types to purple styling', () => {
    render(<EventNode data={nodeData({ type: 'state.saved' })} />)
    expect(screen.getByText('state.saved')).toHaveClass('bg-purple-500/15')
  })

  it('maps tool types to pink styling', () => {
    render(<EventNode data={nodeData({ type: 'tool.web_search' })} />)
    expect(screen.getByText('tool.web_search')).toHaveClass('bg-pink-500/15')
  })

  it('maps analyst types to cyan styling', () => {
    render(<EventNode data={nodeData({ type: 'analyst.step' })} />)
    expect(screen.getByText('analyst.step')).toHaveClass('bg-cyan-500/15')
  })

  it('maps critic types to amber styling', () => {
    render(<EventNode data={nodeData({ type: 'critic.review' })} />)
    expect(screen.getByText('critic.review')).toHaveClass('bg-amber-500/15')
  })

  it('maps writer types to indigo styling', () => {
    render(<EventNode data={nodeData({ type: 'writer.draft' })} />)
    expect(screen.getByText('writer.draft')).toHaveClass('bg-indigo-500/15')
  })

  it('falls back to gray styling for unknown types', () => {
    const { container } = render(<EventNode data={nodeData({ type: 'mystery.event' })} />)
    expect(container.firstChild).toHaveClass('border-gray-600')
  })

  it('filters summary rows by the visible fields selection', () => {
    useFlowStore.getState().setVisibleFields(new Set(['tokens']))
    render(<EventNode data={nodeData({ payload: { model: 'gpt-4', tokens: 900 } })} />)
    expect(screen.getByText('900')).toBeInTheDocument()
    expect(screen.queryByText('gpt-4')).not.toBeInTheDocument()
  })

  it('shows no details when the selection hides every payload key', () => {
    useFlowStore.getState().setVisibleFields(new Set())
    render(<EventNode data={nodeData({ payload: { model: 'gpt-4' } })} />)
    expect(screen.getByText('no details')).toBeInTheDocument()
  })
})
