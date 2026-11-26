// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import EventTypeFilter from './EventTypeFilter'

const types = [
  { type: 'tool.llm', count: 10 },
  { type: 'agent.started', count: 3 },
  { type: 'agent.completed', count: 1 },
]

describe('EventTypeFilter', () => {
  it('shows "all N" when nothing is selected', () => {
    render(<EventTypeFilter types={types} selected={null} onChange={() => {}} />)
    expect(screen.getByText('Filter: all 3')).toBeInTheDocument()
  })

  it('shows "M/N" when a subset is selected', () => {
    render(
      <EventTypeFilter
        types={types}
        selected={new Set(['tool.llm'])}
        onChange={() => {}}
      />
    )
    expect(screen.getByText('Filter: 1/3')).toBeInTheDocument()
  })

  it('disables the button when there are no types', () => {
    render(<EventTypeFilter types={[]} selected={null} onChange={() => {}} />)
    expect(screen.getByRole('button')).toBeDisabled()
  })

  it('lists types with counts when opened', async () => {
    const user = userEvent.setup()
    render(<EventTypeFilter types={types} selected={null} onChange={() => {}} />)
    await user.click(screen.getByRole('button'))
    const listbox = screen.getByRole('listbox')
    expect(listbox).toHaveTextContent('tool.llm')
    expect(listbox).toHaveTextContent('agent.started')
    expect(listbox).toHaveTextContent('10')
    expect(listbox).toHaveTextContent('3')
  })

  it('uses a custom search placeholder and all-checked title', async () => {
    const user = userEvent.setup()
    render(
      <EventTypeFilter
        types={types}
        selected={null}
        onChange={() => {}}
        label="Fields"
        searchPlaceholder="Search fields..."
        allCheckedTitle="Filter by payload field"
      />
    )
    expect(screen.getByRole('button')).toHaveAttribute('title', 'Filter by payload field')
    await user.click(screen.getByRole('button'))
    expect(screen.getByPlaceholderText('Search fields...')).toBeInTheDocument()
  })

  it('renders long event type names unwrapped instead of truncating them', async () => {
    const user = userEvent.setup()
    const longTypes = [
      { type: 'copilot.assistant.streaming_delta', count: 441 },
      { type: 'copilot.assistant.tool_call_delta', count: 369 },
    ]
    render(<EventTypeFilter types={longTypes} selected={null} onChange={() => {}} />)
    await user.click(screen.getByRole('button'))
    const label = screen.getByText('copilot.assistant.streaming_delta')
    expect(label).toHaveTextContent('copilot.assistant.streaming_delta')
    expect(label).not.toHaveClass('truncate')
    expect(label).toHaveClass('break-words')
    expect(screen.getByText('copilot.assistant.tool_call_delta')).toBeInTheDocument()
  })

  it('hides only the toggled type when starting from show-all', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(<EventTypeFilter types={types} selected={null} onChange={onChange} />)
    await user.click(screen.getByRole('button'))
    const listbox = screen.getByRole('listbox')
    const checkbox = Array.from(listbox.querySelectorAll('input[type="checkbox"]'))
      .find((el) => el.parentElement?.textContent?.includes('tool.llm')) as HTMLInputElement
    await user.click(checkbox)
    expect(onChange).toHaveBeenCalledWith(new Set(['agent.started', 'agent.completed']))
  })

  it('adds a type back to a non-null selection', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(
      <EventTypeFilter
        types={types}
        selected={new Set(['tool.llm'])}
        onChange={onChange}
      />
    )
    await user.click(screen.getByRole('button'))
    const listbox = screen.getByRole('listbox')
    const checkbox = Array.from(listbox.querySelectorAll('input[type="checkbox"]'))
      .find((el) => el.parentElement?.textContent?.includes('agent.started')) as HTMLInputElement
    await user.click(checkbox)
    expect(onChange).toHaveBeenCalledWith(new Set(['tool.llm', 'agent.started']))
  })

  it('unchecks a type from an existing selection', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(
      <EventTypeFilter
        types={types}
        selected={new Set(['tool.llm', 'agent.started'])}
        onChange={onChange}
      />
    )
    await user.click(screen.getByRole('button'))
    const listbox = screen.getByRole('listbox')
    const checkbox = Array.from(listbox.querySelectorAll('input[type="checkbox"]'))
      .find((el) => el.parentElement?.textContent?.includes('tool.llm')) as HTMLInputElement
    await user.click(checkbox)
    expect(onChange).toHaveBeenCalledWith(new Set(['agent.started']))
  })

  it('Show all resets to null', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(
      <EventTypeFilter
        types={types}
        selected={new Set(['tool.llm'])}
        onChange={onChange}
      />
    )
    await user.click(screen.getByRole('button'))
    await user.click(screen.getByText('Show all'))
    expect(onChange).toHaveBeenCalledWith(null)
  })

  it('Show none selects an empty set', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(<EventTypeFilter types={types} selected={null} onChange={onChange} />)
    await user.click(screen.getByRole('button'))
    await user.click(screen.getByText('Show none'))
    expect(onChange).toHaveBeenCalledWith(new Set())
  })

  it('filters the list by search query', async () => {
    const user = userEvent.setup()
    render(<EventTypeFilter types={types} selected={null} onChange={() => {}} />)
    await user.click(screen.getByRole('button'))
    await user.type(screen.getByPlaceholderText('Search types...'), 'tool')
    const listbox = screen.getByRole('listbox')
    expect(listbox).toHaveTextContent('tool.llm')
    expect(listbox).not.toHaveTextContent('agent.started')
  })

  it('shows a no-match message when search finds nothing', async () => {
    const user = userEvent.setup()
    render(<EventTypeFilter types={types} selected={null} onChange={() => {}} />)
    await user.click(screen.getByRole('button'))
    await user.type(screen.getByPlaceholderText('Search types...'), 'zzz')
    expect(screen.getByText('No types match')).toBeInTheDocument()
  })

  it('closes on outside click', async () => {
    const user = userEvent.setup()
    render(<EventTypeFilter types={types} selected={null} onChange={() => {}} />)
    await user.click(screen.getByRole('button'))
    expect(screen.getByRole('listbox')).toBeInTheDocument()
    fireEvent.mouseDown(document.body)
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
  })

  it('closes on Escape', async () => {
    const user = userEvent.setup()
    render(<EventTypeFilter types={types} selected={null} onChange={() => {}} />)
    await user.click(screen.getByRole('button'))
    expect(screen.getByRole('listbox')).toBeInTheDocument()
    await user.keyboard('{Escape}')
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
  })
})
