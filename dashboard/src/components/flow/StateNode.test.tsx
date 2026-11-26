// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import StateNode from './StateNode'

vi.mock('@xyflow/react', () => ({
  Handle: () => null,
  Position: { Left: 'left', Right: 'right', Top: 'top', Bottom: 'bottom' },
}))

describe('StateNode', () => {
  it('renders the snapshot header and cursor', () => {
    render(<StateNode data={{ keys: ['a', 'b'], cursor: 12, onClick: vi.fn() }} />)
    expect(screen.getByText('State Snapshot')).toBeInTheDocument()
    expect(screen.getByText('@12')).toBeInTheDocument()
  })

  it('renders up to six keys', () => {
    const keys = ['agent', 'status', 'model', 'tokens', 'cost', 'messages', 'extra1', 'extra2']
    render(<StateNode data={{ keys, cursor: 1, onClick: vi.fn() }} />)
    expect(screen.getByText('agent')).toBeInTheDocument()
    expect(screen.queryByText('extra1')).not.toBeInTheDocument()
    expect(screen.getByText('+2')).toBeInTheDocument()
  })

  it('does not show an overflow badge within the limit', () => {
    render(<StateNode data={{ keys: ['a'], cursor: 1, onClick: vi.fn() }} />)
    expect(screen.queryByText(/^\+/)).not.toBeInTheDocument()
  })

  it('fires onClick', async () => {
    const onClick = vi.fn()
    const user = userEvent.setup()
    render(<StateNode data={{ keys: [], cursor: 1, onClick }} />)
    await user.click(screen.getByText('State Snapshot'))
    expect(onClick).toHaveBeenCalled()
  })
})
