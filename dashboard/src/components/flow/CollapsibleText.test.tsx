// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import CollapsibleText from './CollapsibleText'

const short = 'short text'
const longText = 'x'.repeat(300)

describe('CollapsibleText', () => {
  it('renders short text in full without a toggle', () => {
    render(<CollapsibleText text={short} />)
    expect(screen.getByText(short)).toBeInTheDocument()
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
  })

  it('truncates long text with an ellipsis', () => {
    render(<CollapsibleText text={longText} />)
    expect(screen.getByText(`${'x'.repeat(120)}…`)).toBeInTheDocument()
  })

  it('labels the button with the character count for small payloads', () => {
    render(<CollapsibleText text={longText} />)
    expect(screen.getByRole('button')).toHaveTextContent('show more (300 chars)')
  })

  it('labels the button with KB for large payloads', () => {
    render(<CollapsibleText text={'y'.repeat(2048)} />)
    expect(screen.getByRole('button')).toHaveTextContent('show more (2.0 KB)')
  })

  it('expands and collapses on click', async () => {
    const user = userEvent.setup()
    render(<CollapsibleText text={longText} />)
    await user.click(screen.getByRole('button'))
    expect(screen.getByText(longText)).toBeInTheDocument()
    expect(screen.getByRole('button')).toHaveTextContent('show less')
    await user.click(screen.getByRole('button'))
    expect(screen.getByText(`${'x'.repeat(120)}…`)).toBeInTheDocument()
  })

  it('stops propagation on the toggle click', async () => {
    const user = userEvent.setup()
    const onParentClick = vi.fn()
    render(
      <div onClick={onParentClick}>
        <CollapsibleText text={longText} />
      </div>
    )
    await user.click(screen.getByRole('button'))
    expect(onParentClick).not.toHaveBeenCalled()
  })

  it('handles null or undefined text', () => {
    const { container } = render(<CollapsibleText text={null as any} />)
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
    expect(container.textContent).toBe('')
  })

  it('supports a custom limit', () => {
    render(<CollapsibleText text={'z'.repeat(50)} limit={10} />)
    expect(screen.getByText(`${'z'.repeat(10)}…`)).toBeInTheDocument()
  })
})
