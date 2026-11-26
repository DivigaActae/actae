// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { useRef } from 'react'
import { useFocusTrap } from './useFocusTrap'

function Dialog({ active }: { active: boolean }) {
  const ref = useRef<HTMLDivElement>(null)
  useFocusTrap(ref, active)
  return (
    <div ref={ref}>
      <button>one</button>
      <button>two</button>
    </div>
  )
}

describe('useFocusTrap', () => {
  it('wraps Tab within the container and restores focus + scroll on deactivate', () => {
    const outside = document.createElement('button')
    outside.textContent = 'outside'
    document.body.appendChild(outside)
    outside.focus()

    const { container, rerender } = render(<Dialog active />)
    expect(document.body.style.overflow).toBe('hidden')

    const buttons = Array.from(container.querySelectorAll('button'))
    const first = buttons[0]
    const last = buttons[buttons.length - 1]

    last.focus()
    fireEvent.keyDown(last, { key: 'Tab' })
    expect(document.activeElement).toBe(first)

    first.focus()
    fireEvent.keyDown(first, { key: 'Tab', shiftKey: true })
    expect(document.activeElement).toBe(last)

    rerender(<Dialog active={false} />)
    expect(document.body.style.overflow).not.toBe('hidden')
    expect(document.activeElement).toBe(outside)

    outside.remove()
  })

  it('does nothing when inactive', () => {
    render(<Dialog active={false} />)
    expect(screen.getByText('one')).toBeInTheDocument()
    expect(document.body.style.overflow).not.toBe('hidden')
  })
})
