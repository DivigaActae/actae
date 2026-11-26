// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act } from '@testing-library/react'
import { ToastProvider, useToast } from './Toast'

function Probe({ msg = 'hello', type = 'info' as const }: { msg?: string; type?: 'success' | 'error' | 'info' }) {
  const { addToast } = useToast()
  return <button onClick={() => addToast(msg, type)}>add</button>
}

function renderToasts(msg?: string, type?: 'success' | 'error' | 'info') {
  return render(
    <ToastProvider>
      <Probe msg={msg} type={type} />
    </ToastProvider>
  )
}

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.useRealTimers()
})

describe('ToastProvider', () => {
  it('shows a toast with default info styling after addToast', () => {
    renderToasts()
    fireEvent.click(screen.getByText('add'))
    expect(screen.getByText('hello')).toBeInTheDocument()
    expect(screen.getByText('hello')).toHaveClass('bg-actae-500')
  })

  it('applies success and error styling', () => {
    const { unmount } = renderToasts('ok', 'success')
    fireEvent.click(screen.getByText('add'))
    expect(screen.getByText('ok')).toHaveClass('bg-green-500')
    unmount()

    renderToasts('bad', 'error')
    fireEvent.click(screen.getByText('add'))
    expect(screen.getByText('bad')).toHaveClass('bg-red-500')
  })

  it('auto-dismisses after 4 seconds', () => {
    renderToasts()
    fireEvent.click(screen.getByText('add'))
    expect(screen.getByText('hello')).toBeInTheDocument()
    act(() => { vi.advanceTimersByTime(3999) })
    expect(screen.getByText('hello')).toBeInTheDocument()
    act(() => { vi.advanceTimersByTime(1) })
    expect(screen.queryByText('hello')).not.toBeInTheDocument()
  })

  it('dismisses a toast on click', () => {
    renderToasts()
    fireEvent.click(screen.getByText('add'))
    fireEvent.click(screen.getByText('hello'))
    expect(screen.queryByText('hello')).not.toBeInTheDocument()
  })

  it('keeps at most 5 toasts', () => {
    renderToasts()
    for (let i = 0; i < 6; i++) {
      fireEvent.click(screen.getByText('add'))
    }
    expect(screen.getAllByText('hello')).toHaveLength(5)
  })

  it('assigns unique ids so simultaneous toasts render separately', () => {
    renderToasts()
    fireEvent.click(screen.getByText('add'))
    fireEvent.click(screen.getByText('add'))
    expect(screen.getAllByText('hello')).toHaveLength(2)
  })
})

describe('useToast outside provider', () => {
  it('throws outside the provider', () => {
    expect(() => render(<Probe />)).toThrow('useToast must be used within ToastProvider')
  })
})
