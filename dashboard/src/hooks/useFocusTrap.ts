// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, type RefObject } from 'react'

const FOCUSABLE_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  'textarea:not([disabled])',
  'input:not([disabled])',
  'select:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',')

/**
 * Trap keyboard focus inside `ref` while `active`, lock background scroll,
 * and restore focus to the previously focused element on close. Intended for
 * modal dialogs (command palette, help). No-op on the server.
 */
export function useFocusTrap(ref: RefObject<HTMLElement>, active: boolean) {
  useEffect(() => {
    if (!active || typeof document === 'undefined') return
    const node = ref.current
    if (!node) return

    const previouslyFocused = document.activeElement as HTMLElement | null
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'

    function focusable(): HTMLElement[] {
      return Array.from(node!.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR))
    }

    function onKeyDown(e: KeyboardEvent) {
      if (e.key !== 'Tab') return
      const items = focusable()
      if (items.length === 0) {
        e.preventDefault()
        return
      }
      const first = items[0]
      const last = items[items.length - 1]
      const current = document.activeElement as HTMLElement | null
      if (e.shiftKey) {
        if (current === first || !node!.contains(current)) {
          e.preventDefault()
          last.focus()
        }
      } else if (current === last || !node!.contains(current)) {
        e.preventDefault()
        first.focus()
      }
    }

    node.addEventListener('keydown', onKeyDown)
    return () => {
      node.removeEventListener('keydown', onKeyDown)
      document.body.style.overflow = previousOverflow
      previouslyFocused?.focus?.()
    }
  }, [ref, active])
}
