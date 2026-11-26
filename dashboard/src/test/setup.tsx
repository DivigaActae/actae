// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import '@testing-library/jest-dom/vitest'
import { vi } from 'vitest'
import React from 'react'

vi.mock('next/link', () => ({
  default: ({ href, children, ...rest }: any) => (
    <a
      href={typeof href === 'string' ? href : href?.pathname || '#'}
      {...rest}
    >
      {children}
    </a>
  ),
}))

class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}
;(globalThis as any).ResizeObserver ??= ResizeObserverMock

;(globalThis as any).matchMedia ??= (query: string) => ({
  matches: false,
  media: query,
  onchange: null,
  addListener: () => {},
  removeListener: () => {},
  addEventListener: () => {},
  removeEventListener: () => {},
  dispatchEvent: () => false,
})

if (!Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = () => {}
}
