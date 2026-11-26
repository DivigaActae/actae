// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render } from '@testing-library/react'
import StatesPage from './page'

const replace = vi.hoisted(() => vi.fn())
vi.mock('next/navigation', () => ({ useRouter: () => ({ replace }) }))

beforeEach(() => {
  vi.clearAllMocks()
})

describe('StatesPage', () => {
  it('redirects to the events page on mount', () => {
    render(<StatesPage />)
    expect(replace).toHaveBeenCalledWith('/dashboard/events/')
  })

  it('renders nothing', () => {
    const { container } = render(<StatesPage />)
    expect(container).toBeEmptyDOMElement()
  })
})
