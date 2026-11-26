// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import LayoutSelector from './LayoutSelector'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

beforeEach(() => {
  resetFlowStore()
})

describe('LayoutSelector', () => {
  it('renders the three layout buttons', () => {
    render(<LayoutSelector />)
    expect(screen.getByTitle('Horizontal line')).toBeInTheDocument()
    expect(screen.getByTitle('Grid (adaptive columns)')).toBeInTheDocument()
    expect(screen.getByTitle('Compact')).toBeInTheDocument()
  })

  it('switches layout mode on click', async () => {
    const user = userEvent.setup()
    render(<LayoutSelector />)
    await user.click(screen.getByTitle('Horizontal line'))
    expect(useFlowStore.getState().layoutMode).toBe('horizontal')
    await user.click(screen.getByTitle('Compact'))
    expect(useFlowStore.getState().layoutMode).toBe('compact')
  })

  it('highlights the active layout', () => {
    useFlowStore.getState().setLayoutMode('horizontal')
    render(<LayoutSelector />)
    expect(screen.getByTitle('Horizontal line')).toHaveClass('bg-gray-700')
    expect(screen.getByTitle('Grid (adaptive columns)')).not.toHaveClass('bg-gray-700')
  })
})
