// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import VisualizationSelector from './VisualizationSelector'
import { resetFlowStore } from '@/test/utils'
import { useFlowStore } from '@/lib/flow-store'

beforeEach(resetFlowStore)

describe('VisualizationSelector', () => {
  it('defaults to Timeline and switches both modes accessibly', async () => {
    const user = userEvent.setup()
    render(<VisualizationSelector />)
    expect(screen.getByRole('button', { name: 'Timeline' })).toHaveAttribute('aria-pressed', 'true')
    await user.click(screen.getByRole('button', { name: 'Graph' }))
    expect(useFlowStore.getState().visualizationMode).toBe('graph')
    expect(screen.getByRole('button', { name: 'Graph' })).toHaveAttribute('aria-pressed', 'true')
  })
})
