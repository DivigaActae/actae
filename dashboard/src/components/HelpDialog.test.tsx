// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import HelpDialog from './HelpDialog'
import { useUiStore } from '@/lib/ui-store'

beforeEach(() => {
  useUiStore.setState({ commandOpen: false, helpOpen: false })
})

describe('HelpDialog', () => {
  it('renders nothing while closed', () => {
    const { container } = render(<HelpDialog />)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows the glossary when opened and closes on Escape', async () => {
    const user = userEvent.setup()
    render(<HelpDialog />)
    act(() => useUiStore.getState().openHelp())
    expect(screen.getByText('Help & glossary')).toBeInTheDocument()
    expect(screen.getByText('Fork')).toBeInTheDocument()
    expect(screen.getByText('Cursor')).toBeInTheDocument()
    await user.keyboard('{Escape}')
    expect(useUiStore.getState().helpOpen).toBe(false)
  })
})
