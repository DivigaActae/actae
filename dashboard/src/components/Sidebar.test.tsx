// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import Sidebar from './Sidebar'

const pathname = vi.hoisted(() => ({ value: '/dashboard/' }))
const logout = vi.hoisted(() => vi.fn())
const user = vi.hoisted(() => ({ id: 'u1', email: 'admin@actae.local', name: 'Admin' }))

vi.mock('next/navigation', () => ({
  usePathname: () => pathname.value,
}))

vi.mock('@/lib/auth', () => ({
  useAuth: () => ({ user, logout }),
}))

beforeEach(() => {
  vi.clearAllMocks()
  pathname.value = '/dashboard/'
})

describe('Sidebar', () => {
  it('shows the grouped nav destinations', () => {
    render(<Sidebar />)
    expect(screen.getByText('ACTAE')).toBeInTheDocument()
    expect(screen.getByText('Overview')).toBeInTheDocument()
    expect(screen.getByText('Runs')).toBeInTheDocument()
    expect(screen.getByText('Forks')).toBeInTheDocument()
    expect(screen.getByText('Events')).toBeInTheDocument()
    expect(screen.getByText('Approvals')).toBeInTheDocument()
    expect(screen.queryByText('API Keys')).not.toBeInTheDocument()
  })

  it('links to the correct hrefs', () => {
    render(<Sidebar />)
    expect(screen.getByText('Overview').closest('a')).toHaveAttribute('href', '/dashboard/')
    expect(screen.getByText('Runs').closest('a')).toHaveAttribute('href', '/dashboard/runs/')
    expect(screen.getByText('Forks').closest('a')).toHaveAttribute('href', '/dashboard/experiments/')
    expect(screen.getByText('Events').closest('a')).toHaveAttribute('href', '/dashboard/events/')
    expect(screen.getByText('Approvals').closest('a')).toHaveAttribute('href', '/dashboard/approvals/')
  })

  it('demotes Coordination behind a collapsed Advanced section', async () => {
    const user = userEvent.setup()
    render(<Sidebar />)
    expect(screen.queryByText('Coordination')).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /advanced/i }))
    expect(screen.getByText('Coordination').closest('a')).toHaveAttribute('href', '/dashboard/groups/')
  })

  it('highlights the active route', () => {
    pathname.value = '/dashboard/events/'
    render(<Sidebar />)
    expect(screen.getByText('Events').closest('a')).toHaveClass('bg-white/10')
    expect(screen.getByText('Overview').closest('a')).not.toHaveClass('bg-white/10')
  })

  it('highlights Runs while viewing a run detail', () => {
    pathname.value = '/dashboard/flow/'
    render(<Sidebar />)
    expect(screen.getByText('Runs').closest('a')).toHaveClass('bg-white/10')
  })

  it('shows the user email and initial', () => {
    render(<Sidebar />)
    expect(screen.getByText('admin@actae.local')).toBeInTheDocument()
    expect(screen.getByText('A')).toBeInTheDocument()
  })

  it('falls back to a question mark initial without an email', () => {
    user.email = ''
    render(<Sidebar />)
    expect(screen.getByText('?')).toBeInTheDocument()
    user.email = 'admin@actae.local'
  })

  it('calls logout when the logout button is clicked', async () => {
    const userEvt = userEvent.setup()
    render(<Sidebar />)
    await userEvt.click(screen.getByTitle('Logout'))
    expect(logout).toHaveBeenCalled()
  })
})
