// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import ProtectedLayout from './ProtectedLayout'

const push = vi.hoisted(() => vi.fn())
const authState = vi.hoisted(() => ({ user: null as any, loading: true }))

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push }),
  usePathname: () => '/dashboard/',
}))

vi.mock('@/lib/auth', () => ({
  useAuth: () => ({ user: authState.user, loading: authState.loading }),
}))

beforeEach(() => {
  vi.clearAllMocks()
  authState.user = null
  authState.loading = true
})

describe('ProtectedLayout', () => {
  it('shows the loading screen while loading', () => {
    authState.loading = true
    render(<ProtectedLayout>content</ProtectedLayout>)
    expect(screen.getByText('Loading...')).toBeInTheDocument()
    expect(push).not.toHaveBeenCalled()
  })

  it('redirects to /login/ when unauthenticated and not loading', () => {
    authState.loading = false
    authState.user = null
    render(<ProtectedLayout>content</ProtectedLayout>)
    expect(push).toHaveBeenCalledWith('/login/')
    expect(screen.queryByText('content')).not.toBeInTheDocument()
  })

  it('renders children when authenticated', () => {
    authState.loading = false
    authState.user = { id: 'u1', email: 'a@b.com', name: 'A' }
    render(<ProtectedLayout>content</ProtectedLayout>)
    expect(screen.getByText('content')).toBeInTheDocument()
    expect(push).not.toHaveBeenCalled()
  })
})
