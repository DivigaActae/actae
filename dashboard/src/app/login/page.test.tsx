// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import LoginPage from './page'

const login = vi.hoisted(() => vi.fn())
const signup = vi.hoisted(() => vi.fn())
const getAuthConfig = vi.hoisted(() => vi.fn())
const push = vi.hoisted(() => vi.fn())
const replace = vi.hoisted(() => vi.fn())
const clearError = vi.hoisted(() => vi.fn())
const authState = vi.hoisted(() => ({ user: null as null | { id: string; email: string; name: string }, error: null as string | null }))

vi.mock('next/navigation', () => ({ useRouter: () => ({ push, replace }) }))
vi.mock('@/lib/auth', () => ({
  useAuth: () => ({
    login,
    signup,
    logout: vi.fn(),
    user: authState.user,
    loading: false,
    error: authState.error,
    clearError,
  }),
}))
vi.mock('@/lib/api', () => ({ getAuthConfig, getPreferences: async () => ({ status: 200, data: { prefs: {} } }), putPreferences: async () => ({ status: 200 }) }))

beforeEach(() => {
  vi.clearAllMocks()
  authState.user = null
  authState.error = null
  getAuthConfig.mockResolvedValue({ enable_signup: false, sso: false })
})

describe('LoginPage — email/password', () => {
  it('renders the email/password form', async () => {
    render(<LoginPage />)
    expect(await screen.findByText('Welcome back')).toBeInTheDocument()
    expect(screen.getByPlaceholderText('you@example.com')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Sign In' })).toBeInTheDocument()
    // API key login is gone.
    expect(screen.queryByPlaceholderText('sk-…')).not.toBeInTheDocument()
  })

  it('submits credentials and redirects to the dashboard', async () => {
    login.mockResolvedValue(undefined)
    const user = userEvent.setup()
    render(<LoginPage />)
    await user.type(await screen.findByPlaceholderText('you@example.com'), 'a@b.com')
    await user.type(screen.getByPlaceholderText('••••••••'), 'password123')
    await user.click(screen.getByRole('button', { name: 'Sign In' }))
    await waitFor(() => {
      expect(login).toHaveBeenCalledWith('a@b.com', 'password123', true)
    })
    expect(push).toHaveBeenCalledWith('/dashboard/')
  })

  it('passes rememberMe=false when unchecked', async () => {
    login.mockResolvedValue(undefined)
    const user = userEvent.setup()
    render(<LoginPage />)
    await user.click(await screen.findByLabelText('Remember me'))
    await user.type(screen.getByPlaceholderText('you@example.com'), 'a@b.com')
    await user.type(screen.getByPlaceholderText('••••••••'), 'password123')
    await user.click(screen.getByRole('button', { name: 'Sign In' }))
    await waitFor(() => {
      expect(login).toHaveBeenCalledWith('a@b.com', 'password123', false)
    })
  })

  it('shows auth errors and stays on the page', async () => {
    login.mockRejectedValue(new Error('Invalid email or password'))
    const user = userEvent.setup()
    render(<LoginPage />)
    await user.type(await screen.findByPlaceholderText('you@example.com'), 'a@b.com')
    await user.type(screen.getByPlaceholderText('••••••••'), 'nope')
    await user.click(screen.getByRole('button', { name: 'Sign In' }))
    await waitFor(() => {
      expect(screen.getByText('Invalid email or password')).toBeInTheDocument()
    })
    expect(push).not.toHaveBeenCalled()
  })

  it('surfaces the provider SSO error', async () => {
    authState.error = 'This sign-in link is invalid or has expired.'
    render(<LoginPage />)
    expect(await screen.findByText('This sign-in link is invalid or has expired.')).toBeInTheDocument()
  })

  it('redirects an already-authenticated user to the dashboard', async () => {
    authState.user = { id: 'u1', email: 'a@b.com', name: 'Alice' }
    render(<LoginPage />)
    await waitFor(() => expect(replace).toHaveBeenCalledWith('/dashboard/'))
  })
})

describe('LoginPage — portal SSO', () => {
  it('titles the form as an Actae-account sign-in and hides signup', async () => {
    getAuthConfig.mockResolvedValue({ enable_signup: true, sso: true })
    render(<LoginPage />)
    expect(await screen.findByText('Sign in to Actae')).toBeInTheDocument()
    expect(screen.getByText('Use the email and password of your Actae account.')).toBeInTheDocument()
    expect(screen.queryByText(/Sign up/)).not.toBeInTheDocument()
  })
})

describe('LoginPage — standalone signup gating', () => {
  it('hides the signup toggle when signup is disabled on the server', async () => {
    getAuthConfig.mockResolvedValue({ enable_signup: false, sso: false })
    render(<LoginPage />)
    await waitFor(() => expect(screen.getByRole('button', { name: 'Sign In' })).toBeInTheDocument())
    expect(screen.queryByText(/Sign up/)).not.toBeInTheDocument()
  })

  it('switches to the signup form and submits name when enabled', async () => {
    getAuthConfig.mockResolvedValue({ enable_signup: true, sso: false })
    signup.mockResolvedValue(undefined)
    const user = userEvent.setup()
    render(<LoginPage />)
    await user.click(await screen.findByText(/Sign up/))
    expect(screen.getByText('Create account')).toBeInTheDocument()
    await user.type(screen.getByPlaceholderText('Your name'), 'Alice')
    await user.type(screen.getByPlaceholderText('you@example.com'), 'a@b.com')
    await user.type(screen.getByPlaceholderText('••••••••'), 'password123')
    await user.click(screen.getByRole('button', { name: 'Create Account' }))
    await waitFor(() => {
      expect(signup).toHaveBeenCalledWith('a@b.com', 'password123', 'Alice', true)
    })
    expect(push).toHaveBeenCalledWith('/dashboard/')
  })

  it('hides the remember-me checkbox on the signup form', async () => {
    getAuthConfig.mockResolvedValue({ enable_signup: true, sso: false })
    const user = userEvent.setup()
    render(<LoginPage />)
    await user.click(await screen.findByText(/Sign up/))
    expect(screen.queryByLabelText('Remember me')).not.toBeInTheDocument()
  })
})
