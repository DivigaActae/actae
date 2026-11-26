// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act, waitFor } from '@testing-library/react'
import { AuthProvider, useAuth } from './auth'

const { login: apiLogin, signup: apiSignup, logout: apiLogout, getAuthConfig: apiConfig, localSession: apiLocalSession } = vi.hoisted(() => ({
  login: vi.fn(),
  signup: vi.fn(),
  logout: vi.fn(),
  getAuthConfig: vi.fn(),
  localSession: vi.fn(),
}))

vi.mock('@/lib/api', () => ({
  login: apiLogin,
  signup: apiSignup,
  logout: apiLogout,
  getAuthConfig: apiConfig,
  localSession: apiLocalSession,
  API_BASE: '',
  getPreferences: async () => ({ status: 200, data: { prefs: {} } }),
  putPreferences: async () => ({ status: 200 }),
}))

function renderAuth() {
  return renderHook(() => useAuth(), { wrapper: ({ children }) => <AuthProvider>{children}</AuthProvider> })
}

const USER = { id: 'u1', email: 'a@b.com', name: 'Alice' }

beforeEach(() => {
  localStorage.clear()
  sessionStorage.clear()
  window.history.replaceState(null, '', '/')
  vi.clearAllMocks()
  apiLogin.mockResolvedValue({ data: { token: 'tok', user: USER }, status: 200 })
  apiSignup.mockResolvedValue({ data: { token: 'tok', user: USER }, status: 201 })
  apiLogout.mockResolvedValue({ data: {}, status: 200 })
  apiConfig.mockResolvedValue({ enable_signup: false })
  apiLocalSession.mockResolvedValue(null)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('AuthProvider — initial load', () => {
  it('restores the user from a stored token via /auth/me', async () => {
    localStorage.setItem('actae_auth', JSON.stringify({ token: 'tok.abc.def' }))
    vi.stubGlobal('fetch', vi.fn(async () => ({
      ok: true, status: 200, json: async () => ({ user: USER }),
    })))
    const { result } = renderAuth()
    expect(result.current.loading).toBe(true)
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.user).toEqual(USER)
  })

  it('clears auth when /auth/me returns 401', async () => {
    localStorage.setItem('actae_auth', JSON.stringify({ token: 'tok.abc.def' }))
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: false, status: 401, json: async () => ({}) })))
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.user).toBeNull()
    expect(localStorage.getItem('actae_auth')).toBeNull()
  })

  it('does not fetch when no token is stored', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(fetchMock).not.toHaveBeenCalled()
    expect(result.current.user).toBeNull()
  })

  it('clears an expired JWT and does not fetch', async () => {
    const exp = Math.floor(Date.now() / 1000) - 60
    const payload = btoa(JSON.stringify({ exp }))
    localStorage.setItem('actae_auth', JSON.stringify({ token: `h.${payload}.s` }))
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.user).toBeNull()
    expect(localStorage.getItem('actae_auth')).toBeNull()
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('keeps an unexpired JWT', async () => {
    const exp = Math.floor(Date.now() / 1000) + 3600
    const payload = btoa(JSON.stringify({ exp }))
    localStorage.setItem('actae_auth', JSON.stringify({ token: `h.${payload}.s` }))
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ user: USER }) })))
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.user).toEqual(USER))
  })
})

describe('AuthProvider — local auto-login', () => {
  it('signs in from the local session when the instance advertises it', async () => {
    apiConfig.mockResolvedValue({ enable_signup: false, local_auto_login: true })
    apiLocalSession.mockResolvedValue({ token: 'local-tok', user: USER })
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.user).toEqual(USER))
    expect(apiLocalSession).toHaveBeenCalled()
    expect(localStorage.getItem('actae_auth')).toBe(JSON.stringify({ token: 'local-tok' }))
  })

  it('does not attempt auto-login when the instance does not advertise it', async () => {
    apiConfig.mockResolvedValue({ enable_signup: false, local_auto_login: false })
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(apiLocalSession).not.toHaveBeenCalled()
    expect(result.current.user).toBeNull()
  })

  it('does not auto-login again after an explicit logout', async () => {
    apiConfig.mockResolvedValue({ enable_signup: false, local_auto_login: true })
    apiLocalSession.mockResolvedValue({ token: 'local-tok', user: USER })
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.user).toEqual(USER))
    await act(() => result.current.logout())
    await waitFor(() => expect(result.current.user).toBeNull())
    // The auto-login is attempted once per page load, so logout sticks.
    expect(apiLocalSession).toHaveBeenCalledTimes(1)
  })

  it('falls through to unauthenticated when auto-login is refused', async () => {
    apiConfig.mockResolvedValue({ enable_signup: false, local_auto_login: true })
    apiLocalSession.mockResolvedValue(null)
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.user).toBeNull()
  })
})

describe('AuthProvider — login', () => {
  it('stores token in localStorage when rememberMe is true', async () => {
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(() => result.current.login('a@b.com', 'pw', true))
    expect(localStorage.getItem('actae_auth')).toBe(JSON.stringify({ token: 'tok' }))
    expect(sessionStorage.getItem('actae_auth')).toBeNull()
    expect(result.current.user).toEqual(USER)
    expect(result.current.error).toBeNull()
  })

  it('stores token in sessionStorage when rememberMe is false', async () => {
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(() => result.current.login('a@b.com', 'pw', false))
    expect(sessionStorage.getItem('actae_auth')).toBe(JSON.stringify({ token: 'tok' }))
    expect(localStorage.getItem('actae_auth')).toBeNull()
  })

  it('defaults to rememberMe=true', async () => {
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(() => result.current.login('a@b.com', 'pw'))
    expect(localStorage.getItem('actae_auth')).toBe(JSON.stringify({ token: 'tok' }))
  })

  it('records the error and rethrows on failure', async () => {
    apiLogin.mockResolvedValue({ data: null, error: 'Invalid credentials', status: 401 })
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(async () => {
      await expect(result.current.login('a@b.com', 'pw')).rejects.toThrow('Invalid credentials')
    })
    expect(result.current.error).toBe('Invalid credentials')
    expect(result.current.user).toBeNull()
  })

  it('skips the initial getMe round-trip after a login', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(() => result.current.login('a@b.com', 'pw'))
    // No /auth/me fetch fired.
    expect(fetchMock).not.toHaveBeenCalled()
  })
})

describe('AuthProvider — signup', () => {
  it('signs up, stores the token and sets the user', async () => {
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(() => result.current.signup('a@b.com', 'pw', 'Alice', true))
    expect(apiSignup).toHaveBeenCalledWith('a@b.com', 'pw', 'Alice')
    expect(localStorage.getItem('actae_auth')).toBe(JSON.stringify({ token: 'tok' }))
    expect(result.current.user).toEqual(USER)
  })

  it('rethrows signup errors', async () => {
    apiSignup.mockResolvedValue({ data: null, error: 'Email taken', status: 409 })
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(async () => {
      await expect(result.current.signup('a@b.com', 'pw')).rejects.toThrow('Email taken')
    })
    expect(result.current.error).toBe('Email taken')
  })
})

describe('AuthProvider — SSO handoff', () => {
  it('logs in with an SSO token, stores the session and sets the user', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => ({
      ok: true, status: 200, json: async () => ({ token: 'tok', user: USER }),
    })))
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(() => result.current.loginWithSso('handoff-token', true))
    expect(localStorage.getItem('actae_auth')).toBe(JSON.stringify({ token: 'tok' }))
    expect(result.current.user).toEqual(USER)
  })

  it('stores the session in sessionStorage when rememberMe is false', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => ({
      ok: true, status: 200, json: async () => ({ token: 'tok', user: USER }),
    })))
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(() => result.current.loginWithSso('handoff-token', false))
    expect(sessionStorage.getItem('actae_auth')).toBe(JSON.stringify({ token: 'tok' }))
    expect(localStorage.getItem('actae_auth')).toBeNull()
  })

  it('records the error and rethrows when the handoff is rejected', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: false, status: 401, json: async () => ({}) })))
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(async () => {
      await expect(result.current.loginWithSso('bad')).rejects.toThrow('invalid or has expired')
    })
    expect(result.current.error).toContain('invalid or has expired')
    expect(result.current.user).toBeNull()
  })

  it('exchanges a #sso= fragment on load and clears it from the URL', async () => {
    window.history.replaceState(null, '', '/login/#sso=fragment-token')
    vi.stubGlobal('fetch', vi.fn(async () => ({
      ok: true, status: 200, json: async () => ({ token: 'tok', user: USER }),
    })))
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.user).toEqual(USER))
    expect(localStorage.getItem('actae_auth')).toBe(JSON.stringify({ token: 'tok' }))
    // The one-time token is stripped from the address bar/history.
    expect(window.location.hash).toBe('')
    expect(window.location.pathname).toBe('/login/')
  })
})

describe('AuthProvider — logout', () => {
  it('calls the API, clears storage and resets user', async () => {
    localStorage.setItem('actae_auth', JSON.stringify({ token: 'tok' }))
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ user: USER }) })))
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.user).toEqual(USER))
    await act(() => result.current.logout())
    expect(apiLogout).toHaveBeenCalled()
    expect(localStorage.getItem('actae_auth')).toBeNull()
    expect(sessionStorage.getItem('actae_auth')).toBeNull()
    expect(result.current.user).toBeNull()
  })

  it('clears error via clearError', async () => {
    apiLogin.mockResolvedValue({ data: null, error: 'nope', status: 400 })
    const { result } = renderAuth()
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(() => result.current.login('a', 'b').catch(() => {}))
    expect(result.current.error).toBe('nope')
    act(() => result.current.clearError())
    expect(result.current.error).toBeNull()
  })
})

describe('useAuth outside provider', () => {
  it('throws when used outside AuthProvider', () => {
    expect(() => renderHook(() => useAuth())).toThrow('useAuth must be used within AuthProvider')
  })
})
