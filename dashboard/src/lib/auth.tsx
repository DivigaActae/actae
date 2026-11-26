// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import React, { createContext, useContext, useEffect, useState, useCallback, useRef } from 'react'
import { login as apiLogin, logout as apiLogout, logoutAll as apiLogoutAll, signup as apiSignup, setupAccount as apiSetup, getAuthConfig, localSession, API_BASE } from './api'

interface User {
  id: string
  email: string
  name: string
  image?: string
  role?: string
}

interface AuthContextType {
  user: User | null
  loading: boolean
  error: string | null
  login: (email: string, password: string, rememberMe?: boolean) => Promise<void>
  signup: (email: string, password: string, name?: string, rememberMe?: boolean) => Promise<void>
  setup: (token: string, password: string, name?: string, rememberMe?: boolean) => Promise<void>
  loginWithSso: (token: string, rememberMe?: boolean) => Promise<void>
  logout: () => Promise<void>
  logoutAll: () => Promise<void>
  clearError: () => void
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_MODE === 'true'
const AUTH_KEY = 'actae_auth'

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  // Prevent redundant getMe() after an explicit login or signup action.
  const [sessionStarted, setSessionStarted] = useState(false)
  // Attempt the loopback auto-login at most once per page load, so an explicit
  // logout is not immediately undone by a re-run of the effect.
  const autoLoginAttempted = useRef(false)

  const getStoredAuth = useCallback(() => {
    let stored = localStorage.getItem(AUTH_KEY)
    if (stored) return stored
    stored = sessionStorage.getItem(AUTH_KEY)
    return stored
  }, [])

  const setStoredAuth = useCallback((token: string, rememberMe: boolean) => {
    const data = JSON.stringify({ token })
    if (rememberMe) {
      localStorage.setItem(AUTH_KEY, data)
      sessionStorage.removeItem(AUTH_KEY)
    } else {
      sessionStorage.setItem(AUTH_KEY, data)
      localStorage.removeItem(AUTH_KEY)
    }
  }, [])

  const clearStoredAuth = useCallback(() => {
    localStorage.removeItem(AUTH_KEY)
    sessionStorage.removeItem(AUTH_KEY)
  }, [])

  const isTokenExpired = useCallback((stored: string) => {
    try {
      const parsed = JSON.parse(stored)
      if (parsed.token) {
        const parts = parsed.token.split('.')
        if (parts.length === 3) {
          const payload = JSON.parse(atob(parts[1]))
          if (payload.exp && Date.now() / 1000 > payload.exp) {
            clearStoredAuth()
            setUser(null)
            return true
          }
        }
      }
      return false
    } catch {
      return false
    }
  }, [clearStoredAuth])

  useEffect(() => {
    if (MOCK_MODE) {
      setUser({ id: 'mock-user', email: 'dev@mock.local', name: 'Dev User' })
      setLoading(false)
      return
    }

    // If the user was just set by loginFn/signupFn, skip the getMe() round-trip.
    // This avoids redirect loops when the server is temporarily unreachable or
    // slow after a fresh login.
    if (sessionStarted) {
      setLoading(false)
      return
    }

    // Portal handoff: the SaaS appends `#sso=<token>` when the user clicks
    // "Open dashboard". Exchange it before the stored-session check so the
    // user lands already authenticated. The fragment is consumed (removed
    // from the URL) immediately so the token never lingers in history.
    const ssoToken = takeSsoTokenFromUrl()
    if (ssoToken) {
      exchangeSsoToken(ssoToken)
        .then((result) => {
          if (result) {
            setStoredAuth(result.token, true)
            setUser(result.user)
            setSessionStarted(true)
          } else {
            setError('This sign-in link is invalid or has expired. Please sign in.')
          }
          setLoading(false)
        })
        .catch(() => setLoading(false))
      return
    }

    const stored = getStoredAuth()
    if (!stored || isTokenExpired(stored)) {
      // No session yet: a local self-hosted instance auto-issues one for the
      // same-machine browser (the "no account required" path), so the user
      // lands on the dashboard instead of a login form. Enrolled/remote
      // instances refuse and we fall through to unauthenticated.
      //
      // Attempted at most once per page load: otherwise an explicit logout
      // (which clears the session) would immediately sign the user back in.
      if (autoLoginAttempted.current) {
        setLoading(false)
        return
      }
      autoLoginAttempted.current = true
      tryLocalAutoLogin()
        .then((result) => {
          if (result) {
            setStoredAuth(result.token, true)
            setUser(result.user)
            setSessionStarted(true)
          }
          setLoading(false)
        })
        .catch(() => setLoading(false))
      return
    }

    // Direct fetch instead of api() to avoid the destructive 401 redirect
    // which would otherwise create a redirect loop during initial auth check.
    verifySession(stored)
      .then((userData) => {
        if (userData) {
          setUser(userData)
        } else {
          clearStoredAuth()
        }
        setLoading(false)
      })
      .catch(() => {
        setLoading(false)
      })
  }, [getStoredAuth, isTokenExpired, clearStoredAuth, sessionStarted])

  const loginFn = async (email: string, password: string, rememberMe = true) => {
    setError(null)
    const { data, error: err } = await apiLogin(email, password)
    if (err) {
      setError(err)
      throw new Error(err)
    }
    setStoredAuth(data.token, rememberMe)
    setUser(data.user)
    setSessionStarted(true)
  }

  const signupFn = async (email: string, password: string, name?: string, rememberMe = true) => {
    setError(null)
    const { data, error: err } = await apiSignup(email, password, name)
    if (err) {
      setError(err)
      throw new Error(err)
    }
    setStoredAuth(data.token, rememberMe)
    setUser(data.user)
    setSessionStarted(true)
  }

  // First-run setup: redeem the one-time setup token to set the admin password.
  // The server mints a session on success, so the user lands signed in.
  const setupFn = async (token: string, password: string, name?: string, rememberMe = true) => {
    setError(null)
    const { data, error: err } = await apiSetup(token, password, name)
    if (err) {
      setError(err)
      throw new Error(err)
    }
    setStoredAuth(data.token, rememberMe)
    setUser(data.user)
    setSessionStarted(true)
  }

  const loginWithSsoFn = async (token: string, rememberMe = true) => {    setError(null)
    const result = await exchangeSsoToken(token)
    if (!result) {
      const message = 'This sign-in link is invalid or has expired.'
      setError(message)
      throw new Error(message)
    }
    setStoredAuth(result.token, rememberMe)
    setUser(result.user)
    setSessionStarted(true)
  }

  const logoutFn = async () => {
    await apiLogout()
    clearStoredAuth()
    setUser(null)
    setSessionStarted(false)
  }

  const logoutAllFn = async () => {
    await apiLogoutAll()
    clearStoredAuth()
    setUser(null)
    setSessionStarted(false)
  }

  return (
    <AuthContext.Provider value={{
      user,
      loading,
      error,
      login: loginFn,
      signup: signupFn,
      setup: setupFn,
      loginWithSso: loginWithSsoFn,
      logout: logoutFn,
      logoutAll: logoutAllFn,
      clearError: () => setError(null),
    }}>
      {children}
    </AuthContext.Provider>
  )
}

/** Read and consume `#sso=<token>` from the URL (portal handoff). */
export function takeSsoTokenFromUrl(): string | null {
  if (typeof window === 'undefined') return null
  const hash = window.location.hash
  if (!hash || !hash.startsWith('#sso=')) return null
  let token = ''
  try {
    token = decodeURIComponent(hash.slice('#sso='.length))
  } catch {
    token = hash.slice('#sso='.length)
  }
  // Strip the fragment so the one-time token never lingers in the address
  // bar, history or a screen share.
  window.history.replaceState(null, '', window.location.pathname + window.location.search)
  return token || null
}

/**
 * Direct fetch to exchange an SSO handoff token for a session. Bypasses
 * api() so a failure does not trigger the destructive redirect handler.
 * Returns the session, or null on any failure.
 */
async function exchangeSsoToken(
  token: string
): Promise<{ token: string; user: User } | null> {
  try {
    const base = API_BASE || ''
    const res = await fetch(`${base}/api/v1/auth/sso`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token }),
    })
    if (!res.ok) return null
    const data = await res.json()
    if (!data?.token || !data?.user) return null
    return { token: data.token, user: data.user }
  } catch {
    return null
  }
}

/**
 * Attempt a loopback auto-login for the local self-hosted dashboard. Consults
 * the server's advertised capability first, then exchanges. Returns the session
 * or null on any failure — never throws, never redirects.
 */
async function tryLocalAutoLogin(): Promise<{ token: string; user: User } | null> {
  try {
    const config = await getAuthConfig()
    if (!config?.local_auto_login) return null
    return await localSession()
  } catch {
    return null
  }
}

/**
 * Direct fetch to validate the stored session token.
 * Bypasses api() so a 401 during initial auth check does not trigger the
 * destructive redirect-to-login handler. Returns the user on success, null
 * on auth failure, or throws on network/server errors.
 */
async function verifySession(
  stored: string
): Promise<{ id: string; email: string; name: string; image?: string } | null> {
  try {
    const { token } = JSON.parse(stored)
    if (!token) return null

    const base = API_BASE || ''
    const res = await fetch(`${base}/api/v1/auth/me`, {
      headers: { Authorization: `Bearer ${token}` },
    })
    if (res.status === 401) return null
    if (!res.ok) return null
    const data = await res.json()
    return data.user || null
  } catch {
    return null
  }
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
