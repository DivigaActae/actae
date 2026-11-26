// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { useAuth } from '@/lib/auth'
import { getAuthConfig } from '@/lib/api'

const SITE_URL = process.env.NEXT_PUBLIC_SITE_URL || 'https://actae.dev'

export default function LoginPage() {
  const [signupEnabled, setSignupEnabled] = useState(false)
  const [sso, setSso] = useState(false)
  const [setupRequired, setSetupRequired] = useState(false)
  const [isSignup, setIsSignup] = useState(false)
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [setupToken, setSetupToken] = useState('')
  const [name, setName] = useState('')
  const [rememberMe, setRememberMe] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const { user, login, signup, setup, error: authError, clearError } = useAuth()
  const router = useRouter()

  // The server decides which login modes exist. `sso` means the instance is
  // enrolled: credentials are the Actae portal account (verified by the
  // control plane). Standalone instances use local accounts, and signup is
  // opt-in (ENABLE_SIGNUP). `setup_required` means a fresh instance whose
  // initial admin has no password yet — show the first-run setup form.
  useEffect(() => {
    getAuthConfig().then((cfg) => {
      setSignupEnabled(cfg?.enable_signup ?? false)
      setSso(cfg?.sso ?? false)
      setSetupRequired(cfg?.setup_required ?? false)
    })
  }, [])

  // A successful portal handoff (#sso=…) or landing here while already
  // signed in should move straight to the dashboard.
  useEffect(() => {
    if (user) router.replace('/dashboard/')
  }, [user, router])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    clearError()
    setLoading(true)
    try {
      if (setupRequired) {
        if (password !== confirmPassword) {
          throw new Error('Passwords do not match')
        }
        await setup(setupToken, password, name || undefined, rememberMe)
      } else if (isSignup) {
        await signup(email, password, name || undefined, rememberMe)
      } else {
        await login(email, password, rememberMe)
      }
      router.push('/dashboard/')
    } catch (err: any) {
      setError(err.message || 'Authentication failed')
    } finally {
      setLoading(false)
    }
  }

  const toggleSignup = () => {
    setIsSignup((v) => !v)
    setError(null)
    clearError()
  }

  const showSignupToggle = !sso && signupEnabled && !setupRequired
  const displayedError = error || authError

  return (
    <div className="dashboard-login flex min-h-screen bg-surface-950">
      {/* Left: Gradient Brand Panel */}
      <div className="dashboard-login-story hidden lg:flex w-[46%] max-w-[680px] bg-surface-900 flex-col justify-between p-12 xl:p-16 relative overflow-hidden border-r border-white/10">
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top_left,_var(--tw-gradient-stops))] from-actae-400/10 via-transparent to-transparent" />
        <div className="relative z-10">
          <a href={SITE_URL} target="_blank" rel="noreferrer" className="flex items-center gap-3 mb-20" aria-label="Actae home">
            <img src="/android-chrome-192x192.png" alt="" width={40} height={40} className="h-10 w-10 shrink-0 rounded-xl" />
            <div><h1 className="text-sm font-black text-white tracking-[0.13em]">ACTAE</h1><p className="text-[8px] text-surface-500 tracking-[0.15em] mt-1">EXECUTION CONSOLE</p></div>
          </a>
          <p className="text-[9px] text-actae-400 font-bold tracking-[0.16em] mb-5">SELF-HOSTED OPERATIONS</p>
          <h2 className="text-[46px] xl:text-[58px] font-extrabold text-white leading-[0.97] tracking-[-0.055em] mb-6">
            See every step.<br /><span className="text-actae-400">Keep every state.</span>
          </h2>
          <p className="text-surface-400 text-sm leading-relaxed max-w-xs">
            Inspect execution history, replay any channel, compare stateful forks, and follow live agent activity from the infrastructure you control.
          </p>
        </div>
        <div className="relative z-10 flex flex-wrap gap-3">
          {['Ordered history', 'Versioned state', 'Live delivery', 'Fork lineage'].map(f => (
            <span key={f} className="text-[9px] uppercase tracking-wider text-surface-400 bg-white/[0.03] px-3 py-1.5 rounded-md border border-white/10 font-semibold">{f}</span>
          ))}
        </div>
      </div>

      {/* Right: Login Form */}
      <div className="flex-1 flex items-center justify-center px-4">
        <div className="w-full max-w-[360px] animate-fade-in-up">
          {/* Mobile logo */}
          <div className="lg:hidden flex items-center gap-2 mb-8 justify-center">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-actae-400 to-actae-600 flex items-center justify-center">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="7" r="4"/>
                <polyline points="3 21 12 7 21 21"/>
              </svg>
            </div>
            <span className="text-lg font-bold text-gray-100">Actae</span>
          </div>

          <div className="mb-6 text-center lg:text-left">
            <h2 className="text-2xl font-bold text-gray-100 tracking-tight">
              {setupRequired ? 'Set up your admin account' : isSignup ? 'Create account' : sso ? 'Sign in to Actae' : 'Welcome back'}
            </h2>
            <p className="text-sm text-gray-400 mt-1.5">
              {setupRequired
                ? 'Enter the setup token printed by the server, then choose a password.'
                : isSignup
                  ? 'Create a local account for this instance.'
                  : sso
                    ? 'Use the email and password of your Actae account.'
                    : 'Sign in to your Actae dashboard.'}
            </p>
          </div>

          {displayedError && (
            <div className="bg-red-950 border border-red-800 text-red-400 px-4 py-3 rounded-xl text-sm mb-5 animate-scale-in">
              {displayedError}
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-4">
            {(isSignup || setupRequired) && (
              <div>
                <label className="block text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1.5 ml-1">
                  {setupRequired ? 'Name (optional)' : 'Name'}
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  className="input"
                  placeholder="Your name"
                />
              </div>
            )}

            {setupRequired ? (
              <div>
                <label className="block text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1.5 ml-1">Setup token</label>
                <input
                  type="text"
                  value={setupToken}
                  onChange={(e) => setSetupToken(e.target.value)}
                  className="input font-mono text-xs"
                  placeholder="Paste the token printed at startup"
                  required
                  autoComplete="off"
                  spellCheck={false}
                />
              </div>
            ) : (
              <div>
                <label className="block text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1.5 ml-1">Email</label>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="input"
                  placeholder="you@example.com"
                  required
                  autoComplete="username"
                />
              </div>
            )}

            <div>
              <label className="block text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1.5 ml-1">Password</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="input"
                placeholder="••••••••"
                required
                minLength={isSignup || setupRequired ? 8 : undefined}
                autoComplete={isSignup || setupRequired ? 'new-password' : 'current-password'}
              />
            </div>

            {setupRequired && (
              <div>
                <label className="block text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1.5 ml-1">Confirm password</label>
                <input
                  type="password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  className="input"
                  placeholder="••••••••"
                  required
                  minLength={8}
                  autoComplete="new-password"
                />
              </div>
            )}

            {!isSignup && (
              <label className="flex items-center gap-2 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={rememberMe}
                  onChange={(e) => setRememberMe(e.target.checked)}
                  className="w-4 h-4 rounded border-gray-600 bg-gray-800 text-actae-500 focus:ring-actae-500/30 focus:ring-offset-0"
                />
                <span className="text-sm text-gray-400">Remember me</span>
              </label>
            )}

            <button
              type="submit"
              disabled={loading}
              className="btn-primary w-full mt-2"
            >
              {loading ? (
                <span className="flex items-center gap-2">
                  <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  {setupRequired ? 'Setting up...' : isSignup ? 'Creating account...' : 'Signing in...'}
                </span>
              ) : (
                setupRequired ? 'Create admin account' : isSignup ? 'Create Account' : 'Sign In'
              )}
            </button>
          </form>

          {showSignupToggle && (
            <div className="mt-6 text-center">
              <button
                onClick={toggleSignup}
                className="text-sm text-gray-400 hover:text-gray-200 transition-colors"
              >
                {isSignup ? (
                  <>Already have an account? <span className="text-actae-400 font-medium">Sign in</span></>
                ) : (
                  <>Don&apos;t have an account? <span className="text-actae-400 font-medium">Sign up</span></>
                )}
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
