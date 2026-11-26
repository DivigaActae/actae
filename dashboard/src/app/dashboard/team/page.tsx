// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useCallback, useEffect, useState } from 'react'
import ProtectedLayout from '@/components/ProtectedLayout'
import DashboardPageHeader from '@/components/DashboardPageHeader'
import { useAuth } from '@/lib/auth'
import {
  listUsers,
  createUser,
  updateUser,
  deleteUser,
  resetUserPassword,
} from '@/lib/api'

interface ApiUser {
  id: string
  email: string
  name: string
  role: string
  email_verified: boolean
  created_at: string
}

const ROLES = ['admin', 'member']

export default function TeamPage() {
  const { user } = useAuth()
  const isAdmin = user?.role === 'admin'

  const [users, setUsers] = useState<ApiUser[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [busy, setBusy] = useState<string | null>(null)

  const [email, setEmail] = useState('')
  const [name, setName] = useState('')
  const [password, setPassword] = useState('')
  const [role, setRole] = useState('member')

  const [resetFor, setResetFor] = useState<string | null>(null)
  const [resetPw, setResetPw] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    const { data, error: err } = await listUsers()
    if (err) {
      setError(err)
      setLoading(false)
      return
    }
    setUsers((data as { users?: ApiUser[] })?.users || [])
    setError(null)
    setLoading(false)
  }, [])

  useEffect(() => {
    if (isAdmin) load()
    else setLoading(false)
  }, [isAdmin, load])

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    setNotice(null)
    setBusy('create')
    const { error: err } = await createUser({ email, password, name: name || undefined, role })
    setBusy(null)
    if (err) {
      setError(err)
      return
    }
    setNotice(`Created ${email}.`)
    setEmail('')
    setName('')
    setPassword('')
    setRole('member')
    await load()
  }

  const handleRole = async (id: string, nextRole: string) => {
    setError(null)
    setBusy(id)
    const { error: err } = await updateUser(id, { role: nextRole })
    setBusy(null)
    if (err) {
      setError(err)
      return
    }
    await load()
  }

  const handleDelete = async (u: ApiUser) => {
    if (typeof window !== 'undefined' && !window.confirm(`Delete ${u.email}? This cannot be undone.`)) {
      return
    }
    setError(null)
    setBusy(u.id)
    const { error: err } = await deleteUser(u.id)
    setBusy(null)
    if (err) {
      setError(err)
      return
    }
    setNotice(`Deleted ${u.email}.`)
    await load()
  }

  const handleReset = async (id: string) => {
    setError(null)
    setNotice(null)
    setBusy(id)
    const { error: err } = await resetUserPassword(id, resetPw)
    setBusy(null)
    if (err) {
      setError(err)
      return
    }
    setNotice('Password reset. That user must sign in again.')
    setResetFor(null)
    setResetPw('')
  }

  return (
    <ProtectedLayout>
      <div className="flex-1 overflow-y-auto">
        <DashboardPageHeader eyebrow="Admin" title="Team" description="Dashboard users and roles for this instance" />
        <div className="p-6 max-w-4xl">
          {!isAdmin ? (
            <div className="rounded-xl border border-white/10 bg-surface-900 p-6 text-sm text-surface-400">
              Administrator access is required to manage users.
            </div>
          ) : (
            <>
              {error && (
                <div className="mb-4 rounded-xl border border-red-800 bg-red-950 px-4 py-3 text-sm text-red-400">
                  {error}
                </div>
              )}
              {notice && (
                <div className="mb-4 rounded-xl border border-emerald-800 bg-emerald-950 px-4 py-3 text-sm text-emerald-400">
                  {notice}
                </div>
              )}

              <form onSubmit={handleCreate} className="mb-8 rounded-xl border border-white/10 bg-surface-900 p-5">
                <h2 className="mb-4 text-sm font-semibold text-gray-200">Add a user</h2>
                <div className="grid gap-3 sm:grid-cols-2">
                  <input
                    type="email"
                    className="input"
                    placeholder="email@example.com"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    required
                  />
                  <input
                    type="text"
                    className="input"
                    placeholder="Name (optional)"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                  />
                  <input
                    type="password"
                    className="input"
                    placeholder="Temporary password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    required
                    minLength={8}
                    autoComplete="new-password"
                  />
                  <select
                    className="input"
                    value={role}
                    onChange={(e) => setRole(e.target.value)}
                    aria-label="Role"
                  >
                    {ROLES.map((r) => (
                      <option key={r} value={r}>{r}</option>
                    ))}
                  </select>
                </div>
                <button type="submit" disabled={busy === 'create'} className="btn-primary mt-4">
                  {busy === 'create' ? 'Creating…' : 'Create user'}
                </button>
              </form>

              <div className="rounded-xl border border-white/10 bg-surface-900 overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-white/5 text-surface-400">
                    <tr>
                      <th className="px-4 py-2.5 text-left font-medium">User</th>
                      <th className="px-4 py-2.5 text-left font-medium">Role</th>
                      <th className="px-4 py-2.5 text-right font-medium">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {loading ? (
                      <tr><td colSpan={3} className="px-4 py-6 text-center text-surface-500">Loading…</td></tr>
                    ) : users.length === 0 ? (
                      <tr><td colSpan={3} className="px-4 py-6 text-center text-surface-500">No users.</td></tr>
                    ) : (
                      users.map((u) => (
                        <tr key={u.id} className="border-t border-white/5">
                          <td className="px-4 py-3">
                            <div className="font-medium text-gray-200">{u.name}</div>
                            <div className="text-xs text-surface-500">{u.email}</div>
                            {resetFor === u.id && (
                              <div className="mt-2 flex items-center gap-2">
                                <input
                                  type="password"
                                  className="input"
                                  placeholder="New password"
                                  value={resetPw}
                                  onChange={(e) => setResetPw(e.target.value)}
                                  minLength={8}
                                  aria-label="New password"
                                />
                                <button
                                  type="button"
                                  className="text-actae-400 text-xs font-medium"
                                  disabled={busy === u.id}
                                  onClick={() => handleReset(u.id)}
                                >
                                  Save
                                </button>
                                <button
                                  type="button"
                                  className="text-surface-500 text-xs"
                                  onClick={() => { setResetFor(null); setResetPw('') }}
                                >
                                  Cancel
                                </button>
                              </div>
                            )}
                          </td>
                          <td className="px-4 py-3">
                            <select
                              className="input"
                              value={u.role}
                              aria-label={`Role for ${u.email}`}
                              disabled={busy === u.id}
                              onChange={(e) => handleRole(u.id, e.target.value)}
                            >
                              {ROLES.map((r) => (
                                <option key={r} value={r}>{r}</option>
                              ))}
                            </select>
                          </td>
                          <td className="px-4 py-3 text-right whitespace-nowrap">
                            <button
                              type="button"
                              className="text-xs text-surface-400 hover:text-white mr-3"
                              onClick={() => { setResetFor(u.id); setResetPw('') }}
                            >
                              Reset password
                            </button>
                            <button
                              type="button"
                              className="text-xs text-red-400 hover:text-red-300"
                              disabled={busy === u.id}
                              onClick={() => handleDelete(u)}
                            >
                              Delete
                            </button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      </div>
    </ProtectedLayout>
  )
}
