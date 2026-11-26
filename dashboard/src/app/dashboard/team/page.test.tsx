// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import TeamPage from './page'

const listUsers = vi.hoisted(() => vi.fn())
const createUser = vi.hoisted(() => vi.fn())
const updateUser = vi.hoisted(() => vi.fn())
const deleteUser = vi.hoisted(() => vi.fn())
const resetUserPassword = vi.hoisted(() => vi.fn())
const authState = vi.hoisted(() => ({ role: 'admin' as string }))

vi.mock('@/components/ProtectedLayout', () => ({
  default: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))
vi.mock('@/lib/auth', () => ({
  useAuth: () => ({ user: { id: 'u1', email: 'admin@actae.local', name: 'Admin', role: authState.role } }),
}))
vi.mock('@/lib/api', () => ({ listUsers, createUser, updateUser, deleteUser, resetUserPassword }))

const users = [
  { id: 'u1', email: 'admin@actae.local', name: 'Admin', role: 'admin', email_verified: true, created_at: '2026-10-01T00:00:00Z' },
  { id: 'u2', email: 'dev@actae.local', name: 'Dev', role: 'member', email_verified: true, created_at: '2026-10-02T00:00:00Z' },
]

beforeEach(() => {
  vi.clearAllMocks()
  authState.role = 'admin'
  listUsers.mockResolvedValue({ data: { users }, status: 200 })
  createUser.mockResolvedValue({ data: { user: users[1] }, status: 201 })
  updateUser.mockResolvedValue({ data: { user: users[1] }, status: 200 })
  deleteUser.mockResolvedValue({ data: { deleted: true }, status: 200 })
  resetUserPassword.mockResolvedValue({ data: { reset: true }, status: 200 })
  vi.spyOn(window, 'confirm').mockReturnValue(true)
})

describe('TeamPage', () => {
  it('gates non-admins and never lists users', async () => {
    authState.role = 'member'
    render(<TeamPage />)
    expect(await screen.findByText(/Administrator access is required/i)).toBeInTheDocument()
    expect(listUsers).not.toHaveBeenCalled()
  })

  it('lists users for an admin', async () => {
    render(<TeamPage />)
    expect(await screen.findByText('dev@actae.local')).toBeInTheDocument()
    expect(screen.getByText('admin@actae.local')).toBeInTheDocument()
    expect(listUsers).toHaveBeenCalledTimes(1)
  })

  it('creates a user from the form', async () => {
    const user = userEvent.setup()
    render(<TeamPage />)
    await screen.findByText('dev@actae.local')
    await user.type(screen.getByPlaceholderText('email@example.com'), 'new@actae.local')
    await user.type(screen.getByPlaceholderText('Temporary password'), 'Str0ng-Passw0rd!x')
    await user.click(screen.getByRole('button', { name: /create user/i }))
    await waitFor(() =>
      expect(createUser).toHaveBeenCalledWith({
        email: 'new@actae.local',
        password: 'Str0ng-Passw0rd!x',
        name: undefined,
        role: 'member',
      })
    )
  })

  it('changes a role and deletes a user', async () => {
    const user = userEvent.setup()
    render(<TeamPage />)
    await screen.findByText('dev@actae.local')
    await user.selectOptions(screen.getByLabelText('Role for dev@actae.local'), 'admin')
    await waitFor(() => expect(updateUser).toHaveBeenCalledWith('u2', { role: 'admin' }))
    await user.click(screen.getAllByRole('button', { name: /delete/i })[1])
    await waitFor(() => expect(deleteUser).toHaveBeenCalledWith('u2'))
  })
})
