// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { useAuth } from '@/lib/auth'
import Sidebar from './Sidebar'
import CommandPalette from './CommandPalette'
import HelpDialog from './HelpDialog'

export default function ProtectedLayout({ children }: { children: React.ReactNode }) {
  const { user, loading } = useAuth()
  const router = useRouter()

  useEffect(() => {
    if (!loading && !user) {
      router.push('/login/')
    }
  }, [user, loading, router])

  if (loading) {
    return (
      <div className="dashboard-loading flex items-center justify-center min-h-screen bg-surface-950">
        <div className="flex flex-col items-center gap-3 animate-fade-in-up">
          <div className="w-10 h-10 rounded-xl border border-actae-400/30 bg-actae-400/10 flex items-center justify-center shadow-lg shadow-actae-400/10">
            <div className="w-2 h-2 bg-actae-400 rounded-full animate-pulse-soft shadow-[0_0_10px_#c8ff3d]" />
          </div>
          <p className="text-sm text-gray-400 font-medium">Loading...</p>
        </div>
      </div>
    )
  }

  if (!user) {
    return null
  }

  return (
    <div className="dashboard-shell flex h-screen overflow-hidden">
      <Sidebar />
      <main className="dashboard-main flex-1 overflow-auto bg-surface-950 relative">
        <div className="h-full animate-fade-in-up">
          {children}
        </div>
      </main>
      <CommandPalette />
      <HelpDialog />
    </div>
  )
}
