// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { useAuth } from '@/lib/auth'

export default function Home() {
  const { user, loading } = useAuth()
  const router = useRouter()

  useEffect(() => {
    if (loading) return
    if (user) {
      router.replace('/dashboard/')
    } else {
      router.replace('/login/')
    }
  }, [user, loading, router])

  return (
    <div className="flex items-center justify-center min-h-screen">
      <div className="animate-pulse text-gray-400">Loading...</div>
    </div>
  )
}
