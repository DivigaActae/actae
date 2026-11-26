// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'

export default function StatesPage() {
  const router = useRouter()

  useEffect(() => {
    router.replace('/dashboard/events/')
  }, [router])

  return null
}
