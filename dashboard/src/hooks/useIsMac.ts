// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useState } from 'react'

/**
 * Whether the user is on an Apple platform, for the ⌘ vs Ctrl shortcut hint.
 * Returns `null` until mounted so server-rendered markup (static export) and
 * the first client render agree — callers render a neutral hint meanwhile.
 */
export function useIsMac(): boolean | null {
  const [isMac, setIsMac] = useState<boolean | null>(null)
  useEffect(() => {
    const platform = typeof navigator !== 'undefined'
      ? `${navigator.platform || ''} ${navigator.userAgent || ''}`
      : ''
    setIsMac(/Mac|iPhone|iPad|iPod/i.test(platform))
  }, [])
  return isMac
}

/** Shortcut label for the command palette, e.g. "⌘K" or "Ctrl K". */
export function shortcutLabel(isMac: boolean | null): string {
  if (isMac === null) return '⌘/Ctrl K'
  return isMac ? '⌘K' : 'Ctrl K'
}
