// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useState, useEffect, useCallback, useMemo } from 'react'
import { listChannels } from '@/lib/api'

const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_MODE === 'true'

interface ChannelSelectorProps {
  value: string
  onChange: (channelId: string) => void
  placeholder?: string
  className?: string
  /**
   * When provided, the selector renders this list instead of fetching its
   * own (the owning page owns the channel list — e.g. for live
   * subscriptions to all channels).
   */
  externalChannels?: string[]
}

export default function ChannelSelector({ value, onChange, placeholder = '— select —', className = '', externalChannels }: ChannelSelectorProps) {
  const [channels, setChannels] = useState<string[]>([])
  const [loading, setLoading] = useState(false)

  const loadChannels = useCallback(async () => {
    setLoading(true)
    if (MOCK_MODE) {
      setChannels(['langgraph-agent-prod', 'crewai-research-crew', 'oncall-automation'])
      setLoading(false)
      return
    }
    const { data } = await listChannels()
    const channelList = (data as any)?.channels ?? data
    if (Array.isArray(channelList)) {
      setChannels(channelList.map((c: any) => typeof c === 'string' ? c : c.id || c.channel_id))
    }
    setLoading(false)
  }, [])

  useEffect(() => {
    if (externalChannels) return
    loadChannels()
  }, [loadChannels, externalChannels])

  const shown = externalChannels ?? channels
  // Always include the active value even when it is not (yet) in the fetched
  // list — e.g. a freshly forked channel, or the demo replay channel — so the
  // selector never disagrees with the timeline.
  const options = useMemo(
    () => (value && !shown.includes(value) ? [value, ...shown] : shown),
    [value, shown],
  )
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      disabled={loading}
      title={value || undefined}
      className={`w-full min-w-[260px] max-w-[560px] px-3 py-2 border border-gray-700 rounded-lg text-sm bg-gray-800 text-gray-200 focus:ring-2 focus:ring-actae-500 disabled:opacity-50 ${className}`}
    >
      <option value="">{placeholder}</option>
      {options.map((ch) => (
        <option key={ch} value={ch}>{ch}</option>
      ))}
    </select>
  )
}
