// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect } from 'react'
import { useFlowStore } from '@/lib/flow-store'

export default function ChannelSelector() {
  const channels = useFlowStore(s => s.channels)
  const activeChannel = useFlowStore(s => s.activeChannel)
  const setActiveChannel = useFlowStore(s => s.setActiveChannel)
  const loadChannels = useFlowStore(s => s.loadChannels)
  const loading = useFlowStore(s => s.loading)

  useEffect(() => {
    if (channels.length === 0) {
      loadChannels()
    }
  }, [channels.length, loadChannels])

  return (
    <div className="flex min-w-[260px] max-w-[min(44vw,520px)] items-center">
      <select
        value={activeChannel || ''}
        onChange={(e) => setActiveChannel(e.target.value || null)}
        disabled={loading}
        title={activeChannel || undefined}
        className="w-full min-w-0 px-2 py-1.5 border border-gray-700 rounded-lg text-xs bg-gray-800/50 text-gray-200 focus:outline-none focus:border-actae-500 disabled:opacity-50 transition-colors"
      >
        <option value="">— select —</option>
        {channels.map((ch) => (
          <option key={ch} value={ch}>{ch}</option>
        ))}
      </select>
    </div>
  )
}
