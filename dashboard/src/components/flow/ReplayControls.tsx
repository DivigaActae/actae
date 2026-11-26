// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useState, KeyboardEvent } from 'react'
import { useFlowStore } from '@/lib/flow-store'

export default function ReplayControls() {
  const [inputValue, setInputValue] = useState('')
  const activeChannel = useFlowStore(s => s.activeChannel)
  const events = useFlowStore(s => s.events)
  const replayFromCursor = useFlowStore(s => s.replayFromCursor)
  const setReplayCursor = useFlowStore(s => s.setReplayCursor)
  const loading = useFlowStore(s => s.loading)

  const handleGo = () => {
    const val = parseInt(inputValue, 10)
    if (isNaN(val) || val < 0) return
    if (val === 0 || events.length === 0) {
      setReplayCursor(null)
      return
    }
    const sorted = [...events].sort((a, b) => a.cursor - b.cursor)
    const target = sorted[Math.min(val, sorted.length) - 1]
    setReplayCursor(target.cursor)
  }

  const handleReset = () => {
    setInputValue('')
    setReplayCursor(null)
  }

  const handleKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter') handleGo()
  }

  return (
    <div className="flex items-center gap-1.5">
      <div className="relative">
        <input
          type="number"
          min="0"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="0"
          disabled={!activeChannel}
          title="Skip the first N events (view filter)"
          aria-label="Show events from position"
          className="w-24 px-2 py-1 bg-gray-900 border border-gray-700 rounded-md text-xs text-gray-200 placeholder-gray-600 focus:outline-none focus:border-actae-500 disabled:opacity-50"
        />
      </div>
      <button
        onClick={handleGo}
        disabled={!activeChannel || !inputValue || loading}
        className="px-3 py-1 bg-actae-600 hover:bg-actae-500 disabled:opacity-50 text-white rounded-md text-xs font-medium transition-colors"
      >
        {loading ? '...' : 'Apply'}
      </button>
      {replayFromCursor !== null && (
        <button
          onClick={handleReset}
          title="Show the full timeline"
          className="px-2 py-1 text-gray-400 hover:text-gray-200 text-xs transition-colors"
        >
          Show all
        </button>
      )}
    </div>
  )
}
