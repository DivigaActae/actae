// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useFlowStore } from '@/lib/flow-store'

const LAYOUTS = [
  { value: 'horizontal', label: '→', title: 'Horizontal line' },
  { value: 'grid', label: '⊞', title: 'Grid (adaptive columns)' },
  { value: 'compact', label: '⇉', title: 'Compact' },
] as const

export default function LayoutSelector() {
  const layoutMode = useFlowStore(s => s.layoutMode)
  const setLayoutMode = useFlowStore(s => s.setLayoutMode)

  return (
    <div className="flex items-center gap-0.5">
      {LAYOUTS.map(l => (
        <button
          key={l.value}
          onClick={() => setLayoutMode(l.value)}
          title={l.title}
          className={`px-2 py-1 rounded-md text-xs font-medium transition-colors ${
            layoutMode === l.value
              ? 'bg-gray-700 text-white'
              : 'text-gray-500 hover:text-gray-300'
          }`}
        >
          {l.label}
        </button>
      ))}
    </div>
  )
}
