// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import React from 'react'
import { Handle, Position } from '@xyflow/react'
import { summarizePayload } from '@/lib/node-summary'
import { useFlowStore } from '@/lib/flow-store'

export interface EventNodeData {
  type: string
  cursor: number
  payload: Record<string, unknown>
  timestamp: Date | string
  onClick: () => void
  isInherited?: boolean
  forkedFrom?: string
}

function getTypeColor(type: string): { bg: string; border: string; text: string } {
  const t = type.toLowerCase()
  if (/error|crash/i.test(type)) return { bg: 'bg-red-500/15', border: 'border-red-500/30', text: 'text-red-400' }
  if (/completed|done|finish|pipeline\.complete/i.test(t)) return { bg: 'bg-emerald-500/15', border: 'border-emerald-500/30', text: 'text-emerald-400' }
  if (/started|start/i.test(t)) return { bg: 'bg-blue-500/15', border: 'border-blue-500/30', text: 'text-blue-400' }
  if (/saved|checkpoint|state/i.test(t)) return { bg: 'bg-purple-500/15', border: 'border-purple-500/30', text: 'text-purple-400' }
  if (/writer/i.test(t)) return { bg: 'bg-indigo-500/15', border: 'border-indigo-500/30', text: 'text-indigo-400' }
  if (/critic/i.test(t)) return { bg: 'bg-amber-500/15', border: 'border-amber-500/30', text: 'text-amber-400' }
  if (/analyst|researcher/i.test(t)) return { bg: 'bg-cyan-500/15', border: 'border-cyan-500/30', text: 'text-cyan-400' }
  if (/^tool\./.test(t)) return { bg: 'bg-pink-500/15', border: 'border-pink-500/30', text: 'text-pink-400' }
  return { bg: 'bg-gray-700/50', border: 'border-gray-600', text: 'text-gray-300' }
}

function EventNode({ data }: { data: any }) {
  const inherited = !!data.isInherited
  const colors = getTypeColor(data.type)
  const visibleFields = useFlowStore(s => s.visibleFields)
  const summary = summarizePayload(data.payload, visibleFields)
  return (
    <div
      className={`rounded-xl p-3 cursor-pointer transition-all shadow-lg ${
        inherited
          ? 'bg-gray-900/80 border border-gray-700 opacity-70'
          : `bg-gray-900 border ${colors.border} hover:shadow-xl hover:scale-[1.02]`
      }`}
      style={{ width: 250 }}
      onClick={data.onClick}
    >
      <Handle type="target" position={Position.Left} className={inherited ? '!bg-gray-600' : '!bg-gray-400'} />
      <Handle type="source" position={Position.Right} className={inherited ? '!bg-gray-600' : '!bg-gray-400'} />
      {inherited && (
        <div className="text-[9px] text-gray-500 mb-1 truncate">
          forked from {data.forkedFrom?.slice(0, 14)}
        </div>
      )}
      <div className="flex items-center justify-between gap-2 mb-1.5">
        <span className="flex items-center gap-1.5 min-w-0">
          {data.payload?.context && (
            <span className="w-1.5 h-1.5 rounded-full bg-purple-500 shrink-0" title="Has context" />
          )}
          <span className={`px-1.5 py-0.5 rounded-md text-[10px] font-semibold border truncate ${colors.bg} ${colors.text} ${colors.border}`}>
            {data.type}
          </span>
        </span>
        <span className="text-[10px] font-mono text-gray-500 shrink-0">@{data.cursor}</span>
      </div>
      {summary.length > 0 ? (
        <div className="space-y-0.5 mb-1">
          {summary.map((row, i) => (
            <p key={i} className="text-[10px] text-gray-400 font-mono truncate leading-relaxed" title={row}>
              {row}
            </p>
          ))}
        </div>
      ) : (
        <p className="text-[10px] text-gray-600 mb-1">no details</p>
      )}
      <p className="text-[9px] text-gray-600">
        {data.timestamp instanceof Date
          ? data.timestamp.toLocaleTimeString()
          : String(data.timestamp).slice(11, 19)}
      </p>
    </div>
  )
}

export default React.memo(EventNode)
