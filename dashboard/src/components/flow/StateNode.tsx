// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import React from 'react'
import { Handle, Position } from '@xyflow/react'

function StateNode({ data }: { data: any }) {
  const keys: string[] = data.keys || []
  const displayKeys = keys.slice(0, 6)
  const extra = keys.length - 6

  return (
    <div
      className="bg-gray-900 border-2 border-dashed border-purple-500 rounded-lg p-3 cursor-pointer hover:border-purple-400 transition-all"
      style={{ width: 250 }}
      onClick={data.onClick}
    >
      <Handle type="target" position={Position.Top} className="!bg-purple-500" />
      <Handle type="source" position={Position.Bottom} className="!bg-purple-500" />
      <div className="flex items-center justify-between gap-2 mb-2">
        <div className="flex items-center gap-1.5">
          <span className="w-2 h-2 rounded-full bg-purple-500" />
          <span className="text-xs font-semibold text-purple-400">State Snapshot</span>
        </div>
        <span className="text-[10px] font-mono text-gray-500 shrink-0">@{data.cursor}</span>
      </div>
      <div className="flex flex-wrap gap-1">
        {displayKeys.map((key: string) => (
          <span key={key} className="px-1.5 py-0.5 rounded-full text-[10px] bg-purple-950 text-purple-400 border border-purple-800">
            {key}
          </span>
        ))}
        {extra > 0 && (
          <span className="px-1.5 py-0.5 rounded-full text-[10px] bg-gray-800 text-gray-400">
            +{extra}
          </span>
        )}
      </div>
    </div>
  )
}

export default React.memo(StateNode)
