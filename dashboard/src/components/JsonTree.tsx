// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import React from 'react'

interface JsonTreeProps {
  data: any
  depth?: number
}

export default function JsonTree({ data, depth = 0 }: JsonTreeProps) {
  if (data === null || data === undefined) {
    return <span className="text-gray-500">null</span>
  }

  if (typeof data === 'string') {
    return <span className="text-green-400">"{data}"</span>
  }

  if (typeof data === 'number') {
    return <span className="text-blue-400">{data}</span>
  }

  if (typeof data === 'boolean') {
    return <span className="text-purple-400">{data.toString()}</span>
  }

  if (Array.isArray(data)) {
    if (data.length === 0) return <span className="text-gray-400">[]</span>
    return (
      <div className="ml-4">
        <span className="text-gray-500">[</span>
        {data.map((item, i) => (
          <div key={i} className="flex">
            <JsonTree data={item} depth={depth + 1} />
            {i < data.length - 1 && <span className="text-gray-500">,</span>}
          </div>
        ))}
        <span className="text-gray-500">]</span>
      </div>
    )
  }

  if (typeof data === 'object') {
    const keys = Object.keys(data)
    if (keys.length === 0) return <span className="text-gray-400">{'{}'}</span>
    return (
      <div className="ml-4">
        <span className="text-gray-500">{'{'}</span>
        {keys.map((key, i) => (
          <div key={key} className="flex gap-1">
            <span className="text-red-400 shrink-0">"{key}":</span>
            <JsonTree data={data[key]} depth={depth + 1} />
            {i < keys.length - 1 && <span className="text-gray-500">,</span>}
          </div>
        ))}
        <span className="text-gray-500">{'}'}</span>
      </div>
    )
  }

  return <span>{String(data)}</span>
}
