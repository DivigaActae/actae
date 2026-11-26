// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import React, { useState } from 'react'

interface CollapsibleTextProps {
  text: string
  limit?: number
  className?: string
}

/**
 * Shortens long text with a "show more" toggle. Because full tool results are
 * now stored uncapped, long inputs/outputs (e.g. web_search or MCP results)
 * are collapsed by default and expand fully without re-fetching.
 */
function CollapsibleText({ text, limit = 120, className = '' }: CollapsibleTextProps) {
  const [expanded, setExpanded] = useState(false)

  const str = String(text ?? '')
  if (str.length <= limit) {
    return <span className={className}>{str}</span>
  }

  const kb = str.length >= 1024 ? `${(str.length / 1024).toFixed(1)} KB` : `${str.length} chars`

  return (
    <span className={className}>
      <span className="font-mono whitespace-pre-wrap break-all text-[11px] leading-4">
        {expanded ? str : `${str.slice(0, limit)}…`}
      </span>
      <button
        onClick={(ev: React.MouseEvent) => {
          ev.stopPropagation()
          setExpanded(v => !v)
        }}
        className="ml-1.5 text-actae-400 hover:text-actae-300 text-[10px] font-medium underline underline-offset-2 decoration-dotted whitespace-nowrap"
      >
        {expanded ? 'show less' : `show more (${kb})`}
      </button>
    </span>
  )
}

export default React.memo(CollapsibleText)