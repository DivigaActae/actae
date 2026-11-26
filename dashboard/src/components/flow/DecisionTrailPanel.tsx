// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useState } from 'react'
import { getDecisionTrail, type DecisionTrail } from '@/lib/api'
import JsonTree from '@/components/JsonTree'

/**
 * Decision trail (lineage as audit): every decision in this run traces back
 * to a fork point. Shows the ancestry chain to the origin run, the boundary
 * state the fork forked from, and the idempotent tool-execution ledger
 * rows that happened on this channel.
 */
export function DecisionTrailPanel({ channelId }: { channelId: string }) {
  const [trail, setTrail] = useState<DecisionTrail | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    getDecisionTrail(channelId)
      .then(res => {
        if (cancelled) return
        if (res.data) setTrail(res.data)
        else setError((res as any).error || 'Failed to load decision trail')
      })
      .catch((e: any) => {
        if (!cancelled) setError(e.message || 'Failed to load decision trail')
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [channelId])

  if (loading) return <div className="text-xs text-gray-500 p-2">Loading decision trail...</div>
  if (error) return <div className="text-xs text-red-400 p-2">{error}</div>
  if (!trail) return <div className="text-xs text-gray-600 p-2">No decision trail</div>

  return (
    <div className="p-3 space-y-3">
      {/* Lineage chain */}
      <div className="space-y-1">
        <h4 className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider">Lineage</h4>
        <div className="space-y-0.5">
          {trail.ancestry.map((hop, i) => (
            <div key={`${hop.channel_id}-${i}`} className="flex items-center gap-1.5 text-[11px]">
              {i > 0 && <span className="text-gray-600 shrink-0">└</span>}
              <span className="truncate text-gray-300 max-w-[60%]">
                {hop.display_name || hop.channel_id}
              </span>
              {hop.forked_at_cursor > 0 && (
                <span className="text-purple-400 font-mono text-[10px] shrink-0">@{hop.forked_at_cursor}</span>
              )}
              {hop.reason && <span className="text-gray-600 text-[10px] truncate">{hop.reason}</span>}
            </div>
          ))}
        </div>
        <div className="text-[10px] text-gray-600 pt-0.5">
          origin run: <span className="font-mono text-gray-500">{trail.origin_run_id}</span>
        </div>
      </div>

      {/* Boundary state */}
      {trail.boundary && (
        <div>
          <h4 className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1">
            Fork boundary state
          </h4>
          <div className="bg-gray-800/30 rounded border border-gray-800 p-2 text-[10px] text-gray-500 mb-1">
            {trail.boundary.channel_id} @{trail.boundary.cursor}
          </div>
          <div className="max-h-32 overflow-y-auto bg-gray-800/30 rounded border border-gray-800 p-2">
            <JsonTree data={trail.boundary.state} />
          </div>
        </div>
      )}

      {/* Tool-execution ledger */}
      <div>
        <h4 className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1.5">
          Tool executions ({trail.executions.length})
        </h4>
        {trail.executions.length === 0 ? (
          <div className="text-xs text-gray-600">No tool executions on this fork</div>
        ) : (
          <div className="space-y-1.5">
            {trail.executions.map((ex, i) => (
              <div key={ex.id || i} className="bg-gray-800/40 rounded-lg border border-gray-800 p-2">
                <div className="flex items-center justify-between gap-2">
                  <span className="text-[11px] font-medium text-white truncate">{ex.tool_name}</span>
                  <ExecutionStatusBadge status={ex.status} />
                </div>
                <div className="text-[10px] text-gray-500 font-mono truncate mt-0.5">{ex.key_name}</div>
                {(ex.started_cursor !== null && ex.started_cursor !== undefined) && (
                  <div className="text-[10px] text-gray-600 mt-0.5">
                    @{ex.started_cursor}
                    {ex.completed_cursor !== null && ex.completed_cursor !== undefined && ` → @${ex.completed_cursor}`}
                  </div>
                )}
                {ex.result !== null && ex.result !== undefined && (
                  <div className="mt-1 max-h-20 overflow-y-auto bg-gray-900/40 rounded p-1.5">
                    <JsonTree data={ex.result} />
                  </div>
                )}
                {ex.error !== null && ex.error !== undefined && (
                  <div className="mt-1 max-h-20 overflow-y-auto bg-red-950/40 rounded p-1.5 text-red-300 text-[10px]">
                    <JsonTree data={ex.error} />
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function ExecutionStatusBadge({ status }: { status: string }) {
  const color =
    status === 'completed'
      ? 'bg-emerald-500/10 text-emerald-400'
      : status === 'running' || status === 'claimed'
        ? 'bg-blue-500/10 text-blue-400'
        : status === 'failed'
          ? 'bg-red-500/10 text-red-400'
          : 'bg-gray-500/10 text-gray-400'
  return (
    <span className={`text-[9px] px-1.5 py-0.5 rounded uppercase tracking-wide ${color}`}>
      {status}
    </span>
  )
}
