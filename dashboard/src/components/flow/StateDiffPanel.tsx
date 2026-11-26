// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useState } from 'react'
import { diffChannelStates, type StateDiff, type StateDiffEntry } from '@/lib/api'
import { buildDiffTree, summarizeDiff, kindLabel, type DiffTreeNode } from '@/lib/state-diff'
import JsonTree from '@/components/JsonTree'

/**
 * Structural state diff between the active fork and a sibling fork.
 *
 * Answers "what did fork B know that fork A didn't at the point of
 * divergence" — the counterfactual-debugging view. Loads the diff from
 * the server, shows the shared ancestor + divergence point, and renders
 * added/removed/changed entries as a colored tree.
 */
export function StateDiffPanel({
  activeChannel,
  compareChannel,
}: {
  activeChannel: string
  compareChannel: string
}) {
  const [diff, setDiff] = useState<StateDiff | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    diffChannelStates(activeChannel, compareChannel)
      .then(res => {
        if (cancelled) return
        if (res.data) setDiff(res.data)
        else setError((res as any).error || 'Failed to load state diff')
      })
      .catch((e: any) => {
        if (!cancelled) setError(e.message || 'Failed to load state diff')
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [activeChannel, compareChannel])

  if (loading) return <div className="text-xs text-gray-500 p-2">Diffing states...</div>
  if (error) return <div className="text-xs text-red-400 p-2">{error}</div>
  if (!diff) return <div className="text-xs text-gray-600 p-2">No diff available</div>

  const summary = summarizeDiff(diff.entries)
  const tree = buildDiffTree(diff.entries)

  return (
    <div className="p-3 space-y-2">
      {(diff.left_diverged_at_cursor !== null || diff.right_diverged_at_cursor !== null) && (
        <div className="bg-gray-800/50 rounded px-2 py-1.5 text-[10px] text-gray-400">
          <span className="text-emerald-400 font-mono">@{diff.left_diverged_at_cursor ?? '—'}</span>
          {' '}·{' '}
          <span className="text-sky-400 font-mono">@{diff.right_diverged_at_cursor ?? '—'}</span>
          {' '}divergence (left · right)
        </div>
      )}

      {diff.truncated && (
        <div className="bg-amber-500/10 rounded px-2 py-1.5 text-[10px] text-amber-400">
          Diff truncated — showing {diff.entries.length} of {diff.entry_count_total} differing paths.
        </div>
      )}

      {diff.common && (
        <div className="bg-gray-800/50 rounded px-2 py-1.5 text-[10px] text-gray-500">
          Shared ancestor: <span className="text-gray-300">{diff.common.channel_id}</span> @{diff.common.cursor}
        </div>
      )}

      <div className="flex gap-2 text-[10px]">
        <span className="px-1.5 py-0.5 rounded bg-emerald-500/10 text-emerald-400">{summary.added} added</span>
        <span className="px-1.5 py-0.5 rounded bg-red-500/10 text-red-400">{summary.removed} removed</span>
        <span className="px-1.5 py-0.5 rounded bg-amber-500/10 text-amber-400">{summary.changed} changed</span>
      </div>

      {tree.length === 0 ? (
        <div className="text-xs text-gray-600">No structural differences.</div>
      ) : (
        <div className="space-y-1 max-h-96 overflow-y-auto">
          {tree.map((node, i) => (
            <DiffRow key={i} node={node} depth={0} />
          ))}
        </div>
      )}
    </div>
  )
}

function DiffRow({ node, depth }: { node: DiffTreeNode; depth: number }) {
  return (
    <div>
      {node.entry ? (
        <div
          className={`flex items-start gap-1.5 rounded px-1.5 py-1 text-[11px] font-mono ${
            node.entry.kind === 'added'
              ? 'bg-emerald-500/10 text-emerald-300'
              : node.entry.kind === 'removed'
                ? 'bg-red-500/10 text-red-300'
                : 'bg-amber-500/10 text-amber-300'
          }`}
          style={{ marginLeft: depth * 12 }}
        >
          <span className="shrink-0 uppercase text-[9px] opacity-70">{kindLabel(node.entry.kind)}</span>
          <span className="shrink-0">{node.key}:</span>
          <span className="min-w-0 break-all">
            <DiffValue entry={node.entry} />
          </span>
        </div>
      ) : (
        <div
          className="text-[11px] font-medium text-gray-300 py-0.5 truncate"
          style={{ marginLeft: depth * 12 }}
        >
          {node.key}
        </div>
      )}
      {node.children.map((child, i) => (
        <DiffRow key={i} node={child} depth={depth + 1} />
      ))}
    </div>
  )
}

function DiffValue({ entry }: { entry: StateDiffEntry }) {
  if (entry.kind === 'added') return <ValueInline value={entry.right} />
  if (entry.kind === 'removed') return <ValueInline value={entry.left} />
  return (
    <span className="flex items-center gap-1 flex-wrap">
      <ValueInline value={entry.left} />
      <span className="text-gray-500">→</span>
      <ValueInline value={entry.right} />
    </span>
  )
}

function ValueInline({ value }: { value: unknown }) {
  if (value === null || value === undefined) return <span className="text-gray-600">null</span>
  if (typeof value === 'object') return <JsonTree data={value} />
  if (typeof value === 'string') return <span>"{value}"</span>
  return <span>{String(value)}</span>
}
