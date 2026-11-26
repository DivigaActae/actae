// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useState } from 'react'
import { useFlowStore } from '@/lib/flow-store'
import { getForkTree, type ForkInfo } from '@/lib/api'

export function ForkTree() {
  const { activeChannel, setActiveChannel, loadForks } = useFlowStore()
  const [tree, setTree] = useState<ForkInfo | null>(null)
  const [loading, setLoading] = useState(false)
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  useEffect(() => {
    if (!activeChannel) {
      setTree(null)
      return
    }
    setLoading(true)
    Promise.all([
      getForkTree(activeChannel),
      loadForks(),
    ])
      .then(([res]) => {
        const t = res.data ?? null
        setTree(t)
        // Auto-expand the path to the active channel
        if (t) {
          const path = findPath(t, activeChannel)
          const nextExpanded: Record<string, boolean> = {}
          path.forEach(id => { nextExpanded[id] = true })
          setExpanded(nextExpanded)
        }
      })
      .catch(() => setTree(null))
      .finally(() => setLoading(false))
  }, [activeChannel])

  if (!activeChannel) return null

  return (
    <div className="select-none">
      <div className="flex items-center justify-between px-3 py-2.5 border-b border-gray-800/50">
        <h3 className="text-[10px] font-semibold text-gray-400 uppercase tracking-widest">
          Execution Tree
        </h3>
        <div className="flex items-center gap-1">
          <button
            onClick={() => setExpanded({})}
            className="p-1 text-gray-500 hover:text-white rounded hover:bg-gray-800 transition-colors"
            title="Collapse all"
          >
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M5 15l7-7 7 7"/>
            </svg>
          </button>
          <button
            onClick={() => {
              if (!tree) return
              const allIds = collectIds(tree)
              setExpanded(Object.fromEntries(allIds.map(id => [id, true])))
            }}
            className="p-1 text-gray-500 hover:text-white rounded hover:bg-gray-800 transition-colors"
            title="Expand all"
          >
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M19 9l-7 7-7-7"/>
            </svg>
          </button>
        </div>
      </div>

      <div className="p-3">
        {loading ? (
          <div className="text-gray-500 text-xs p-2">Loading execution tree...</div>
        ) : tree ? (
          <TreeNode
            node={tree}
            activeChannel={activeChannel}
            onSelect={setActiveChannel}
            depth={0}
            expanded={expanded}
            setExpanded={setExpanded}
          />
        ) : (
          <div className="text-xs text-gray-600 p-2">No execution tree available</div>
        )}
      </div>
    </div>
  )
}

function TreeNode({
  node,
  activeChannel,
  onSelect,
  depth,
  expanded,
  setExpanded,
}: {
  node: ForkInfo
  activeChannel: string | null
  onSelect: (id: string) => void
  depth: number
  expanded: Record<string, boolean>
  setExpanded: (next: Record<string, boolean>) => void
}) {
  const hasChildren = node.children && node.children.length > 0
  const isExpanded = expanded[node.channel_id] ?? depth === 0
  const isActive = node.channel_id === activeChannel
  const isRoot = depth === 0

  function toggle(e: React.MouseEvent) {
    e.stopPropagation()
    setExpanded({ ...expanded, [node.channel_id]: !isExpanded })
  }

  return (
    <div className="relative">
      {/* Vertical connector line */}
      {depth > 0 && (
        <div
          className="absolute left-2 top-0 bottom-0 w-px bg-gray-800"
          style={{ top: -8, bottom: hasChildren && isExpanded ? 0 : 8 }}
        />
      )}

      {/* Node card */}
      <div
        onClick={() => onSelect(node.channel_id)}
        className={`relative cursor-pointer rounded-lg border transition-all group ${
          isActive
            ? 'bg-blue-600/10 border-blue-500/30 shadow-sm shadow-blue-900/10'
            : 'bg-gray-800/30 border-gray-800 hover:border-gray-700 hover:bg-gray-800/50'
        }`}
        style={{ marginLeft: depth * 16 }}
      >
        {/* Active indicator line */}
        {isActive && (
          <div className="absolute left-0 top-2 bottom-2 w-0.5 bg-blue-500 rounded-full" />
        )}

        <div className="px-3 py-2.5">
          <div className="flex items-start justify-between gap-2">
            <div className="flex items-center gap-2 min-w-0">
              {hasChildren ? (
                <button
                  onClick={toggle}
                  className={`shrink-0 text-gray-500 hover:text-gray-300 transition-transform ${isExpanded ? 'rotate-90' : ''}`}
                >
                  <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="9 18 15 12 9 6"/>
                  </svg>
                </button>
              ) : (
                <span className="w-2.5 shrink-0" />
              )}
              <span className={`shrink-0 ${isRoot ? 'text-blue-400' : 'text-purple-400'}`}>
                {isRoot ? (
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <circle cx="12" cy="12" r="10"/>
                    <polygon points="10 8 16 12 10 16 10 8"/>
                  </svg>
                ) : (
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <line x1="6" y1="3" x2="6" y2="15"/>
                    <circle cx="18" cy="6" r="3"/>
                    <circle cx="6" cy="12" r="3"/>
                    <circle cx="18" cy="18" r="3"/>
                    <path d="M6 12c0-2 2-3 6-3h3"/>
                    <path d="M15 9c3 0 3 3 3 6"/>
                  </svg>
                )}
              </span>
              <div className="min-w-0">
                <div className={`text-xs font-medium truncate ${isActive ? 'text-white' : 'text-gray-300'}`}>
                  {node.display_name || node.channel_id.slice(0, 28)}
                </div>
                {node.reason && (
                  <div className="text-[10px] text-gray-500 truncate mt-0.5">{node.reason}</div>
                )}
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2 mt-2 pl-6">
            <span className="text-[10px] text-gray-500 bg-gray-800/60 rounded px-1.5 py-0.5">
              {node.event_count} events
            </span>
            {node.latest_cursor !== null && node.latest_cursor !== undefined && (
              <span className="text-[10px] text-gray-500 bg-gray-800/60 rounded px-1.5 py-0.5">
                cursor {node.latest_cursor}
              </span>
            )}
            {!isRoot && node.forked_at_cursor > 0 && (
              <span className="text-[10px] text-purple-400/80 bg-purple-500/10 rounded px-1.5 py-0.5">
                forked @{node.forked_at_cursor}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Children */}
      {hasChildren && isExpanded && (
        <div className="mt-1 space-y-1">
          {node.children.map((child) => (
            <TreeNode
              key={child.channel_id}
              node={child}
              activeChannel={activeChannel}
              onSelect={onSelect}
              depth={depth + 1}
              expanded={expanded}
              setExpanded={setExpanded}
            />
          ))}
        </div>
      )}
    </div>
  )
}

function findPath(root: ForkInfo, targetId: string): string[] {
  if (root.channel_id === targetId) return [root.channel_id]
  for (const child of root.children || []) {
    const path = findPath(child, targetId)
    if (path.length > 0) return [root.channel_id, ...path]
  }
  return []
}

function collectIds(root: ForkInfo): string[] {
  const ids = [root.channel_id]
  for (const child of root.children || []) {
    ids.push(...collectIds(child))
  }
  return ids
}
