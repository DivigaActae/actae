// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useCallback, useRef, useEffect, useState } from 'react'
import { useFlowStore } from '@/lib/flow-store'
import { filterPayload } from '@/lib/payload-fields'
import JsonTree from '@/components/JsonTree'
import { DecisionTrailPanel } from '@/components/flow/DecisionTrailPanel'
import { StateDiffPanel } from '@/components/flow/StateDiffPanel'

interface InspectorPanelProps {
  width: number
  onClose: () => void
  onResize: (width: number) => void
  liveSlot?: React.ReactNode
}

export default function InspectorPanel({ width, onClose, onResize, liveSlot }: InspectorPanelProps) {
  const events = useFlowStore(s => s.events)
  const stateSnapshot = useFlowStore(s => s.stateSnapshot)
  const selectedNode = useFlowStore(s => s.selectedNode)
  const activeChannel = useFlowStore(s => s.activeChannel)
  const allForks = useFlowStore(s => s.forks)
  const forkedAtCursor = useFlowStore(s => s.forkedAtCursor)
  const visibleFields = useFlowStore(s => s.visibleFields)
  const dragging = useRef(false)
  const startX = useRef(0)
  const startWidth = useRef(0)

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    dragging.current = true
    startX.current = e.clientX
    startWidth.current = width
    e.preventDefault()
  }, [width])

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!dragging.current) return
      const delta = startX.current - e.clientX
      onResize(startWidth.current + delta)
    }
    const handleMouseUp = () => {
      dragging.current = false
    }
    document.addEventListener('mousemove', handleMouseMove)
    document.addEventListener('mouseup', handleMouseUp)
    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [onResize])

  // Event type breakdown
  const typeCounts: Record<string, number> = {}
  events.forEach(evt => {
    const t = evt.type || 'unknown'
    typeCounts[t] = (typeCounts[t] || 0) + 1
  })
  const sortedTypes = Object.entries(typeCounts)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 10)

  const latestCursor = events.length > 0
    ? Math.max(...events.map(e => e.cursor))
    : null

  const eventsLoaded = events.filter(e => !e.isState).length

  const currentFork = allForks.find(b => b.channel_id === activeChannel)
  const forks = allForks.filter(b => b.parent_channel_id === activeChannel)

  return (
    <div
      className="shrink-0 border-l border-gray-800 bg-gray-900 overflow-hidden h-full flex"
      style={{ width }}
    >
      {/* Drag handle */}
      <div
        className="w-1 shrink-0 cursor-col-resize hover:bg-actae-500 active:bg-actae-400 bg-transparent transition-colors"
        onMouseDown={handleMouseDown}
      />

      {/* Content */}
      <div className="flex-1 overflow-y-auto min-w-0">
        {/* Header */}
        <div className="sticky top-0 z-20 flex items-center justify-between border-b border-gray-800 bg-gray-900/95 p-4 backdrop-blur">
          <div><h2 className="text-sm font-semibold text-white">Inspector</h2><p className="mt-0.5 text-[10px] text-gray-600">Selected evidence and run context</p></div>
          <div className="flex items-center gap-2">
            {liveSlot}
            <button
              onClick={onClose}
              className="flex h-7 w-7 items-center justify-center rounded-md text-gray-500 hover:bg-gray-800 hover:text-white"
              aria-label="Close inspector"
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 6 6 18M6 6l12 12"/></svg>
            </button>
          </div>
        </div>

        {selectedNode ? (
          <div className="border-b border-blue-500/15 bg-blue-500/[0.03] p-4">
            <div className="mb-2 flex items-center justify-between"><h3 className="text-[10px] font-semibold uppercase tracking-wider text-blue-400">Selected Event @{selectedNode.cursor}</h3><span className="rounded bg-blue-500/10 px-1.5 py-0.5 text-[9px] font-semibold text-blue-300">evidence</span></div>
            <div className="mb-3 truncate text-xs font-semibold text-gray-200">{selectedNode.type}</div>
            <SelectedEventDetail
              data={selectedNode.payload && Object.keys(selectedNode.payload).length > 0 ? filterPayload(selectedNode.payload, visibleFields) : selectedNode.state || {}}
              contextDelta={(selectedNode.payload as Record<string, unknown>)?.context as Record<string, unknown> | undefined}
              cumulativeContext={selectedNode.cumulativeContext}
              cursor={selectedNode.cursor}
            />
          </div>
        ) : (
          <div className="border-b border-gray-800 p-4">
            <h3 className="text-[10px] font-semibold uppercase tracking-wider text-gray-400">No step selected</h3>
            <p className="mt-1.5 text-xs leading-5 text-gray-500">
              Click any step in the timeline to inspect its payload, context, and state. This
              run&apos;s stats, forks, and lineage are under <span className="font-semibold text-gray-400">Run details</span> below.
            </p>
          </div>
        )}

        {/* Everything else lives behind one disclosure so a run reads as a
            single timeline until the operator asks for the deeper context. */}
        <details className="group border-t border-gray-800">
          <summary className="flex cursor-pointer select-none list-none items-center justify-between px-4 py-3 hover:bg-white/[0.02] [&::-webkit-details-marker]:hidden">
            <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-400">Run details</span>
            <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="text-gray-600 transition-transform group-open:rotate-180"><polyline points="6 9 12 15 18 9"/></svg>
          </summary>
          <div className="space-y-5 px-4 pb-5">
            {stateSnapshot && (
              <div>
                <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-purple-400">
                  Latest State @{stateSnapshot.cursor}
                </h3>
                <LatestStateDetail data={stateSnapshot.state} />
              </div>
            )}

            <div>
              <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-gray-400">Channel Stats</h3>
              <div className="grid grid-cols-4 gap-1.5">
                <div className="bg-blue-600/10 p-2 rounded-lg border border-blue-500/20">
                  <div className="text-lg font-bold text-blue-400">{eventsLoaded}</div>
                  <div className="text-[10px] text-blue-400/70 uppercase tracking-wider font-medium">events loaded</div>
                </div>
                <div className="bg-emerald-600/10 p-2 rounded-lg border border-emerald-500/20">
                  <div className="text-lg font-bold text-emerald-400">{latestCursor ?? '—'}</div>
                  <div className="text-[10px] text-emerald-400/70 uppercase tracking-wider font-medium">latest cursor</div>
                </div>
                <div className="bg-purple-600/10 p-2 rounded-lg border border-purple-500/20">
                  <div className="text-lg font-bold text-purple-400">@{stateSnapshot?.cursor ?? '—'}</div>
                  <div className="text-[10px] text-purple-400/70 uppercase tracking-wider font-medium">state cursor</div>
                </div>
                <div className="bg-amber-600/10 p-2 rounded-lg border border-amber-500/20">
                  <div className="text-lg font-bold text-amber-400">{stateSnapshot ? 'Yes' : 'No'}</div>
                  <div className="text-[10px] text-amber-400/70 uppercase tracking-wider font-medium">has state</div>
                </div>
              </div>
            </div>

            {sortedTypes.length > 0 && (
              <div>
                <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-gray-400">Event Types</h3>
                <div className="space-y-1">
                  {sortedTypes.map(([type, count]) => (
                    <div key={type} className="flex items-center justify-between text-xs">
                      <span className="truncate text-gray-300 max-w-[70%]">{type}</span>
                      <span className="text-gray-500 font-mono ml-2">{count}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div>
              <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-gray-400">Current Fork</h3>
              <div className="bg-gray-800/30 rounded-lg border border-gray-800 p-3">
                <div className="text-xs font-medium text-white truncate mb-1">
                  {currentFork?.display_name || activeChannel || '—'}
                </div>
                <div className="text-[10px] text-gray-500 font-mono truncate mb-2">
                  {activeChannel || '—'}
                </div>
                <div className="flex items-center gap-2 text-[10px]">
                  <span className="text-gray-500">{eventsLoaded} events</span>
                  {forkedAtCursor !== null && forkedAtCursor > 0 && (
                    <span className="text-purple-400">forked at @{forkedAtCursor}</span>
                  )}
                </div>
              </div>
            </div>

            <div>
              <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-gray-400">Forks</h3>
              {forks.length === 0 ? (
                <div className="text-xs text-gray-500">No forks from this fork</div>
              ) : (
                <div className="space-y-2">
                  {forks.map(fork => (
                    <div key={fork.channel_id} className="bg-gray-800/30 rounded-lg border border-gray-800 p-2.5">
                      <div className="text-xs font-medium text-white truncate">
                        {fork.display_name || fork.channel_id.slice(0, 24)}
                      </div>
                      <div className="text-[10px] text-purple-400 mt-0.5">
                        Forked at @{fork.forked_at_cursor}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* State diff: what did the other fork know that this one didn't */}
            {activeChannel && forks.length > 0 && (
              <div>
                <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-purple-400">State Diff</h3>
                <StateDiffPicker
                  activeChannel={activeChannel}
                  forks={forks.map(f => ({ id: f.channel_id, label: f.display_name || f.channel_id.slice(0, 24) }))}
                />
              </div>
            )}

            {/* Decision trail: lineage as audit */}
            {activeChannel && (
              <div>
                <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-purple-400">Decision Trail</h3>
                <DecisionTrailPanel channelId={activeChannel} />
              </div>
            )}
          </div>
        </details>

      </div>
    </div>
  )
}

function SelectedEventDetail({
  data,
  contextDelta,
  cumulativeContext,
  cursor,
}: {
  data: Record<string, unknown>
  contextDelta?: Record<string, unknown>
  cumulativeContext?: Record<string, unknown>
  cursor?: number
}) {
  const [view, setView] = useState<'text' | 'json'>('text')

  const payloadWithoutContext = { ...data }
  delete payloadWithoutContext.context

  return (
    <div className="max-h-80 overflow-y-auto">
      <div className="flex items-center justify-end mb-2">
        <div className="flex items-center gap-1 bg-gray-800 rounded p-0.5">
          <button
            onClick={() => setView('text')}
            className={`px-2 py-0.5 text-[10px] font-medium rounded transition-colors ${
              view === 'text' ? 'bg-gray-700 text-white' : 'text-gray-500 hover:text-gray-300'
            }`}
          >
            Text
          </button>
          <button
            onClick={() => setView('json')}
            className={`px-2 py-0.5 text-[10px] font-medium rounded transition-colors ${
              view === 'json' ? 'bg-gray-700 text-white' : 'text-gray-500 hover:text-gray-300'
            }`}
          >
            JSON
          </button>
        </div>
      </div>

      <div className="text-xs space-y-3">
        {/* Context Delta — what this step contributed */}
        {contextDelta && Object.keys(contextDelta).length > 0 && (
          <div className="bg-purple-900/20 rounded border border-purple-500/30 p-2.5">
            <h4 className="text-[10px] font-semibold text-purple-400 uppercase tracking-wider mb-1.5">
              Context Delta
            </h4>
            {view === 'json' ? (
              <div className="bg-gray-800/30 rounded border border-gray-800 p-2">
                <JsonTree data={contextDelta} />
              </div>
            ) : (
              <ContextBlock data={contextDelta} />
            )}
          </div>
        )}

        {/* Full Context — reconstructed cumulative state up to this cursor */}
        {cumulativeContext && Object.keys(cumulativeContext).length > 0 && (
          <div className="bg-purple-900/30 rounded border border-purple-500/40 p-2.5">
            <h4 className="text-[10px] font-semibold text-purple-300 uppercase tracking-wider mb-1.5">
              Full Context {cursor !== undefined ? `@${cursor}` : ''}
            </h4>
            {view === 'json' ? (
              <div className="bg-gray-800/30 rounded border border-gray-800 p-2">
                <JsonTree data={cumulativeContext} />
              </div>
            ) : (
              <ContextBlock data={cumulativeContext} />
            )}
          </div>
        )}

        {/* Standard Input / Output / Details */}
        {view === 'json' ? (
          <div className="bg-gray-800/30 rounded border border-gray-800 p-2">
            <JsonTree data={payloadWithoutContext} />
          </div>
        ) : (
          <FormattedPayload data={payloadWithoutContext} />
        )}
      </div>
    </div>
  )
}

function ContextBlock({ data }: { data: Record<string, unknown> }) {
  return (
    <div className="space-y-1">
      {Object.entries(data).map(([key, value]) => (
        <div key={key} className="flex items-start gap-2">
          <span className="text-gray-500 shrink-0 font-mono text-[10px]">{key}:</span>
          <PayloadValue value={value} className="text-gray-300 text-[11px]" compact />
        </div>
      ))}
    </div>
  )
}

function LatestStateDetail({ data }: { data: Record<string, unknown> }) {
  const [view, setView] = useState<'text' | 'json'>('text')

  return (
    <div className="max-h-80 overflow-y-auto">
      <div className="flex items-center justify-end mb-2">
        <div className="flex items-center gap-1 bg-gray-800 rounded p-0.5">
          <button
            onClick={() => setView('text')}
            className={`px-2 py-0.5 text-[10px] font-medium rounded transition-colors ${
              view === 'text' ? 'bg-gray-700 text-white' : 'text-gray-500 hover:text-gray-300'
            }`}
          >
            Text
          </button>
          <button
            onClick={() => setView('json')}
            className={`px-2 py-0.5 text-[10px] font-medium rounded transition-colors ${
              view === 'json' ? 'bg-gray-700 text-white' : 'text-gray-500 hover:text-gray-300'
            }`}
          >
            JSON
          </button>
        </div>
      </div>

      {view === 'json' ? (
        <div className="text-xs bg-gray-800/30 rounded border border-gray-800 p-2">
          <JsonTree data={data} />
        </div>
      ) : (
        <div className="text-xs space-y-2">
          <FormattedPayload data={data} />
        </div>
      )}
    </div>
  )
}

function FormattedPayload({ data }: { data: Record<string, unknown> }) {
  const input = data.input
  const output = data.output
  const otherKeys = Object.keys(data).filter(k => k !== 'input' && k !== 'output')

  return (
    <div className="space-y-3">
      {hasDisplayValue(input) && (
        <div className="bg-gray-800/30 rounded border border-gray-800 p-2.5">
          <div className="text-[10px] font-semibold text-blue-400 uppercase tracking-wider mb-1.5">Input</div>
          <PayloadValue value={input} />
        </div>
      )}

      {hasDisplayValue(output) && (
        <div className="bg-gray-800/30 rounded border border-gray-800 p-2.5">
          <div className="text-[10px] font-semibold text-emerald-400 uppercase tracking-wider mb-1.5">Output</div>
          <PayloadValue value={output} />
        </div>
      )}

      {otherKeys.length > 0 && (
        <div className="bg-gray-800/30 rounded border border-gray-800 p-2.5">
          <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1.5">Details</div>
          <div className="space-y-1">
            {otherKeys.map(k => (
              <div key={k} className="flex items-start gap-2">
                <span className="text-gray-500 shrink-0">{k}:</span>
                <PayloadValue value={data[k]} compact />
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function hasDisplayValue(value: unknown): boolean {
  if (value === null || value === undefined) return false
  if (typeof value === 'string') return value.length > 0
  if (Array.isArray(value)) return value.length > 0
  if (typeof value === 'object') return Object.keys(value as Record<string, unknown>).length > 0
  return true
}

function PayloadValue({ value, compact = false, className = '' }: { value: unknown; compact?: boolean; className?: string }) {
  if (typeof value === 'string') {
    return <div className={`${compact ? 'break-words' : 'whitespace-pre-wrap break-words leading-relaxed'} text-gray-300 ${className}`}>{value}</div>
  }
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return <div className="space-y-1">{Object.entries(value as Record<string, unknown>).map(([key, child]) => (
      <div key={key} className="grid grid-cols-[auto_minmax(0,1fr)] items-start gap-2">
        <span className="shrink-0 text-gray-500">{key}:</span>
        <PayloadValue value={child} compact className={className} />
      </div>
    ))}</div>
  }
  return <span className={`break-words text-gray-300 ${className}`}>{formatValue(value)}</span>
}

function formatValue(v: unknown): string {
  if (v === null) return 'null'
  if (v === undefined) return 'undefined'
  if (typeof v === 'string') return v
  if (typeof v === 'number' || typeof v === 'boolean') return String(v)
  if (v instanceof Date) return v.toISOString()
  try {
    return JSON.stringify(v)
  } catch {
    return String(v)
  }
}

function StateDiffPicker({
  activeChannel,
  forks,
}: {
  activeChannel: string
  forks: { id: string; label: string }[]
}) {
  const [compareChannel, setCompareChannel] = useState<string | null>(null)

  return (
    <div className="space-y-2">
      <select
        value={compareChannel || ''}
        onChange={(e) => setCompareChannel(e.target.value || null)}
        className="w-full bg-gray-800/50 border border-gray-700 rounded-lg px-2 py-1.5 text-xs text-gray-200 focus:outline-none focus:border-purple-500 transition-colors"
      >
        <option value="">Compare against a fork...</option>
        {forks.map(f => (
          <option key={f.id} value={f.id}>{f.label}</option>
        ))}
      </select>
      {compareChannel ? (
        <StateDiffPanel activeChannel={activeChannel} compareChannel={compareChannel} />
      ) : (
        <div className="text-[10px] text-gray-600">
          Pick a fork to see what each fork knew that the other didn't.
        </div>
      )}
    </div>
  )
}
