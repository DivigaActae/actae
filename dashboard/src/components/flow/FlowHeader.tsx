// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useMemo, useRef, useState } from 'react'
import Link from 'next/link'
import { useFlowStore } from '@/lib/flow-store'
import { computeTypeCounts } from '@/lib/flow-helpers'
import { computeFieldCounts } from '@/lib/payload-fields'
import { gridColumnsForCount, nodeTypeKey } from '@/lib/flow-layout'
import { getChannelMetadata, buildChannelExport, downloadJsonFile, copyToClipboard, updateChannelMetadata, type ChannelMetadata } from '@/lib/api'
import LayoutSelector from './LayoutSelector'
import ReplayControls from './ReplayControls'
import EventTypeFilter from '@/components/EventTypeFilter'
import VisualizationSelector from './VisualizationSelector'

const RECENT_ACTIVITY_MS = 60_000

function getExecutionStatus(events: { type: string; timestamp?: Date | string }[], liveConnected: boolean, now: number) {
  if (events.length === 0) return 'idle'
  const lastEvent = events[events.length - 1]
  const last = lastEvent?.type?.toLowerCase() || ''
  if (last.includes('complete') || last.includes('done') || last.includes('finish')) return 'completed'
  if (last.includes('error') || last.includes('crash') || last.includes('fail')) return 'failed'
  if (!liveConnected) return 'offline'
  const timestamp = lastEvent?.timestamp ? new Date(lastEvent.timestamp).getTime() : NaN
  return Number.isFinite(timestamp) && now - timestamp <= RECENT_ACTIVITY_MS ? 'active' : 'stale'
}

function formatDuration(ms: number) {
  if (ms < 1000) return `${ms}ms`
  const seconds = Math.floor(ms / 1000)
  const mins = Math.floor(seconds / 60)
  const hrs = Math.floor(mins / 60)
  if (hrs > 0) return `${hrs}h ${mins % 60}m`
  if (mins > 0) return `${mins}m ${seconds % 60}s`
  return `${seconds}s`
}

function formatTime(ts: Date | string) {
  const d = typeof ts === 'string' ? new Date(ts) : ts
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function truncateId(id: string, max = 26) {
  return id.length > max ? `…${id.slice(-(max - 1))}` : id
}

export default function FlowHeader({ channelSelector }: { channelSelector?: React.ReactNode }) {
  const activeChannel = useFlowStore(s => s.activeChannel)
  const events = useFlowStore(s => s.events)
  const visibleEventTypes = useFlowStore(s => s.visibleEventTypes)
  const setVisibleEventTypes = useFlowStore(s => s.setVisibleEventTypes)
  const visibleFields = useFlowStore(s => s.visibleFields)
  const setVisibleFields = useFlowStore(s => s.setVisibleFields)
  const isLive = useFlowStore(s => s.isLive)
  const connection = useFlowStore(s => s.connection)
  const [metadata, setMetadata] = useState<ChannelMetadata | null>(null)
  const [now, setNow] = useState(() => Date.now())
  const [exportState, setExportState] = useState<{ status: 'idle' | 'busy' | 'done' | 'error'; kind?: 'download' | 'copy'; message?: string }>({ status: 'idle' })
  const [menuOpen, setMenuOpen] = useState(false)
  const [editingTitle, setEditingTitle] = useState(false)
  const [titleDraft, setTitleDraft] = useState('')
  const [renameError, setRenameError] = useState<string | null>(null)
  const renameInFlight = useRef(false)
  const menuRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 15_000)
    return () => window.clearInterval(timer)
  }, [])

  useEffect(() => {
    if (!menuOpen) return
    const onMouseDown = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setMenuOpen(false)
    }
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setMenuOpen(false) }
    document.addEventListener('mousedown', onMouseDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onMouseDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [menuOpen])

  const typeCounts = useMemo(() => computeTypeCounts(events), [events])
  const fieldCounts = useMemo(() => computeFieldCounts(events), [events])
  const layoutMode = useFlowStore(s => s.layoutMode)
  const visualizationMode = useFlowStore(s => s.visualizationMode)

  // Grid columns adapt to the number of nodes actually displayed (after the
  // type filter), so the hint reflects what the canvas will render.
  const gridVisibleCount = useMemo(() => {
    if (!visibleEventTypes) return events.length
    return events.filter(e => visibleEventTypes.has(nodeTypeKey(e))).length
  }, [events, visibleEventTypes])
  const gridCols = gridColumnsForCount(gridVisibleCount)

  useEffect(() => {
    if (!activeChannel) {
      setMetadata(null)
      return
    }
    getChannelMetadata(activeChannel).then(res => {
      setMetadata(res.data ?? null)
    }).catch(() => setMetadata(null))
  }, [activeChannel])

  const title = metadata?.display_name || activeChannel || 'Untitled execution'
  const status = getExecutionStatus(events, isLive && connection.authenticated, now)
  const executionId = activeChannel?.slice(-10) || '—'

  const nonStateEvents = events.filter(e => !e.isState)
  const startTs = nonStateEvents[0]?.timestamp
  const endTs = nonStateEvents[nonStateEvents.length - 1]?.timestamp
  const durationMs = startTs && endTs
    ? new Date(endTs).getTime() - new Date(startTs).getTime()
    : 0

  const statusBadge = {
    completed: { text: 'Completed', class: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/25' },
    failed: { text: 'Failed', class: 'bg-red-500/15 text-red-400 border-red-500/25' },
    active: { text: 'Active', class: 'bg-blue-500/15 text-blue-400 border-blue-500/25' },
    stale: { text: 'No recent activity', class: 'bg-amber-500/15 text-amber-300 border-amber-500/25' },
    offline: { text: 'Offline', class: 'bg-red-500/15 text-red-300 border-red-500/25' },
    idle: { text: 'Idle', class: 'bg-gray-500/15 text-gray-400 border-gray-500/25' },
  }[status]

  function startRename() {
    if (!activeChannel) return
    setRenameError(null)
    setTitleDraft(metadata?.display_name || activeChannel)
    setEditingTitle(true)
  }

  async function saveRename() {
    if (renameInFlight.current) return
    const name = titleDraft.trim()
    setEditingTitle(false)
    if (!name || !activeChannel || name === (metadata?.display_name || '')) return
    renameInFlight.current = true
    const { error } = await updateChannelMetadata({ channel_id: activeChannel, display_name: name })
    renameInFlight.current = false
    if (error) {
      setRenameError(error)
      window.setTimeout(() => setRenameError(null), 4000)
      return
    }
    setRenameError(null)
    setMetadata(prev => (prev ? { ...prev, display_name: name } : prev))
  }

  // The export mirrors the current filtered view: the store's loaded timeline
  // (exactly what the canvas renders) plus the active Type and Field filters.
  const runExport = async (kind: 'download' | 'copy') => {
    const state = useFlowStore.getState()
    const channelId = state.activeChannel
    if (!channelId || exportState.status === 'busy') return
    setExportState({ status: 'busy', kind })
    try {
      const { data, error } = await buildChannelExport(channelId, {
        events: state.events,
        visibleEventTypes: state.visibleEventTypes,
        visibleFields: state.visibleFields,
      })
      if (!data || error) {
        throw new Error(error || 'Export failed')
      }
      if (kind === 'copy') {
        await copyToClipboard(JSON.stringify(data, null, 2))
      } else {
        downloadJsonFile(`${channelId}.json`, data)
      }
      setExportState({
        status: 'done',
        kind,
        message: kind === 'copy' ? `Copied ${data.event_count} events` : `Exported ${data.event_count} events`,
      })
    } catch (e: any) {
      setExportState({ status: 'error', kind, message: e.message || 'Export failed' })
    }
  }

  const exportLabel = (kind: 'download' | 'copy') => {
    const { status, kind: k } = exportState
    if (status === 'busy') return kind === 'download' ? 'Exporting…' : 'Copying…'
    if (status === 'done' && k === kind) return kind === 'download' ? 'Exported' : 'Copied'
    if (status === 'error' && k === kind) return kind === 'download' ? 'Export failed' : 'Copy failed'
    return kind === 'download' ? 'Export JSON' : 'Copy JSON'
  }

  const exportTitle = (kind: 'download' | 'copy') => {
    if (exportState.status === 'done' && exportState.kind === kind) return exportState.message
    if (exportState.status === 'error' && exportState.kind === kind) return exportState.message
    return kind === 'download'
      ? 'Download this run as a JSON file (respects the current Type and Field filters)'
      : 'Copy this run as JSON to the clipboard (respects the current Type and Field filters)'
  }

  const parentId = metadata?.parent_channel_id ?? null

  return (
    <div className="shrink-0 border-b border-gray-800 bg-gray-900/95 backdrop-blur relative z-40">
      <div className="px-4 py-2 flex items-center justify-between gap-4">
        <div className="flex flex-col min-w-0 gap-0.5">
          {/* Breadcrumb */}
          <div className="text-[10px] text-gray-500 flex items-center gap-1 min-w-0">
            <Link href="/dashboard/runs/" className="hover:text-gray-300 transition-colors shrink-0">Runs</Link>
            <span>/</span>
            {parentId && (
              <>
                <Link href={`/dashboard/flow/?channel=${encodeURIComponent(parentId)}`} className="hover:text-gray-300 transition-colors font-mono truncate max-w-[160px]" title={parentId}>{truncateId(parentId)}</Link>
                <span>/</span>
              </>
            )}
            <span className="text-gray-400 font-mono truncate" title={activeChannel ?? undefined}>{executionId}</span>
          </div>

          {/* Title */}
          <div className="flex items-center gap-2 min-w-0">
            {editingTitle ? (
              <input
                autoFocus
                value={titleDraft}
                onChange={e => setTitleDraft(e.target.value)}
                onBlur={saveRename}
                onKeyDown={e => {
                  if (e.key === 'Enter') saveRename()
                  if (e.key === 'Escape') setEditingTitle(false)
                }}
                aria-label="Run name"
                className="min-w-0 max-w-[420px] rounded-md border border-actae-400/40 bg-gray-950 px-2 py-0.5 text-base font-semibold text-white outline-none"
              />
            ) : (
              <h1 className="text-base font-semibold text-white truncate tracking-tight">{title}</h1>
            )}
            {activeChannel && !editingTitle && (
              <button onClick={startRename} className="text-gray-500 hover:text-gray-300 transition-colors shrink-0" title="Rename run" aria-label="Rename run">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
                  <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
                </svg>
              </button>
            )}
            <span className={`px-1.5 py-0.5 rounded-full text-[10px] font-semibold border ${statusBadge.class}`}>
              {statusBadge.text}
            </span>
          </div>
          {renameError && <span className="text-[10px] text-red-400 truncate" title={renameError}>Rename failed: {renameError}</span>}
        </div>

        <div className="flex items-center gap-3 text-[10px] text-gray-400 shrink-0">
          {startTs && (
            <div className="flex items-center gap-1">
              <span className="text-gray-500">Started</span>
              <span className="text-gray-300">{formatTime(startTs)}</span>
            </div>
          )}
          {durationMs > 0 && (
            <div className="flex items-center gap-1">
              <span className="text-gray-500">Duration</span>
              <span className="text-gray-300">{formatDuration(durationMs)}</span>
            </div>
          )}
        </div>
      </div>

      {/* Toolbar */}
      <div className="px-4 py-1.5 flex items-center justify-between gap-4 border-t border-gray-800/50">
        <div className="flex items-center gap-2 min-w-0">
          <VisualizationSelector />
          {channelSelector && (
            <div className="flex items-center gap-1 bg-gray-800 rounded-lg p-0.5 border border-gray-700/50 pr-2 min-w-0 max-w-[min(52vw,620px)]">
              <span className="text-[10px] text-gray-500 px-2 uppercase tracking-wider font-semibold shrink-0">Run</span>
              <div className="w-px h-3.5 bg-gray-700 shrink-0" />
              <div className="min-w-0 flex-1">{channelSelector}</div>
            </div>
          )}

          <div className="shrink-0">
            <EventTypeFilter
              types={typeCounts}
              selected={visibleEventTypes}
              onChange={setVisibleEventTypes}
              label="Types"
            />
          </div>

          <div className="shrink-0">
            <EventTypeFilter
              types={fieldCounts}
              selected={visibleFields}
              onChange={setVisibleFields}
              label="Fields"
              searchPlaceholder="Search fields..."
              allCheckedTitle="Filter by payload field"
            />
          </div>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          <div className="flex items-center gap-2 bg-gray-800 rounded-lg px-2 py-1 border border-gray-700/50">
            <span className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold">Show from event</span>
            <ReplayControls />
          </div>

          <div className="flex items-center gap-1.5 shrink-0">
            <button
              onClick={() => runExport('download')}
              disabled={exportState.status === 'busy' || !activeChannel}
              title={exportTitle('download')}
              className="flex items-center gap-1.5 bg-gray-800 rounded-lg px-2.5 py-1 border border-gray-700/50 text-[10px] font-semibold text-gray-300 hover:text-white hover:border-gray-600 transition-colors disabled:opacity-60 disabled:cursor-wait"
            >
              <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                <polyline points="7 10 12 15 17 10"/>
                <line x1="12" y1="15" x2="12" y2="3"/>
              </svg>
              {exportLabel('download')}
            </button>
            <button
              onClick={() => runExport('copy')}
              disabled={exportState.status === 'busy' || !activeChannel}
              title={exportTitle('copy')}
              className="flex items-center gap-1.5 bg-gray-800 rounded-lg px-2.5 py-1 border border-gray-700/50 text-[10px] font-semibold text-gray-300 hover:text-white hover:border-gray-600 transition-colors disabled:opacity-60 disabled:cursor-wait"
            >
              <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
              </svg>
              {exportLabel('copy')}
            </button>
          </div>

          {visualizationMode === 'graph' && (
            <div className="relative" ref={menuRef}>
              <button
                onClick={() => setMenuOpen(open => !open)}
                className="p-1.5 text-gray-400 hover:text-white hover:bg-gray-800 rounded-lg transition-colors"
                title="More options"
                aria-haspopup="menu"
                aria-expanded={menuOpen}
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <circle cx="12" cy="12" r="1"/>
                  <circle cx="19" cy="12" r="1"/>
                  <circle cx="5" cy="12" r="1"/>
                </svg>
              </button>
              {menuOpen && (
                <div role="menu" className="absolute right-0 top-full z-50 mt-1 w-56 rounded-lg border border-gray-700 bg-gray-900 p-2 shadow-2xl shadow-black/50">
                  <div className="flex items-center justify-between px-1.5 py-1.5">
                    <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Layout</span>
                    <div className="flex items-center gap-1">
                      <LayoutSelector />
                      {layoutMode === 'grid' && (
                        <span className="text-[10px] text-gray-500 px-1" title="Adaptive columns based on visible node count">{gridCols} cols</span>
                      )}
                    </div>
                  </div>
                  <div className="my-1 border-t border-gray-800" />
                  <button
                    onClick={() => useFlowStore.getState().canvasController?.fitView()}
                    title="Fit view"
                    className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-xs text-gray-300 hover:bg-gray-800 hover:text-white"
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M8 3H5a2 2 0 0 0-2 2v3"/><path d="M21 8V5a2 2 0 0 0-2-2h-3"/><path d="M3 16v3a2 2 0 0 0 2 2h3"/><path d="M16 21h3a2 2 0 0 0 2-2v-3"/></svg>
                    Fit view
                  </button>
                  <button
                    onClick={() => useFlowStore.getState().canvasController?.zoomIn()}
                    title="Zoom in"
                    className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-xs text-gray-300 hover:bg-gray-800 hover:text-white"
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/><line x1="11" y1="8" x2="11" y2="14"/><line x1="8" y1="11" x2="14" y2="11"/></svg>
                    Zoom in
                  </button>
                  <button
                    onClick={() => useFlowStore.getState().canvasController?.zoomOut()}
                    title="Zoom out"
                    className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-xs text-gray-300 hover:bg-gray-800 hover:text-white"
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/><line x1="8" y1="11" x2="14" y2="11"/></svg>
                    Zoom out
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
