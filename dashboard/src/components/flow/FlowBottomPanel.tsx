// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { useFlowStore } from '@/lib/flow-store'
import { filterPayload } from '@/lib/payload-fields'
import JsonTree from '@/components/JsonTree'
import CollapsibleText from '@/components/flow/CollapsibleText'
import { CompareView } from '@/components/flow/CompareView'

const TABS = [
  { id: 'timeline', label: 'Timeline' },
  { id: 'event-log', label: 'Event Log' },
  { id: 'metadata', label: 'Metadata' },
  { id: 'compare', label: 'Compare' },
] as const

function formatTime(ts: Date | string) {
  const d = typeof ts === 'string' ? new Date(ts) : ts
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function getStep(type: string) {
  const t = type.toLowerCase()
  if (t.includes('start') && t.includes('pipeline')) return 'pipeline.start'
  if (t.includes('complete') && t.includes('pipeline')) return 'pipeline.complete'
  if (t.includes('.')) {
    const parts = type.split('.')
    if (parts.length >= 2) return `${parts[0]}.${parts[1]}`
  }
  return type
}

function getAgent(type: string) {
  const t = type.toLowerCase()
  if (t.includes('writer')) return 'writer'
  if (t.includes('critic')) return 'critic'
  if (t.includes('analyst')) return 'analyst'
  if (t.includes('researcher')) return 'researcher'
  return '—'
}

function getDuration(payload: Record<string, unknown>) {
  if (typeof payload.elapsed_ms === 'number') return `${payload.elapsed_ms}ms`
  if (typeof payload.elapsed_seconds === 'number') return `${payload.elapsed_seconds}s`
  return '—'
}

function getInputOutput(payload: Record<string, unknown>) {
  if (payload.input && typeof payload.input === 'object') return JSON.stringify(payload.input)
  if (payload.output && typeof payload.output === 'object') return JSON.stringify(payload.output)
  return JSON.stringify(payload)
}

export default function FlowBottomPanel() {
  const [activeTab, setActiveTab] = useState<(typeof TABS)[number]['id']>('timeline')
  const events = useFlowStore(s => s.events)
  const selectedNode = useFlowStore(s => s.selectedNode)
  const setSelectedNode = useFlowStore(s => s.setSelectedNode)
  const activeChannel = useFlowStore(s => s.activeChannel)
  const visibleFields = useFlowStore(s => s.visibleFields)
  const router = useRouter()
  const selectedRowRef = useRef<HTMLTableRowElement | null>(null)

  // When a node is clicked in the canvas, bring its timeline row into view —
  // with thousands of rows the selected one may be far off-screen otherwise.
  useEffect(() => {
    selectedRowRef.current?.scrollIntoView({ block: 'nearest' })
  }, [selectedNode?.cursor, selectedNode?.type, selectedNode?.channel])

  const nonStateEvents = events.filter(e => !e.isState)

  function openInEventsPage(evt: (typeof nonStateEvents)[number]) {
    const channel = evt.channel || activeChannel
    const params = new URLSearchParams()
    if (channel) params.set('channel', channel)
    if (evt.cursor !== undefined && evt.cursor !== null) params.set('cursor', String(evt.cursor))
    router.push(`/dashboard/events/?${params.toString()}`)
  }

  return (
    <div className="h-full shrink-0 border-t border-gray-800 bg-gray-900 flex flex-col">
      {/* Tabs */}
      <div className="flex items-center gap-1 px-4 border-b border-gray-800/50">
        {TABS.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`px-3 py-2.5 text-[11px] font-semibold uppercase tracking-wider transition-colors border-b-2 ${
              activeTab === tab.id
                ? 'text-white border-actae-500'
                : 'text-gray-500 border-transparent hover:text-gray-300'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto">
        {activeTab === 'timeline' && (
          <table className="w-full text-left border-collapse">
            <thead className="bg-gray-800/50 sticky top-0">
              <tr>
                <th className="px-4 py-2 text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Cursor</th>
                <th className="px-4 py-2 text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Time</th>
                <th className="px-4 py-2 text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Type</th>
                <th className="px-4 py-2 text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Step</th>
                <th className="px-4 py-2 text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Input / Output</th>
                <th className="px-4 py-2 text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Agent</th>
                <th className="px-4 py-2 text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Duration</th>
              </tr>
            </thead>
            <tbody>
              {nonStateEvents.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-xs text-gray-500">
                    No events recorded yet
                  </td>
                </tr>
              ) : (
                nonStateEvents.map(evt => {
                  const isSelected =
                    selectedNode?.cursor === evt.cursor &&
                    selectedNode?.type === evt.type &&
                    (!selectedNode?.channel || selectedNode.channel === evt.channel)
                  return (
                    <tr
                      key={`${evt.channel}-${evt.cursor}-${evt.type}`}
                      ref={isSelected ? selectedRowRef : undefined}
                      onClick={() => setSelectedNode({ type: evt.type, cursor: evt.cursor, channel: evt.channel, payload: evt.payload, state: evt.state })}
                      className={`border-b border-gray-800/50 text-xs cursor-pointer transition-colors ${
                        isSelected
                          ? 'bg-actae-950/40 hover:bg-actae-950/60'
                          : 'hover:bg-gray-800/30'
                      }`}
                    >
                      <td className="px-4 py-2 font-mono text-gray-400">{evt.cursor}</td>
                      <td className="px-4 py-2 text-gray-400">{formatTime(evt.timestamp)}</td>
                      <td className="px-4 py-2">
                        <span className={`inline-flex px-1.5 py-0.5 rounded text-[10px] font-medium ${getTypeBadgeClass(evt.type)}`}>
                          {evt.type}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-gray-300">{getStep(evt.type)}</td>
                      <td className="px-4 py-2 text-gray-400 font-mono max-w-[320px] min-w-[220px]">
  <CollapsibleText text={getInputOutput(filterPayload(evt.payload, visibleFields))} />
</td>
                      <td className="px-4 py-2 text-gray-300">{getAgent(evt.type)}</td>
                      <td className="px-4 py-2 text-gray-400">{getDuration(filterPayload(evt.payload, visibleFields))}</td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        )}

        {activeTab === 'event-log' && (
          <div className="p-4">
            {nonStateEvents.length === 0 ? (
              <div className="text-xs text-gray-500 text-center py-8">No events to display</div>
            ) : (
              <div className="space-y-2">
                {nonStateEvents.map(evt => (
                  <button
                    key={`${evt.channel}-${evt.cursor}-${evt.type}`}
                    onClick={() => openInEventsPage(evt)}
                    title="Open in Events page"
                    className="w-full text-left p-2 bg-gray-800/30 rounded border border-gray-800 hover:border-actae-500/60 hover:bg-gray-800/50 transition-colors cursor-pointer group"
                  >
                    <div className="flex items-center gap-2 text-xs mb-1">
                      <span className="font-mono text-gray-500">@{evt.cursor}</span>
                      <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${getTypeBadgeClass(evt.type)}`}>{evt.type}</span>
                      <span className="text-gray-500">{formatTime(evt.timestamp)}</span>
                      <span className="ml-auto text-[10px] text-actae-400 opacity-0 group-hover:opacity-100 transition-opacity flex items-center gap-1">
                        Open details
                        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="7" y1="17" x2="17" y2="7"/><polyline points="7 7 17 7 17 17"/></svg>
                      </span>
                    </div>
                    <pre className="text-[10px] text-gray-400 font-mono overflow-x-auto">{JSON.stringify(filterPayload(evt.payload, visibleFields), null, 2)}</pre>
                  </button>
                ))}
              </div>
            )}
          </div>
        )}

        {activeTab === 'metadata' && (
          <div className="p-4">
            <div className="text-xs text-gray-400 mb-2">Channel: <span className="font-mono text-gray-300">{activeChannel || '—'}</span></div>
            {selectedNode ? (
              <div className="bg-gray-800/30 rounded border border-gray-800 p-3 max-h-48 overflow-auto">
                <JsonTree
                  data={selectedNode.payload && Object.keys(selectedNode.payload).length > 0
                    ? filterPayload(selectedNode.payload, visibleFields)
                    : selectedNode.state || {}}
                />
              </div>
            ) : (
              <div className="text-xs text-gray-500 text-center py-8">Select an event to view its metadata</div>
            )}
          </div>
        )}

        {activeTab === 'compare' && (
          <div>
            <div className="sticky top-0 z-10 flex items-center justify-between border-b border-gray-800 bg-gray-900/95 px-4 py-2 backdrop-blur">
              <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Compare with another run</span>
              <Link href="/dashboard/experiments/" className="text-[11px] text-actae-400 hover:text-actae-300">Open Forks lab →</Link>
            </div>
            <CompareView />
          </div>
        )}
      </div>
    </div>
  )
}

function getTypeBadgeClass(type: string) {
  const t = type.toLowerCase()
  if (t.includes('error') || t.includes('crash')) return 'bg-red-500/15 text-red-400 border border-red-500/20'
  if (t.includes('complete') || t.includes('done')) return 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/20'
  if (t.includes('started') || t.includes('start')) return 'bg-blue-500/15 text-blue-400 border border-blue-500/20'
  if (t.includes('saved') || t.includes('checkpoint')) return 'bg-purple-500/15 text-purple-400 border border-purple-500/20'
  if (t.includes('tool')) return 'bg-pink-500/15 text-pink-400 border border-pink-500/20'
  return 'bg-gray-700 text-gray-300 border border-gray-600'
}
