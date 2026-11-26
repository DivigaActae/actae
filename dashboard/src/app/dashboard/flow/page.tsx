// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useState, useRef, useCallback, Suspense } from 'react'
import { useSearchParams } from 'next/navigation'
import ProtectedLayout from '@/components/ProtectedLayout'
import EventFlowCanvas from '@/components/flow/EventFlowCanvas'
import ExecutionTimeline from '@/components/flow/ExecutionTimeline'
import ChannelSelector from '@/components/flow/ChannelSelector'
import LiveToggle from '@/components/flow/LiveToggle'
import InspectorPanel from '@/components/flow/InspectorPanel'
import FlowHeader from '@/components/flow/FlowHeader'
import FlowBottomPanel from '@/components/flow/FlowBottomPanel'
import { useFlowStore } from '@/lib/flow-store'

const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_MODE === 'true'

export default function FlowPage() {
  return (
    <Suspense fallback={
      <ProtectedLayout>
        <div className="p-6 lg:p-8">
          <div className="skeleton h-8 w-48 mb-6" />
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {[1, 2, 3, 4].map(i => <div key={i} className="skeleton h-52 rounded-xl" />)}
          </div>
        </div>
      </ProtectedLayout>
    }>
      <FlowContent />
    </Suspense>
  )
}
function FlowContent() {
  const searchParams = useSearchParams()
  const activeChannel = useFlowStore(s => s.activeChannel)
  const loadChannels = useFlowStore(s => s.loadChannels)
  const setActiveChannel = useFlowStore(s => s.setActiveChannel)
  const startChannelPolling = useFlowStore(s => s.startChannelPolling)
  const stopChannelPolling = useFlowStore(s => s.stopChannelPolling)
  const loadPreferences = useFlowStore(s => s.loadPreferences)
  const isLive = useFlowStore(s => s.isLive)
  const liveEvents = useFlowStore(s => s.liveEvents)
  const inspectorWidth = useFlowStore(s => s.inspectorWidth)
  const visualizationMode = useFlowStore(s => s.visualizationMode)
  const setInspectorWidth = useFlowStore(s => s.setInspectorWidth)
  const selectedNode = useFlowStore(s => s.selectedNode)
  const [showLiveBar, setShowLiveBar] = useState(false)
  // Progressive disclosure: a run opens as just the readable timeline. The
  // inspector appears when a step is selected, and the activity panel is opened
  // on demand — so a first-time user is not hit with three views of the run.
  const [showInspector, setShowInspector] = useState(false)
  const [bottomPanelOpen, setBottomPanelOpen] = useState(false)

  useEffect(() => {
    if (selectedNode) setShowInspector(true)
  }, [selectedNode])

  const [bottomPanelHeight, setBottomPanelHeight] = useState(240)
  const bottomDrag = useRef<{ startY: number; startHeight: number } | null>(null)

  const onBottomMouseDown = useCallback((e: React.MouseEvent) => {
    bottomDrag.current = { startY: e.clientY, startHeight: bottomPanelHeight }
    e.preventDefault()
  }, [bottomPanelHeight])

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!bottomDrag.current) return
      const delta = bottomDrag.current.startY - e.clientY
      setBottomPanelHeight(Math.max(140, Math.min(500, bottomDrag.current.startHeight + delta)))
    }
    const handleMouseUp = () => {
      bottomDrag.current = null
    }
    document.addEventListener('mousemove', handleMouseMove)
    document.addEventListener('mouseup', handleMouseUp)
    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [])

  useEffect(() => {
    const channelParam = searchParams.get('channel')
    if (!channelParam && !activeChannel) {
      // No deep-link: the store auto-selects the most recently active
      // channel after loadChannels() resolves.
      loadChannels()
    }
    // Restore the user's stored flow view (layout + filters) server-side.
    loadPreferences()
    // Auto-refresh the channel list so channels/forks created by SDK code
    // appear without a page reload.
    startChannelPolling()
    return () => stopChannelPolling()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    const channel = searchParams.get('channel')
    if (channel && channel !== activeChannel) {
      if (MOCK_MODE) {
        setActiveChannel(channel)
      } else {
        loadChannels()
        setActiveChannel(channel)
      }
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (liveEvents.length > 0) setShowLiveBar(true)
  }, [liveEvents.length])

  return (
    <ProtectedLayout>
      <div className="flow-workstation absolute inset-0 overflow-hidden flex">
        {/* Main content */}
        <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
          <div className="flow-command-bar"><FlowHeader channelSelector={<ChannelSelector />} /></div>

          <div className="flex-1 flex min-h-0 overflow-hidden">
            {/* Canvas area */}
            <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
              <div className={`flow-stage flex-1 relative min-h-0 overflow-hidden ${visualizationMode === 'timeline' ? 'timeline-mode' : 'graph-mode'}`}>
                {visualizationMode === 'timeline' ? <ExecutionTimeline /> : <EventFlowCanvas />}
              </div>
              {activeChannel && (
                <>
                  <div className="flex h-7 shrink-0 items-center justify-between border-y border-gray-800 bg-gray-900/95 px-3">
                    <div
                      className={`group flex flex-1 items-center gap-2 ${bottomPanelOpen ? 'cursor-row-resize' : ''}`}
                      onMouseDown={bottomPanelOpen ? onBottomMouseDown : undefined}
                      title={bottomPanelOpen ? 'Drag to resize activity panel' : undefined}
                    >
                      <span className="h-1 w-8 rounded-full bg-gray-700 transition-colors group-hover:bg-purple-500" />
                      <span className="text-[9px] font-semibold uppercase tracking-[0.14em] text-gray-600">Activity</span>
                    </div>
                    <button onClick={() => setBottomPanelOpen(open => !open)} className="rounded px-2 py-1 text-[10px] font-medium text-gray-400 hover:bg-gray-800 hover:text-white" aria-label={bottomPanelOpen ? 'Hide activity panel' : 'Show activity panel'}>
                      {bottomPanelOpen ? 'Hide activity' : 'Show activity'}
                    </button>
                  </div>
                  {bottomPanelOpen && <div style={{ height: bottomPanelHeight }} className="shrink-0 overflow-hidden"><FlowBottomPanel /></div>}
                </>
              )}
            </div>

            {/* Right sidebar: inspector */}
            {showInspector ? <InspectorPanel
              width={inspectorWidth}
              onClose={() => setShowInspector(false)}
              onResize={setInspectorWidth}
              liveSlot={<LiveToggle />}
            /> : <button onClick={() => setShowInspector(true)} className="absolute right-4 top-28 z-30 flex items-center gap-2 rounded-lg border border-gray-700 bg-gray-900/95 px-3 py-2 text-xs font-medium text-gray-300 shadow-xl hover:border-purple-500/50 hover:text-white" aria-label="Open inspector"><span className="h-2 w-2 rounded-full bg-purple-400" /> Inspector</button>}
          </div>

          {/* Live feed bar */}
          {isLive && showLiveBar && (
            <div className="h-7 border-t border-gray-800 bg-gray-900 flex items-center justify-between px-3 shrink-0">
              <span className="text-[10px] text-gray-500">
                {visualizationMode === 'graph'
                  ? 'scroll to zoom · drag to pan · click a node to inspect'
                  : 'click any step to inspect it · new events stream in live'}
              </span>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setShowLiveBar(!showLiveBar)}
                  className="text-[10px] text-gray-400 hover:text-gray-600"
                >
                  Hide live feed
                </button>
                {liveEvents.length > 0 && (
                  <div className="flex gap-3 overflow-x-auto max-w-[400px]">
                    {liveEvents.slice(0, 5).map((e, i) => (
                      <span key={e.id || i} className="text-[10px] font-mono text-gray-500 whitespace-nowrap">
                        {e.timestamp instanceof Date
                          ? e.timestamp.toLocaleTimeString()
                          : new Date().toLocaleTimeString()}
                        {' · @'}{e.cursor}{' · '}{e.type}
                      </span>
                    ))}
                  </div>
                )}
                {liveEvents.length === 0 && (
                  <span className="text-[10px] text-gray-500 italic">Waiting for events...</span>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </ProtectedLayout>
  )
}
