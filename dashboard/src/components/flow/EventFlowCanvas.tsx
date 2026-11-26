// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import React, { useMemo, useCallback, useEffect } from 'react'
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  useReactFlow,
} from '@xyflow/react'
import type { Node } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import EventNode from './EventNode'
import StateNode from './StateNode'
import { useFlowStore } from '@/lib/flow-store'
import { buildFlowNodes, nodeMatchesSelection } from '@/lib/flow-layout'

const nodeTypes = { event: EventNode, state: StateNode } as any

function CanvasInner() {
  const events = useFlowStore(s => s.events)
  const error = useFlowStore(s => s.error)
  const activeChannel = useFlowStore(s => s.activeChannel)
  const setSelectedNode = useFlowStore(s => s.setSelectedNode)
  const selectedNode = useFlowStore(s => s.selectedNode)
  const loading = useFlowStore(s => s.loading)
  const layoutMode = useFlowStore(s => s.layoutMode)
  const visibleEventTypes = useFlowStore(s => s.visibleEventTypes)
  const setCanvasController = useFlowStore(s => s.setCanvasController)
  const { zoomIn, zoomOut, fitView } = useReactFlow()

  useEffect(() => {
    setCanvasController({
      zoomIn: () => zoomIn({ duration: 200 }),
      zoomOut: () => zoomOut({ duration: 200 }),
      fitView: () => fitView({ padding: 0.12, duration: 300, maxZoom: 1 }),
    })
    return () => setCanvasController(null)
  }, [zoomIn, zoomOut, fitView, setCanvasController])

  const onNodeClick = useCallback(
    (data: { type: string; cursor: number; payload?: any; state?: any }) => {
      setSelectedNode(data)
    },
    [setSelectedNode]
  )

  const { nodes: initialNodes, edges: initialEdges } = useMemo(
    () => buildFlowNodes(events, layoutMode, onNodeClick, visibleEventTypes),
    [events, layoutMode, onNodeClick, visibleEventTypes]
  )

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)

  React.useEffect(() => {
    setNodes(initialNodes)
    setEdges(initialEdges)
  }, [initialNodes, initialEdges, setNodes, setEdges])

  // Keep the canvas highlight in sync with the store selection: clicking a
  // timeline row (or a node) highlights the matching node here too.
  React.useEffect(() => {
    setNodes(nds => nds.map(n => ({ ...n, selected: nodeMatchesSelection(n, selectedNode) })))
  }, [selectedNode, setNodes])

  // Only fit view when the actual event/layout data changes, not when the user
  // merely selects a node in the timeline/inspector.
  React.useEffect(() => {
    if (events.length > 0) {
      fitView({ padding: 0.12, duration: 300, maxZoom: 1 })
    }
  }, [events, layoutMode, fitView])

  if (!activeChannel) {
    return (
      <div
        className="w-full h-full flex items-center justify-center"
        style={{
          backgroundImage: 'radial-gradient(circle, #374151 1px, transparent 1px)',
          backgroundSize: '24px 24px',
        }}
      >
        <div className="text-center max-w-sm px-8 py-10 rounded-2xl border border-gray-800 bg-gray-900/80 backdrop-blur shadow-2xl shadow-black/20">
          <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-actae-500/20 to-purple-500/20 border border-actae-500/20 flex items-center justify-center mx-auto mb-5">
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="url(#emptyGradient)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <defs>
                <linearGradient id="emptyGradient" x1="0" y1="0" x2="24" y2="24">
                  <stop offset="0%" stopColor="#c8ff3d" />
                  <stop offset="100%" stopColor="#a855f7" />
                </linearGradient>
              </defs>
              <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
            </svg>
          </div>
          <h2 className="text-xl font-semibold text-white mb-2">No execution selected</h2>
          <p className="text-sm text-gray-400 leading-relaxed">
            Choose a channel from the dropdown in the toolbar to visualize its event flow.
          </p>
        </div>
      </div>
    )
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="animate-pulse text-gray-500">Loading events...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <p className="text-red-500 mb-2">{error}</p>
          <button
            onClick={() => useFlowStore.getState().loadEvents()}
            className="px-3 py-1 bg-actae-500 text-white rounded text-sm"
          >
            Retry
          </button>
        </div>
      </div>
    )
  }

  if (events.length === 0) {
    return (
      <div className="w-full h-full flex items-center justify-center">
        <div className="text-center max-w-sm px-6 py-8 rounded-2xl border border-gray-800 bg-gray-900/50">
          <div className="w-12 h-12 rounded-xl bg-gray-800 border border-gray-700 flex items-center justify-center mx-auto mb-4">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className="text-gray-500">
              <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
            </svg>
          </div>
          <h2 className="text-base font-semibold text-gray-300 mb-1">No events recorded yet</h2>
          <p className="text-sm text-gray-500">Events will appear here once recorded on this channel.</p>
        </div>
      </div>
    )
  }

  if (events.length > 0 && initialNodes.length === 0) {
    return (
      <div className="w-full h-full flex items-center justify-center">
        <div className="text-center max-w-sm px-6 py-8 rounded-2xl border border-gray-800 bg-gray-900/50">
          <div className="w-12 h-12 rounded-xl bg-gray-800 border border-gray-700 flex items-center justify-center mx-auto mb-4">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className="text-gray-500">
              <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" />
            </svg>
          </div>
          <h2 className="text-base font-semibold text-gray-300 mb-1">No nodes match the filter</h2>
          <p className="text-sm text-gray-500 mb-4">All event types are currently hidden by the type filter in the toolbar.</p>
          <button
            onClick={() => useFlowStore.getState().resetVisibleEventTypes()}
            className="px-3 py-1.5 bg-actae-500 hover:bg-actae-400 text-white rounded-lg text-sm transition-colors"
          >
            Show all types
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="relative w-full h-full">
      <div className="pointer-events-none absolute left-4 top-4 z-10 flex items-center gap-2 rounded-lg border border-gray-800 bg-gray-950/75 px-3 py-2 shadow-lg backdrop-blur">
        <span className="h-2 w-2 rounded-full bg-purple-400" />
        <span className="text-[10px] font-semibold text-gray-300">{initialNodes.length} visible nodes</span>
        <span className="text-[10px] text-gray-600">· click a node to inspect</span>
      </div>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        minZoom={0.1}
        maxZoom={2}
        attributionPosition="bottom-right"
        className="!bg-transparent w-full h-full"
      >
        <Background color="#374151" gap={16} size={1} />
        <Controls position="bottom-right" />
        <MiniMap
          position="bottom-left"
          nodeColor={(n) => (n.type === 'state' ? '#a855f7' : '#c8ff3d')}
          pannable
          zoomable
          style={{ width: 150, height: 90, borderRadius: 10, overflow: 'hidden', border: '1px solid #374151', background: '#111827' }}
        />
      </ReactFlow>
    </div>
  )
}

export default function EventFlowCanvas() {
  return (
    <ReactFlowProvider>
      <CanvasInner />
    </ReactFlowProvider>
  )
}
