// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import type { Node, Edge } from '@xyflow/react'
import { MarkerType } from '@xyflow/react'
import type { DashboardEvent } from './flow-store'

export const START_X = 80
export const START_Y = 200

export interface FlowNodeClickData {
  type: string
  cursor: number
  channel?: string
  payload?: Record<string, unknown>
  state?: Record<string, unknown>
}

export interface FlowNodeData extends Record<string, unknown> {
  type: string
  cursor: number
  channel?: string
  payload?: Record<string, unknown>
  timestamp?: Date | string
  isInherited?: boolean
  forkedFrom?: string | null
  state?: Record<string, unknown>
  keys?: string[]
  onClick?: () => void
}

/**
 * True when a React Flow node matches a selection (node click / timeline
 * row click). Matching is by type + global cursor, disambiguated by channel
 * so inherited parent events and fork-owned events with the same cursor
 * never both match. A selection without a channel matches any channel.
 */
export function nodeMatchesSelection(
  node: Node,
  selected: { type: string; cursor: number; channel?: string | null } | null
): boolean {
  if (!selected) return false
  const d = node.data as FlowNodeData | undefined
  if (!d || typeof d.cursor !== 'number') return false
  if (d.type !== selected.type) return false
  if (d.cursor !== selected.cursor) return false
  if (selected.channel && d.channel && d.channel !== selected.channel) return false
  return true
}

/**
 * The filter key for an event: state snapshots are grouped under the
 * pseudo-type `"state"`, everything else under its event type string.
 * Must stay in sync with `computeTypeCounts` in flow-helpers.
 */
export function nodeTypeKey(evt: DashboardEvent): string {
  return evt.isState ? 'state' : evt.type || 'unknown'
}

/**
 * Generate a globally unique React Flow node id for an event.
 *
 * Cursor-only ids (`event-${cursor}`) are unsafe because inherited parent
 * events and fork-owned events can share the same cursor value. This
 * function disambiguates by channel, event id, and list index.
 */
export function makeNodeId(evt: DashboardEvent, index: number): string {
  if (evt.isState) {
    return `state-${evt.channel}-${evt.cursor}-${index}`
  }
  if (evt.id) {
    return `evt-${evt.channel}-${evt.id}`
  }
  return `event-${evt.channel}-${evt.cursor}-${index}`
}

/**
 * Adaptive grid columns: the fixed 4-per-row grid wastes space with a few
 * nodes and produces an unwieldy tall wall with hundreds. Columns scale with
 * the number of nodes actually displayed (so the type filter narrows the
 * grid too), capped at 16 so a row never exceeds ~2.9k px.
 *
 * @example 10 nodes → 4 columns; 100 → 8; 1000 → 16
 */
export function gridColumnsForCount(count: number): number {
  if (count <= 12) return 4
  if (count <= 30) return 5
  if (count <= 60) return 6
  if (count <= 120) return 8
  if (count <= 250) return 10
  if (count <= 500) return 12
  return 16
}

function computeLayout(
  layoutMode: 'horizontal' | 'grid' | 'compact',
  nodeCount: number
): { spacingX: number; spacingY: number; cols: number } {
  switch (layoutMode) {
    case 'compact':
      return { spacingX: 170, spacingY: 100, cols: Infinity }
    case 'grid':
      return { spacingX: 260, spacingY: 170, cols: gridColumnsForCount(nodeCount) }
    default: // horizontal
      return { spacingX: 260, spacingY: 130, cols: Infinity }
  }
}

function computePosition(
  index: number,
  layoutMode: 'horizontal' | 'grid' | 'compact',
  isState: boolean,
  spacingX: number,
  spacingY: number,
  cols: number
): { x: number; y: number } {
  if (layoutMode === 'grid') {
    const row = Math.floor(index / cols)
    const col = index % cols
    if (isState) {
      return {
        x: START_X + col * spacingX,
        y: START_Y + row * spacingY * 2 - spacingY * 0.5,
      }
    }
    return {
      x: START_X + col * spacingX,
      y: START_Y + row * spacingY * 2 + spacingY * 0.5,
    }
  }

  return {
    x: START_X + index * spacingX,
    y: isState ? START_Y - spacingY : START_Y,
  }
}

/**
 * Build React Flow nodes and edges from a list of dashboard events.
 *
 * Events are sorted by cursor, then laid out according to the requested mode.
 * Each node gets a unique id via {@link makeNodeId} so inherited parent events
 * and fork-owned events never collide.
 *
 * When `visibleTypes` is provided (a set of event-type strings; `null`/omitted
 * means show everything), only matching events are rendered. State nodes match
 * the pseudo-type `"state"`. Filtering happens before layout, so the remaining
 * nodes are re-chained sequentially in cursor order — links stay correct.
 */
export function buildFlowNodes(
  events: DashboardEvent[],
  layoutMode: 'horizontal' | 'grid' | 'compact',
  onNodeClick?: (data: FlowNodeClickData) => void,
  visibleTypes?: ReadonlySet<string> | null
): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = []
  const edges: Edge[] = []

  if (!events || events.length === 0) {
    return { nodes, edges }
  }

  const filtered = visibleTypes
    ? events.filter(evt => visibleTypes.has(nodeTypeKey(evt)))
    : events
  if (filtered.length === 0) {
    return { nodes, edges }
  }

  const sorted = [...filtered].sort((a, b) => a.cursor - b.cursor)
  const { spacingX, spacingY, cols } = computeLayout(layoutMode, sorted.length)

  sorted.forEach((evt, i) => {
    const id = makeNodeId(evt, i)
    const { x, y } = computePosition(
      i,
      layoutMode,
      !!evt.isState,
      spacingX,
      spacingY,
      cols
    )

    if (evt.isState) {
      const data: FlowNodeData = {
        cursor: evt.cursor,
        type: 'state',
        channel: evt.channel,
        keys: evt.state ? Object.keys(evt.state) : [],
        state: evt.state || {},
        onClick:
          onNodeClick
            ? () =>
                onNodeClick({
                  type: 'state',
                  cursor: evt.cursor,
                  channel: evt.channel,
                  state: evt.state,
                })
            : undefined,
      }
      nodes.push({
        id,
        type: 'state',
        position: { x, y },
        data,
      })
    } else {
      const data: FlowNodeData = {
        type: evt.type,
        cursor: evt.cursor,
        channel: evt.channel,
        payload: evt.payload,
        timestamp: evt.timestamp,
        isInherited: !!evt.isInherited,
        forkedFrom: evt.forkedFrom,
        onClick:
          onNodeClick
            ? () =>
                onNodeClick({
                  type: evt.type,
                  cursor: evt.cursor,
                  channel: evt.channel,
                  payload: evt.payload,
                  state: evt.state,
                })
            : undefined,
      }
      nodes.push({
        id,
        type: 'event',
        position: { x, y },
        data,
      })
    }

    // Connect to previous node
    if (i > 0) {
      const prev = sorted[i - 1]
      const prevId = makeNodeId(prev, i - 1)
      const isStateEdge = evt.isState || prev.isState
      const isInheritedEdge = !!evt.isInherited
      edges.push({
        id: `${prevId}->${id}`,
        source: prevId,
        target: id,
        type: 'smoothstep',
        animated: false,
        style: {
          stroke: isInheritedEdge
            ? '#6b7280'
            : isStateEdge
              ? '#a855f7'
              : '#9ca3af',
          strokeDasharray: isInheritedEdge ? '5 5' : isStateEdge ? '5 5' : undefined,
          opacity: isInheritedEdge ? 0.4 : 1,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: isInheritedEdge
            ? '#6b7280'
            : isStateEdge
              ? '#a855f7'
              : '#9ca3af',
        },
      })
    }
  })

  return { nodes, edges }
}
