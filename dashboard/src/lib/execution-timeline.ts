// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import type { DashboardEvent } from './flow-store'

export type TimelineKind = 'default' | 'success' | 'active' | 'tool' | 'state' | 'failure' | 'recovery' | 'fork'

export function classifyTimelineEvent(event: DashboardEvent): TimelineKind {
  if (event.isState) return 'state'
  const type = (event.type || '').toLowerCase()
  if (/fork\.started|fork(?:ed|\.started)?/.test(type)) return 'fork'
  if (/recover|resume|restor|replay\.completed/.test(type)) return 'recovery'
  if (/error|crash|fail|exit|timeout|cancel/.test(type)) return 'failure'
  if (/complete|completed|done|finish|succeed|success/.test(type)) return 'success'
  if (/^tool\.|tool_|operation|claim/.test(type)) return 'tool'
  if (/start|running|progress|stream|message|response|llm|agent/.test(type)) return 'active'
  return 'default'
}

/**
 * Event types emitted by a framework checkpointer — infrastructure, not the
 * agent's business steps. The timeline opens with these hidden when the run
 * ALSO has business events, so a newcomer sees "Filtered 8 of 40 papers →
 * Extracted 7 findings → …" instead of a wall of `langgraph.checkpoint`.
 */
const FRAMEWORK_INTERNAL_PREFIXES = ['langgraph.', 'checkpointer.', 'checkpoint.']

export function isFrameworkInternalType(type: string | null | undefined): boolean {
  if (!type) return false
  const t = type.toLowerCase()
  return FRAMEWORK_INTERNAL_PREFIXES.some(prefix => t.startsWith(prefix))
}

export function timelineTitle(event: DashboardEvent): string {
  if (event.isState) return 'State snapshot committed'
  const payload = event.payload || {}
  for (const key of ['title', 'name', 'message', 'operation', 'tool', 'action', 'status']) {
    const value = payload[key]
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  return (event.type || 'event')
    .replace(/[._-]+/g, ' ')
    .replace(/\b\w/g, character => character.toUpperCase())
}

export function formatTimelineElapsed(start: Date | string, current: Date | string): string {
  const delta = Math.max(0, new Date(current).getTime() - new Date(start).getTime())
  if (!Number.isFinite(delta)) return '00:00.0'
  const minutes = Math.floor(delta / 60000)
  const seconds = Math.floor((delta % 60000) / 1000)
  const tenths = Math.floor((delta % 1000) / 100)
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}.${tenths}`
}

export function timelineEventKey(event: DashboardEvent, index: number): string {
  return event.id || `${event.channel}:${event.cursor}:${event.type}:${index}`
}
