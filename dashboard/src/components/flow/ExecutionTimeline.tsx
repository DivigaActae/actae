// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useMemo, useRef } from 'react'
import { useFlowStore, type DashboardEvent } from '@/lib/flow-store'
import { filterPayload } from '@/lib/payload-fields'
import { summarizePayload } from '@/lib/node-summary'
import { classifyTimelineEvent, formatTimelineElapsed, timelineEventKey, timelineTitle, type TimelineKind } from '@/lib/execution-timeline'

export default function ExecutionTimeline() {
  const events = useFlowStore(s => s.events)
  const activeChannel = useFlowStore(s => s.activeChannel)
  const loading = useFlowStore(s => s.loading)
  const error = useFlowStore(s => s.error)
  const isLive = useFlowStore(s => s.isLive)
  const demoLive = useFlowStore(s => s.demoLive)
  const connection = useFlowStore(s => s.connection)
  const liveEvents = useFlowStore(s => s.liveEvents)
  const selected = useFlowStore(s => s.selectedNode)
  const visibleTypes = useFlowStore(s => s.visibleEventTypes)
  const visibleFields = useFlowStore(s => s.visibleFields)
  const select = useFlowStore(s => s.setSelectedNode)
  const viewport = useRef<HTMLOListElement>(null)
  const followLive = useRef(true)
  const selectedRefs = useRef(new Map<string, HTMLButtonElement>())

  const visible = useMemo(() => {
    const filtered = visibleTypes ? events.filter(event => visibleTypes.has(event.isState ? 'state' : event.type || 'unknown')) : events
    return [...filtered].sort((a, b) => {
      const timeDelta = new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
      return timeDelta || a.cursor - b.cursor
    })
  }, [events, visibleTypes])
  const liveIds = useMemo(() => new Set(liveEvents.map(event => event.id).filter(Boolean)), [liveEvents])
  const firstTimestamp = visible[0]?.timestamp
  const latestCursor = visible.reduce((maximum, event) => Math.max(maximum, event.cursor), 0)
  const liveConnected = isLive && connection.authenticated
  // "Connecting" is reserved for an open transport that is awaiting auth.
  // A pending or failed WebSocket attempt is offline, never live.
  const isConnecting = isLive && connection.connected && !connection.authenticated
  const isOffline = isLive && !liveConnected && !connection.connected
  const connectionLabel = demoLive ? 'DEMO' : liveConnected ? 'LIVE' : isConnecting ? 'CONNECTING' : isOffline ? 'OFFLINE' : 'PAUSED'

  useEffect(() => {
    if (!isLive || !followLive.current || !viewport.current) return
    viewport.current.scrollTo({ top: viewport.current.scrollHeight, behavior: liveEvents.length ? 'smooth' : 'auto' })
  }, [visible.length, isLive, liveEvents.length])

  useEffect(() => {
    if (!selected) return
    const key = `${selected.channel || ''}:${selected.cursor}:${selected.type}`
    selectedRefs.current.get(key)?.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
  }, [selected])

  function handleScroll() {
    const element = viewport.current
    if (!element) return
    followLive.current = element.scrollHeight - element.scrollTop - element.clientHeight < 80
  }

  if (!activeChannel) return <TimelineEmpty title="No execution selected" body="Choose a channel to open its live execution timeline." />
  if (loading) return <div className="execution-timeline-state"><span className="timeline-loader" />Loading execution history…</div>
  if (error) return <div className="execution-timeline-state error"><strong>Timeline unavailable</strong><span>{error}</span><button onClick={() => void useFlowStore.getState().loadEvents()}>Retry</button></div>
  if (events.length === 0) return <TimelineEmpty title="Waiting for the first event" body="This timeline will update automatically as the agent records work." />
  if (visible.length === 0) return <div className="execution-timeline-state"><strong>No events match the current filter</strong><button onClick={() => useFlowStore.getState().resetVisibleEventTypes()}>Show all types</button></div>

  return (
    <section className="execution-timeline-shell" aria-label="Execution timeline">
      <header className="execution-timeline-head">
        <div><span>Execution timeline</span><small>{visible.length} visible / {events.length} recorded</small></div>
        <div className="execution-timeline-head__right"><code>cursor {String(latestCursor).padStart(4, '0')}</code><span className={demoLive || liveConnected ? 'live' : isOffline ? 'offline' : ''}><i />{connectionLabel}</span></div>
      </header>
      <ol ref={viewport} onScroll={handleScroll} className="execution-timeline-viewport" aria-live="polite" aria-relevant="additions">
        <li className="execution-timeline-rail" aria-hidden="true" />
        {visible.map((event, index) => {
          const selectionKey = `${event.channel}:${event.cursor}:${event.isState ? 'state' : event.type}`
          const isSelected = !!selected && selected.cursor === event.cursor && selected.type === (event.isState ? 'state' : event.type) && (!selected.channel || selected.channel === event.channel)
          return <TimelineRow key={timelineEventKey(event, index)} event={event} elapsed={firstTimestamp ? formatTimelineElapsed(firstTimestamp, event.timestamp) : '00:00.0'} kind={classifyTimelineEvent(event)} isLive={!!event.id && liveIds.has(event.id)} isSelected={isSelected} visibleFields={visibleFields} buttonRef={element => { if (element) selectedRefs.current.set(selectionKey, element); else selectedRefs.current.delete(selectionKey) }} onSelect={() => select({ type: event.isState ? 'state' : event.type, cursor: event.cursor, channel: event.channel, payload: event.payload, state: event.state })} />
        })}
        {demoLive && <li className="timeline-awaiting" role="status"><i /><span>Demo replay — streaming a recorded run</span></li>}
        {isLive && !demoLive && <li className="timeline-awaiting" role="status"><i /><span>{liveConnected ? 'Listening for the next event' : isConnecting ? 'Authenticating live events' : `Live connection unavailable${connection.error ? `: ${connection.error}` : ''}`}</span></li>}
      </ol>
      <footer className="execution-timeline-footer"><span>◉ {demoLive ? 'demo replay' : liveConnected ? 'events live' : isLive ? 'live connection unavailable' : 'event history'}</span><span>{'{ }'} state {events.filter(event => event.isState).length}</span><span>⑂ {new Set(events.map(event => event.channel)).size} execution path{new Set(events.map(event => event.channel)).size === 1 ? '' : 's'}</span></footer>
    </section>
  )
}

function TimelineRow({ event, elapsed, kind, isLive, isSelected, visibleFields, onSelect, buttonRef }: { event: DashboardEvent; elapsed: string; kind: TimelineKind; isLive: boolean; isSelected: boolean; visibleFields: Set<string> | null; onSelect: () => void; buttonRef: (element: HTMLButtonElement | null) => void }) {
  const details = event.isState
    ? Object.keys(event.state || {}).slice(0, 3).map(key => `state.${key}`)
    : summarizePayload(filterPayload(event.payload || {}, visibleFields), visibleFields)
  return (
    <li className="timeline-list-item"><button ref={buttonRef} type="button" aria-label={`${timelineTitle(event)}, cursor ${event.cursor}`} aria-current={isSelected ? 'step' : undefined} onClick={onSelect} className={`execution-timeline-row kind-${kind}${event.isInherited ? ' inherited' : ''}${isSelected ? ' selected' : ''}${isLive ? ' arrived' : ''}`}>
      <span className="timeline-marker" aria-hidden="true">{kind === 'failure' ? '×' : kind === 'recovery' ? '↻' : kind === 'fork' ? '⑂' : kind === 'state' ? '{}' : kind === 'active' ? '✦' : '✓'}</span>
      <span className="timeline-row-content">
        <span className="timeline-row-meta"><code>{elapsed}</code><i>·</i><code>CURSOR {String(event.cursor).padStart(4, '0')}</code>{event.isInherited && <em>INHERITED</em>}{isLive && <em>JUST NOW</em>}</span>
        <strong>{timelineTitle(event)}</strong>
        <span className="timeline-event-type">{event.type}</span>
        {details.length > 0 && <span className="timeline-row-details">{details.join(' · ')}</span>}
        {kind === 'failure' && <span className="timeline-boundary failure"><b>PROCESS INTERRUPTED</b><code>{String(event.payload?.error || event.payload?.reason || event.type)}</code></span>}
        {kind === 'recovery' && <span className="timeline-boundary recovery"><b>ACTAE RECOVERY</b><strong>Execution continued from durable history</strong><small>No committed steps rerun.</small></span>}
        {kind === 'fork' && <span className="timeline-boundary fork"><b>EXECUTION FORK</b><strong>{String(event.payload?.reason || 'A new continuation begins here')}</strong><small>Immutable lineage preserved at cursor {event.cursor}.</small></span>}
      </span>
    </button></li>
  )
}

function TimelineEmpty({ title, body }: { title: string; body: string }) { return <div className="execution-timeline-state empty"><span className="timeline-empty-mark">⑂</span><strong>{title}</strong><p>{body}</p></div> }
