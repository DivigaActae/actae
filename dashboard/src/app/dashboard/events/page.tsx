// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useState, useCallback, useEffect, useRef, useMemo, Suspense } from 'react'
import { useSearchParams, useRouter } from 'next/navigation'
import ProtectedLayout from '@/components/ProtectedLayout'
import ChannelSelector from '@/components/ChannelSelector'
import EventTypeFilter from '@/components/EventTypeFilter'
import JsonTree from '@/components/JsonTree'
import DashboardPageHeader from '@/components/DashboardPageHeader'
import { queryEvents, listChannels, getPreferences, putPreferences } from '@/lib/api'
import { computeFieldCounts, filterPayload } from '@/lib/payload-fields'
import { subscribe, addMessageListener, addStatusListener, type LiveStatus } from '@/lib/live'

const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_MODE === 'true'

const PAGE_SIZE = 100
const CHANNEL_POLL_MS = 15000

export interface QueryEvent {
  id?: string
  channel_id?: string
  type?: string
  payload?: Record<string, unknown> | null
  actor?: string | null
  cursor?: number | null
  channel_cursor?: number | null
  timestamp?: string
  depends_on?: string | null
}

function getTypeColor(type: string) {
  if (type.includes('error') || type.includes('crash')) return 'bg-red-900 text-red-400'
  if (type.includes('completed') || type.includes('done')) return 'bg-emerald-900 text-emerald-400'
  if (type.includes('started')) return 'bg-blue-900 text-blue-400'
  if (type.includes('tool.')) return 'bg-pink-900 text-pink-400'
  if (type.includes('saved') || type.includes('checkpoint')) return 'bg-purple-900 text-purple-400'
  return 'bg-gray-800 text-gray-400'
}

function eventKey(event: QueryEvent) {
  return `${event.channel_id || ''}:${event.channel_cursor ?? event.cursor ?? ''}`
}

function payloadPreview(payload: Record<string, unknown> | null | undefined) {
  if (!payload || Object.keys(payload).length === 0) return 'No payload'
  const preferred = ['output', 'input', 'message', 'content', 'status', 'error']
  const key = preferred.find(candidate => candidate in payload) || Object.keys(payload)[0]
  const value = payload[key]
  const text = typeof value === 'string' ? value : JSON.stringify(value)
  return `${key}: ${text || '—'}`.replace(/\s+/g, ' ').trim()
}

export default function EventsPage() {
  return (
    <Suspense fallback={<ProtectedLayout><div className="p-6 lg:p-8"><div className="skeleton h-8 w-40 mb-6" /><div className="space-y-2">{Array.from({ length: 3 }).map((_, i) => <div key={i} className="skeleton h-24 rounded-lg" />)}</div></div></ProtectedLayout>}>
      <EventsContent />
    </Suspense>
  )
}

function EventsContent() {
  const searchParams = useSearchParams()
  const router = useRouter()
  const [channelId, setChannelId] = useState(searchParams.get('channel') || '')
  const [events, setEvents] = useState<QueryEvent[]>([])
  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [hasMore, setHasMore] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [highlightCursor, setHighlightCursor] = useState<number | null>(null)
  const [highlightChannel, setHighlightChannel] = useState<string | null>(null)
  const [typeFilter, setTypeFilter] = useState<Set<string> | null>(null)
  const [fieldFilter, setFieldFilter] = useState<Set<string> | null>(null)
  const [allChannels, setAllChannels] = useState<string[]>([])
  const [liveStatus, setLiveStatus] = useState<LiveStatus | null>(null)
  const [search, setSearch] = useState(searchParams.get('q') || '')
  const [selectedEventKey, setSelectedEventKey] = useState<string | null>(null)
  const [detailView, setDetailView] = useState<'tree' | 'json'>('tree')
  const eventRefs = useRef<Map<string, HTMLElement>>(new Map())
  const prefsTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Restore the user's stored Events filters (type + field) on mount.
  useEffect(() => {
    if (MOCK_MODE) return
    let cancelled = false
    void getPreferences().then(({ data }) => {
      if (cancelled) return
      const ev = (data as any)?.prefs?.events ?? {}
      if (Array.isArray(ev.visibleEventTypes)) {
        setTypeFilter((ev.visibleEventTypes as string[]).length > 0 ? new Set(ev.visibleEventTypes as string[]) : null)
      }
      if (Array.isArray(ev.visibleFields)) {
        setFieldFilter((ev.visibleFields as string[]).length > 0 ? new Set(ev.visibleFields as string[]) : null)
      }
    }).catch(() => { /* never block the page on prefs */ })
    return () => { cancelled = true }
  }, [])

  // Debounced best-effort persistence of the Events filters.
  const persistEventFilters = useCallback((types: Set<string> | null, fields: Set<string> | null) => {
    if (MOCK_MODE) return
    if (prefsTimer.current) clearTimeout(prefsTimer.current)
    prefsTimer.current = setTimeout(() => {
      prefsTimer.current = null
      putPreferences({
        events: {
          visibleEventTypes: types ? Array.from(types) : [],
          visibleFields: fields ? Array.from(fields) : [],
        },
      }).catch(() => { /* best-effort */ })
    }, 300)
  }, [])

  // Never fire a pending save after the page unmounts.
  useEffect(() => () => {
    if (prefsTimer.current) {
      clearTimeout(prefsTimer.current)
      prefsTimer.current = null
    }
  }, [])

  // Channels we are live-subscribed to: the selected channel when one is
  // chosen, otherwise every known channel (so "all channels" is live too).
  const liveTopics = useMemo(() => {
    if (MOCK_MODE) return []
    return channelId ? [channelId] : allChannels
  }, [channelId, allChannels])

  // The message listener reads the current topic set through a ref to avoid
  // stale closures re-subscribing on every event.
  const liveTopicsRef = useRef<string[]>(liveTopics)
  useEffect(() => {
    liveTopicsRef.current = liveTopics
  }, [liveTopics])

  // Refresh the channel list so SDK-created channels (and forks) go live
  // here without a reload.
  useEffect(() => {
    if (MOCK_MODE) return
    let cancelled = false
    async function refresh() {
      const { data } = await listChannels()
      if (cancelled) return
      const channelList = (data as any)?.channels ?? data
      if (Array.isArray(channelList)) {
        setAllChannels(channelList.map((c: any) => typeof c === 'string' ? c : c.id || c.channel_id))
      }
    }
    void refresh()
    const timer = setInterval(() => void refresh(), CHANNEL_POLL_MS)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [])

  // Live event subscription: append new events as they arrive, deduplicated
  // against what the paged query has already loaded.
  useEffect(() => {
    if (MOCK_MODE || liveTopics.length === 0) return
    const unsubs = liveTopics.map((topic) => subscribe(topic))
    const offStatus = addStatusListener(setLiveStatus)
    const offMessage = addMessageListener((evt) => {
      if (!liveTopicsRef.current.includes(evt.channel)) return
      setEvents((prev) => {
        const key = evt.id || `${evt.channel}:${evt.cursor}`
        const exists = prev.some(
          (e) => (e.id || `${e.channel_id || ''}:${e.cursor}`) === key
        )
        if (exists) return prev
        const live: QueryEvent = {
          id: evt.id,
          channel_id: evt.channel,
          type: evt.type,
          payload: evt.payload,
          cursor: evt.cursor,
          channel_cursor: evt.cursor,
          timestamp: evt.timestamp.toISOString(),
        }
        return [...prev, live]
      })
    })
    return () => {
      unsubs.forEach((unsub) => unsub())
      offStatus()
      offMessage()
    }
  }, [liveTopics])

  const deepLinkCursor = useRef<number | null>(
    searchParams.get('cursor') !== null ? Number(searchParams.get('cursor')) : null
  )
  // Track the last `?q=` we applied so an unrelated URL change (selecting a
  // row/channel calls router.replace) does not re-apply a search the user has
  // since cleared.
  const lastAppliedQ = useRef<string | null>(searchParams.get('q'))

  const fetchEvents = useCallback(async (channel: string, offset: number, aroundCursor?: number | null) => {
    setLoading(offset === 0)
    setLoadingMore(offset > 0)
    setError(null)

    if (MOCK_MODE) {
      const { MOCK_EVENTS } = await import('@/lib/mock-data')
      const all: QueryEvent[] = []
      for (const [ch, evts] of Object.entries(MOCK_EVENTS)) {
        if (channel && ch !== channel) continue
        for (const e of evts as any[]) {
          if (e.isState) continue
          all.push({
            id: e.id,
            channel_id: e.channel_id || ch,
            type: e.event_type || e.type,
            payload: e.payload || {},
            cursor: e.cursor,
            channel_cursor: e.cursor,
            timestamp: e.timestamp,
          })
        }
      }
      all.sort((a, b) => (a.cursor ?? 0) - (b.cursor ?? 0))
      const page = all.slice(offset, offset + PAGE_SIZE)
      if (offset === 0) setEvents(page)
      else setEvents(prev => [...prev, ...page])
      setHasMore(offset + page.length < all.length)
      setLoading(false)
      setLoadingMore(false)
      return
    }

    const { data, error: err } = await queryEvents({
      ...(channel ? { channel_ids: [channel] } : {}),
      ...(aroundCursor !== null && aroundCursor !== undefined
        ? {
            cursor_start: Math.max(0, aroundCursor - PAGE_SIZE + 1),
            cursor_end: aroundCursor,
          }
        : {}),
      limit: PAGE_SIZE,
      ...(aroundCursor === null || aroundCursor === undefined ? { offset } : {}),
    })
    setLoading(false)
    setLoadingMore(false)
    if (err) { setError(err); return }
    const page: QueryEvent[] = (data as any)?.events ?? []
    if (offset === 0 || aroundCursor !== null && aroundCursor !== undefined) setEvents(page)
    else setEvents(prev => {
      const seen = new Set(prev.map(e => e.id || `${e.channel_id}:${e.cursor}`))
      return [...prev, ...page.filter(e => !seen.has(e.id || `${e.channel_id}:${e.cursor}`))]
    })
    setHasMore(aroundCursor !== null && aroundCursor !== undefined ? false : page.length === PAGE_SIZE)
  }, [])

  useEffect(() => {
    setEvents([])
    setHasMore(true)
    setHighlightCursor(null)
    setHighlightChannel(null)
    const target = deepLinkCursor.current
    deepLinkCursor.current = null
    fetchEvents(channelId, 0, target)
  }, [channelId, fetchEvents])

  useEffect(() => {
    const channel = searchParams.get('channel')
    const selectedChannel = searchParams.get('selected_channel')
    const cursor = searchParams.get('cursor')
    if (channel && channel !== channelId) {
      setChannelId(channel)
    }
    if (cursor !== null && Number.isFinite(Number(cursor))) {
      deepLinkCursor.current = Number(cursor)
      setHighlightCursor(Number(cursor))
      setHighlightChannel(selectedChannel || channel)
    }
    const q = searchParams.get('q')
    if (q !== null && q !== lastAppliedQ.current) {
      lastAppliedQ.current = q
      setSearch(q)
    }
  }, [searchParams]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (highlightCursor === null) return
    let target = eventRefs.current.get(`${highlightChannel || ''}:${highlightCursor}`)
    if (!target) {
      const entries = Array.from(eventRefs.current.entries())
      for (const [k, el] of entries) {
        if (k.endsWith(`:${highlightCursor}`)) { target = el; break }
      }
    }
    if (target) {
      target.scrollIntoView({ behavior: 'smooth', block: 'center' })
    }
  }, [highlightCursor, highlightChannel, events])

  function handleSelectChannel(id: string) {
    const params = new URLSearchParams(searchParams.toString())
    if (id) params.set('channel', id)
    else params.delete('channel')
    params.delete('cursor')
    params.delete('selected_channel')
    deepLinkCursor.current = null
    setSelectedEventKey(null)
    router.replace(`/dashboard/events/${params.toString() ? `?${params.toString()}` : ''}`)
    setChannelId(id)
  }

  function handleSelectEvent(evt: QueryEvent) {
    const params = new URLSearchParams(searchParams.toString())
    // Selecting a row must not silently turn "All channels" into a channel
    // filter. Keep a separate disambiguator for the selected event instead.
    if (!channelId && evt.channel_id) {
      params.delete('channel')
      params.set('selected_channel', evt.channel_id)
    } else {
      params.delete('selected_channel')
    }
    const cursor = evt.channel_cursor ?? evt.cursor
    if (cursor !== null && cursor !== undefined) params.set('cursor', String(cursor))
    router.replace(`/dashboard/events/?${params.toString()}`)
    setSelectedEventKey(eventKey(evt))
    setHighlightCursor(null)
    setHighlightChannel(null)
  }

  const totalShown = events.length

  // The API returns events newest-first (cursor DESC); display them oldest
  // first so a run reads top-to-bottom like a log.
  const typeCounts = useMemo(() => {
    const counts = new Map<string, number>()
    for (const e of events) {
      const t = e.type || 'unknown'
      counts.set(t, (counts.get(t) || 0) + 1)
    }
    return Array.from(counts.entries())
      .map(([type, count]) => ({ type, count }))
      .sort((a, b) => b.count - a.count)
  }, [events])

  const fieldCounts = useMemo(() => computeFieldCounts(events), [events])

  const displayEvents = useMemo(() => {
    const filtered = typeFilter
      ? events.filter(e => typeFilter.has(e.type || 'unknown'))
      : events
    const q = search.trim().toLowerCase()
    const searched = q ? filtered.filter(event =>
      [event.type, event.channel_id, event.actor, event.id, JSON.stringify(event.payload || {})]
        .some(value => String(value || '').toLowerCase().includes(q))
    ) : filtered
    return [...searched].sort((a, b) => {
      // A channel cursor is only ordered within its own channel. The all-
      // channels ledger must use timestamps or unrelated cursor sequences
      // become interleaved arbitrarily.
      if (!channelId) {
        const timeDelta = new Date(a.timestamp || 0).getTime() - new Date(b.timestamp || 0).getTime()
        if (timeDelta !== 0) return timeDelta
        const channelDelta = String(a.channel_id || '').localeCompare(String(b.channel_id || ''))
        if (channelDelta !== 0) return channelDelta
      }
      return (a.channel_cursor ?? a.cursor ?? 0) - (b.channel_cursor ?? b.cursor ?? 0)
    })
  }, [events, typeFilter, search, channelId])

  const selectedEvent = displayEvents.find(event => eventKey(event) === selectedEventKey)
    || displayEvents.find(event => (event.channel_cursor ?? event.cursor) === highlightCursor && event.channel_id === highlightChannel)
    || displayEvents[0]
  const selectedIndex = selectedEvent ? displayEvents.indexOf(selectedEvent) : -1

  function moveSelection(delta: number) {
    if (displayEvents.length === 0) return
    const next = displayEvents[Math.min(displayEvents.length - 1, Math.max(0, selectedIndex + delta))]
    if (next) handleSelectEvent(next)
  }

  const shownOf = typeFilter
    ? `${displayEvents.length} of ${events.length} event${events.length === 1 ? '' : 's'}`
    : `${events.length} event${events.length === 1 ? '' : 's'}`

  return (
    <ProtectedLayout>
      <div className="console-page console-events w-full max-w-[1720px]">
        <DashboardPageHeader
          eyebrow="Immutable event ledger"
          title="Events"
          description="Search the exact history of every agent run, then inspect its payload without losing your place."
          meta={<div className="flex items-center gap-3">
            {!MOCK_MODE && (
              <span
                title="Live event stream"
                className={`inline-flex items-center gap-1.5 text-[10px] font-medium px-2 py-0.5 rounded-full border ${
                  liveStatus?.authenticated
                    ? 'text-emerald-400 border-emerald-500/30 bg-emerald-500/10'
                    : liveStatus?.connected
                      ? 'text-amber-400 border-amber-500/30 bg-amber-500/10'
                      : 'text-gray-500 border-gray-700 bg-gray-800/50'
                }`}
              >
                <span className={`w-1.5 h-1.5 rounded-full ${liveStatus?.authenticated ? 'bg-emerald-400 animate-pulse' : liveStatus?.connected ? 'bg-amber-400' : 'bg-gray-500'}`} />
                {liveStatus?.authenticated ? 'Live' : liveStatus?.connected ? 'Connecting…' : 'Offline'}
              </span>
            )}
            <span className="text-xs text-gray-500">
              {totalShown > 0 && `${shownOf} shown`}
            </span>
          </div>}
        />

        <div className="console-toolbar flex flex-wrap items-center gap-2">
          <div className="min-w-[280px] flex-[1.2]">
            <ChannelSelector
              value={channelId}
              onChange={handleSelectChannel}
              placeholder="All channels"
              externalChannels={allChannels}
            />
          </div>
          <EventTypeFilter
            types={typeCounts}
            selected={typeFilter}
            onChange={(next: Set<string> | null) => {
              setTypeFilter(next)
              persistEventFilters(next, fieldFilter)
            }}
            label="Types"
          />
          {(search || typeFilter || fieldFilter) && <button onClick={() => {
            setSearch('')
            setTypeFilter(null)
            setFieldFilter(null)
            persistEventFilters(null, null)
          }} className="btn-ghost btn-sm whitespace-nowrap">Reset view</button>}
          <div className="relative min-w-[220px] flex-1 max-w-md">
            <svg className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
            <input aria-label="Search events" value={search} onChange={event => setSearch(event.target.value)} placeholder="Search type, channel, actor, id, or payload…" className="w-full rounded-lg border border-gray-700 bg-gray-800/70 py-2 pl-9 pr-3 text-xs text-gray-200 outline-none placeholder:text-gray-600 focus:border-actae-500" />
          </div>
          <EventTypeFilter
            types={fieldCounts}
            selected={fieldFilter}
            onChange={(next: Set<string> | null) => {
              setFieldFilter(next)
              persistEventFilters(typeFilter, next)
            }}
            label="Fields"
            searchPlaceholder="Search fields..."
            allCheckedTitle="Filter by payload field"
          />
        </div>

        {error && (
          <div className="bg-red-950 border border-red-800 text-red-400 px-4 py-3 rounded-lg text-sm mb-4">{error}</div>
        )}

        {loading && (
          <div className="space-y-2">
            {[1, 2, 3].map(i => <div key={i} className="skeleton h-24 rounded-lg" />)}
          </div>
        )}

        {!loading && events.length === 0 && !error && (
          <div className="text-center text-gray-500 py-16">
            <p className="text-sm">No events found{channelId ? ' for this channel' : ''}.</p>
          </div>
        )}

        {!loading && events.length > 0 && displayEvents.length === 0 && (
          <div className="text-center text-gray-500 py-16">
            <p className="text-sm">No events match the current type filter.</p>
            <button
              onClick={() => {
                setTypeFilter(null)
                persistEventFilters(null, fieldFilter)
              }}
              className="mt-3 px-3 py-1.5 bg-actae-500 hover:bg-actae-400 text-white rounded-lg text-sm transition-colors"
            >
              Show all types
            </button>
          </div>
        )}

        {!loading && displayEvents.length > 0 && (
          <div className="console-split-view grid grid-cols-1 xl:grid-cols-[minmax(420px,0.9fr)_minmax(440px,1.1fr)] items-start">
            <section className="console-ledger">
              <div className="console-panel-label"><span>Chronological record</span><span>{displayEvents.length} visible</span></div>
              <div className="space-y-1 p-2">
            {displayEvents.map((evt) => {
              const key = eventKey(evt)
              const cursor = evt.channel_cursor ?? evt.cursor
              const isHighlighted = highlightCursor === cursor && highlightChannel === (evt.channel_id || null)
              return (
                <button
                  type="button"
                  key={key}
                  ref={el => { if (el) eventRefs.current.set(key, el); else eventRefs.current.delete(key) }}
                  onClick={() => handleSelectEvent(evt)}
                  className={`block w-full bg-gray-900/70 border rounded-lg cursor-pointer text-left transition-all ${
                    isHighlighted
                      ? 'border-actae-500 ring-2 ring-actae-500/40'
                      : selectedEvent === evt ? 'border-purple-500/50 bg-purple-500/5' : 'border-gray-800 hover:border-gray-700 hover:bg-gray-800/50'
                  }`}
                >
                  <div className="grid grid-cols-[52px_minmax(110px,auto)_minmax(0,1fr)_auto] items-center gap-3 px-3 pt-2.5">
                    <span className="text-[11px] font-mono text-gray-500 shrink-0 w-12">
                      @{cursor}
                    </span>
                    <span className={`px-2 py-0.5 rounded text-[11px] font-medium shrink-0 ${getTypeColor(evt.type || '')}`}>
                      {evt.type}
                    </span>
                    {evt.channel_id && (
                      <span className="text-[11px] font-mono text-gray-400 truncate min-w-0" title={evt.channel_id}>
                        {evt.channel_id}
                      </span>
                    )}
                    <span className="ml-auto text-[10px] text-gray-600 shrink-0">
                      {evt.timestamp ? new Date(evt.timestamp).toLocaleTimeString() : ''}
                    </span>
                  </div>
                  <div className="truncate px-3 pb-2.5 pl-[77px] font-mono text-[10px] text-gray-600" title={payloadPreview(evt.payload)}>{payloadPreview(evt.payload)}</div>
                </button>
              )
            })}

            {hasMore && (
              <div className="flex justify-center pt-2">
                <button
                  onClick={() => fetchEvents(channelId, events.length)}
                  disabled={loadingMore}
                  className="btn-secondary btn-sm"
                >
                  {loadingMore ? 'Loading...' : 'Load more'}
                </button>
              </div>
            )}
              </div>
            </section>

            {selectedEvent && (
              <aside className="console-inspector sticky top-6 overflow-hidden">
                <div className="border-b border-gray-800 p-4">
                  <div className="flex items-center justify-between gap-3"><div className="flex min-w-0 items-center gap-2"><span className="text-[10px] font-semibold uppercase tracking-wider text-purple-400">Selected event</span><span className="truncate font-mono text-xs text-gray-500">{selectedEvent.type} · cursor {selectedEvent.channel_cursor ?? selectedEvent.cursor}</span></div><div className="flex shrink-0 items-center gap-1"><button aria-label="Previous event" disabled={selectedIndex <= 0} onClick={() => moveSelection(-1)} className="btn-ghost btn-xs disabled:opacity-30">↑</button><span className="px-1 text-[9px] text-gray-600">{selectedIndex + 1}/{displayEvents.length}</span><button aria-label="Next event" disabled={selectedIndex >= displayEvents.length - 1} onClick={() => moveSelection(1)} className="btn-ghost btn-xs disabled:opacity-30">↓</button></div></div>
                  <div className="mt-2 break-all font-mono text-xs text-gray-300">{selectedEvent.channel_id}</div>
                  <div className="mt-2 flex flex-wrap gap-x-5 gap-y-1 text-[10px] text-gray-500">
                    {selectedEvent.timestamp && <span>{new Date(selectedEvent.timestamp).toLocaleString()}</span>}
                    {selectedEvent.actor && <span>actor: <b className="font-mono font-normal text-gray-300">{selectedEvent.actor}</b></span>}
                    {selectedEvent.id && <span className="font-mono">id: {selectedEvent.id}</span>}
                    {selectedEvent.depends_on && <span className="font-mono">depends_on: {selectedEvent.depends_on}</span>}
                  </div>
                </div>
                <div className="flex items-center justify-between border-b border-gray-800 px-4 py-2">
                  <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Payload</span>
                  <div className="flex rounded-md bg-gray-800 p-0.5">
                    {(['tree', 'json'] as const).map(view => <button key={view} onClick={() => setDetailView(view)} className={`rounded px-2 py-1 text-[10px] uppercase ${detailView === view ? 'bg-gray-700 text-white' : 'text-gray-500'}`}>{view}</button>)}
                  </div>
                </div>
                <div className="max-h-[calc(100vh-320px)] overflow-auto bg-gray-950 p-4">
                  {detailView === 'tree' ? <JsonTree data={filterPayload(selectedEvent.payload || {}, fieldFilter)} /> : <pre className="whitespace-pre-wrap break-all font-mono text-[11px] leading-relaxed text-gray-300">{JSON.stringify(filterPayload(selectedEvent.payload || {}, fieldFilter), null, 2)}</pre>}
                </div>
              </aside>
            )}
          </div>
        )}
      </div>
    </ProtectedLayout>
  )
}
