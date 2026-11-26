// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import Link from 'next/link'
import ProtectedLayout from '@/components/ProtectedLayout'
import DashboardPageHeader from '@/components/DashboardPageHeader'
import { getChannelMetadata, listChannels, type ChannelMetadata } from '@/lib/api'
import { useFlowStore, type DashboardEvent } from '@/lib/flow-store'
import { DEMO_CHANNELS, DEMO_CHANNEL, DEMO_FORK_CHANNEL } from '@/lib/demo-scenario'

const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_MODE === 'true'
const ACTIVE_WINDOW_MS = 60_000
const RECENT_WINDOW_MS = 60 * 60 * 1000
const ENRICH_LIMIT = 60
const ENRICH_CONCURRENCY = 6
const META_CACHE_MAX = 500

/** Run `fn` over `items` with at most `limit` in flight. */
async function mapLimit<T>(items: T[], limit: number, fn: (item: T) => Promise<void>): Promise<void> {
  const queue = [...items]
  const workers = Array.from({ length: Math.min(limit, queue.length) }, async () => {
    while (queue.length > 0) {
      const item = queue.shift() as T
      await fn(item)
    }
  })
  await Promise.all(workers)
}

export interface RunRow {
  id: string
  displayName: string | null
  eventCount: number | null
  latestCursor: number | null
  lastEventAt: string | null
  parentId: string | null
  isFork: boolean
}

function runStatus(lastEventAt: string | null): { label: string; dot: string } {
  if (!lastEventAt) return { label: 'Idle', dot: 'bg-gray-500' }
  const age = Date.now() - new Date(lastEventAt).getTime()
  if (age <= ACTIVE_WINDOW_MS) return { label: 'Active', dot: 'bg-emerald-400' }
  if (age <= RECENT_WINDOW_MS) return { label: 'Recent', dot: 'bg-sky-400' }
  return { label: 'Idle', dot: 'bg-gray-500' }
}

/** Prepend rows for the demo replay channels, derived from the flow store's
 * live events, so a replayed run shows up (and its counts grow) in the Runs
 * list. Channels with no events yet are omitted. */
function withDemoRows(base: RunRow[], events: DashboardEvent[]): RunRow[] {
  const demo = (DEMO_CHANNELS as readonly string[]).flatMap(channel => {
    const channelEvents = events.filter(event => event.channel === channel)
    if (channelEvents.length === 0) return []
    const latestCursor = channelEvents.reduce((max, event) => Math.max(max, event.cursor), 0)
    const lastEventAt = channelEvents.map(event => String(event.timestamp)).filter(Boolean).sort().slice(-1)[0] ?? null
    return [{
      id: channel,
      displayName: channel === DEMO_FORK_CHANNEL ? 'Demo run · fork' : 'Demo run · live',
      eventCount: channelEvents.length,
      latestCursor,
      lastEventAt,
      parentId: channel === DEMO_FORK_CHANNEL ? DEMO_CHANNEL : null,
      isFork: channel === DEMO_FORK_CHANNEL,
    }]
  })
  return [...demo, ...base]
}

function formatRelative(iso: string | null): string {
  if (!iso) return '—'
  const then = new Date(iso).getTime()
  if (!Number.isFinite(then)) return '—'
  const secs = Math.max(0, Math.round((Date.now() - then) / 1000))
  if (secs < 60) return `${secs}s ago`
  const mins = Math.round(secs / 60)
  if (mins < 60) return `${mins}m ago`
  const hours = Math.round(mins / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.round(hours / 24)
  return `${days}d ago`
}

export default function RunsPage() {
  const [runs, setRuns] = useState<RunRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [query, setQuery] = useState('')
  // Metadata is stable, so cache it across the 15s poll instead of re-fetching
  // every run's metadata on every tick. `generation` discards the results of a
  // superseded load so an in-flight enrichment can't clobber a newer list.
  const metaCache = useRef<Map<string, ChannelMetadata | null>>(new Map())
  const generation = useRef(0)

  const load = useCallback(async () => {
    const gen = ++generation.current
    setError(null)
    if (MOCK_MODE) {
      const { MOCK_CHANNELS } = await import('@/lib/mock-data')
      if (gen !== generation.current) return
      setRuns(withDemoRows(MOCK_CHANNELS.map(id => ({
        id,
        displayName: id.replace(/[-_]/g, ' '),
        eventCount: 5,
        latestCursor: 5,
        lastEventAt: new Date().toISOString(),
        parentId: id.includes('exp') ? 'agent-research-run' : null,
        isFork: id.includes('exp'),
      })), useFlowStore.getState().events))
      setLoading(false)
      return
    }

    const { data, error: err } = await listChannels()
    if (gen !== generation.current) return
    if (err) { setError(err); setLoading(false); return }
    const channelList = (data as any)?.channels ?? data
    if (!Array.isArray(channelList)) { setLoading(false); return }
    const ids: string[] = channelList.map((c: any) => typeof c === 'string' ? c : c.id || c.channel_id)
    type StatEntry = {
      event_count?: number
      latest_cursor?: number | null
      last_event_at?: string | null
      display_name?: string | null
      parent_channel_id?: string | null
    }
    const stats = (data as any)?.stats as Record<string, StatEntry> | undefined
    // Newer servers include the friendly name + fork parent in `stats`, so the
    // dashboard needs no per-channel metadata lookups. Only fall back to
    // enrichment when the fields are absent (older servers).
    const statsHaveMeta = !!stats && Object.values(stats).some(s => s && 'display_name' in s)

    const applyMeta = (run: RunRow): RunRow => {
      const meta = metaCache.current.get(run.id)
      if (!meta) return run
      return {
        ...run,
        displayName: meta.display_name ?? run.displayName,
        parentId: meta.parent_channel_id ?? null,
        isFork: !!meta.parent_channel_id,
      }
    }

    const base: RunRow[] = ids.map(id => {
      const s = stats?.[id] ?? {}
      return applyMeta({
        id,
        displayName: s.display_name ?? null,
        eventCount: s.event_count ?? null,
        latestCursor: s.latest_cursor ?? null,
        lastEventAt: s.last_event_at ?? null,
        parentId: s.parent_channel_id ?? null,
        isFork: !!s.parent_channel_id,
      })
    })
    setRuns(base)
    setLoading(false)

    if (statsHaveMeta) return

    // Best-effort: resolve friendly names + fork lineage for runs we have not
    // enriched yet, bounded in count and concurrency so a large list never
    // storms the API.
    if (metaCache.current.size > META_CACHE_MAX) metaCache.current.clear()
    const uncached = ids.filter(id => !metaCache.current.has(id)).slice(0, ENRICH_LIMIT)
    if (uncached.length === 0) return
    await mapLimit(uncached, ENRICH_CONCURRENCY, async id => {
      try {
        const { data: meta } = await getChannelMetadata(id)
        metaCache.current.set(id, meta ?? null)
      } catch {
        metaCache.current.set(id, null)
      }
    })
    if (gen !== generation.current) return
    setRuns(current => current.map(applyMeta))
  }, [])

  useEffect(() => {
    void load()
    const timer = setInterval(() => void load(), 15000)
    return () => clearInterval(timer)
  }, [load])

  // Keep the demo-run rows in sync with the live replay (mock mode only).
  const storeEvents = useFlowStore(s => s.events)
  useEffect(() => {
    if (!MOCK_MODE) return
    setRuns(previous => {
      if (previous.length === 0) return previous
      const demo = new Set<string>(DEMO_CHANNELS)
      return withDemoRows(previous.filter(run => !demo.has(run.id)), storeEvents)
    })
  }, [storeEvents])

  const visible = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return runs
    return runs.filter(run =>
      run.id.toLowerCase().includes(q) || (run.displayName || '').toLowerCase().includes(q)
    )
  }, [runs, query])

  const forkCount = runs.filter(run => run.isFork).length

  return (
    <ProtectedLayout>
      <div className="console-page w-full max-w-[1720px]">
        <DashboardPageHeader
          eyebrow="Runs"
          title="Agent runs"
          description="Every recorded run, newest first. Open one to trace its steps, or search the full event ledger."
          actions={
            <>
              <Link href="/dashboard/events/" className="btn-secondary">Search events</Link>
              <Link href="/dashboard/experiments/" className="btn-secondary">Compare forks</Link>
            </>
          }
          meta={<span className="console-kicker">{runs.length} RUN{runs.length === 1 ? '' : 'S'} · {forkCount} FORK{forkCount === 1 ? '' : 'S'}</span>}
        />

        {error && <div role="alert" className="mb-4 rounded-xl border border-red-800 bg-red-950/50 px-4 py-3 text-sm text-red-300">{error}</div>}

        <div className="console-toolbar flex flex-wrap items-center gap-2">
          <div className="relative min-w-[240px] flex-1 max-w-md">
            <svg className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
            <input
              aria-label="Filter runs"
              value={query}
              onChange={e => setQuery(e.target.value)}
              placeholder="Filter runs by name or id…"
              className="w-full rounded-lg border border-gray-700 bg-gray-800/70 py-2 pl-9 pr-3 text-xs text-gray-200 outline-none placeholder:text-gray-600 focus:border-actae-500"
            />
          </div>
          <span className="text-xs text-gray-500">{visible.length} of {runs.length} shown</span>
        </div>

        {loading ? (
          <div className="space-y-2">
            {[1, 2, 3, 4].map(i => <div key={i} className="skeleton h-16 rounded-xl" />)}
          </div>
        ) : runs.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-gray-700 bg-gray-900/40 py-20 text-center">
            <div className="text-base font-semibold text-gray-200">No runs yet</div>
            <p className="mx-auto mt-2 max-w-md text-sm text-gray-500">
              Start an agent with the SDK and its run will appear here automatically. Replays and forks show up the same way.
            </p>
            <Link href="/dashboard/flow/" className="mt-5 inline-flex btn-secondary">Open the run viewer</Link>
          </div>
        ) : visible.length === 0 ? (
          <div className="text-center text-gray-500 py-16">
            <p className="text-sm">No runs match “{query}”.</p>
            <button onClick={() => setQuery('')} className="mt-3 btn-secondary btn-sm">Clear filter</button>
          </div>
        ) : (
          <div className="overflow-x-auto rounded-xl border border-gray-800">
            <div className="min-w-[720px]">
            <div className="grid grid-cols-[minmax(0,2fr)_110px_100px_120px_170px] gap-3 border-b border-gray-800 bg-gray-800/50 px-4 py-2 text-[10px] font-semibold uppercase tracking-wider text-gray-500">
              <span>Run</span><span>Status</span><span>Events</span><span>Last activity</span><span className="text-right">Actions</span>
            </div>
            {visible.map(run => {
              const status = runStatus(run.lastEventAt)
              return (
                <div key={run.id} className="grid grid-cols-[minmax(0,2fr)_110px_100px_120px_170px] items-center gap-3 border-b border-gray-800/70 px-4 py-3 last:border-0 hover:bg-gray-800/20">
                  <div className="min-w-0">
                    <div className="flex items-center gap-2 min-w-0">
                      <span className="truncate text-sm font-medium text-gray-100" title={run.id}>
                        {run.displayName || run.id}
                      </span>
                      {run.isFork && <span className="shrink-0 rounded-full bg-purple-500/10 px-1.5 py-0.5 text-[9px] font-semibold uppercase text-purple-300">fork</span>}
                    </div>
                    {run.displayName && <div className="truncate font-mono text-[10px] text-gray-600" title={run.id}>{run.id}</div>}
                    {run.isFork && run.parentId && <div className="truncate font-mono text-[10px] text-gray-600">from {run.parentId}</div>}
                  </div>
                  <span className="inline-flex w-fit items-center gap-1.5 text-[11px] text-gray-400">
                    <span className={`h-1.5 w-1.5 rounded-full ${status.dot}`} />
                    {status.label}
                  </span>
                  <span className="font-mono text-xs text-gray-400">{run.eventCount != null ? run.eventCount.toLocaleString() : '—'}</span>
                  <span className="text-xs text-gray-500" title={run.lastEventAt ?? undefined}>{formatRelative(run.lastEventAt)}</span>
                  <div className="flex justify-end gap-2">
                    <Link href={`/dashboard/flow/?channel=${encodeURIComponent(run.id)}`} className="btn-secondary btn-xs">Open run</Link>
                    <Link href={`/dashboard/events/?channel=${encodeURIComponent(run.id)}`} className="btn-ghost btn-xs">Events</Link>
                  </div>
                </div>
              )
            })}
            </div>
          </div>
        )}
      </div>
    </ProtectedLayout>
  )
}
