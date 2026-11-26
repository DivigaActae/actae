// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useState, useRef, useCallback } from 'react'
import Link from 'next/link'
import ProtectedLayout from '@/components/ProtectedLayout'
import DashboardPageHeader from '@/components/DashboardPageHeader'
import { getMetrics, listChannels, getLatestCursor, getPreferences, putPreferences, type MetricsResponse, type SubscriptionStatus } from '@/lib/api'
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, AreaChart, Area, Cell } from 'recharts'

const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_MODE === 'true'

const RECENT_RUN_LIMIT = 8

function generateMockMetrics(): MetricsResponse {
  const connections = Math.floor(Math.random() * 80) + 20
  const sent = Math.floor(Math.random() * 5000) + 1000
  const received = Math.floor(Math.random() * 3000) + 500
  return {
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptime_seconds: 86400 + Math.floor(Math.random() * 3600),
    mode: 'dev',
    embedded_pg: true,
    events: {
      total: 1240,
      channels: 3,
      state_snapshots: 8,
    },
    subscription: {
      mode: 'standalone',
      status: 'standalone',
      subscription: 'unknown',
      rev: 0,
      lock_imminent: false,
      docs_url: 'https://docs.diviga.com/actae/subscriptions',
    },
    websocket: {
      connections,
      topics: 2,
      topic_stats: { 'langgraph-agent-prod': 1, 'crewai-research-crew': 1 },
      messages_sent: sent,
      messages_received: received,
      total_messages: sent + received,
      back_pressure: {
        total_consumers: 2,
        slow_consumers: 0,
        total_queued_messages: 0,
        total_dropped_messages: 0,
        available_permits: 1000,
        max_queue_size: 10000,
      },
    },
  }
}

const MOCK_CHANNELS = ['langgraph-agent-prod', 'crewai-research-crew', 'oncall-automation']

const INTENTS = [
  { href: '/dashboard/runs/', title: 'Browse runs', body: 'Every agent run, newest first. Open one to trace its steps.' },
  { href: '/dashboard/events/', title: 'Find an event', body: 'Search the exact history of everything that was recorded.' },
  { href: '/dashboard/experiments/', title: 'Compare forks', body: 'See how two runs diverged and promote the stronger one.' },
  { href: '/dashboard/approvals/', title: 'Approve a request', body: 'Decisions an agent is waiting on a person to make.' },
]

const CHART_COLORS = {
  blue: '#c8ff3d',
  purple: '#8cb8ff',
  green: '#10b981',
  amber: '#f59e0b',
  red: '#ef4444',
  slate: '#94a3b8',
}

function formatUptime(seconds: number): string {
  const d = Math.floor(seconds / 86400)
  const h = Math.floor((seconds % 86400) / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = seconds % 60
  const parts: string[] = []
  if (d > 0) parts.push(`${d}d`)
  if (h > 0) parts.push(`${h}h`)
  if (m > 0) parts.push(`${m}m`)
  parts.push(`${s}s`)
  return parts.join(' ')
}

export default function OverviewPage() {
  const [metrics, setMetrics] = useState<MetricsResponse | null>(null)
  const [history, setHistory] = useState<{ time: string; connections: number; sent: number; received: number }[]>([])
  const [channels, setChannels] = useState<{ id: string; displayName: string | null; eventCount: number | null; latestCursor: number | null; lastEventAt: string | null }[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [startedDismissed, setStartedDismissed] = useState(true)
  const [systemOpen, setSystemOpen] = useState(false)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const mountedRef = useRef(true)

  const fetchMetrics = useCallback(async () => {
    if (MOCK_MODE) {
      const m = generateMockMetrics()
      if (mountedRef.current) {
        setMetrics(m)
        setHistory(prev => {
          const point = { time: new Date().toLocaleTimeString(), connections: m.websocket.connections, sent: m.websocket.messages_sent, received: m.websocket.messages_received }
          return [...prev, point].slice(-30)
        })
        setLoading(false)
        setError(null)
      }
      return
    }
    const { data, error: err } = await getMetrics()
    if (!mountedRef.current) return
    if (data) {
      setMetrics(data)
      setHistory(prev => {
        const point = { time: new Date().toLocaleTimeString(), connections: data.websocket.connections, sent: data.websocket.messages_sent, received: data.websocket.messages_received }
        return [...prev, point].slice(-30)
      })
      setError(null)
    }
    if (err) setError(err)
    setLoading(false)
  }, [])

  const loadChannels = useCallback(async () => {
    if (MOCK_MODE) {
      if (mountedRef.current) {
        setChannels(MOCK_CHANNELS.map(id => ({ id, displayName: null, eventCount: 5, latestCursor: 5, lastEventAt: null })))
      }
      return
    }
    const { data } = await listChannels()
    const channelList = (data as any)?.channels ?? data
    if (!Array.isArray(channelList)) return
    const ids = channelList.map((c: any) => typeof c === 'string' ? c : c.id || c.channel_id)
    // Prefer the server's per-channel stats (single round-trip); fall back
    // to a per-channel cursor lookup for older servers.
    const stats = (data as any)?.stats as Record<string, { event_count?: number; latest_cursor?: number | null; last_event_at?: string | null; display_name?: string | null }> | undefined
    if (stats) {
      if (mountedRef.current) {
        setChannels(ids.map(id => {
          const s = stats[id] || {}
          return {
            id,
            displayName: s.display_name ?? null,
            eventCount: s.event_count ?? null,
            latestCursor: s.latest_cursor ?? null,
            lastEventAt: s.last_event_at ?? null,
          }
        }))
      }
      return
    }
    const withCursors = await Promise.all(
      ids.slice(0, 30).map(async (id: string) => {
        try {
          const { data: cursorData } = await getLatestCursor(id)
          return { id, displayName: null, eventCount: null, latestCursor: (cursorData as any)?.latest_cursor ?? (cursorData as any)?.cursor ?? null, lastEventAt: null }
        } catch {
          return { id, displayName: null, eventCount: null, latestCursor: null, lastEventAt: null }
        }
      })
    )
    if (mountedRef.current) setChannels(withCursors)
  }, [])

  // Restore the "Getting started" dismissal from the user's preferences.
  useEffect(() => {
    if (MOCK_MODE) {
      // Mock/demo mode has no preferences API — show it (dismiss stays local).
      setStartedDismissed(false)
      return
    }
    let cancelled = false
    void getPreferences().then(({ data }) => {
      if (cancelled) return
      const onboarding = (data as any)?.prefs?.onboarding ?? {}
      setStartedDismissed(onboarding.seen === true)
    }).catch(() => { /* never block the page on prefs */ })
    return () => { cancelled = true }
  }, [])

  function dismissGettingStarted() {
    setStartedDismissed(true)
    if (MOCK_MODE) return
    putPreferences({ onboarding: { seen: true } }).catch(() => { /* best-effort */ })
  }

  useEffect(() => {
    mountedRef.current = true
    fetchMetrics()
    loadChannels()
    pollRef.current = setInterval(fetchMetrics, 5000)
    return () => { mountedRef.current = false; if (pollRef.current) clearInterval(pollRef.current) }
  }, [fetchMetrics, loadChannels])

  const recentRuns = channels.slice(0, RECENT_RUN_LIMIT)

  return (
    <ProtectedLayout>
      <div className="console-page console-overview w-full max-w-[1720px] space-y-6">
        <DashboardPageHeader
          eyebrow="Overview"
          title="Agent runs at a glance"
          description="See what's running, open any run to trace its steps, and jump to the exact event behind a signal."
          actions={<><Link href="/dashboard/runs/" className="btn-primary">Browse runs</Link><Link href="/dashboard/events/" className="btn-secondary">Search events</Link></>}
          meta={<span className="console-kicker">ACTAE / OVERVIEW</span>}
        />

        {error && <div className="bg-red-950 border border-red-800 text-red-400 px-4 py-3 rounded-xl text-sm">{error}</div>}

        {/* Orientation: what you can do here (dismissible). */}
        {!startedDismissed && (
          <section className="rounded-2xl border border-actae-400/20 bg-actae-400/[0.04] p-5">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 className="text-sm font-semibold text-white">Getting started</h2>
                <p className="mt-1 text-xs text-gray-400">New here, or back after a while? Pick the question you're trying to answer.</p>
              </div>
              <button onClick={dismissGettingStarted} className="shrink-0 text-[11px] text-gray-500 hover:text-gray-300" aria-label="Dismiss getting started">Dismiss</button>
            </div>
            <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
              {INTENTS.map(intent => (
                <Link key={intent.href} href={intent.href} className="group rounded-xl border border-white/10 bg-black/20 p-4 transition-colors hover:border-actae-400/30 hover:bg-black/30">
                  <p className="text-[13px] font-semibold text-gray-100 group-hover:text-white">{intent.title}</p>
                  <p className="mt-1 text-[11px] leading-5 text-gray-500">{intent.body}</p>
                </Link>
              ))}
            </div>
            <div className="mt-4 rounded-xl border border-white/10 bg-black/20 p-4">
              <p className="text-[13px] font-semibold text-gray-100">Connect your SDK</p>
              <p className="mt-1 text-[11px] leading-5 text-gray-500">
                Instrument your agent with <code className="text-gray-300">pip install actae-client</code>,{" "}
                <code className="text-gray-300">go get github.com/actae-dev/actae/sdks/go</code>, or{" "}
                <code className="text-gray-300">npm install @actae/sdk</code>. Point the client at this instance&apos;s URL with your API key and recorded runs appear here. Run <code className="text-gray-300">actae add</code> in your project to detect your stack and print the exact setup.
              </p>
            </div>
          </section>
        )}

        {metrics?.subscription && <SubscriptionBanner sub={metrics.subscription} />}

        {loading && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {[1, 2, 3, 4].map(i => <div key={i} className="skeleton h-52 rounded-xl" />)}
          </div>
        )}

        {metrics && (
          <>
            {/* Status row */}
            <section className="overview-status-deck">
              <div className="overview-status-deck__intro">
                <span className="console-panel-number">01</span>
                <div><p>Runtime pulse</p><span>Refreshed every five seconds</span></div>
              </div>
              <div className="flex flex-wrap gap-3">
              <StatusPill label="System" status={metrics.status} />
              <div className={`card px-3 py-1.5 flex items-center gap-2 text-sm border ${
                metrics.mode === 'dev'
                  ? 'bg-sky-950/50 text-sky-400 border-sky-800'
                  : 'bg-emerald-950 text-emerald-400 border-emerald-800'
              }`}>
                <span className="w-1.5 h-1.5 rounded-full bg-current" />
                <span className="text-xs font-medium text-gray-400 uppercase tracking-wider">Mode</span>
                <span className="text-xs font-semibold capitalize">
                  {metrics.mode === 'dev' ? 'Dev' : 'Production'}
                  {metrics.embedded_pg ? ' · embedded PG' : ''}
                </span>
              </div>
              <div className="card px-4 py-2 flex items-center gap-2 text-sm">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={CHART_COLORS.slate} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                <span className="text-gray-400">Uptime</span>
                <span className="font-mono font-semibold text-gray-200">{formatUptime(metrics.uptime_seconds)}</span>
              </div>
              <div className="card px-4 py-2 flex items-center gap-2 text-sm">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={CHART_COLORS.slate} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                <span className="text-gray-400">Updated</span>
                <span className="font-mono font-semibold text-gray-200">{new Date(metrics.timestamp).toLocaleTimeString()}</span>
              </div>
              </div>
            </section>

            {/* Stat cards */}
            <div className="overview-metric-grid grid grid-cols-2 md:grid-cols-4 gap-4">
              <MiniStat label="Connections" value={metrics.websocket.connections} color="indigo" />
              <MiniStat label="Channels" value={metrics.events?.channels ?? channels.length} color="purple" />
              <MiniStat label="Total Events" value={(metrics.events?.total ?? 0).toLocaleString()} color="emerald" />
              <MiniStat label="Live Topics" value={metrics.websocket.topics} color="amber" />
            </div>
          </>
        )}

        {/* Recent runs */}
        <section>
          <div className="console-section-heading">
            <span>02 / Recent runs</span>
            <p>Most recently active first.{channels.length > RECENT_RUN_LIMIT ? ` Showing ${RECENT_RUN_LIMIT} of ${channels.length}.` : ''}</p>
          </div>
          <ChartCard
            title="Recent runs"
            subtitle={`${channels.length} run${channels.length === 1 ? '' : 's'} total`}
            action={<Link href="/dashboard/runs/" className="btn-ghost btn-xs">View all runs</Link>}
          >
            {recentRuns.length === 0 ? (
              <p className="text-center text-gray-400 text-sm py-6">No runs yet.</p>
            ) : (
              <div className="overflow-x-auto -mx-2">
                <table className="w-full text-sm">
                  <thead><tr className="border-b border-gray-800 text-left text-[10px] text-gray-400 uppercase font-semibold tracking-wider"><th className="pb-2 pl-2 pr-4">Run</th><th className="pb-2 pr-4">Events</th><th className="pb-2 pr-4">Latest cursor</th><th className="pb-2 pr-4">Last activity</th><th className="pb-2 pr-2"></th></tr></thead>
                  <tbody>
                    {recentRuns.map(ch => (
                      <tr key={ch.id} className="border-b border-gray-800 last:border-0">
                        <td className="py-2.5 pl-2 pr-4 max-w-[280px] truncate" title={ch.id}>
                          {ch.displayName
                            ? <><span className="block truncate text-xs text-gray-200">{ch.displayName}</span><span className="block truncate font-mono text-[10px] text-gray-600">{ch.id}</span></>
                            : <span className="font-mono text-xs text-gray-300">{ch.id}</span>}
                        </td>
                        <td className="py-2.5 pr-4">
                          {ch.eventCount !== null
                            ? <span className="font-mono text-xs text-gray-400">{ch.eventCount.toLocaleString()}</span>
                            : <span className="text-xs text-gray-600">—</span>}
                        </td>
                        <td className="py-2.5 pr-4">
                          {ch.latestCursor !== null
                            ? <span className="font-mono text-xs text-gray-400">@{ch.latestCursor}</span>
                            : <span className="text-xs text-gray-600">—</span>}
                        </td>
                        <td className="py-2.5 pr-4">
                          {ch.lastEventAt
                            ? <span className="text-xs text-gray-500">{new Date(ch.lastEventAt).toLocaleString()}</span>
                            : <span className="text-xs text-gray-600">—</span>}
                        </td>
                        <td className="py-2.5 pr-2">
                          <div className="flex gap-1.5 justify-end">
                            <Link href={`/dashboard/flow/?channel=${encodeURIComponent(ch.id)}`} className="btn-secondary btn-xs">Open run</Link>
                            <Link href={`/dashboard/events/?channel=${encodeURIComponent(ch.id)}`} className="btn-ghost btn-xs">Events</Link>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </ChartCard>
        </section>

        {/* System details: demoted behind a disclosure so the page opens calm. */}
        {metrics && (
          <section>
            <div className="console-section-heading">
              <span>03 / System details</span>
              <button
                onClick={() => setSystemOpen(open => !open)}
                aria-expanded={systemOpen}
                className="text-[11px] font-semibold text-gray-400 hover:text-white inline-flex items-center gap-1.5"
              >
                {systemOpen ? 'Hide' : 'Show'}
                <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className={`transition-transform ${systemOpen ? 'rotate-180' : ''}`}><polyline points="6 9 12 15 18 9"/></svg>
              </button>
            </div>
            {systemOpen && (
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <ChartCard title="Active Connections" subtitle="Rolling window · 30 points · 5s interval">
                  <ResponsiveContainer width="100%" height={220}>
                    <AreaChart data={history}>
                      <defs><linearGradient id="grad-connections" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={CHART_COLORS.blue} stopOpacity={0.15} /><stop offset="100%" stopColor={CHART_COLORS.blue} stopOpacity={0} /></linearGradient></defs>
                      <XAxis dataKey="time" tick={{ fontSize: 10, fill: CHART_COLORS.slate }} axisLine={false} tickLine={false} interval="preserveStartEnd" />
                      <YAxis tick={{ fontSize: 10, fill: CHART_COLORS.slate }} axisLine={false} tickLine={false} />
                      <Tooltip contentStyle={{ borderRadius: 10, border: '1px solid #374151', background: '#1f2937', color: '#e5e7eb', boxShadow: 'none', fontSize: 12 }} />
                      <Area type="monotone" dataKey="connections" stroke={CHART_COLORS.blue} fill="url(#grad-connections)" strokeWidth={2} dot={false} />
                    </AreaChart>
                  </ResponsiveContainer>
                </ChartCard>

                <ChartCard title="WebSocket Message Rate" subtitle="Sent vs received · rolling window · 30 points · 5s interval">
                  <ResponsiveContainer width="100%" height={220}>
                    <LineChart data={history}>
                      <XAxis dataKey="time" tick={{ fontSize: 10, fill: CHART_COLORS.slate }} axisLine={false} tickLine={false} interval="preserveStartEnd" />
                      <YAxis tick={{ fontSize: 10, fill: CHART_COLORS.slate }} axisLine={false} tickLine={false} />
                      <Tooltip contentStyle={{ borderRadius: 10, border: '1px solid #374151', background: '#1f2937', color: '#e5e7eb', boxShadow: 'none', fontSize: 12 }} />
                      <Line type="monotone" dataKey="sent" stroke={CHART_COLORS.blue} strokeWidth={2} dot={false} name="Sent" />
                      <Line type="monotone" dataKey="received" stroke={CHART_COLORS.purple} strokeWidth={2} dot={false} name="Received" />
                    </LineChart>
                  </ResponsiveContainer>
                </ChartCard>

                <ChartCard title="Queue Health">
                  <ResponsiveContainer width="100%" height={120}>
                    <BarChart data={[
                      { name: 'Queued', value: metrics.websocket.back_pressure.total_queued_messages },
                      { name: 'Dropped', value: metrics.websocket.back_pressure.total_dropped_messages },
                      { name: 'Slow Cons.', value: metrics.websocket.back_pressure.slow_consumers },
                    ]} barSize={36}>
                      <XAxis dataKey="name" tick={{ fontSize: 10, fill: CHART_COLORS.slate }} axisLine={false} tickLine={false} />
                      <YAxis tick={{ fontSize: 10, fill: CHART_COLORS.slate }} axisLine={false} tickLine={false} />
                      <Tooltip contentStyle={{ borderRadius: 10, border: '1px solid #374151', background: '#1f2937', color: '#e5e7eb', boxShadow: 'none', fontSize: 12 }} />
                      <Bar dataKey="value" radius={[6, 6, 0, 0]}>
                        <Cell fill="#f59e0b" />
                        <Cell fill="#ef4444" />
                        <Cell fill="#f97316" />
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                  <div className="grid grid-cols-3 gap-3 mt-3">
                    <div className="text-center"><p className="text-[10px] text-gray-400 uppercase font-semibold tracking-wider">Queue</p><p className="text-sm font-mono font-bold text-gray-200">{metrics.websocket.back_pressure.total_queued_messages}<span className="text-gray-500 font-normal text-xs"> / {metrics.websocket.back_pressure.max_queue_size}</span></p></div>
                    <div className="text-center"><p className="text-[10px] text-gray-400 uppercase font-semibold tracking-wider">Dropped</p><p className="text-sm font-mono font-bold text-red-400">{metrics.websocket.back_pressure.total_dropped_messages}</p></div>
                    <div className="text-center"><p className="text-[10px] text-gray-400 uppercase font-semibold tracking-wider">Permits</p><p className="text-sm font-mono font-bold text-gray-200">{metrics.websocket.back_pressure.available_permits}</p></div>
                  </div>
                </ChartCard>

                <ChartCard title="Topic Subscriptions">
                  {metrics.websocket.topic_stats && Object.keys(metrics.websocket.topic_stats).length > 0 ? (
                    <div className="overflow-x-auto -mx-2">
                      <table className="w-full text-sm">
                        <thead><tr className="border-b border-gray-800 text-left text-[10px] text-gray-400 uppercase font-semibold tracking-wider"><th className="pb-2 pl-2 pr-4">Channel</th><th className="pb-2 text-right pr-2">Subscribers</th></tr></thead>
                        <tbody>
                          {Object.entries(metrics.websocket.topic_stats).map(([topic, count]) => (
                            <tr key={topic} className="border-b border-gray-800 last:border-0">
                              <td className="py-2.5 pl-2 pr-4 font-mono text-xs text-gray-300">{topic}</td>
                              <td className="py-2.5 text-right pr-2"><span className="badge-blue">{count}</span></td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="text-center text-gray-400 text-sm py-6">No active topic subscriptions</p>
                  )}
                </ChartCard>
              </div>
            )}
          </section>
        )}
      </div>
    </ProtectedLayout>
  )
}

function formatGrace(secs: number | null | undefined): string {
  if (secs == null) return ''
  if (secs >= 86400) return `${Math.floor(secs / 86400)}d ${Math.floor((secs % 86400) / 3600)}h`
  if (secs >= 3600) return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`
  if (secs >= 60) return `${Math.floor(secs / 60)}m ${secs % 60}s`
  return `${secs}s`
}

function SubscriptionBanner({ sub }: { sub: SubscriptionStatus }) {
  if (sub.mode !== 'enrolled') return null
  if (sub.status === 'active' && !sub.lock_imminent && !sub.last_error) return null

  const locked = sub.status === 'locked'
  const stale = sub.status === 'unreachable'
  const syncFailing = sub.status === 'active' && !!sub.last_error
  const title = locked
    ? 'Subscription inactive — requests are being blocked'
    : stale
    ? 'Control plane unreachable — requests will be blocked when grace expires'
    : syncFailing
    ? 'Control plane sync failing — synced API keys may not validate'
    : 'Subscription grace period expiring soon'

  const detail = syncFailing
    ? `Last sync error: ${sub.last_error}. Run \`actae doctor\` on the instance to diagnose (DNS, TLS interception, proxy, egress).`
    : stale
    ? `Last successful sync: ${sub.last_sync_at ?? 'never'}. Remaining grace: ${formatGrace(sub.remaining_grace_secs)}.`
    : locked
    ? 'Renew your subscription in the Diviga portal, then run `actae auth sync` or wait for the next sync.'
    : `Offline grace remaining: ${formatGrace(sub.remaining_grace_secs)}. Restore control-plane connectivity.`

  const palette = locked || syncFailing
    ? 'bg-red-950/70 border-red-800 text-red-300'
    : 'bg-amber-950/70 border-amber-800 text-amber-300'

  return (
    <div className={`border rounded-xl px-4 py-3 flex items-start gap-3 ${palette}`} role="alert">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="mt-0.5 shrink-0">
        <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
        <line x1="12" y1="9" x2="12" y2="13"/>
        <line x1="12" y1="17" x2="12.01" y2="17"/>
      </svg>
      <div className="min-w-0">
        <p className="text-sm font-semibold">{title}</p>
        <p className="text-xs mt-0.5 text-current/80 opacity-80">{detail}</p>
        {!syncFailing && sub.last_error && <p className="text-xs mt-0.5 font-mono truncate opacity-70">Last sync error: {sub.last_error}</p>}
        <p className="text-xs mt-1.5">
          <a href={sub.docs_url} target="_blank" rel="noreferrer" className="underline underline-offset-2">Subscription documentation</a>
        </p>
      </div>
    </div>
  )
}

function StatusPill({ label, status }: { label: string; status: string }) {
  const bg = status === 'ok' || status === 'healthy'
    ? 'bg-emerald-950 text-emerald-400 border-emerald-800'
    : status === 'degraded' || status === 'warning'
    ? 'bg-amber-950 text-amber-400 border-amber-800'
    : 'bg-red-950 text-red-400 border-red-800'
  return (
    <div className={`card px-3 py-1.5 flex items-center gap-2 text-sm border ${bg}`}>
      <span className="w-1.5 h-1.5 rounded-full bg-current" />
      <span className="text-xs font-medium text-gray-400 uppercase tracking-wider">{label}</span>
      <span className="text-xs font-semibold capitalize">{status}</span>
    </div>
  )
}

function MiniStat({ label, value, color }: { label: string; value: string | number; color: string }) {
  const textColors: Record<string, string> = { indigo: 'text-indigo-400', purple: 'text-purple-400', emerald: 'text-emerald-400', amber: 'text-amber-400' }
  const bgColors: Record<string, string> = { indigo: 'bg-indigo-950', purple: 'bg-purple-950', emerald: 'bg-emerald-950', amber: 'bg-amber-950' }
  return (
    <div className={`card p-4 ${bgColors[color] || 'bg-gray-900'} card-hover`}>
      <p className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-2">{label}</p>
      <p className={`text-2xl font-bold tracking-tight ${textColors[color] || 'text-gray-200'}`}>{value}</p>
    </div>
  )
}

function ChartCard({ title, subtitle, action, children }: { title: string; subtitle?: string; action?: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="card p-5">
      <div className="mb-1 flex items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-gray-200">{title}</h3>
          {subtitle && <p className="text-[10px] text-gray-400 uppercase font-semibold tracking-wider mt-0.5">{subtitle}</p>}
        </div>
        {action}
      </div>
      <div className="mt-2">
        {children}
      </div>
    </div>
  )
}
