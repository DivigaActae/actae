// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useCallback, useEffect, useState } from 'react'
import Link from 'next/link'
import ProtectedLayout from '@/components/ProtectedLayout'
import DashboardPageHeader from '@/components/DashboardPageHeader'
import { useAuth } from '@/lib/auth'
import { listWakeups, queryEvents, recordEvent, type WakeupView } from '@/lib/api'
import { buildApprovals, type ApprovalView, type QueryEvent } from '@/lib/approvals'

const APPROVAL_REQUESTED = 'approval.requested'
const APPROVAL_DECIDED = 'approval.decided'
const APPROVAL_EXPIRED = 'approval.expired'

function asString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined
}

function statusTone(status: ApprovalView['status']): string {
  switch (status) {
    case 'pending': return 'bg-amber-500/10 text-amber-400'
    case 'approved': return 'bg-emerald-500/10 text-emerald-400'
    case 'rejected': return 'bg-red-500/10 text-red-400'
    case 'expired': return 'bg-gray-700/40 text-gray-400'
  }
}

export default function ApprovalsPage() {
  const { user } = useAuth()
  const [approvals, setApprovals] = useState<ApprovalView[]>([])
  const [wakeups, setWakeups] = useState<WakeupView[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    const [requested, decided, expired, wake] = await Promise.all([
      queryEvents({ event_type: APPROVAL_REQUESTED, limit: 500, offset: 0 }),
      queryEvents({ event_type: APPROVAL_DECIDED, limit: 500, offset: 0 }),
      queryEvents({ event_type: APPROVAL_EXPIRED, limit: 500, offset: 0 }),
      listWakeups({ status: 'pending' }),
    ])
    const firstError = requested.error || decided.error || expired.error || wake.error
    if (firstError) {
      setError(firstError)
      setLoading(false)
      return
    }
    const eventsOf = (res: { data?: unknown }): QueryEvent[] => {
      const events = (res.data as { events?: QueryEvent[] } | undefined)?.events
      return Array.isArray(events) ? events : []
    }
    setApprovals(
      buildApprovals(eventsOf(requested), eventsOf(decided), eventsOf(expired), wake.data?.wakeups || [])
    )
    setWakeups(wake.data?.wakeups || [])
    setError(null)
    setLoading(false)
  }, [])

  useEffect(() => {
    void load()
    // Auto-refresh like the Flow/Events pages so a decision made elsewhere
    // shows up without a manual reload.
    const timer = setInterval(() => { void load() }, 15000)
    return () => clearInterval(timer)
  }, [load])

  async function resolve(approval: ApprovalView, decision: 'approved' | 'rejected') {
    setBusy(approval.request_id)
    setError(null)
    try {
      const actor = user?.email || user?.name || 'dashboard'
      const res = await recordEvent(
        approval.channel_id,
        APPROVAL_DECIDED,
        { request_id: approval.request_id, decision },
        actor
      )
      if (res.error) setError(res.error)
      else await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to record decision')
    } finally {
      setBusy(null)
    }
  }

  const pending = approvals.filter(a => a.status === 'pending')

  return (
    <ProtectedLayout>
      <div className="console-page console-approvals min-h-screen">
        <div className="mx-auto max-w-[1400px]">
          <DashboardPageHeader
            eyebrow="Human in the loop"
            title="Approvals"
            description="Durable approval requests waiting on a person, and their deadlines. Actae records the decision; it never resumes the agent itself."
            actions={<button onClick={() => void load()} className="btn-secondary">Refresh</button>}
          />

          {error && <div role="alert" className="mb-4 rounded-xl border border-red-800 bg-red-950/50 px-4 py-3 text-sm text-red-300">{error}</div>}

          {loading ? <ApprovalsSkeleton /> : (
            <>
              <div className="mb-6 grid grid-cols-2 gap-3 sm:grid-cols-4">
                <Stat label="Pending" value={pending.length} accent />
                <Stat label="Approved" value={approvals.filter(a => a.status === 'approved').length} />
                <Stat label="Rejected" value={approvals.filter(a => a.status === 'rejected').length} />
                <Stat label="Expired" value={approvals.filter(a => a.status === 'expired').length} />
              </div>

              <section className="mb-8">
                <h2 className="mb-2 text-sm font-semibold uppercase tracking-wider text-gray-400">Awaiting decision</h2>
                {pending.length === 0 ? (
                  <div className="rounded-xl border border-dashed border-gray-700 bg-gray-900/40 px-4 py-10 text-center text-sm text-gray-500">
                    No pending approvals. Agents that call <code className="font-mono text-gray-400">request_approval</code> will appear here.
                  </div>
                ) : (
                  <div className="space-y-2">
                    {pending.map(approval => (
                      <div key={approval.request_id} className="flex flex-wrap items-center gap-3 rounded-xl border border-amber-500/20 bg-amber-500/[0.04] px-4 py-3">
                        <div className="min-w-0 flex-1">
                          <div className="truncate text-sm font-semibold text-gray-100" title={approval.summary}>{approval.summary}</div>
                          <div className="mt-0.5 flex flex-wrap items-center gap-2 text-[11px] text-gray-500">
                            <ChannelLink channelId={approval.channel_id} />
                            <span className="font-mono text-gray-600" title={approval.request_id}>{approval.request_id.slice(0, 12)}…</span>
                            {approval.deadline && <span>· due {new Date(approval.deadline).toLocaleString()}</span>}
                          </div>
                        </div>
                        <div className="flex gap-2">
                          <button disabled={busy === approval.request_id} onClick={() => void resolve(approval, 'rejected')} className="btn-ghost btn-xs disabled:opacity-50">Reject</button>
                          <button disabled={busy === approval.request_id} onClick={() => void resolve(approval, 'approved')} className="rounded-md bg-emerald-600 px-2.5 py-1 text-[11px] font-semibold text-white hover:bg-emerald-500 disabled:opacity-50">Approve</button>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </section>

              <section className="mb-8">
                <h2 className="mb-2 text-sm font-semibold uppercase tracking-wider text-gray-400">All approvals</h2>
                {approvals.length === 0 ? (
                  <div className="rounded-xl border border-dashed border-gray-700 bg-gray-900/40 px-4 py-8 text-center text-sm text-gray-500">No approval events yet.</div>
                ) : (
                  <div className="overflow-hidden rounded-xl border border-gray-800">
                    <div className="grid grid-cols-[110px_minmax(200px,1.4fr)_minmax(140px,1fr)_110px_minmax(140px,1fr)] gap-3 border-b border-gray-800 bg-gray-800/50 px-4 py-2 text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                      <span>Status</span><span>Summary</span><span>Channel</span><span>Decided by</span><span>Requested</span>
                    </div>
                    {approvals.map(approval => (
                      <div key={approval.request_id} className="grid grid-cols-[110px_minmax(200px,1.4fr)_minmax(140px,1fr)_110px_minmax(140px,1fr)] items-center gap-3 border-b border-gray-800/70 px-4 py-2.5 last:border-0 hover:bg-gray-800/20">
                        <span className={`w-fit rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase ${statusTone(approval.status)}`}>{approval.status}</span>
                        <span className="truncate text-sm text-gray-200" title={approval.summary}>{approval.summary}</span>
                        <ChannelLink channelId={approval.channel_id} />
                        <span className="truncate text-[11px] text-gray-500">{approval.decided_by || (approval.reason ? approval.reason : '—')}</span>
                        <span className="text-[11px] text-gray-500">{approval.requested_at ? new Date(approval.requested_at).toLocaleString() : '—'}</span>
                      </div>
                    ))}
                  </div>
                )}
              </section>

              <section>
                <h2 className="mb-2 text-sm font-semibold uppercase tracking-wider text-gray-400">Pending wake-ups</h2>
                {wakeups.length === 0 ? (
                  <div className="rounded-xl border border-dashed border-gray-700 bg-gray-900/40 px-4 py-8 text-center text-sm text-gray-500">No scheduled wake-ups.</div>
                ) : (
                  <div className="overflow-hidden rounded-xl border border-gray-800">
                    <div className="grid grid-cols-[minmax(160px,1fr)_minmax(160px,1fr)_180px_90px] gap-3 border-b border-gray-800 bg-gray-800/50 px-4 py-2 text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                      <span>Channel</span><span>Kind</span><span>Run at</span><span>Attempts</span>
                    </div>
                    {wakeups.map(wakeup => (
                      <div key={wakeup.id} className="grid grid-cols-[minmax(160px,1fr)_minmax(160px,1fr)_180px_90px] items-center gap-3 border-b border-gray-800/70 px-4 py-2.5 last:border-0 hover:bg-gray-800/20">
                        <ChannelLink channelId={wakeup.channel_id} />
                        <span className="truncate font-mono text-[11px] text-gray-400">{asString(wakeup.payload?.kind) || '—'}</span>
                        <span className="text-[11px] text-gray-500">{new Date(wakeup.run_at).toLocaleString()}</span>
                        <span className="text-[11px] text-gray-500">{wakeup.attempts}</span>
                      </div>
                    ))}
                  </div>
                )}
              </section>
            </>
          )}
        </div>
      </div>
    </ProtectedLayout>
  )
}

function ChannelLink({ channelId }: { channelId: string }) {
  if (!channelId) return <span className="text-[11px] text-gray-600">—</span>
  return (
    <Link href={`/dashboard/flow/?channel=${encodeURIComponent(channelId)}`} className="truncate font-mono text-[11px] text-sky-400 hover:text-sky-300" title={channelId}>
      {channelId}
    </Link>
  )
}

function Stat({ label, value, accent }: { label: string; value: number; accent?: boolean }) {
  return (
    <div className="rounded-xl border border-gray-800 bg-gray-950/60 p-3">
      <div className="text-[9px] font-semibold uppercase tracking-wider text-gray-600">{label}</div>
      <div className={`mt-1 text-lg font-bold ${accent ? 'text-amber-300' : 'text-gray-200'}`}>{value}</div>
    </div>
  )
}

function ApprovalsSkeleton() {
  return <div className="space-y-3">{[1, 2, 3].map(i => <div key={i} className="skeleton h-16 rounded-xl" />)}</div>
}
