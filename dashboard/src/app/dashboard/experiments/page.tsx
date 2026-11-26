// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import ProtectedLayout from '@/components/ProtectedLayout'
import DashboardPageHeader from '@/components/DashboardPageHeader'
import {
  getExperiment,
  queryEvents,
  listChannels,
  listExperiments,
  promoteChannel,
  rankExperiment,
  setChannelOutcome,
  type ChannelOutcome,
  type ExperimentGroup,
  type RankedExperimentMember,
} from '@/lib/api'
import { deriveForkGroups } from '@/lib/fork-families'
import { ExperimentDiff } from './ExperimentDiff'


export default function ExperimentsPage() {
  const [groups, setGroups] = useState<ExperimentGroup[]>([])
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [selected, setSelected] = useState<ExperimentGroup | null>(null)
  const [ranking, setRanking] = useState<RankedExperimentMember[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState<string | null>(null)

  async function loadGroups() {
    setLoading(true)
    const [explicit, forkEvents, channels] = await Promise.all([
      listExperiments(),
      queryEvents({ event_type: 'fork.started', limit: 1000, offset: 0 }),
      listChannels(),
    ])
    // Resolve channel ids to their metadata display names so the lab shows
    // "Fork A · +fact-check" instead of an opaque hash (the Runs page already
    // does this; the lab was the odd one out).
    const stats = (channels.data as any)?.stats as
      | Record<string, { display_name?: string | null }>
      | undefined
    const nameFor = (id: string): string | null => stats?.[id]?.display_name ?? null
    const withNames = (group: ExperimentGroup): ExperimentGroup => ({
      ...group,
      members: group.members.map(member => ({
        ...member,
        display_name: member.display_name ?? nameFor(member.channel_id),
      })),
    })
    const derived = deriveForkGroups((forkEvents.data as any)?.events || [], nameFor)
    if (explicit.data || forkEvents.data) {
      const merged = [
        ...(explicit.data || []),
        ...derived.filter(d => !(explicit.data || []).some(g => g.baseline_channel_id === d.baseline_channel_id)),
      ].map(withNames)
      setGroups(merged)
      setSelectedId(current => current && merged.some(g => g.group_id === current)
        ? current
        : merged[0]?.group_id ?? null)
      setError(null)
    } else setError(explicit.error || forkEvents.error || 'Failed to load experiments')
    setLoading(false)
  }

  async function loadExperiment(groupId: string) {
    const local = groups.find(group => group.group_id === groupId)
    if (local?.derived) {
      setSelected(local)
      setRanking(local.members)
      return
    }
    const [detail, rank] = await Promise.all([getExperiment(groupId), rankExperiment(groupId)])
    if (!detail.data) {
      setError(detail.error || 'Failed to load experiment')
      return
    }
    setSelected(detail.data)
    setRanking(rank.data?.ranked || detail.data.members || [])
  }

  useEffect(() => { void loadGroups() }, [])
  useEffect(() => { if (selectedId) void loadExperiment(selectedId) }, [selectedId])

  function applyOutcomeLocally(channelId: string, outcome: ChannelOutcome) {
    const updateMembers = <T extends { channel_id: string; outcome?: ChannelOutcome | null }>(members: T[]) =>
      members.map(member => member.channel_id === channelId ? { ...member, outcome } : member)
    setRanking(current => updateMembers(current))
    setSelected(current => current ? { ...current, members: updateMembers(current.members) } : current)
    setGroups(current => current.map(group => ({ ...group, members: updateMembers(group.members) })))
  }

  async function markOutcome(channelId: string, outcome: ChannelOutcome) {
    setBusy(channelId)
    setError(null)
    try {
      const res = await setChannelOutcome(channelId, outcome)
      if (res.error) setError(res.error)
      else {
        if (selectedId && !selected?.derived) await loadExperiment(selectedId)
        applyOutcomeLocally(channelId, outcome)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to update fork outcome')
    } finally {
      setBusy(null)
    }
  }

  async function promote(channelId: string) {
    if (!window.confirm('Promote this fork? Its latest state will be appended to the parent run.')) return
    setBusy(channelId)
    setError(null)
    try {
      const res = await promoteChannel(channelId)
      if (res.error) setError(res.error)
      else {
        if (selectedId && !selected?.derived) await loadExperiment(selectedId)
        applyOutcomeLocally(channelId, 'promoted')
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to promote fork')
    } finally {
      setBusy(null)
    }
  }

  return (
    <ProtectedLayout>
      <div className="console-page console-experiments min-h-screen">
        <div className="mx-auto max-w-[1720px]">
          <DashboardPageHeader eyebrow="Fork laboratory" title="Experiments" description="Turn competing forks into evidence. Rank outcomes, compare deltas, and promote the run that earned it." actions={<button onClick={() => void loadGroups()} className="btn-secondary">Refresh laboratory</button>} />

          {error && <div role="alert" className="mb-4 rounded-xl border border-red-800 bg-red-950/50 px-4 py-3 text-sm text-red-300">{error}</div>}

          {loading ? <ExperimentsSkeleton /> : groups.length === 0 ? (
            <div className="rounded-2xl border border-dashed border-gray-700 bg-gray-900/40 py-20 text-center">
              <div className="text-base font-semibold text-gray-200">No forks or experiment groups yet</div>
              <p className="mx-auto mt-2 max-w-md text-sm text-gray-500">Fork a recorded run or create a first-class experiment group and it will appear here automatically.</p>
              <Link href="/dashboard/flow/" className="mt-5 inline-flex btn-secondary">Open Flow</Link>
            </div>
          ) : (
            <div className="experiment-workbench grid grid-cols-1 lg:h-[calc(100vh-13rem)] lg:min-h-[560px] lg:grid-cols-[300px_minmax(0,1fr)] overflow-hidden">
              <aside className="experiment-index max-h-[38vh] overflow-y-auto p-3 lg:max-h-none">
                <div className="px-2 pb-2 text-[10px] font-semibold uppercase tracking-wider text-gray-500">{groups.length} experiment{groups.length === 1 ? '' : 's'}</div>
                <div className="space-y-1">
                  {groups.map(group => (
                    <button key={group.group_id} onClick={() => setSelectedId(group.group_id)} className={`w-full rounded-xl border p-3 text-left transition-colors ${selectedId === group.group_id ? 'border-purple-500/30 bg-purple-500/10' : 'border-transparent hover:bg-gray-800/70'}`}>
                      <div className="flex items-center gap-2">
                        <span className={`h-2 w-2 rounded-full ${group.status === 'completed' ? 'bg-emerald-400' : 'bg-sky-400'}`} />
                        <span className="truncate text-sm font-semibold text-gray-100">{group.name}</span>
                      </div>
                      <div className="mt-1 truncate font-mono text-[10px] text-gray-600">{group.group_id}</div>
                    </button>
                  ))}
                </div>
              </aside>
              <main className="experiment-stage min-w-0 overflow-y-auto p-5 lg:p-7">
                {selected ? <ExperimentDetail group={selected} ranking={ranking} busy={busy} onOutcome={markOutcome} onPromote={promote} /> : <ExperimentsSkeleton />}
              </main>
            </div>
          )}

          {/* The fork diff lives at the end of the page (full width), below the
              workbench, so it never competes with the experiment list/summary
              for vertical space. */}
          {!loading && selected && ranking.length >= 2 && <ExperimentDiff members={ranking} />}
        </div>
      </div>
    </ProtectedLayout>
  )
}

function ExperimentDetail({ group, ranking, busy, onOutcome, onPromote }: {
  group: ExperimentGroup
  ranking: RankedExperimentMember[]
  busy: string | null
  onOutcome: (id: string, outcome: ChannelOutcome) => void
  onPromote: (id: string) => void
}) {
  const scored = ranking.filter(member => member.result_score != null)
  const winner = scored[0]?.channel_id
  return (
    <div>
      <div className="mb-6 flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2"><h2 className="text-xl font-bold text-white">{group.name}</h2><StatusBadge status={group.status} />{group.derived && <span className="rounded-full bg-purple-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase text-purple-300">fork lineage</span>}</div>
          <p className="mt-1 max-w-3xl text-sm text-gray-400">{group.description || 'No experiment description recorded.'}</p>
        </div>
        <div className="text-right text-[10px] uppercase tracking-wider text-gray-600">Updated<br/><span className="text-xs normal-case text-gray-400">{new Date(group.updated_at).toLocaleString()}</span></div>
      </div>

      <div className="mb-5 grid grid-cols-3 gap-3">
        <Stat label="Variants" value={ranking.filter(m => m.role === 'variant').length} />
        <Stat label="Scored" value={`${scored.length}/${ranking.length}`} />
        <Stat label="Leader" value={winner ? shortName(winner) : 'Awaiting scores'} accent />
      </div>

      <div className="overflow-hidden rounded-xl border border-gray-800">
        <div className="grid grid-cols-[48px_minmax(180px,1.3fr)_minmax(150px,1fr)_100px_110px_250px] gap-3 border-b border-gray-800 bg-gray-800/50 px-4 py-2 text-[10px] font-semibold uppercase tracking-wider text-gray-500">
          <span>Rank</span><span>Fork</span><span>Declared change</span><span>Score</span><span>Outcome</span><span className="text-right">Actions</span>
        </div>
        {ranking.map((member, index) => (
          <div key={member.channel_id} className="grid grid-cols-[48px_minmax(180px,1.3fr)_minmax(150px,1fr)_100px_110px_250px] items-center gap-3 border-b border-gray-800/70 px-4 py-3 last:border-0 hover:bg-gray-800/20">
            <div className={`text-lg font-bold ${index === 0 && member.result_score != null ? 'text-amber-400' : 'text-gray-600'}`}>{member.result_score == null ? '—' : index + 1}</div>
            <div className="min-w-0"><div className="truncate text-sm font-medium text-gray-200" title={member.channel_id}>{member.display_name || shortName(member.channel_id)}</div><div className="text-[10px] uppercase text-gray-600">{member.role}</div></div>
            <div className="truncate font-mono text-[11px] text-purple-300" title={JSON.stringify(member.declared_delta)}>{formatDelta(member.declared_delta)}</div>
            <div className="font-mono text-lg font-semibold text-gray-200">{member.result_score ?? '—'}</div>
            <OutcomeBadge outcome={member.outcome} />
            <div className="flex justify-end gap-2">
              <Link href={`/dashboard/flow/?channel=${encodeURIComponent(member.channel_id)}`} className="btn-ghost btn-xs">Inspect</Link>
              {member.role !== 'baseline' && member.outcome !== 'promoted' && <>
                <button disabled={busy === member.channel_id} onClick={() => onOutcome(member.channel_id, 'rejected')} className="btn-ghost btn-xs">Reject</button>
                <button disabled={busy === member.channel_id} onClick={() => onPromote(member.channel_id)} className="rounded-md bg-purple-600 px-2.5 py-1 text-[11px] font-semibold text-white hover:bg-purple-500 disabled:opacity-50">Promote</button>
              </>}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function shortName(id: string) { return id.length > 28 ? `…${id.slice(-27)}` : id }
function formatDelta(delta: Record<string, unknown> | null) { return !delta || Object.keys(delta).length === 0 ? 'baseline' : Object.entries(delta).map(([k, v]) => `${k}=${String(v)}`).join(' · ') }
function StatusBadge({ status }: { status: string }) { return <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase ${status === 'completed' ? 'bg-emerald-500/10 text-emerald-400' : 'bg-sky-500/10 text-sky-400'}`}>{status}</span> }
function OutcomeBadge({ outcome }: { outcome?: string | null }) { const tone = outcome === 'promoted' ? 'text-emerald-400 bg-emerald-500/10' : outcome === 'rejected' || outcome === 'crashed' ? 'text-red-400 bg-red-500/10' : 'text-gray-500 bg-gray-800'; return <span className={`w-fit rounded-full px-2 py-1 text-[10px] font-semibold uppercase ${tone}`}>{outcome || 'pending'}</span> }
function Stat({ label, value, accent }: { label: string; value: string | number; accent?: boolean }) { return <div className="rounded-xl border border-gray-800 bg-gray-950/60 p-3"><div className="text-[9px] font-semibold uppercase tracking-wider text-gray-600">{label}</div><div className={`mt-1 truncate text-lg font-bold ${accent ? 'text-purple-300' : 'text-gray-200'}`}>{value}</div></div> }
function ExperimentsSkeleton() { return <div className="grid grid-cols-3 gap-3">{[1,2,3].map(i => <div key={i} className="skeleton h-24 rounded-xl" />)}</div> }
