// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useCallback, useEffect, useState } from 'react'
import ProtectedLayout from '@/components/ProtectedLayout'
import DashboardPageHeader from '@/components/DashboardPageHeader'
import { getCausalGraph, getExecutionGroupFork, listExecutionGroupMembers, listExecutionGroupMessages, listExecutionGroups, promoteExecutionGroupMember, type ExecutionGroupMemberView, type ExecutionGroupView, type GroupForkView, type GroupMessageView } from '@/lib/api'

const AGENT_COLORS = [
  { accent: '#60a5fa', soft: 'rgba(96,165,250,.13)' },
  { accent: '#c084fc', soft: 'rgba(192,132,252,.13)' },
  { accent: '#34d399', soft: 'rgba(52,211,153,.13)' },
  { accent: '#fbbf24', soft: 'rgba(251,191,36,.13)' },
  { accent: '#fb7185', soft: 'rgba(251,113,133,.13)' },
  { accent: '#22d3ee', soft: 'rgba(34,211,238,.13)' },
  { accent: '#f97316', soft: 'rgba(249,115,22,.13)' },
  { accent: '#a3e635', soft: 'rgba(163,230,53,.13)' },
]

function agentColor(memberId: string) {
  let hash = 0
  for (let index = 0; index < memberId.length; index += 1) hash = (hash * 31 + memberId.charCodeAt(index)) >>> 0
  return AGENT_COLORS[hash % AGENT_COLORS.length]!
}

function RawToggle({ data }: { data: unknown }) {
  const [open, setOpen] = useState(false)
  return (
    <div className="mt-3">
      <button onClick={() => setOpen(o => !o)} aria-expanded={open} className="text-[11px] font-semibold text-gray-500 hover:text-gray-300">
        {open ? 'Hide raw JSON' : 'View raw JSON'}
      </button>
      {open && <pre className="mt-2 max-h-72 overflow-auto rounded-lg border border-white/10 bg-black/30 p-3 text-[11px] leading-5 text-surface-300">{JSON.stringify(data, null, 2)}</pre>}
    </div>
  )
}

export default function GroupsPage() {
  const [groups, setGroups] = useState<ExecutionGroupView[]>([])
  const [selected, setSelected] = useState('')
  const [members, setMembers] = useState<ExecutionGroupMemberView[]>([])
  const [messages, setMessages] = useState<GroupMessageView[]>([])
  const [fork, setFork] = useState<GroupForkView | null>(null)
  const [graph, setGraph] = useState<any>(null)
  const [error, setError] = useState('')
  const load = useCallback(async () => {
    const result = await listExecutionGroups()
    if (result.error) { setError(result.error); return }
    const values = result.data?.groups ?? []
    setGroups(values); setSelected(value => value || values[0]?.group_id || '')
  }, [])
  useEffect(() => { void load(); const timer = setInterval(() => void load(), 10000); return () => clearInterval(timer) }, [load])
  useEffect(() => {
    if (!selected) return
    let cancelled = false
    void Promise.all([listExecutionGroupMembers(selected), getExecutionGroupFork(selected)]).then(async ([memberResult, forkResult]) => {
      if (cancelled) return
      const nextMembers = memberResult.data?.members ?? []
      setMembers(nextMembers); setFork(forkResult.status === 200 ? forkResult.data?.fork ?? null : null)
      const batches = await Promise.all(nextMembers.map(member => listExecutionGroupMessages(selected, member.member_id)))
      if (!cancelled) setMessages(batches.flatMap(value => value.data?.messages ?? []).filter((message, index, all) => all.findIndex(other => other.message_id === message.message_id) === index).sort((a, b) => a.created_at.localeCompare(b.created_at)))
    })
    return () => { cancelled = true }
  }, [selected])
  async function inspect(eventId: string) { const result = await getCausalGraph(eventId); if (result.error) setError(result.error); else setGraph(result.data) }
  async function promote(memberId: string) { const result = await promoteExecutionGroupMember(selected, memberId); if (result.error) setError(result.error); else setFork(result.data?.fork ?? null) }

  const selectedGroup = groups.find(group => group.group_id === selected)

  return <ProtectedLayout><div className="console-page min-h-screen"><div className="mx-auto max-w-[1500px] space-y-6">
    <DashboardPageHeader
      eyebrow="Advanced"
      title="Coordination"
      description="Multi-agent groups that share work through leases and messages, and the counterfactual history of a group fork."
      meta={selectedGroup ? <span className="console-kicker">{selectedGroup.status.toUpperCase()}</span> : undefined}
    />
    {error && <div role="alert" className="rounded border border-red-800 bg-red-950/40 p-3 text-red-300">{error}</div>}

    {groups.length === 0 ? (
      <div className="rounded-2xl border border-dashed border-gray-700 bg-gray-900/40 py-16 text-center text-sm text-gray-500">
        No coordination groups yet. Groups appear here once agents join one through the SDK.
      </div>
    ) : (
      <>
        <div className="flex flex-wrap gap-2">{groups.map(group => <button key={group.group_id} onClick={() => setSelected(group.group_id)} className={`rounded-lg border px-3 py-2 text-sm transition-colors ${selected === group.group_id ? 'border-actae-400 text-actae-400 bg-actae-400/5' : 'border-white/10 text-surface-300 hover:text-white hover:border-white/20'}`}>{group.group_id} <span className="ml-1 text-[10px] uppercase tracking-wider text-surface-500">{group.status}</span></button>)}</div>

        {selected && <>
          <section className="rounded-xl border border-white/10 bg-surface-900 p-4">
            <div className="mb-3 flex items-center justify-between">
              <h2 className="font-semibold">Members</h2>
              <span className="text-xs text-surface-500">Colors stay consistent across groups and forks</span>
            </div>
            <div className="grid md:grid-cols-2 xl:grid-cols-3 gap-3">{members.map(member => { const policy = fork?.member_policies?.[member.member_id]; const color = agentColor(member.member_id); return <article key={member.member_id} className="rounded-lg bg-black/20 p-3" style={{ borderLeft: `3px solid ${color.accent}`, backgroundColor: color.soft }}><div className="flex justify-between items-center"><strong className="flex items-center gap-2" style={{ color: color.accent }}><span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: color.accent }} aria-hidden="true" />{member.member_id}</strong><span className="text-xs uppercase text-actae-400">{policy ?? 'production'}</span></div><div className="text-xs text-surface-400 mt-1 font-mono truncate" title={member.channel_id}>{member.channel_id}</div><div className="text-xs mt-2">owner {member.owner_id ?? 'unclaimed'} · generation {member.generation}</div>{policy === 'frozen' && <button onClick={() => void promote(member.member_id)} className="mt-3 text-xs border border-actae-400 rounded px-2 py-1 text-actae-400">Make reactive</button>}</article> })}</div>
          </section>

          <section className="rounded-xl border border-white/10 bg-surface-900 p-4">
            <h2 className="font-semibold mb-3">Causal messages</h2>
            {messages.length === 0 ? <p className="text-surface-500 text-sm">No messages recorded.</p> : (
              <ol className="space-y-2">
                {messages.map(message => { const fromColor = agentColor(message.from_member_id); const toColor = agentColor(message.to_member_id); return (
                  <li key={message.message_id}>
                    <button onClick={() => void inspect(message.delivery_event_id)} className="w-full text-left rounded bg-black/20 p-3 hover:bg-white/5">
                      <div className="flex flex-wrap items-center gap-2 text-xs">
                        <span style={{ color: fromColor.accent }} className="inline-flex items-center gap-1"><span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: fromColor.accent }} aria-hidden="true" />{message.from_member_id}</span>
                        <span className="text-surface-500">→</span>
                        <span style={{ color: toColor.accent }} className="inline-flex items-center gap-1"><span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: toColor.accent }} aria-hidden="true" />{message.to_member_id}</span>
                        <strong className="ml-1">{message.type}</strong>
                        <span className="ml-auto rounded-full bg-white/5 px-2 py-0.5 text-[10px] uppercase tracking-wider text-surface-400">{message.status}</span>
                      </div>
                      <div className="mt-1 text-[10px] text-surface-500">{new Date(message.created_at).toLocaleString()}</div>
                    </button>
                  </li>
                ) })}
              </ol>
            )}
          </section>

          {fork && (
            <section className="rounded-xl border border-white/10 bg-surface-900 p-4">
              <div className="flex items-center gap-2">
                <h2 className="font-semibold">Counterfactual fork</h2>
                <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase ${fork.status === 'completed' ? 'bg-emerald-500/10 text-emerald-400' : 'bg-sky-500/10 text-sky-400'}`}>{fork.status}</span>
              </div>
              <div className="mt-3 grid grid-cols-1 gap-x-6 gap-y-2 text-xs sm:grid-cols-2">
                <Field label="Source group" value={fork.source_group_id} mono />
                <Field label="Intervention member" value={fork.intervention_member_id} mono />
                <Field label="Intervention cursor" value={`@${fork.intervention_cursor}`} mono />
                <Field label="First divergence" value={fork.first_divergence_event_id || '—'} mono />
              </div>
              {Object.keys(fork.member_policies ?? {}).length > 0 && (
                <div className="mt-4">
                  <div className="text-[10px] font-semibold uppercase tracking-wider text-surface-500 mb-1.5">Member policies</div>
                  <div className="flex flex-wrap gap-2">
                    {Object.entries(fork.member_policies).map(([member, policy]) => (
                      <span key={member} className="rounded-md border border-white/10 bg-black/20 px-2 py-1 text-[11px]"><span className="text-surface-300">{member}</span> <span className="text-actae-400 uppercase">{policy}</span></span>
                    ))}
                  </div>
                </div>
              )}
              <RawToggle data={fork} />
            </section>
          )}

          {graph && (
            <section className="rounded-xl border border-white/10 bg-surface-900 p-4">
              <h2 className="font-semibold">Causal graph</h2>
              <p className="mt-1 text-sm text-surface-400">{graph.events?.length ?? 0} events · {graph.dependencies?.length ?? 0} dependency edges{graph.truncated ? ' · truncated' : ''}</p>
              <RawToggle data={graph} />
            </section>
          )}
        </>}
      </>
    )}
  </div></div></ProtectedLayout>
}

function Field({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex items-baseline justify-between gap-3 border-b border-white/5 pb-1.5">
      <span className="text-surface-500">{label}</span>
      <span className={`truncate text-surface-300 ${mono ? 'font-mono text-[11px]' : ''}`} title={value}>{value}</span>
    </div>
  )
}
