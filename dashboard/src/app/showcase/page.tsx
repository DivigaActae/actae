// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import Sidebar from '@/components/Sidebar'

type EventRow = { id: string; type: string; agent: string; channel: string; cursor: number; time: string; payload: Record<string, unknown> }
type ShowcaseData = {
  version: number
  generatedFrom: string
  topic: string
  metrics: Record<string, number>
  agents: Array<{ id: string; label: string; status: string; color: string; events: number }>
  events: EventRow[]
  states: Array<{ version: number; cursor: number; agent: string; label: string; time: string; fields: number }>
  fork: { parent: string; child: string; cursor: number; status: string; receipt: string }
}

const tabs = [
  ['Overview', '/showcase/'], ['Flow', '/showcase/#flow'], ['Groups', '/showcase/#groups'],
  ['Experiments', '/showcase/#fork'], ['Events', '/showcase/#events'],
]

export default function ShowcasePage() {
  const [data, setData] = useState<ShowcaseData | null>(null)
  const [selected, setSelected] = useState<EventRow | null>(null)
  const [query, setQuery] = useState('')

  useEffect(() => {
    fetch('/demo/actae-langgraph.json').then(r => r.json()).then((value: ShowcaseData) => {
      setData(value)
      setSelected(value.events[0])
    })
  }, [])

  if (!data) return <div className="flex h-screen items-center justify-center bg-surface-950 text-surface-400">Loading showcase…</div>

  const events = data.events.filter(event => `${event.type} ${event.agent} ${event.channel}`.toLowerCase().includes(query.toLowerCase()))
  const metricCards = [['Events', data.metrics.events], ['State versions', data.metrics.states], ['Forks', data.metrics.forks], ['Agents', data.metrics.connections]]

  return <div className="dashboard-shell flex h-screen overflow-hidden">
    <Sidebar demoMode />
    <main className="dashboard-main min-w-0 flex-1 overflow-y-auto bg-surface-950">
      <div className="mx-auto max-w-[1500px] px-6 py-7 lg:px-9">
        <div className="mb-7 flex flex-wrap items-start justify-between gap-4">
          <div><p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-actae-400">Read-only showcase</p><h2 className="mt-2 text-2xl font-bold text-white">Execution overview</h2><p className="mt-1 max-w-2xl text-sm text-surface-400">A recorded LangGraph research run, preserved as inspectable Actae history.</p></div>
          <span className="rounded-full border border-actae-400/25 bg-actae-400/10 px-3 py-1.5 text-xs font-medium text-actae-300">Synthetic data · no live backend</span>
        </div>
        <nav className="mb-6 flex gap-1 overflow-x-auto border-b border-white/10 pb-px">{tabs.map(([label, href], i) => <Link key={label} href={href} className={`whitespace-nowrap px-3 py-2 text-xs font-medium ${i === 0 ? 'border-b-2 border-actae-400 text-white' : 'text-surface-400 hover:text-white'}`}>{label}</Link>)}</nav>

        <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">{metricCards.map(([label, value]) => <div className="card p-5" key={label}><p className="text-xs uppercase tracking-widest text-surface-500">{label}</p><p className="mt-3 text-3xl font-semibold text-white">{value}</p><p className="mt-1 text-xs text-emerald-400">captured in this run</p></div>)}</section>

        <section id="flow" className="mt-6 card p-6"><div className="flex items-center justify-between"><div><h3 className="text-base font-semibold text-white">Agent flow</h3><p className="mt-1 text-xs text-surface-500">{data.topic}</p></div><span className="badge-green">completed</span></div><div className="mt-7 grid gap-3 md:grid-cols-4">{data.agents.map((agent, index) => <div key={agent.id} className="relative rounded-xl border border-white/10 bg-black/20 p-4">{index < data.agents.length - 1 && <span className="absolute -right-4 top-1/2 z-10 hidden text-surface-600 md:block">→</span>}<div className="flex items-center gap-3"><span className="h-3 w-3 rounded-full" style={{ backgroundColor: agent.color }} /><div><p className="text-sm font-semibold text-white">{agent.label}</p><p className="text-[11px] text-surface-500">{agent.events} events</p></div></div><div className="mt-4 h-1.5 overflow-hidden rounded-full bg-white/10"><div className="h-full w-full rounded-full" style={{ backgroundColor: agent.color }} /></div></div>)}</div></section>

        <div className="mt-6 grid gap-6 xl:grid-cols-[1.35fr_0.65fr]">
          <section id="events" className="card overflow-hidden"><div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/10 p-5"><div><h3 className="text-base font-semibold text-white">Event history</h3><p className="mt-1 text-xs text-surface-500">Durable events can be replayed from any cursor.</p></div><input aria-label="Filter events" value={query} onChange={e => setQuery(e.target.value)} placeholder="Filter events…" className="input max-w-[210px] text-xs" /></div><div className="overflow-x-auto"><table className="table"><thead><tr><th>Type</th><th>Agent</th><th>Channel</th><th>Cursor</th><th>Time</th></tr></thead><tbody>{events.map(event => <tr key={event.id} onClick={() => setSelected(event)} className={selected?.id === event.id ? 'selected cursor-pointer' : 'cursor-pointer'}><td><span className="font-mono text-xs text-white">{event.type}</span></td><td><span className="text-xs text-surface-300">{event.agent}</span></td><td><span className="font-mono text-[11px] text-surface-500">{event.channel}</span></td><td><span className="font-mono text-xs text-actae-300">{event.cursor}</span></td><td><span className="text-xs text-surface-500">{event.time}</span></td></tr>)}</tbody></table></div></section>
          <section className="card min-w-0 p-5"><p className="text-[11px] font-semibold uppercase tracking-widest text-surface-500">Selected event</p>{selected ? <><h3 className="mt-3 break-words font-mono text-sm text-white">{selected.type}</h3><div className="mt-4 space-y-2 text-xs"><p className="flex justify-between gap-4"><span className="text-surface-500">Identity</span><span className="font-mono text-surface-300">{selected.id}</span></p><p className="flex justify-between gap-4"><span className="text-surface-500">Cursor</span><span className="font-mono text-actae-300">{selected.cursor}</span></p><p className="flex justify-between gap-4"><span className="text-surface-500">Agent</span><span className="text-surface-300">{selected.agent}</span></p></div><pre className="mt-5 max-h-56 overflow-auto rounded-lg border border-white/10 bg-black/30 p-3 text-[11px] leading-5 text-surface-300">{JSON.stringify(selected.payload, null, 2)}</pre></> : <p className="mt-4 text-sm text-surface-500">Select an event to inspect its payload.</p>}</section>
        </div>

        <section id="groups" className="mt-6 card p-6"><div className="flex flex-wrap items-center justify-between gap-3"><div><h3 className="text-base font-semibold text-white">Execution group · langgraph-research</h3><p className="mt-1 text-xs text-surface-500">Four agents coordinated through one durable execution history.</p></div><span className="badge-blue">4 members</span></div><div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">{data.agents.map(agent => <div key={agent.id} className="rounded-lg border border-white/10 p-3"><div className="flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: agent.color }} /><span className="text-sm text-white">{agent.label}</span></div><p className="mt-2 text-[11px] text-emerald-400">lease completed · {agent.events} events</p></div>)}</div></section>

        <section id="fork" className="mt-6 grid gap-6 pb-10 lg:grid-cols-2"><div className="card p-6"><p className="text-[11px] font-semibold uppercase tracking-widest text-surface-500">State timeline</p><div className="mt-5 space-y-4">{data.states.map(state => <div key={state.version} className="flex gap-3"><div className="flex flex-col items-center"><span className="mt-1 h-2.5 w-2.5 rounded-full bg-actae-400" />{state.version < data.states.length && <span className="h-full w-px bg-white/10" />}</div><div className="pb-3"><p className="text-sm text-white">v{state.version} · {state.label}</p><p className="mt-1 text-xs text-surface-500">cursor {state.cursor} · {state.agent} · {state.fields} fields · {state.time}</p></div></div>)}</div></div><div className="card p-6"><p className="text-[11px] font-semibold uppercase tracking-widest text-surface-500">Fork receipt</p><h3 className="mt-3 text-lg font-semibold text-white">Compare a different continuation</h3><p className="mt-2 text-sm leading-6 text-surface-400">The alternate fork inherits the known-good state at cursor {data.fork.cursor}; only the continuation diverges.</p><div className="mt-5 rounded-lg border border-actae-400/20 bg-actae-400/5 p-4 font-mono text-xs text-surface-300"><p><span className="text-surface-500">parent</span> {data.fork.parent}</p><p className="mt-2"><span className="text-surface-500">child</span> {data.fork.child}</p><p className="mt-2"><span className="text-surface-500">receipt</span> {data.fork.receipt}</p><p className="mt-2 text-emerald-400">status · {data.fork.status}</p></div></div></section>
      </div>
    </main>
  </div>
}
