// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useMemo, useState } from 'react'
import JsonTree from '@/components/JsonTree'
import { compareChannels, type CompareResponse, type RankedExperimentMember, type StateDiffEntry } from '@/lib/api'
import { meaningfulStateEntries } from '@/lib/state-diff'

type DiffTab = 'state' | 'config' | 'tools' | 'output' | 'metrics'

export function ExperimentDiff({ members }: { members: RankedExperimentMember[] }) {
  const baseline = members.find(member => member.role === 'baseline')?.channel_id || members[0]?.channel_id || ''
  const firstVariant = members.find(member => member.channel_id !== baseline)?.channel_id || ''
  const [left, setLeft] = useState(baseline)
  const [right, setRight] = useState(firstVariant)
  const [comparison, setComparison] = useState<CompareResponse | null>(null)
  const [tab, setTab] = useState<DiffTab>('state')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLeft(current => members.some(member => member.channel_id === current) ? current : baseline)
    setRight(current => members.some(member => member.channel_id === current && current !== baseline) ? current : firstVariant)
  }, [members, baseline, firstVariant])

  useEffect(() => {
    if (!left || !right || left === right) {
      setComparison(null)
      return
    }
    let cancelled = false
    setLoading(true)
    setError(null)
    void compareChannels(left, right).then(result => {
      if (cancelled) return
      if (result.data) setComparison(result.data)
      else {
        setComparison(null)
        setError(result.error || 'Comparison failed')
      }
    }).catch(err => {
      if (!cancelled) {
        setComparison(null)
        setError(err instanceof Error ? err.message : 'Comparison failed')
      }
    }).finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [left, right])

  if (members.length < 2) return null

  // Hide framework-adapter envelope noise from the State tab (see
  // meaningfulStateEntries); the business change is in Config/Output/Metrics.
  const stateEntries = useMemo(
    () => meaningfulStateEntries(comparison?.state_diff.entries),
    [comparison],
  )
  const tabs: { id: DiffTab; label: string; count: number }[] = [
    { id: 'state', label: 'State', count: stateEntries.length },
    { id: 'config', label: 'Config', count: comparison?.manifest_diff.length || 0 },
    { id: 'tools', label: 'Tools', count: comparison?.tool_diff.length || 0 },
    { id: 'output', label: 'Output', count: comparison?.output_changed ? 1 : 0 },
    { id: 'metrics', label: 'Metrics', count: comparison?.metrics_delta.length || 0 },
  ]
  const entries = tab === 'state' ? stateEntries
    : tab === 'config' ? comparison?.manifest_diff
      : tab === 'tools' ? comparison?.tool_diff
        : tab === 'metrics' ? comparison?.metrics_delta : undefined
  const opaqueStateOnly = tab === 'state'
    && stateEntries.length === 0
    && (comparison?.state_diff.entries.length ?? 0) > 0

  return (
    <section className="mt-6 overflow-hidden rounded-xl border border-gray-800 bg-gray-950/50" aria-label="Fork comparison">
      <div className="border-b border-gray-800 p-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h3 className="text-sm font-semibold text-white">Fork diff</h3>
            <p className="mt-0.5 text-[11px] text-gray-500">Authoritative side-by-side comparison from the server.</p>
          </div>
          {comparison && <span className="rounded-full bg-emerald-500/10 px-2 py-1 text-[10px] font-semibold text-emerald-400">Compared</span>}
        </div>
        <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-[minmax(0,1fr)_24px_minmax(0,1fr)] sm:items-end">
          <ForkSelect label="Before" tone="red" value={left} members={members} onChange={setLeft} />
          <div className="hidden pb-2 text-center text-gray-600 sm:block">→</div>
          <ForkSelect label="After" tone="green" value={right} members={members} onChange={setRight} />
        </div>
      </div>

      <div className="flex overflow-x-auto border-b border-gray-800 bg-gray-900/50">
        {tabs.map(item => <button key={item.id} onClick={() => setTab(item.id)} className={`min-w-24 flex-1 px-3 py-2 text-[10px] font-semibold uppercase tracking-wider ${tab === item.id ? 'border-b-2 border-purple-400 bg-gray-800/60 text-purple-300' : 'text-gray-500 hover:text-gray-300'}`}>{item.label}{item.count > 0 && <span className="ml-1.5 rounded-full bg-gray-700 px-1.5 py-0.5 text-[9px] text-gray-300">{item.count}</span>}</button>)}
      </div>

      <div className="min-h-56 overflow-auto">
        {loading ? <div className="p-10 text-center text-sm text-gray-500">Computing diff…</div>
          : error ? <div role="alert" className="m-4 rounded-lg border border-red-900 bg-red-950/40 p-3 text-xs text-red-300">{error}</div>
            : !comparison ? <div className="p-10 text-center text-sm text-gray-600">Choose two different forks.</div>
              : tab === 'output' ? <OutputDiff comparison={comparison} />
                : entries?.length ? <DiffRows entries={entries} />
                  : opaqueStateOnly ? <div className="p-10 text-center text-sm leading-6 text-gray-500">These runs differ only in the framework&apos;s internal checkpoint envelope (opaque to the diff). See <span className="font-semibold text-gray-300">Config</span>, <span className="font-semibold text-gray-300">Output</span> and <span className="font-semibold text-gray-300">Metrics</span> for the real change.</div>
                    : <div className="p-10 text-center text-sm text-gray-600">No {tab} differences.</div>}
      </div>
    </section>
  )
}

function ForkSelect({ label, tone, value, members, onChange }: { label: string; tone: 'red' | 'green'; value: string; members: RankedExperimentMember[]; onChange: (value: string) => void }) {
  return <label className="min-w-0"><span className={`mb-1 block text-[10px] font-semibold uppercase tracking-wider ${tone === 'red' ? 'text-red-400' : 'text-emerald-400'}`}>{label}</span><select aria-label={`${label} fork`} value={value} onChange={event => onChange(event.target.value)} className={`w-full truncate rounded-lg border bg-gray-900 px-3 py-2 font-mono text-xs text-gray-200 outline-none ${tone === 'red' ? 'border-red-500/25 focus:border-red-500' : 'border-emerald-500/25 focus:border-emerald-500'}`}>{members.map(member => <option key={member.channel_id} value={member.channel_id}>{member.role === 'baseline' ? 'Baseline · ' : ''}{member.display_name || member.channel_id}</option>)}</select></label>
}

function DiffRows({ entries }: { entries: StateDiffEntry[] }) {
  return <div className="min-w-[720px] font-mono text-[11px]">
    <div className="grid grid-cols-[220px_minmax(250px,1fr)_minmax(250px,1fr)] border-b border-gray-800 bg-gray-900/70 text-[9px] font-semibold uppercase tracking-wider text-gray-500"><div className="px-3 py-2">Path</div><div className="border-l border-red-900/40 bg-red-950/20 px-3 py-2 text-red-400">Before</div><div className="border-l border-emerald-900/40 bg-emerald-950/20 px-3 py-2 text-emerald-400">After</div></div>
    {entries.map((entry, index) => <div key={`${entry.path.join('.')}-${index}`} className="grid grid-cols-[220px_minmax(250px,1fr)_minmax(250px,1fr)] border-b border-gray-800/70 last:border-0"><div className="break-all px-3 py-2 text-gray-400">{entry.path.length ? entry.path.join('.') : '(root)'}</div><DiffCell value={entry.left} tone="red" empty={entry.kind === 'added'} /><DiffCell value={entry.right} tone="green" empty={entry.kind === 'removed'} /></div>)}
  </div>
}

function DiffCell({ value, tone, empty }: { value: unknown; tone: 'red' | 'green'; empty: boolean }) {
  return <div className={`overflow-x-auto border-l px-3 py-2 ${tone === 'red' ? 'border-red-900/40 bg-red-950/20' : 'border-emerald-900/40 bg-emerald-950/20'}`}>{empty ? <span className="italic text-gray-600">not present</span> : <JsonTree data={value} depth={0} />}</div>
}

function OutputDiff({ comparison }: { comparison: CompareResponse }) {
  return <div><div className="grid min-w-[720px] grid-cols-2 text-[9px] font-semibold uppercase tracking-wider"><div className="bg-red-950/30 px-4 py-2 text-red-400">Before</div><div className="border-l border-emerald-900/40 bg-emerald-950/30 px-4 py-2 text-emerald-400">After</div></div><div className="grid min-w-[720px] grid-cols-2 font-mono text-[11px]"><div className="overflow-auto bg-red-950/20 p-4"><JsonTree data={comparison.output_left} /></div><div className="overflow-auto border-l border-emerald-900/40 bg-emerald-950/20 p-4"><JsonTree data={comparison.output_right} /></div></div><div className={`border-t border-gray-800 px-4 py-2 text-[10px] ${comparison.output_changed ? 'text-amber-400' : 'text-emerald-400'}`}>{comparison.output_changed ? 'Outputs differ' : 'Outputs are identical'}</div></div>
}
