// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useMemo, useState } from 'react'
import { useFlowStore } from '@/lib/flow-store'
import { compareChannels, getChannelMetadata, type CompareResponse, type StateDiff } from '@/lib/api'
import { buildDiffTree, kindLabel, meaningfulStateEntries, type DiffTreeNode } from '@/lib/state-diff'
import JsonTree from '@/components/JsonTree'

type Tab = 'state' | 'config' | 'tools' | 'output' | 'metrics'

/**
 * "What changed?" between the active fork and a sibling fork.
 *
 * Uses the server's combined `/channels/compare` document: structural state
 *   diff, fork-manifest diff, tool-call diff, output diff and
 *   latency/token/cost metrics delta — rendered as tabs.
 */
export function CompareView() {
  const { activeChannel, forks } = useFlowStore()
  const [compareChannel, setCompareChannel] = useState<string | null>(null)
  const [serverCompare, setServerCompare] = useState<CompareResponse | null>(null)
  const [tab, setTab] = useState<Tab>('state')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [activeName, setActiveName] = useState<string | null>(null)

  const candidates = useMemo(
    () => forks.filter(fork => fork.channel_id !== activeChannel),
    [activeChannel, forks],
  )

  useEffect(() => {
    setCompareChannel(current => candidates.some(c => c.channel_id === current)
      ? current
      : candidates[0]?.channel_id ?? null)
    setServerCompare(null)
    setError(null)
  }, [activeChannel, candidates])

  // Resolve the active run's friendly name (it isn't in `forks` — those are its
  // children) so the "Current fork" label reads as a name, not a hashed id.
  useEffect(() => {
    if (!activeChannel) { setActiveName(null); return }
    let cancelled = false
    getChannelMetadata(activeChannel)
      .then(res => { if (!cancelled) setActiveName(res.data?.display_name ?? null) })
      .catch(() => { /* name is cosmetic */ })
    return () => { cancelled = true }
  }, [activeChannel])

  async function runComparison() {
    if (!activeChannel || !compareChannel) return
    setLoading(true)
    setError(null)
    try {
      const cmp = await compareChannels(activeChannel, compareChannel)
      if (!cmp.data) throw new Error((cmp as any).error || 'Comparison failed')
      setServerCompare(cmp.data)
    } catch (e: any) {
      setError(e.message || 'Comparison failed')
    } finally {
      setLoading(false)
    }
  }

  const stateDiff = serverCompare?.state_diff
  // Drop framework-adapter envelope noise (see meaningfulStateEntries) so the
  // State tab shows business-level changes, not channel_id/checkpoint plumbing.
  const stateEntries = useMemo(
    () => meaningfulStateEntries(stateDiff?.entries),
    [stateDiff],
  )
  const manifestTree = serverCompare?.manifest_diff
    ? buildDiffTree(serverCompare.manifest_diff)
    : []
  const toolTree = serverCompare?.tool_diff ? buildDiffTree(serverCompare.tool_diff) : []
  const metricsTree = serverCompare?.metrics_delta
    ? buildDiffTree(serverCompare.metrics_delta)
    : []

  function DiffTreeRows({ nodes, path = [] }: { nodes: DiffTreeNode[]; path?: string[] }) {
    return (
      <>
        {nodes.map((node, i) => (
          <div key={i} className="ml-3 border-l border-gray-800 pl-2">
            <div className="flex gap-1.5 items-baseline">
              <span className="text-gray-300 font-mono">{node.key}</span>
              {node.entry && (
                <span
                  className={`text-[9px] px-1 rounded ${
                    node.entry.kind === 'added'
                      ? 'bg-emerald-500/10 text-emerald-400'
                      : node.entry.kind === 'removed'
                        ? 'bg-red-500/10 text-red-400'
                        : 'bg-amber-500/10 text-amber-400'
                  }`}
                >
                  {kindLabel(node.entry.kind)}
                </span>
              )}
              {node.entry?.left !== undefined && node.entry.left !== null && (
                <JsonTree data={node.entry.left} depth={0} />
              )}
              {node.entry?.kind === 'changed' && node.entry.right !== undefined && (
                <span className="text-gray-600">→</span>
              )}
              {node.entry?.right !== undefined && node.entry.right !== null && (
                <JsonTree data={node.entry.right} depth={0} />
              )}
            </div>
            {node.children && node.children.length > 0 && (
              <DiffTreeRows nodes={node.children} path={[...path, node.key]} />
            )}
          </div>
        ))}
      </>
    )
  }

  const tabs: { id: Tab; label: string; count: number }[] = [
    { id: 'state', label: 'State', count: stateEntries.length },
    { id: 'config', label: 'Config', count: serverCompare?.manifest_diff?.length ?? 0 },
    { id: 'tools', label: 'Tools', count: serverCompare?.tool_diff?.length ?? 0 },
    { id: 'output', label: 'Output', count: serverCompare?.output_changed ? 1 : 0 },
    { id: 'metrics', label: 'Metrics', count: serverCompare?.metrics_delta?.length ?? 0 },
  ]

  return (
    <div className="p-4 min-h-full bg-gradient-to-b from-gray-900 to-gray-950">
      <div className="flex flex-wrap items-start justify-between gap-3 mb-4">
        <div>
          <h3 className="text-sm font-semibold text-white">What changed?</h3>
          <p className="text-[11px] text-gray-500 mt-0.5">Compare fork state, configuration, tools, output, and performance.</p>
        </div>
        {serverCompare && (
          <div className="flex items-center gap-2 text-[10px]">
            <span className="rounded-full bg-emerald-500/10 px-2 py-1 text-emerald-400">Authoritative server diff</span>
          </div>
        )}
      </div>

      {candidates.length === 0 ? (
        <div className="rounded-xl border border-dashed border-gray-700 bg-gray-900/60 px-6 py-8 text-center">
          <div className="text-sm font-medium text-gray-300">No other forks yet</div>
          <div className="mt-1 text-xs text-gray-500">Fork this run to compare its state and outcome here.</div>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-[minmax(0,1fr)_32px_minmax(0,1fr)_auto] items-end gap-3 mb-4">
            <ForkLabel label="Current fork" name={activeName || activeChannel || '—'} tone="emerald" />
            <div className="pb-2 text-center text-gray-600">vs</div>
            <label className="min-w-0">
              <span className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-gray-500">Compare with</span>
              <select
                aria-label="Compare with fork"
                value={compareChannel || ''}
                onChange={(e) => setCompareChannel(e.target.value || null)}
                className="w-full bg-gray-800/70 border border-gray-700 rounded-lg px-3 py-2 text-xs text-gray-200 focus:outline-none focus:border-actae-500 transition-colors"
              >
            {candidates.map((b) => (
              <option key={b.channel_id} value={b.channel_id}>
                {b.display_name || b.channel_id.slice(0, 24)}
              </option>
            ))}
              </select>
            </label>

          <button
            onClick={runComparison}
            disabled={!compareChannel || loading}
            className="px-4 py-2 text-xs font-semibold bg-purple-600 hover:bg-purple-500 disabled:opacity-50 text-white rounded-lg transition-colors shadow-lg shadow-purple-950/30"
          >
            {loading ? 'Comparing...' : 'Compare'}
          </button>
          </div>

          {error && <div className="text-red-400 text-xs mb-2">{error}</div>}

          {serverCompare && (
            <div className="space-y-3">
                <DivergenceSummary stateDiff={stateDiff} />
                <div className="bg-gray-900/70 rounded-xl border border-gray-800 overflow-hidden">
                  <div className="flex border-b border-gray-800/60">
                    {tabs.map((t) => (
                      <button
                        key={t.id}
                        onClick={() => setTab(t.id)}
                        className={`flex-1 px-2 py-1.5 text-[10px] uppercase tracking-wide transition-colors ${
                          tab === t.id
                            ? 'bg-gray-800 text-purple-300'
                            : 'text-gray-500 hover:text-gray-300'
                        }`}
                      >
                        {t.label}
                        {t.count > 0 && (
                          <span className="ml-1 text-[9px] text-purple-400">{t.count}</span>
                        )}
                      </button>
                    ))}
                  </div>
                  <div className="p-3 min-h-40 max-h-80 overflow-y-auto text-[11px]">
                    {tab === 'state' && (
                      <div className="space-y-0.5">
                        {stateEntries.length ? (
                          <>
                            {stateDiff?.truncated && (
                              <div className="text-[9px] text-amber-400 mb-1">
                                Truncated — showing {stateEntries.length} of{' '}
                                {stateDiff.entry_count_total} differing paths.
                              </div>
                            )}
                            <DiffTreeRows nodes={buildDiffTree(stateEntries)} />
                          </>
                        ) : (stateDiff?.entries?.length ?? 0) > 0 ? (
                          <OpaqueStateNote />
                        ) : (
                          <div className="text-gray-600">No structural state differences.</div>
                        )}
                      </div>
                    )}
                    {tab === 'config' && (
                      <div className="space-y-0.5">
                        {manifestTree.length ? (
                          <DiffTreeRows nodes={manifestTree} />
                        ) : (
                          <div className="text-gray-600">No manifest differences.</div>
                        )}
                      </div>
                    )}
                    {tab === 'tools' && (
                      <div className="space-y-0.5">
                        {toolTree.length ? (
                          <DiffTreeRows nodes={toolTree} />
                        ) : (
                          <div className="text-gray-600">No tool-call differences.</div>
                        )}
                      </div>
                    )}
                    {tab === 'output' && (
                      <div className="space-y-1">
                        <div className="text-gray-500">Current</div>
                        <div className="bg-gray-900/60 rounded p-1.5">
                          <JsonTree data={serverCompare.output_left} depth={0} />
                        </div>
                        <div className="text-gray-500">Compare</div>
                        <div className="bg-gray-900/60 rounded p-1.5">
                          <JsonTree data={serverCompare.output_right} depth={0} />
                        </div>
                        {serverCompare.output_changed ? (
                          <div className="text-[9px] text-amber-400">Outputs differ</div>
                        ) : (
                          <div className="text-[9px] text-emerald-400">Outputs identical</div>
                        )}
                      </div>
                    )}
                    {tab === 'metrics' && (
                      <div className="space-y-0.5">
                        {metricsTree.length ? (
                          <DiffTreeRows nodes={metricsTree} />
                        ) : (
                          <div className="text-gray-600">No metrics deltas recorded.</div>
                        )}
                      </div>
                    )}
                  </div>
                </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

/**
 * Shown when the raw state diff is non-empty but consists entirely of
 * framework-adapter internals (e.g. LangGraph's opaque checkpoint envelope).
 * Explains where the real, business-level change actually lives.
 */
function OpaqueStateNote() {
  return (
    <div className="max-w-xl text-[11px] leading-5 text-gray-500">
      Both runs differ only in the framework&apos;s internal checkpoint envelope
      (opaque to the diff) — not in business state. The meaningful change is in{' '}
      <span className="font-semibold text-gray-300">Output</span>,{' '}
      <span className="font-semibold text-gray-300">Config</span> and{' '}
      <span className="font-semibold text-gray-300">Metrics</span>.
    </div>
  )
}

function ForkLabel({ label, name, tone }: { label: string; name: string; tone: 'emerald' }) {
  return (
    <div className="min-w-0">
      <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-gray-500">{label}</div>
      <div className={`truncate rounded-lg border px-3 py-2 text-xs font-mono ${tone === 'emerald' ? 'border-emerald-500/20 bg-emerald-500/5 text-emerald-300' : ''}`} title={name}>
        {name}
      </div>
    </div>
  )
}

function DivergenceSummary({ stateDiff }: { stateDiff?: StateDiff }) {
  const common = stateDiff?.common
  return (
    <div className="grid grid-cols-1 gap-2 text-[11px] sm:grid-cols-3">
      <div className="rounded-lg border border-gray-800 bg-gray-900/70 p-2.5">
        <div className="text-[9px] uppercase tracking-wider text-gray-600">Shared ancestor</div>
        <div className="mt-1 truncate text-gray-300" title={common?.channel_id}>{common?.channel_id || 'Unrelated runs'}</div>
      </div>
      <div className="rounded-lg border border-emerald-500/15 bg-emerald-500/5 p-2.5">
        <div className="text-[9px] uppercase tracking-wider text-emerald-500/70">Current diverged</div>
        <div className="mt-1 font-mono text-emerald-300">{stateDiff?.left_diverged_at_cursor == null ? 'ancestor' : `@${stateDiff.left_diverged_at_cursor}`}</div>
      </div>
      <div className="rounded-lg border border-sky-500/15 bg-sky-500/5 p-2.5">
        <div className="text-[9px] uppercase tracking-wider text-sky-500/70">Compared diverged</div>
        <div className="mt-1 font-mono text-sky-300">{stateDiff?.right_diverged_at_cursor == null ? 'ancestor' : `@${stateDiff.right_diverged_at_cursor}`}</div>
      </div>
    </div>
  )
}
