// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import type { DashboardEvent } from './mock-data'

/**
 * A recorded, deterministic dry-run the dashboard can replay in the browser.
 *
 * This is the interactive "watch it run" path for the hosted demo (the
 * marketing site embeds the dashboard in mock mode and streams this in). It is
 * deliberately fixed: same steps, same cursors, same pacing on every visit, so
 * the demo never depends on a backend, network, LLM, or random source — the
 * "dry-run" is the whole point. The steps mirror
 * `examples/research_agent/demo.py --dry-run`: a research agent runs to a saved
 * boundary, then a fork continues from that boundary with a fact-check step.
 */
export interface DemoStep {
  /** Milliseconds to wait after the previous step (the previous step's own
   * delay elapses before this one fires). */
  delayMs: number
  channel: string
  type: string
  payload: Record<string, unknown>
  cursor: number
  actor: string
}

// Dedicated channels with NO recorded history, so the replay builds them from
// empty (no duplication with the mock dataset) and the timeline grows live.
export const DEMO_CHANNEL = 'demo-live-run'
export const DEMO_FORK_CHANNEL = 'demo-live-run-fork'
export const DEMO_CHANNELS = [DEMO_CHANNEL, DEMO_FORK_CHANNEL] as const

/** Fixed wall-clock base for the replay, so timestamps are identical on every
 * visit (the timeline sorts by timestamp; ordering is deterministic). */
export const DEMO_BASE_MS = Date.UTC(2026, 6, 25, 9, 0, 0)

export const DEMO_SCENARIO: DemoStep[] = [
  // ── Baseline research run ──────────────────────────────────────────────
  { delayMs: 200, channel: DEMO_CHANNEL, cursor: 1, actor: 'research-v1', type: 'agent.started', payload: { agent: 'research-v1', model: 'gpt-4', task: 'Analyze the competitor landscape' } },
  { delayMs: 650, channel: DEMO_CHANNEL, cursor: 2, actor: 'research-v1', type: 'tool.web_search', payload: { tool: 'web_search', query: 'top AI agent frameworks 2026', results: 8, latency_ms: 1200 } },
  { delayMs: 620, channel: DEMO_CHANNEL, cursor: 3, actor: 'research-v1', type: 'tool.llm_call', payload: { model: 'gpt-4', tokens: 1200, latency_ms: 3100, operation: 'summarize_findings' } },
  { delayMs: 260, channel: DEMO_CHANNEL, cursor: 3, actor: 'system', type: 'state.saved', payload: { version: 1, fields: 6, cursor: 3, note: 'boundary: research committed' } },
  { delayMs: 560, channel: DEMO_CHANNEL, cursor: 4, actor: 'research-v1', type: 'tool.web_search', payload: { tool: 'web_search', query: 'enterprise agent adoption 2026', results: 12, latency_ms: 980 } },
  { delayMs: 700, channel: DEMO_CHANNEL, cursor: 5, actor: 'research-v1', type: 'agent.completed', payload: { agent: 'research-v1', findings: 15, sources: 20, tokens_total: 4200, cost_usd: 0.18 } },
  { delayMs: 260, channel: DEMO_CHANNEL, cursor: 5, actor: 'system', type: 'state.saved', payload: { version: 2, fields: 7, cursor: 5 } },

  // ── Fork at the saved boundary; add a fact-check step ──────────────────
  { delayMs: 900, channel: DEMO_FORK_CHANNEL, cursor: 1, actor: 'system', type: 'fork.started', payload: { source_channel_id: DEMO_CHANNEL, requested_cursor: 3, resolved_cursor: 3, reason: 'Add a fact-check step before concluding', reproducibility: 'exact' } },
  { delayMs: 500, channel: DEMO_FORK_CHANNEL, cursor: 2, actor: 'research-v1', type: 'experiment.run_started', payload: { forked_from: DEMO_CHANNEL, at_cursor: 3, inherited_state: 'state v1' } },
  { delayMs: 640, channel: DEMO_FORK_CHANNEL, cursor: 3, actor: 'research-v1', type: 'tool.web_search', payload: { tool: 'web_search', query: 'verify: enterprise agent adoption statistics', results: 6, latency_ms: 870 } },
  { delayMs: 620, channel: DEMO_FORK_CHANNEL, cursor: 4, actor: 'research-v1', type: 'tool.llm_call', payload: { model: 'gpt-4', tokens: 900, latency_ms: 2100, operation: 'fact_check_claims' } },
  { delayMs: 520, channel: DEMO_FORK_CHANNEL, cursor: 5, actor: 'research-v1', type: 'experiment.step_completed', payload: { step: 'fact_check', claims_checked: 9, corroborated: 7 } },
  { delayMs: 240, channel: DEMO_FORK_CHANNEL, cursor: 5, actor: 'system', type: 'state.saved', payload: { version: 1, fields: 5, cursor: 5, note: 'boundary: fact-check committed' } },
  { delayMs: 720, channel: DEMO_FORK_CHANNEL, cursor: 6, actor: 'research-v1', type: 'agent.completed', payload: { agent: 'research-v1', findings: 18, verdict: 'revised conclusion', score: 9, cost_usd: 0.11 } },
]

/** Total wall-clock for a full replay (sum of step delays). Deterministic. */
export const DEMO_TOTAL_MS = DEMO_SCENARIO.reduce((total, step) => total + step.delayMs, 0)

/** Cumulative start offset (ms) for each step — used by the dock's progress
 * bar and by tests to assert the timeline is stable. */
export function demoOffsets(steps: DemoStep[] = DEMO_SCENARIO): number[] {
  let running = 0
  return steps.map(step => (running += step.delayMs))
}

/** Deterministic ISO timestamp for step `index` (fixed base + cumulative delay),
 * so the replay is identical on every visit. */
export function demoTimestampAt(index: number): string {
  const offsets = demoOffsets()
  const elapsed = index > 0 ? offsets[index - 1] ?? 0 : 0
  return new Date(DEMO_BASE_MS + elapsed).toISOString()
}

/** Build the store event for a step. IDs are stable (channel + cursor + type)
 * so a replay is idempotent and never duplicates on restart. */
export function demoStepToEvent(step: DemoStep, index: number): DashboardEvent {
  return {
    id: `demo-${step.channel}-${step.cursor}-${step.type}`,
    cursor: step.cursor,
    type: step.type,
    channel: step.channel,
    timestamp: demoTimestampAt(index),
    payload: step.payload,
    isState: false,
  }
}
