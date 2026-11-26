// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

/**
 * Compact per-node summaries for the flow canvas.
 *
 * Turns a raw event payload into up to 3 short display rows, preferring
 * well-known keys (model, status, tool, tokens, ...) and falling back to
 * the first scalar top-level entries. Object values and known noise keys
 * (context/metadata/state) are skipped so nodes stay readable.
 */

const KNOWN_KEYS: Array<{ key: string; render: (v: unknown) => string | null }> = [
  { key: 'model', render: (v) => (typeof v === 'string' ? v : null) },
  { key: 'status', render: (v) => (typeof v === 'string' ? v : null) },
  { key: 'tool', render: (v) => (typeof v === 'string' ? v : null) },
  { key: 'agent', render: (v) => (typeof v === 'string' ? v : null) },
  { key: 'agent_id', render: (v) => (typeof v === 'string' ? v : null) },
  { key: 'duration_ms', render: (v) => (typeof v === 'number' ? `${v}ms` : null) },
  { key: 'tokens', render: (v) => (typeof v === 'number' ? String(v) : null) },
  { key: 'rows', render: (v) => (typeof v === 'number' ? `${v} rows` : null) },
  {
    key: 'error',
    render: (v) => (typeof v === 'string' ? v.slice(0, 40) : v && typeof v === 'object' ? JSON.stringify(v).slice(0, 40) : null),
  },
]

const NOISE_KEYS = new Set(['context', 'metadata', 'state', 'args', 'kwargs', 'config'])

const MAX_ROWS = 3

function isScalar(v: unknown): v is string | number | boolean {
  return typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean'
}

export function summarizePayload(
  payload: unknown,
  visibleKeys?: ReadonlySet<string> | null
): string[] {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return []

  const obj = payload as Record<string, unknown>
  const rows: string[] = []

  // Preferred known keys first (declared order = display priority).
  for (const { key, render } of KNOWN_KEYS) {
    if (rows.length >= MAX_ROWS) break
    if (!(key in obj)) continue
    if (visibleKeys && !visibleKeys.has(key)) continue
    const rendered = render(obj[key])
    if (rendered) rows.push(rendered)
  }

  // Fallback: first scalar entries not already shown.
  for (const [key, value] of Object.entries(obj)) {
    if (rows.length >= MAX_ROWS) break
    if (NOISE_KEYS.has(key)) continue
    if (visibleKeys && !visibleKeys.has(key)) continue
    if (!isScalar(value)) continue
    if (KNOWN_KEYS.some((k) => k.key === key)) continue
    rows.push(`${key}=${value}`)
  }

  return rows
}
