// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

/**
 * Payload field filtering shared by the Flow and Events pages.
 *
 * `computeFieldCounts` drives the "Fields" dropdown (reuses the
 * EventTypeFilter UI: `{ type: key, count }` entries); `filterPayload`
 * applies the selected key set to a payload before rendering.
 */

export interface FieldCount {
  type: string
  count: number
}

/** Returns a shallow copy of `payload` containing only keys in
 * `visibleFields`. `null` (show everything) returns the payload unchanged. */
export function filterPayload(
  payload: Record<string, unknown> | null | undefined,
  visibleFields: ReadonlySet<string> | null
): Record<string, unknown> {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return {}
  if (!visibleFields) return payload
  const out: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(payload)) {
    if (visibleFields.has(key)) out[key] = value
  }
  return out
}

/** Distinct top-level payload keys across `events`, with per-key event
 * counts, sorted count-descending (then key-ascending). */
export function computeFieldCounts(
  events: Array<{ payload?: Record<string, unknown> | null }>
): FieldCount[] {
  const counts = new Map<string, number>()
  for (const e of events) {
    if (!e.payload || typeof e.payload !== 'object' || Array.isArray(e.payload)) continue
    for (const key of Object.keys(e.payload)) {
      counts.set(key, (counts.get(key) || 0) + 1)
    }
  }
  return Array.from(counts.entries())
    .map(([key, count]) => ({ type: key, count }))
    .sort((a, b) => b.count - a.count || a.type.localeCompare(b.type))
}
