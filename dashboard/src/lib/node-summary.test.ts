// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest'
import { summarizePayload } from './node-summary'

describe('summarizePayload', () => {
  it('renders known keys with their display formats', () => {
    expect(summarizePayload({ model: 'gpt-4o', tokens: 1200 })).toEqual(['gpt-4o', '1200'])
    expect(summarizePayload({ tool: 'search', duration_ms: 2400 })).toEqual(['search', '2400ms'])
    expect(summarizePayload({ rows: 42 })).toEqual(['42 rows'])
    expect(summarizePayload({ agent: 'researcher' })).toEqual(['researcher'])
  })

  it('prefers known keys over fallback entries', () => {
    expect(summarizePayload({ tool: 'search', query: 'weather' })).toEqual(['search', 'query=weather'])
  })

  it('falls back to the first scalar top-level entries as key=value', () => {
    expect(summarizePayload({ region: 'eu-west', retries: 2 })).toEqual(['region=eu-west', 'retries=2'])
  })

  it('skips object values, nulls and noise keys', () => {
    expect(summarizePayload({ context: { deep: true }, metadata: { a: 1 }, nested: { x: 1 } })).toEqual([])
    expect(summarizePayload({ tool: null, tool_id: 't-1', nested: { x: 1 } })).toEqual(['tool_id=t-1'])
  })

  it('skips arrays and booleans-only payloads where they are not scalar rows', () => {
    expect(summarizePayload(['a', 'b'])).toEqual([])
    expect(summarizePayload({ done: true })).toEqual(['done=true'])
  })

  it('caps at 3 rows', () => {
    const rows = summarizePayload({ a: 1, b: 2, c: 3, d: 4, e: 5 })
    expect(rows).toHaveLength(3)
  })

  it('truncates long error strings', () => {
    const long = 'x'.repeat(100)
    expect(summarizePayload({ error: long })[0]).toHaveLength(40)
  })

  it('returns an empty list for non-object payloads', () => {
    expect(summarizePayload(null)).toEqual([])
    expect(summarizePayload(undefined)).toEqual([])
    expect(summarizePayload('plain string')).toEqual([])
    expect(summarizePayload(42)).toEqual([])
  })

  it('passing null visibleKeys keeps the full summary', () => {
    expect(summarizePayload({ model: 'gpt-4o', query: 'hi' }, null)).toEqual(['gpt-4o', 'query=hi'])
  })

  it('drops known keys not in visibleKeys', () => {
    expect(summarizePayload({ model: 'gpt-4o', tool: 'search' }, new Set(['tool']))).toEqual(['search'])
  })

  it('drops fallback keys not in visibleKeys', () => {
    expect(summarizePayload({ model: 'gpt-4o', query: 'hi' }, new Set(['model']))).toEqual(['gpt-4o'])
  })

  it('returns an empty list when visibleKeys hides everything', () => {
    expect(summarizePayload({ model: 'gpt-4o' }, new Set())).toEqual([])
  })
})
