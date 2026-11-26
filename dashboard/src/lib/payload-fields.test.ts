// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest'
import { filterPayload, computeFieldCounts } from './payload-fields'

describe('filterPayload', () => {
  it('returns the payload unchanged when visibleFields is null', () => {
    const payload = { model: 'gpt-4', tokens: 42 }
    expect(filterPayload(payload, null)).toBe(payload)
  })

  it('returns an empty object for null/undefined/non-object payloads', () => {
    expect(filterPayload(null, new Set(['x']))).toEqual({})
    expect(filterPayload(undefined, new Set(['x']))).toEqual({})
    expect(filterPayload(['a'] as any, new Set(['a']))).toEqual({})
  })

  it('keeps only the selected keys (shallow)', () => {
    const payload = { model: 'gpt-4', tokens: 42, nested: { keep: 1 } }
    expect(filterPayload(payload, new Set(['model', 'nested']))).toEqual({
      model: 'gpt-4',
      nested: { keep: 1 },
    })
  })

  it('returns an empty object when the selection is empty', () => {
    expect(filterPayload({ model: 'gpt-4' }, new Set())).toEqual({})
  })
})

describe('computeFieldCounts', () => {
  it('counts distinct payload keys across events, sorted by count desc', () => {
    const events = [
      { payload: { model: 'gpt-4', tokens: 1 } },
      { payload: { model: 'gpt-4', status: 'ok' } },
      { payload: { tokens: 2 } },
    ]
    expect(computeFieldCounts(events)).toEqual([
      { type: 'model', count: 2 },
      { type: 'tokens', count: 2 },
      { type: 'status', count: 1 },
    ])
  })

  it('ties break by key name ascending', () => {
    expect(computeFieldCounts([{ payload: { b: 1 } }, { payload: { a: 1 } }])).toEqual([
      { type: 'a', count: 1 },
      { type: 'b', count: 1 },
    ])
  })

  it('skips events without a payload object', () => {
    expect(computeFieldCounts([{ payload: null }, {}, { payload: ['x'] as any }])).toEqual([])
  })
})
