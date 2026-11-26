// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest'
import { buildDiffTree, summarizeDiff, kindLabel, isOpaqueStateEntry, meaningfulStateEntries } from './state-diff'
import type { StateDiffEntry } from './api'

function entry(path: string[], kind: StateDiffEntry['kind']): StateDiffEntry {
  return { path, kind, left: null, right: null }
}

describe('buildDiffTree', () => {
  it('nests entries by path segments', () => {
    const tree = buildDiffTree([
      entry(['cfg', 'retries'], 'changed'),
      entry(['cfg', 'timeout'], 'removed'),
      entry(['tags', '2'], 'added'),
    ])
    expect(tree).toHaveLength(2)
    const cfg = tree.find(n => n.key === 'cfg')!
    expect(cfg.children).toHaveLength(2)
    expect(cfg.children[0].key).toBe('retries')
    expect(cfg.children[0].entry!.kind).toBe('changed')
    const tags = tree.find(n => n.key === 'tags')!
    expect(tags.children[0].key).toBe('2')
    expect(tags.children[0].entry!.kind).toBe('added')
  })

  it('collapses shared ancestors', () => {
    const tree = buildDiffTree([
      entry(['a', 'b', 'c'], 'changed'),
      entry(['a', 'b', 'd'], 'added'),
    ])
    expect(tree).toHaveLength(1)
    expect(tree[0].key).toBe('a')
    expect(tree[0].children).toHaveLength(1)
    expect(tree[0].children[0].key).toBe('b')
    expect(tree[0].children[0].children).toHaveLength(2)
  })

  it('handles empty paths as a bare root leaf', () => {
    const tree = buildDiffTree([entry([], 'changed')])
    expect(tree).toHaveLength(1)
    expect(tree[0].key).toBe('(root)')
    expect(tree[0].entry!.kind).toBe('changed')
  })

  it('returns an empty array for no entries', () => {
    expect(buildDiffTree([])).toEqual([])
  })
})

describe('summarizeDiff', () => {
  it('counts entries by kind', () => {
    expect(summarizeDiff([
      entry(['a'], 'added'),
      entry(['b'], 'added'),
      entry(['c'], 'removed'),
      entry(['d'], 'changed'),
      entry(['e'], 'changed'),
      entry(['f'], 'changed'),
    ])).toEqual({ added: 2, removed: 1, changed: 3 })
  })

  it('zeroes when empty', () => {
    expect(summarizeDiff([])).toEqual({ added: 0, removed: 0, changed: 0 })
  })
})

describe('kindLabel', () => {
  it('labels each kind', () => {
    expect(kindLabel('added')).toBe('added')
    expect(kindLabel('removed')).toBe('removed')
    expect(kindLabel('changed')).toBe('changed')
  })
})

describe('meaningfulStateEntries', () => {
  it('drops framework-adapter envelope paths, keeps business paths', () => {
    const entries = [
      entry(['_actae_adapter', 'channel_id'], 'changed'),
      entry(['checkpoint_id'], 'changed'),
      entry(['metadata', 'b'], 'changed'),
      entry(['plan', 'steps'], 'changed'),
      entry(['report'], 'changed'),
    ]
    expect(isOpaqueStateEntry(entries[0])).toBe(true)
    expect(isOpaqueStateEntry(entries[3])).toBe(false)
    const meaningful = meaningfulStateEntries(entries)
    expect(meaningful.map(e => e.path.join('.'))).toEqual(['plan.steps', 'report'])
  })

  it('returns an empty array for null/undefined input', () => {
    expect(meaningfulStateEntries(null)).toEqual([])
    expect(meaningfulStateEntries(undefined)).toEqual([])
  })
})
