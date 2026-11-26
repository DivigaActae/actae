// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useMemo, useRef, useState } from 'react'

export interface EventTypeCount {
  type: string
  count: number
}

interface EventTypeFilterProps {
  /** Distinct items (event types or payload fields) with counts, sorted by
   * count descending. */
  types: EventTypeCount[]
  /** Currently visible items; `null` = show everything. */
  selected: ReadonlySet<string> | null
  /** Called with the new selection; pass `null` to show everything. */
  onChange: (next: Set<string> | null) => void
  label?: string
  disabled?: boolean
  /** Search-box placeholder, e.g. "Search fields..." for the Fields filter. */
  searchPlaceholder?: string
  /** Tooltip on the trigger button when every item is shown. */
  allCheckedTitle?: string
}

/**
 * Multi-select dropdown filter over event types or payload fields, shared by
 * the flow canvas and the events page. Shows a checkbox list with per-item
 * counts, a search box for long lists, and "Show all"/"Show none"
 * shortcuts. Closes on outside click or Escape.
 */
export default function EventTypeFilter({
  types,
  selected,
  onChange,
  label = 'Filter',
  disabled = false,
  searchPlaceholder = 'Search types...',
  allCheckedTitle = 'Filter by node type',
}: EventTypeFilterProps) {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const rootRef = useRef<HTMLDivElement>(null)
  const searchRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (!open) return
    const onMouseDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    document.addEventListener('keydown', onKeyDown)
    return () => {
      document.removeEventListener('mousedown', onMouseDown)
      document.removeEventListener('keydown', onKeyDown)
    }
  }, [open])

  useEffect(() => {
    if (open) {
      setQuery('')
      searchRef.current?.focus()
    }
  }, [open])

  const visible = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return types
    return types.filter(t => t.type.toLowerCase().includes(q))
  }, [types, query])

  const allChecked = selected === null
  const shownCount = selected === null ? types.length : selected.size

  function toggleType(type: string) {
    if (selected === null) {
      // First toggle starts from "all shown": hide only this type.
      const next = new Set(types.map(t => t.type))
      next.delete(type)
      onChange(next.size === 0 ? null : next)
      return
    }
    const next = new Set(selected)
    if (next.has(type)) next.delete(type)
    else next.add(type)
    onChange(next.size === 0 ? new Set() : next)
  }

  function showAll() {
    onChange(null)
  }

  function showNone() {
    onChange(new Set())
  }

  return (
    <div ref={rootRef} className="relative">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        disabled={disabled || types.length === 0}
        aria-haspopup="listbox"
        aria-expanded={open}
        title={allChecked ? allCheckedTitle : 'Filter is active'}
        className={`flex items-center gap-1.5 px-2 py-1 text-[11px] font-medium rounded transition-colors ${
          allChecked
            ? 'text-gray-400 hover:text-white hover:bg-gray-700/60'
            : 'text-actae-300 bg-actae-500/10 hover:bg-actae-500/20'
        } ${disabled || types.length === 0 ? 'opacity-50 cursor-not-allowed' : ''}`}
      >
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" />
        </svg>
        <span className="whitespace-nowrap">
          {label}: {allChecked ? `all ${types.length}` : `${shownCount}/${types.length}`}
        </span>
        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={`transition-transform ${open ? 'rotate-180' : ''}`}>
          <polyline points="6 9 12 15 18 9" />
        </svg>
      </button>

      {open && (
        <div
          role="listbox"
          aria-multiselectable="true"
          className="absolute left-0 top-full mt-1 z-50 w-72 max-w-[320px] max-h-[340px] flex flex-col rounded-lg border border-gray-700 bg-gray-900 shadow-2xl shadow-black/40"
        >
          <div className="p-2 border-b border-gray-800">
            <input
              ref={searchRef}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder={searchPlaceholder}
              className="w-full px-2 py-1 text-xs bg-gray-800 border border-gray-700 rounded-md text-gray-200 placeholder-gray-500 focus:outline-none focus:border-actae-500"
            />
          </div>

          <div className="flex-1 overflow-y-auto min-h-0">
            {visible.length === 0 && (
              <p className="px-3 py-4 text-xs text-gray-500 text-center">No types match</p>
            )}
            {visible.map(({ type, count }) => {
              const checked = allChecked || selected.has(type)
              return (
              <label
                key={type}
                className="flex items-start gap-2 px-3 py-1.5 text-xs hover:bg-gray-800/70 cursor-pointer"
              >
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => toggleType(type)}
                  className="accent-actae-500 mt-0.5 shrink-0"
                />
                <span className="flex-1 min-w-0 whitespace-normal break-words text-gray-200 font-mono leading-snug" title={type}>
                  {type}
                </span>
                <span className="text-[10px] text-gray-500 shrink-0 leading-snug pt-0.5">{count}</span>
              </label>
              )
            })}
          </div>

          <div className="flex items-center justify-between px-2 py-1.5 border-t border-gray-800">
            <button
              type="button"
              onClick={showAll}
              className="px-2 py-1 text-[11px] text-gray-400 hover:text-white hover:bg-gray-800 rounded"
            >
              Show all
            </button>
            <button
              type="button"
              onClick={showNone}
              className="px-2 py-1 text-[11px] text-gray-400 hover:text-white hover:bg-gray-800 rounded"
            >
              Show none
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
