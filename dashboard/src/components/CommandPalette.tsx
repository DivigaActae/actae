// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useMemo, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import { listChannels } from '@/lib/api'
import { useUiStore } from '@/lib/ui-store'
import { useFocusTrap } from '@/hooks/useFocusTrap'
import { useIsMac, shortcutLabel } from '@/hooks/useIsMac'

const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_MODE === 'true'

interface PaletteItem {
  id: string
  label: string
  hint: string
  href: string
}

/** Global command palette (Cmd/Ctrl-K): jump to a run, or search the event
 * ledger. Runs come from `listChannels` (best-effort); the palette always
 * offers an event-search fallback for whatever is typed. */
export default function CommandPalette() {
  const router = useRouter()
  const open = useUiStore(s => s.commandOpen)
  const setOpen = useUiStore(s => s.setCommandOpen)
  const [query, setQuery] = useState('')
  const [channels, setChannels] = useState<string[]>([])
  const [active, setActive] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const dialogRef = useRef<HTMLDivElement>(null)
  const listRef = useRef<HTMLDivElement>(null)
  const isMac = useIsMac()
  useFocusTrap(dialogRef, open)

  // Global Cmd/Ctrl-K toggle, available on every page.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault()
        useUiStore.getState().openCommand()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  useEffect(() => {
    if (!open) return
    setQuery('')
    setActive(0)
    const t = window.setTimeout(() => inputRef.current?.focus(), 0)
    return () => window.clearTimeout(t)
  }, [open])

  useEffect(() => {
    if (!open) return
    let cancelled = false
    async function load() {
      if (MOCK_MODE) {
        const { MOCK_CHANNELS } = await import('@/lib/mock-data')
        if (!cancelled) setChannels([...MOCK_CHANNELS])
        return
      }
      const { data } = await listChannels()
      const list = (data as any)?.channels ?? data
      if (!cancelled && Array.isArray(list)) {
        setChannels(list.map((c: any) => typeof c === 'string' ? c : c.id || c.channel_id))
      }
    }
    void load()
    return () => { cancelled = true }
  }, [open])

  const items: PaletteItem[] = useMemo(() => {
    const q = query.trim().toLowerCase()
    const runs: PaletteItem[] = channels
      .filter(id => !q || id.toLowerCase().includes(q))
      .slice(0, 8)
      .map(id => ({
        id: `run:${id}`,
        label: id,
        hint: 'Open run',
        href: `/dashboard/flow/?channel=${encodeURIComponent(id)}`,
      }))
    const extra: PaletteItem[] = []
    if (q) {
      extra.push({ id: 'search-events', label: `Search events for \u201c${query.trim()}\u201d`, hint: 'Event ledger', href: `/dashboard/events/?q=${encodeURIComponent(query.trim())}` })
      extra.push({ id: 'open-runs', label: 'Browse all runs', hint: 'Runs', href: '/dashboard/runs/' })
    } else {
      extra.push({ id: 'open-runs', label: 'Browse all runs', hint: 'Runs', href: '/dashboard/runs/' })
      extra.push({ id: 'open-forks', label: 'Compare forks', hint: 'Forks', href: '/dashboard/experiments/' })
      extra.push({ id: 'open-approvals', label: 'Approvals', hint: 'Review', href: '/dashboard/approvals/' })
    }
    return [...extra, ...runs]
  }, [channels, query])

  useEffect(() => {
    setActive(0)
  }, [query])

  // Keep the highlighted row visible as the user arrows through a long list.
  useEffect(() => {
    const el = listRef.current?.querySelectorAll<HTMLElement>('[data-palette-item]')[active]
    el?.scrollIntoView({ block: 'nearest' })
  }, [active, items])

  if (!open) return null

  function go(item: PaletteItem | undefined) {
    if (!item) return
    setOpen(false)
    router.push(item.href)
  }

  function onKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Escape') { setOpen(false); return }
    if (e.key === 'ArrowDown') { e.preventDefault(); setActive(a => Math.min(items.length - 1, a + 1)) }
    if (e.key === 'ArrowUp') { e.preventDefault(); setActive(a => Math.max(0, a - 1)) }
    if (e.key === 'Enter') { e.preventDefault(); go(items[active]) }
  }

  return (
    <div className="fixed inset-0 z-[100] flex items-start justify-center bg-black/60 p-4 pt-[12vh]" onClick={() => setOpen(false)}>
      <div ref={dialogRef} role="dialog" aria-label="Command palette" aria-modal="true" className="w-full max-w-xl overflow-hidden rounded-xl border border-gray-700 bg-gray-900 shadow-2xl shadow-black/60" onClick={e => e.stopPropagation()}>
        <div className="flex items-center gap-2 border-b border-gray-800 px-3">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="#697069" strokeWidth="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
          <input
            ref={inputRef}
            aria-label="Search runs and events"
            value={query}
            onChange={e => setQuery(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder="Search runs, or type to search events…"
            className="w-full bg-transparent py-3 text-sm text-gray-200 outline-none placeholder:text-gray-600"
          />
          <kbd className="hidden shrink-0 rounded border border-gray-700 px-1.5 py-0.5 text-[10px] text-gray-500 sm:block">Esc</kbd>
        </div>
        <div ref={listRef} className="max-h-[50vh] overflow-y-auto p-1.5">
          {items.length === 0 ? (
            <p className="px-3 py-6 text-center text-xs text-gray-500">No matches.</p>
          ) : items.map((item, index) => (
            <button
              key={item.id}
              type="button"
              data-palette-item
              onMouseEnter={() => setActive(index)}
              onClick={() => go(item)}
              className={`flex w-full items-center justify-between gap-3 rounded-lg px-3 py-2 text-left transition-colors ${index === active ? 'bg-white/10' : 'hover:bg-white/5'}`}
            >
              <span className="min-w-0 truncate text-sm text-gray-200">{item.label}</span>
              <span className="shrink-0 text-[10px] uppercase tracking-wider text-gray-500">{item.hint}</span>
            </button>
          ))}
        </div>
        <div className="border-t border-gray-800 px-3 py-1.5 text-[10px] text-gray-600">↑↓ to navigate · Enter to open · {shortcutLabel(isMac)} to toggle</div>
      </div>
    </div>
  )
}
