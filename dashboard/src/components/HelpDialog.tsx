// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useRef } from 'react'
import { useUiStore } from '@/lib/ui-store'
import { useFocusTrap } from '@/hooks/useFocusTrap'
import { useIsMac, shortcutLabel } from '@/hooks/useIsMac'

const GLOSSARY: { term: string; body: string }[] = [
  { term: 'Run', body: 'One agent execution, recorded end to end. In the API a run is a “channel”.' },
  { term: 'Event', body: 'A single recorded step in a run — a model call, a tool call, a state save, a completion.' },
  { term: 'Cursor', body: 'The position of an event within its run. Cursors start at 1 and increase, so you can replay from any point.' },
  { term: 'Fork', body: 'A new run that inherits another run’s history up to a chosen point, then diverges. Forks let you try a different continuation without re-running the prefix.' },
  { term: 'Snapshot (state)', body: 'A saved copy of an agent’s state at a cursor. Resuming and forking load the snapshot nearest the boundary.' },
  { term: 'Approval', body: 'A durable request from an agent for a human decision. Actae records the decision; it never resumes the agent itself.' },
  { term: 'Tool execution', body: 'Actae never runs tools. It records the claim/complete/fail lifecycle so retries stay idempotent.' },
  { term: 'Coordination group', body: 'A set of runs that share work through leases and messages, across machines.' },
]

/** Contextual help: a short glossary for the domain terms the console uses.
 * Open from the sidebar footer or the command palette. */
export default function HelpDialog() {
  const open = useUiStore(s => s.helpOpen)
  const setOpen = useUiStore(s => s.setHelpOpen)
  const closeButtonRef = useRef<HTMLButtonElement>(null)
  const dialogRef = useRef<HTMLDivElement>(null)
  const isMac = useIsMac()
  useFocusTrap(dialogRef, open)

  useEffect(() => {
    if (!open) return
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false)
    }
    window.addEventListener('keydown', onKey)
    closeButtonRef.current?.focus()
    return () => window.removeEventListener('keydown', onKey)
  }, [open, setOpen])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-[100] flex items-start justify-center bg-black/60 p-4 pt-[9vh]" onClick={() => setOpen(false)}>
      <div ref={dialogRef} role="dialog" aria-label="Help and glossary" aria-modal="true" className="w-full max-w-2xl overflow-hidden rounded-xl border border-gray-700 bg-gray-900 shadow-2xl shadow-black/60" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between border-b border-gray-800 px-5 py-3">
          <div>
            <h2 className="text-sm font-semibold text-white">Help &amp; glossary</h2>
            <p className="mt-0.5 text-[11px] text-gray-500">The words this console uses, in plain language.</p>
          </div>
          <button ref={closeButtonRef} onClick={() => setOpen(false)} aria-label="Close help" className="flex h-7 w-7 items-center justify-center rounded-md text-gray-500 hover:bg-gray-800 hover:text-white">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 6 6 18M6 6l12 12"/></svg>
          </button>
        </div>
        <div className="max-h-[65vh] overflow-y-auto p-5">
          <dl className="space-y-3">
            {GLOSSARY.map(({ term, body }) => (
              <div key={term} className="grid grid-cols-[130px_minmax(0,1fr)] gap-3">
                <dt className="text-[12px] font-semibold text-gray-200">{term}</dt>
                <dd className="text-[12px] leading-5 text-gray-400">{body}</dd>
              </div>
            ))}
          </dl>
        </div>
        <div className="border-t border-gray-800 px-5 py-2.5 text-[11px] text-gray-500">
          Tip: press <kbd className="rounded border border-gray-700 px-1.5 py-0.5 text-[10px]">{shortcutLabel(isMac)}</kbd> anywhere to jump to a run.
        </div>
      </div>
    </div>
  )
}
