// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import { useFlowStore } from '@/lib/flow-store'
import { DEMO_CHANNEL, DEMO_FORK_CHANNEL, DEMO_SCENARIO, DEMO_TOTAL_MS, demoStepToEvent } from '@/lib/demo-scenario'

const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_MODE === 'true'

/**
 * Mock-mode-only dock that replays the recorded dry-run (`demo-scenario.ts`)
 * into the dashboard's live feed, so the demo "runs" — events stream into the
 * Flow timeline and live bar, then a fork continues from the saved boundary.
 *
 * Deterministic: the steps, cursors and pacing are fixed, so every visitor
 * sees the identical run. Auto-starts when the URL carries `?replay=1` (the
 * marketing site's embed adds it), otherwise it waits for a click.
 */
export default function DemoReplayDock() {
  if (!MOCK_MODE) return null
  return <DemoReplayDockInner />
}

function DemoReplayDockInner() {
  const [index, setIndex] = useState(0)
  const [running, setRunning] = useState(false)
  const [started, setStarted] = useState(false)
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const cancelled = useRef(false)

  const clearTimer = useCallback(() => {
    if (timer.current) {
      clearTimeout(timer.current)
      timer.current = null
    }
  }, [])

  const emitUpTo = useCallback(
    (count: number) => DEMO_SCENARIO.slice(0, count).reduce((total, step) => total + step.delayMs, 0),
    [],
  )

  const run = useCallback(
    (from: number) => {
      cancelled.current = false
      clearTimer()
      setStarted(true)
      setRunning(true)
      setIndex(from)
      if (from === 0) useFlowStore.getState().resetDemoReplay([DEMO_CHANNEL, DEMO_FORK_CHANNEL])
      let i = from

      const tick = () => {
        if (cancelled.current) return
        if (i >= DEMO_SCENARIO.length) {
          setRunning(false)
          // The replay is done; drop the DEMO badge so the run reads as history.
          useFlowStore.setState({ demoLive: false })
          return
        }
        useFlowStore.getState().injectDemoEvent(demoStepToEvent(DEMO_SCENARIO[i], i))
        i += 1
        setIndex(i)
        const nextDelay = DEMO_SCENARIO[i]?.delayMs ?? 500
        timer.current = setTimeout(tick, nextDelay)
      }

      timer.current = setTimeout(tick, DEMO_SCENARIO[from]?.delayMs ?? 200)
    },
    [clearTimer],
  )

  const pause = useCallback(() => {
    cancelled.current = true
    clearTimer()
    setRunning(false)
  }, [clearTimer])

  const restart = useCallback(() => run(0), [run])

  // Auto-start when the embed asks for it.
  useEffect(() => {
    if (typeof window === 'undefined') return
    const params = new URLSearchParams(window.location.search)
    if (params.get('replay') !== '1') return
    const handle = setTimeout(() => run(0), 700)
    return () => {
      clearTimeout(handle)
      cancelled.current = true
      clearTimer()
    }
  }, [run, clearTimer])

  useEffect(() => () => { cancelled.current = true; clearTimer() }, [clearTimer])

  const completed = index >= DEMO_SCENARIO.length
  const progress = DEMO_TOTAL_MS === 0 ? 1 : Math.min(1, emitUpTo(index) / DEMO_TOTAL_MS)

  return (
    <div className="fixed bottom-4 right-4 z-50 w-72 rounded-xl border border-actae-400/25 bg-surface-900/95 p-3 shadow-2xl backdrop-blur">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className={`h-2 w-2 rounded-full ${running ? 'animate-pulse bg-actae-400' : completed ? 'bg-emerald-400' : 'bg-surface-500'}`} />
          <span className="text-[11px] font-semibold uppercase tracking-widest text-surface-300">Demo replay</span>
        </div>
        <span className="font-mono text-[10px] text-surface-500">{index}/{DEMO_SCENARIO.length}</span>
      </div>

      <p className="mt-1.5 text-[11px] leading-4 text-surface-500">
        {running
          ? 'Streaming a recorded dry-run into the live timeline…'
          : completed
            ? 'Replay complete — the fork inherited state at cursor 3.'
            : 'Deterministic replay of a recorded research run + fork. No backend.'}
      </p>

      <div className="mt-2 h-1 overflow-hidden rounded-full bg-white/10">
        <div className="h-full rounded-full bg-actae-400 transition-[width] duration-200" style={{ width: `${Math.round(progress * 100)}%` }} />
      </div>

      <div className="mt-3 flex items-center gap-2">
        {!running ? (
          <button
            type="button"
            onClick={() => run(completed ? 0 : index)}
            className="flex-1 rounded-md bg-actae-400 px-3 py-1.5 text-xs font-semibold text-black hover:bg-actae-300"
          >
            {started ? 'Resume replay' : 'Run demo'}
          </button>
        ) : (
          <button type="button" onClick={pause} className="flex-1 rounded-md bg-white/10 px-3 py-1.5 text-xs font-semibold text-white hover:bg-white/15">
            Pause
          </button>
        )}
        <button type="button" onClick={restart} className="rounded-md border border-white/10 px-3 py-1.5 text-xs font-medium text-surface-300 hover:text-white">
          Restart
        </button>
      </div>
    </div>
  )
}
