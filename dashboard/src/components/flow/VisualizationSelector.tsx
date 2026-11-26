// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useFlowStore } from '@/lib/flow-store'

export default function VisualizationSelector() {
  const mode = useFlowStore(s => s.visualizationMode)
  const setMode = useFlowStore(s => s.setVisualizationMode)

  return (
    <div className="flow-view-switch" role="group" aria-label="Flow visualization">
      <button aria-pressed={mode === 'timeline'} onClick={() => setMode('timeline')} className={mode === 'timeline' ? 'active' : ''}>
        <TimelineIcon /> Timeline
      </button>
      <button aria-pressed={mode === 'graph'} onClick={() => setMode('graph')} className={mode === 'graph' ? 'active' : ''}>
        <GraphIcon /> Graph
      </button>
    </div>
  )
}

function TimelineIcon() { return <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3 2v12M3 4h3m-3 4h6m-6 4h9" /></svg> }
function GraphIcon() { return <svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="3" cy="4" r="1.5"/><circle cx="12.5" cy="3" r="1.5"/><circle cx="12.5" cy="12.5" r="1.5"/><path d="M4.5 4h3a3 3 0 0 1 3 3v4"/></svg> }
