// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect, useState } from 'react'
import { useFlowStore } from '@/lib/flow-store'
import { getChannelMetadata, updateChannelMetadata, type ChannelMetadata } from '@/lib/api'
import JsonTree from '@/components/JsonTree'

export function MetadataEditor() {
  const { activeChannel, loadForks } = useFlowStore()
  const [meta, setMeta] = useState<ChannelMetadata | null>(null)
  const [displayName, setDisplayName] = useState('')
  const [reason, setReason] = useState('')
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<string | null>(null)

  useEffect(() => {
    if (!activeChannel) {
      setMeta(null)
      return
    }
    getChannelMetadata(activeChannel).then((res) => {
      const m = res.data ?? null
      setMeta(m)
      setDisplayName(m?.display_name || '')
      setReason(m?.reason || '')
    }).catch(() => setMeta(null))
  }, [activeChannel])

  if (!activeChannel || !meta) return null

  async function handleSave() {
    setSaving(true)
    setMessage(null)
    const { error } = await updateChannelMetadata({
      channel_id: activeChannel!,
      display_name: displayName || undefined,
      reason: reason || undefined,
    })
    if (error) {
      setMessage(error)
    } else {
      setMessage('Saved')
      await loadForks()
      setTimeout(() => setMessage(null), 2000)
    }
    setSaving(false)
  }

  return (
    <div className="p-3 border-b border-gray-800/50">
      <h3 className="text-[10px] font-semibold text-gray-400 uppercase tracking-widest mb-3">
        Experiment Metadata
      </h3>
      <div className="space-y-2.5">
        <div>
          <label className="text-[10px] text-gray-500 mb-1 block">Name</label>
          <input
            type="text"
            value={displayName}
            onChange={(e) => setDisplayName(e.target.value)}
            className="w-full bg-gray-800/50 border border-gray-700 rounded-lg px-2.5 py-1.5 text-xs text-gray-200 focus:outline-none focus:border-actae-500 transition-colors"
            placeholder="Experiment name..."
          />
        </div>
        <div>
          <label className="text-[10px] text-gray-500 mb-1 block">Reason</label>
          <textarea
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            className="w-full bg-gray-800/50 border border-gray-700 rounded-lg px-2.5 py-1.5 text-xs text-gray-200 focus:outline-none focus:border-actae-500 transition-colors resize-none"
            rows={2}
            placeholder="Why this experiment..."
          />
        </div>
        <button
          onClick={handleSave}
          disabled={saving}
          className="w-full px-3 py-1.5 text-xs font-medium bg-actae-600 hover:bg-actae-500 disabled:opacity-50 text-white rounded-lg transition-colors"
        >
          {saving ? 'Saving...' : 'Save'}
        </button>
        {message && (
          <div className={`text-xs ${message === 'Saved' ? 'text-emerald-400' : 'text-red-400'}`}>
            {message}
          </div>
        )}
        {meta.parent_channel_id && (
          <div className="text-[10px] text-gray-500">
            Forked at resolved cursor{' '}
            <span className="text-purple-400 font-mono">
              @{meta.resolved_cursor ?? meta.forked_at_cursor}
            </span>
            {meta.requested_at_cursor !== undefined && meta.requested_at_cursor !== 0 && (
              <> (requested @{meta.requested_at_cursor})</>
            )}
          </div>
        )}
        {meta.reproducibility && (
          <div className="flex items-center gap-1.5 text-[10px] text-gray-500">
            <span
              className={`px-1.5 py-0.5 rounded ${
                meta.reproducibility === 'context_exact'
                  ? 'bg-emerald-500/10 text-emerald-400'
                  : meta.reproducibility === 'state_exact' || meta.reproducibility === 'execution_replayable'
                    ? 'bg-sky-500/10 text-sky-400'
                    : 'bg-amber-500/10 text-amber-400'
              }`}
            >
              {meta.reproducibility}
            </span>
            <span>{meta.restorable ? 'restorable' : 'lineage-only'}</span>
          </div>
        )}
        {meta.manifest && Object.keys(meta.manifest).length > 0 && (
          <div>
            <div className="text-[10px] text-gray-500 mb-1 mt-1">Fork manifest (read-only)</div>
            <div className="bg-gray-900/50 rounded border border-gray-800/60 p-2">
              <JsonTree data={meta.manifest} depth={1} />
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
