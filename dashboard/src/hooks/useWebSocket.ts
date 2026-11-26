// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useEffect } from 'react'
import { useFlowStore } from '@/lib/flow-store'
import { subscribe, addMessageListener, addStatusListener } from '@/lib/live'

/**
 * Thin wrapper over the shared live client (`src/lib/live.ts`): subscribes
 * the active channel while live mode is on, forwards parsed live events into
 * the flow store (canvas merge + live feed), and mirrors the shared
 * connection status into the store. Connection management, reconnects and
 * heartbeats are the live client's job.
 */
export function useWebSocket(channel: string | null, isLive: boolean) {
  const addLiveEvent = useFlowStore(s => s.addLiveEvent)
  const setConnectionStatus = useFlowStore(s => s.setConnectionStatus)

  useEffect(() => {
    if (!isLive || !channel) return
    const unsubscribe = subscribe(channel)
    const offStatus = addStatusListener(setConnectionStatus)
    const offMessage = addMessageListener(event => {
      addLiveEvent({
        id: event.id,
        cursor: event.cursor,
        type: event.type,
        channel: event.channel || channel,
        timestamp: event.timestamp,
        payload: event.payload,
        isState: false,
      })
    })
    return () => {
      unsubscribe()
      offStatus()
      offMessage()
    }
  }, [channel, isLive, addLiveEvent, setConnectionStatus])
}
