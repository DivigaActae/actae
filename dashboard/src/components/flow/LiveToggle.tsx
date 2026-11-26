// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { useFlowStore } from '@/lib/flow-store'
import { useWebSocket } from '@/hooks/useWebSocket'

export default function LiveToggle() {
  const activeChannel = useFlowStore(s => s.activeChannel)
  const isLive = useFlowStore(s => s.isLive)
  const setIsLive = useFlowStore(s => s.setIsLive)
  const connection = useFlowStore(s => s.connection)
  const isAuthenticated = isLive && connection.authenticated
  // A pending WebSocket constructor is not evidence of a live connection.
  // Only show "Connecting" once the transport is actually open and waiting
  // for the server's authentication acknowledgement.
  const isConnecting = isLive && connection.connected && !connection.authenticated
  const isOffline = isLive && !connection.authenticated && !connection.connected

  useWebSocket(activeChannel, isLive)

  return (
    <button
      onClick={() => setIsLive(!isLive)}
      disabled={!activeChannel}
      className={`flex items-center gap-2 px-3 py-1.5 rounded text-sm font-medium transition-colors ${
        isAuthenticated
          ? 'bg-emerald-950 text-emerald-400 border border-emerald-800'
          : isLive
            ? 'bg-amber-950/60 text-amber-300 border border-amber-800'
          : 'bg-gray-800 text-gray-400 border border-gray-700 hover:bg-gray-700'
      } disabled:opacity-50`}
    >
      <span className={`w-2 h-2 rounded-full ${isAuthenticated ? 'bg-green-500 animate-pulse' : isConnecting ? 'bg-amber-400 animate-pulse' : isOffline ? 'bg-red-400' : 'bg-gray-400'}`} />
      <span className="text-[10px] uppercase font-semibold">
        {isAuthenticated ? 'Live' : isConnecting ? 'Connecting' : isOffline ? 'Offline' : 'Historical'}
      </span>
      {isConnecting && (
        <span className="text-[10px] text-gray-400">connecting...</span>
      )}
      {isOffline && connection.error && (
        <span className="text-[10px] text-red-300">{connection.error}</span>
      )}
    </button>
  )
}
