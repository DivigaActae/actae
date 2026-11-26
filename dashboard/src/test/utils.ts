// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { useFlowStore } from '@/lib/flow-store'

/**
 * Resets the global zustand flow store to its initial state.
 * Call in beforeEach() of any test that exercises the store.
 */
export function resetFlowStore() {
  useFlowStore.getState().stopChannelPolling()
  useFlowStore.setState({
    channels: [],
    activeChannel: null,
    events: [],
    stateSnapshot: null,
    loading: false,
    error: null,
    isLive: false,
    liveEvents: [],
    connection: { connected: false, authenticated: false, error: null },
    selectedNode: null,
    replayFromCursor: null,
    inspectorOpen: false,
    inspectorWidth: 320,
    visualizationMode: 'timeline',
    layoutMode: 'grid',
    forks: [],
    forkedAtCursor: null,
    canvasController: null,
    visibleEventTypes: null,
    visibleFields: null,
  })
}

/** Minimal fake user shared by auth-mocking tests. */
export const fakeUser = {
  id: 'u-001',
  email: 'admin@actae.local',
  name: 'Admin User',
}

/** Fake WebSocket used by useWebSocket tests. */
export class FakeWebSocket {
  static OPEN = 1
  static CLOSED = 3
  static CONNECTING = 0
  static instances: FakeWebSocket[] = []

  url: string
  readyState = FakeWebSocket.CONNECTING
  sent: string[] = []
  closeCount = 0
  onopen: ((ev?: any) => void) | null = null
  onmessage: ((ev: any) => void) | null = null
  onclose: ((ev?: any) => void) | null = null
  onerror: ((ev?: any) => void) | null = null

  constructor(url: string) {
    this.url = url
    FakeWebSocket.instances.push(this)
  }

  send(data: string) {
    this.sent.push(data)
  }

  close() {
    this.closeCount++
    this.readyState = FakeWebSocket.CLOSED
    this.onclose?.({})
  }

  // Test helpers
  open() {
    this.readyState = FakeWebSocket.OPEN
    this.onopen?.({})
  }

  serverMessage(raw: unknown) {
    this.onmessage?.({ data: JSON.stringify(raw) })
  }

  /** Deliver a raw string payload without JSON stringification (e.g. invalid JSON). */
  rawMessage(data: string) {
    this.onmessage?.({ data })
  }

  remoteClose() {
    this.readyState = FakeWebSocket.CLOSED
    this.onclose?.({})
  }

  remoteError() {
    this.onerror?.({})
  }

  static reset() {
    FakeWebSocket.instances = []
  }

  static last(): FakeWebSocket | undefined {
    return FakeWebSocket.instances[FakeWebSocket.instances.length - 1]
  }
}
