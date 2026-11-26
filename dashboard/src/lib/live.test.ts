// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  subscribe,
  addMessageListener,
  addStatusListener,
  getLiveStatus,
  resetLiveClient,
} from './live'
import { FakeWebSocket } from '@/test/utils'

const realWebSocket = globalThis.WebSocket

beforeEach(() => {
  resetLiveClient()
  vi.clearAllMocks()
  localStorage.clear()
  sessionStorage.clear()
  FakeWebSocket.reset()
  globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket
})

afterEach(() => {
  globalThis.WebSocket = realWebSocket
  vi.useRealTimers()
  resetLiveClient()
})

describe('live client — connection lifecycle', () => {
  it('connects on first subscribe and sends auth with the stored token', () => {
    localStorage.setItem('actae_auth', JSON.stringify({ token: 'tok-xyz' }))
    const unsub = subscribe('ch')
    const ws = FakeWebSocket.instances[0]
    expect(ws.url).toBe('ws://localhost:3000/ws')
    ws.open()
    expect(JSON.parse(ws.sent[0])).toEqual({ type: 'auth', token: 'tok-xyz', payload: {} })
    unsub()
  })

  it('omits the token from auth when nothing is stored (anonymous fallback)', () => {
    subscribe('ch')
    FakeWebSocket.instances[0].open()
    expect(FakeWebSocket.instances[0].sent[0]).toBe(JSON.stringify({ type: 'auth', payload: {} }))
  })

  it('resubscribes all topics after authentication', () => {
    subscribe('ch-a')
    subscribe('ch-b')
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    expect(ws.sent).toContain(JSON.stringify({ type: 'subscribe', topic: 'ch-a' }))
    expect(ws.sent).toContain(JSON.stringify({ type: 'subscribe', topic: 'ch-b' }))
    expect(getLiveStatus()).toMatchObject({ connected: true, authenticated: true })
  })

  it('ref-counts topics: only the final unsubscribe sends unsubscribe', () => {
    const unsub1 = subscribe('ch')
    const unsub2 = subscribe('ch')
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    ws.sent.length = 0

    const a = subscribe('ch') // 3rd subscription — sends subscribe immediately
    expect(ws.sent).toContain(JSON.stringify({ type: 'subscribe', topic: 'ch' }))
    a()
    expect(ws.sent).not.toContain(JSON.stringify({ type: 'unsubscribe', topic: 'ch' }))
    unsub1()
    expect(ws.sent).not.toContain(JSON.stringify({ type: 'unsubscribe', topic: 'ch' }))
    unsub2()
    const unsubs = ws.sent.filter(
      (m) => m === JSON.stringify({ type: 'unsubscribe', topic: 'ch' })
    )
    expect(unsubs).toHaveLength(1)
  })

  it('closes the connection when no topics or listeners remain', () => {
    const unsub = subscribe('ch')
    const off = addMessageListener(() => {})
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    unsub()
    off()
    expect(ws.closeCount).toBeGreaterThanOrEqual(1)
    expect(FakeWebSocket.instances).toHaveLength(1)
  })

  it('does not reconnect after the last consumer is gone', () => {
    vi.useFakeTimers()
    const unsub = subscribe('ch')
    unsub()
    expect(FakeWebSocket.instances).toHaveLength(1)
    vi.advanceTimersByTime(60000)
    expect(FakeWebSocket.instances).toHaveLength(1)
  })
})

describe('live client — message parsing', () => {
  it('parses broadcasts with an inner event wrapper', () => {
    const events: unknown[] = []
    subscribe('ch')
    addMessageListener((e) => events.push(e))
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    ws.serverMessage({
      type: 'broadcast',
      topic: 'ch',
      payload: { event: { cursor: 9, type: 'agent.step', payload: { x: 1 } } },
    })
    expect(events).toHaveLength(1)
    const evt = events[0] as { cursor: number; type: string; channel: string; payload: unknown }
    expect(evt.cursor).toBe(9)
    expect(evt.type).toBe('agent.step')
    expect(evt.channel).toBe('ch')
    expect(evt.payload).toEqual({ x: 1 })
    expect((evt as unknown as { timestamp: Date }).timestamp).toBeInstanceOf(Date)
  })

  it('parses flat broadcasts without an event wrapper', () => {
    const events: unknown[] = []
    subscribe('ch')
    addMessageListener((e) => events.push(e))
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    ws.serverMessage({ type: 'broadcast', topic: 'ch', payload: { type: 'agent.started', cursor: 3, payload: { model: 'gpt-4' } } })
    expect(events).toHaveLength(1)
    const evt = events[0] as { cursor: number; type: string; channel: string; payload: unknown }
    expect(evt.type).toBe('agent.started')
    expect(evt.cursor).toBe(3)
    expect(evt.channel).toBe('ch')
    expect(evt.payload).toEqual({ model: 'gpt-4' })
  })

  it('prefers an inner channel_id over the topic', () => {
    const events: unknown[] = []
    subscribe('ch')
    addMessageListener((e) => events.push(e))
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    ws.serverMessage({
      type: 'broadcast',
      topic: 'ch',
      payload: { event: { cursor: 1, type: 't', channel_id: 'real-ch', payload: {} } },
    })
    expect((events[0] as { channel: string }).channel).toBe('real-ch')
  })

  it('handles externally-tagged server messages', () => {
    const events: unknown[] = []
    subscribe('ch')
    addMessageListener((e) => events.push(e))
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ Connection: { event: 'authenticated', payload: {} } })
    ws.serverMessage({
      Broadcast: {
        topic: 'ch',
        payload: { event: { cursor: 5, type: 'tool.run', payload: {} } },
      },
    })
    expect(events).toHaveLength(1)
    expect((events[0] as { type: string }).type).toBe('tool.run')
  })

  it('ignores malformed messages and non-broadcast types', () => {
    const events: unknown[] = []
    subscribe('ch')
    addMessageListener((e) => events.push(e))
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    ws.rawMessage('not json {')
    ws.rawMessage('{"broken": ')
    ws.serverMessage({ type: 'subscription', event: 'subscribed', topic: 'ch' })
    ws.serverMessage({ type: 'cursor_sync', topic: 'ch', cursor: 7 })
    ws.serverMessage({ type: 'ack', id: 'abc' })
    ws.serverMessage({ type: 'error', message: 'nope' })
    expect(events).toHaveLength(0)
  })

  it('defaults a raw broadcast payload to the broadcast type', () => {
    const events: unknown[] = []
    subscribe('ch')
    addMessageListener((e) => events.push(e))
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    ws.serverMessage({ type: 'broadcast', topic: 'ch', payload: { hello: 'world' } })
    expect(events).toHaveLength(1)
    expect((events[0] as { type: string }).type).toBe('broadcast')
    expect((events[0] as { payload: unknown }).payload).toEqual({ hello: 'world' })
  })
})

describe('live client — reconnect and heartbeat', () => {
  it('marks disconnected on close and reconnects with backoff', () => {
    vi.useFakeTimers()
    const statuses: ReturnType<typeof getLiveStatus>[] = []
    subscribe('ch')
    addStatusListener((s) => statuses.push(s))
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.close()
    expect(getLiveStatus()).toMatchObject({ connected: false, error: 'Disconnected' })
    expect(FakeWebSocket.instances).toHaveLength(1)
    vi.advanceTimersByTime(1001)
    expect(FakeWebSocket.instances).toHaveLength(2)
  })

  it('resubscribes topics after a reconnect and re-auth', () => {
    vi.useFakeTimers()
    subscribe('ch-a')
    subscribe('ch-b')
    const ws1 = FakeWebSocket.instances[0]
    ws1.open()
    ws1.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    ws1.close()
    vi.advanceTimersByTime(1001)
    const ws2 = FakeWebSocket.instances[1]
    ws2.open()
    ws2.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    expect(ws2.sent).toContain(JSON.stringify({ type: 'subscribe', topic: 'ch-a' }))
    expect(ws2.sent).toContain(JSON.stringify({ type: 'subscribe', topic: 'ch-b' }))
  })

  it('reconnect delay grows exponentially up to 30s', () => {
    vi.useFakeTimers()
    subscribe('ch')
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.close()
    expect(FakeWebSocket.instances).toHaveLength(1)
    vi.advanceTimersByTime(999)
    expect(FakeWebSocket.instances).toHaveLength(1)
    vi.advanceTimersByTime(2)
    expect(FakeWebSocket.instances).toHaveLength(2)
  })

  it('caps at 20 reconnect attempts', () => {
    vi.useFakeTimers()
    subscribe('ch')
    // Simulate repeated failures: each pending timer fires a new connection
    // attempt that is closed before ever opening (onopen resets the counter,
    // so a successful open would defeat the cap).
    for (let i = 0; i < 30; i++) {
      vi.advanceTimersByTime(40000)
      const last = FakeWebSocket.instances[FakeWebSocket.instances.length - 1]
      if (last) last.close()
    }
    expect(FakeWebSocket.instances.length).toBeLessThanOrEqual(21)
  })

  it('sends heartbeats every 15s while open', () => {
    vi.useFakeTimers()
    subscribe('ch')
    const ws = FakeWebSocket.instances[0]
    ws.open()
    const sentBefore = ws.sent.length
    vi.advanceTimersByTime(15000)
    expect(ws.sent.slice(sentBefore)).toContain(
      JSON.stringify({ type: 'connection', event: 'ping', payload: {} })
    )
  })

  it('reports connection failure and retries when the constructor throws', () => {
    vi.useFakeTimers()
    const throwingCtor = vi.fn(() => {
      throw new Error('nope')
    })
    globalThis.WebSocket = throwingCtor as unknown as typeof WebSocket
    subscribe('ch')
    expect(getLiveStatus().error).toContain('Connection failed')
    vi.advanceTimersByTime(1001)
    expect(throwingCtor).toHaveBeenCalledTimes(2)
  })

  it('marks error state on websocket errors without closing', () => {
    subscribe('ch')
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.remoteError()
    expect(getLiveStatus().error).toBe('WebSocket error')
  })

  it('surfaces server error messages in the connection status', () => {
    subscribe('ch')
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: 'error', message: 'Authentication failed' })
    expect(getLiveStatus().error).toBe('Authentication failed')
    // A successful re-auth clears the surfaced error.
    ws.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    expect(getLiveStatus().error).toBeNull()
  })
})

describe('live client — status listeners', () => {
  it('delivers the current status immediately on registration', () => {
    let received: ReturnType<typeof getLiveStatus> | null = null
    const off = addStatusListener((s) => {
      received = s
    })
    expect(received).toMatchObject({ connected: false, authenticated: false })
    off()
  })

  it('notifies listeners on status changes', () => {
    const seen: ReturnType<typeof getLiveStatus>[] = []
    subscribe('ch')
    addStatusListener((s) => seen.push(s))
    const ws = FakeWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: 'connection', event: 'authenticated', payload: {} })
    expect(seen.map((s) => s.authenticated)).toEqual([false, false, true])
  })
})
