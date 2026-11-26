// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useWebSocket } from './useWebSocket'
import { useFlowStore } from '@/lib/flow-store'
import { resetFlowStore } from '@/test/utils'

const live = vi.hoisted(() => ({
  subscribe: vi.fn(),
  addMessageListener: vi.fn(),
  addStatusListener: vi.fn(),
}))

vi.mock('@/lib/live', () => live)

let messageCb: ((e: any) => void) | null = null
let statusCb: ((s: any) => void) | null = null
const subscribeCalls: string[] = []
const unsubscribers: (() => void)[] = []

beforeEach(() => {
  resetFlowStore()
  vi.clearAllMocks()
  messageCb = null
  statusCb = null
  subscribeCalls.length = 0
  unsubscribers.length = 0
  live.subscribe.mockImplementation((topic: string) => {
    subscribeCalls.push(topic)
    const unsub = vi.fn()
    unsubscribers.push(unsub)
    return unsub
  })
  live.addMessageListener.mockImplementation((cb: (e: any) => void) => {
    messageCb = cb
    return vi.fn()
  })
  live.addStatusListener.mockImplementation((cb: (s: any) => void) => {
    statusCb = cb
    return vi.fn()
  })
})

describe('useWebSocket — subscription lifecycle', () => {
  it('does not subscribe when live is disabled', () => {
    renderHook(() => useWebSocket('ch', false))
    expect(subscribeCalls).toHaveLength(0)
  })

  it('does not subscribe without a channel', () => {
    renderHook(() => useWebSocket(null, true))
    expect(subscribeCalls).toHaveLength(0)
  })

  it('subscribes to the channel topic when live', () => {
    renderHook(() => useWebSocket('ch', true))
    expect(subscribeCalls).toEqual(['ch'])
  })

  it('unsubscribes when live is turned off', () => {
    const { rerender } = renderHook(({ ch, liveFlag }) => useWebSocket(ch, liveFlag), {
      initialProps: { ch: 'ch', liveFlag: true },
    })
    rerender({ ch: 'ch', liveFlag: false })
    expect(unsubscribers).toHaveLength(1)
    expect(unsubscribers[0]).toHaveBeenCalled()
  })

  it('unsubscribes on unmount', () => {
    const { unmount } = renderHook(() => useWebSocket('ch', true))
    unmount()
    expect(unsubscribers[0]).toHaveBeenCalled()
  })

  it('switching channels unsubscribes the old topic and subscribes the new one', () => {
    const { rerender } = renderHook(({ ch }) => useWebSocket(ch, true), {
      initialProps: { ch: 'ch1' },
    })
    rerender({ ch: 'ch2' })
    expect(unsubscribers[0]).toHaveBeenCalled()
    expect(subscribeCalls).toEqual(['ch1', 'ch2'])
  })
})

describe('useWebSocket — event and status forwarding', () => {
  it('forwards parsed live events into the store', () => {
    useFlowStore.getState().setIsLive(true)
    renderHook(() => useWebSocket('ch', true))
    act(() => {
      messageCb!({
        id: 'e1',
        cursor: 9,
        type: 'agent.step',
        channel: 'ch',
        timestamp: new Date('2026-07-25T09:00:00Z'),
        payload: { x: 1 },
      })
    })
    const live = useFlowStore.getState().liveEvents
    expect(live).toHaveLength(1)
    expect(live[0].cursor).toBe(9)
    expect(live[0].type).toBe('agent.step')
    expect(live[0].channel).toBe('ch')
    expect(live[0].payload).toEqual({ x: 1 })
  })

  it('falls back to the hook channel when the event has none', () => {
    useFlowStore.getState().setIsLive(true)
    renderHook(() => useWebSocket('ch', true))
    act(() => {
      messageCb!({ cursor: 1, type: 't', channel: '', timestamp: new Date(), payload: {} })
    })
    expect(useFlowStore.getState().liveEvents[0].channel).toBe('ch')
  })

  it('mirrors the shared connection status into the store', () => {
    renderHook(() => useWebSocket('ch', true))
    act(() => {
      statusCb!({ connected: true, authenticated: true, error: null })
    })
    expect(useFlowStore.getState().connection).toMatchObject({
      connected: true,
      authenticated: true,
      error: null,
    })
    act(() => {
      statusCb!({ connected: false, authenticated: false, error: 'Disconnected' })
    })
    expect(useFlowStore.getState().connection).toMatchObject({
      connected: false,
      authenticated: false,
      error: 'Disconnected',
    })
  })
})
