// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

/**
 * Shared WebSocket live-event client.
 *
 * A single connection is shared by every consumer in the dashboard (Flow
 * page, Events page). Consumers subscribe to topics and register message
 * listeners; the client keeps the connection alive while at least one
 * consumer is active, reconnects with exponential backoff, re-subscribes
 * all topics after a reconnect, and sends heartbeats while open.
 *
 * The server wire format mirrors the Python SDK (see
 * `_normalize_msg`/`_dispatch` in `sdks/python/src/actae_client/client.py`):
 * messages may arrive flat (`{"type": "broadcast", ...}`) or externally
 * tagged (`{"Broadcast": {...}}`); broadcast payloads carry the event either
 * wrapped in `payload.event` or directly.
 */

import { getWsUrl } from './api'

export interface LiveEvent {
  id?: string
  cursor: number
  type: string
  channel: string
  timestamp: Date
  payload: Record<string, unknown>
}

export interface LiveStatus {
  connected: boolean
  authenticated: boolean
  error: string | null
}

type MessageListener = (event: LiveEvent) => void
type StatusListener = (status: LiveStatus) => void

const MAX_RECONNECT_ATTEMPTS = 20
const HEARTBEAT_MS = 15000

const EXTERNAL_TAG_TYPES: Record<string, string> = {
  Connection: 'connection',
  Broadcast: 'broadcast',
  Subscription: 'subscription',
  Ack: 'ack',
  CursorSync: 'cursor_sync',
  Error: 'error',
  Nack: 'nack',
}

interface ParsedMessage {
  type?: string
  event?: string
  topic?: string
  channel?: string
  channel_id?: string
  cursor?: number
  payload?: unknown
  [key: string]: unknown
}

let ws: WebSocket | null = null
let reconnectAttempts = 0
let reconnectTimer: ReturnType<typeof setTimeout> | null = null
let heartbeatTimer: ReturnType<typeof setInterval> | null = null

const topicCounts = new Map<string, number>()
const messageListeners = new Set<MessageListener>()
const statusListeners = new Set<StatusListener>()

let status: LiveStatus = { connected: false, authenticated: false, error: null }

// ── Public API ────────────────────────────────────────────────────────────

export function getLiveStatus(): LiveStatus {
  return status
}

/** Subscribe to a topic. Returns an unsubscribe function (ref-counted). */
export function subscribe(topic: string): () => void {
  topicCounts.set(topic, (topicCounts.get(topic) ?? 0) + 1)
  ensureConnection()
  sendSubscribe(topic)
  return () => {
    const count = (topicCounts.get(topic) ?? 0) - 1
    if (count <= 0) {
      topicCounts.delete(topic)
      sendUnsubscribe(topic)
    } else {
      topicCounts.set(topic, count)
    }
    maybeClose()
  }
}

/** Register a listener for parsed live events. Returns an unsubscribe fn. */
export function addMessageListener(listener: MessageListener): () => void {
  messageListeners.add(listener)
  ensureConnection()
  return () => {
    messageListeners.delete(listener)
    maybeClose()
  }
}

/** Register a listener for connection-status changes. Returns an unsubscribe fn. */
export function addStatusListener(listener: StatusListener): () => void {
  statusListeners.add(listener)
  listener(status)
  ensureConnection()
  return () => {
    statusListeners.delete(listener)
    maybeClose()
  }
}

/**
 * Tear down all state. Used by tests (and harmless elsewhere): closes the
 * socket, clears timers, listeners and topic subscriptions.
 */
export function resetLiveClient(): void {
  teardown()
  topicCounts.clear()
  messageListeners.clear()
  statusListeners.clear()
  status = { connected: false, authenticated: false, error: null }
}

// ── Connection lifecycle ──────────────────────────────────────────────────

function ensureConnection(): void {
  if (ws || reconnectTimer) return
  connect()
}

function connect(): void {
  try {
    const socket = new WebSocket(getWsUrl())
    ws = socket
    socket.onopen = () => {
      reconnectAttempts = 0
      notifyStatus({ connected: true, error: null })
      // The server validates a JWT in `token` and an API key in `api_key`
      // (trying JWT first). The dashboard stores the session JWT from
      // api-key-login; sending it as `api_key` would silently downgrade the
      // connection to anonymous (dev) or fail it outright (prod), because an
      // API-key lookup cannot resolve a JWT. Omit the field when no token is
      // stored so anonymous fallback still works for public instances.
      const authMsg: Record<string, unknown> = { type: 'auth', payload: {} }
      const token = getAuthToken()
      if (token) authMsg.token = token
      socket.send(JSON.stringify(authMsg))
      startHeartbeat()
    }
    socket.onmessage = (event) => handleMessage(event.data)
    socket.onclose = () => handleDisconnect('Disconnected')
    socket.onerror = () => notifyStatus({ error: 'WebSocket error' })
  } catch (e) {
    ws = null
    notifyStatus({ connected: false, authenticated: false, error: `Connection failed: ${e}` })
    scheduleReconnect()
  }
}

function handleDisconnect(reason: string): void {
  if (ws) {
    ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null
    ws = null
  }
  stopHeartbeat()
  notifyStatus({ connected: false, authenticated: false, error: reason })
  scheduleReconnect()
}

function scheduleReconnect(): void {
  if (reconnectTimer || reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) return
  const delay = Math.min(1000 * Math.pow(1.5, reconnectAttempts), 30000)
  reconnectAttempts += 1
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null
    if (topicCounts.size === 0 && messageListeners.size === 0 && statusListeners.size === 0) {
      teardown()
      return
    }
    connect()
  }, delay)
}

function teardown(): void {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer)
    reconnectTimer = null
  }
  stopHeartbeat()
  if (ws) {
    ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null
    ws.close()
    ws = null
  }
  reconnectAttempts = 0
  notifyStatus({ connected: false, authenticated: false, error: null })
}

function maybeClose(): void {
  if (topicCounts.size === 0 && messageListeners.size === 0 && statusListeners.size === 0) {
    teardown()
  }
}

function startHeartbeat(): void {
  stopHeartbeat()
  heartbeatTimer = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'connection', event: 'ping', payload: {} }))
    }
  }, HEARTBEAT_MS)
}

function stopHeartbeat(): void {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer)
    heartbeatTimer = null
  }
}

// ── Messaging ─────────────────────────────────────────────────────────────

function notifyStatus(partial: Partial<LiveStatus>): void {
  status = { ...status, ...partial }
  Array.from(statusListeners).forEach((listener) => {
    try {
      listener(status)
    } catch {
      // A consumer's status handler must never break the shared connection.
    }
  })
}

function sendSubscribe(topic: string): void {
  if (ws && ws.readyState === WebSocket.OPEN && status.authenticated) {
    ws.send(JSON.stringify({ type: 'subscribe', topic }))
  }
}

function sendUnsubscribe(topic: string): void {
  if (ws && ws.readyState === WebSocket.OPEN && status.authenticated) {
    ws.send(JSON.stringify({ type: 'unsubscribe', topic }))
  }
}

function resubscribeAll(): void {
  Array.from(topicCounts.keys()).forEach((topic) => {
    sendSubscribe(topic)
  })
}

/** Normalize externally-tagged server messages to the flat `type` form. */
function normalizeMsg(data: unknown): ParsedMessage {
  if (data && typeof data === 'object') {
    for (const [tag, type] of Object.entries(EXTERNAL_TAG_TYPES)) {
      const inner = (data as Record<string, unknown>)[tag]
      if (inner && typeof inner === 'object') {
        return { ...(inner as Record<string, unknown>), type }
      }
    }
  }
  return (data ?? {}) as ParsedMessage
}

function handleMessage(raw: string): void {
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return
  }
  const msg = normalizeMsg(parsed)

  if (msg.type === 'connection') {
    if (msg.event === 'authenticated') {
      reconnectAttempts = 0
      notifyStatus({ connected: true, authenticated: true, error: null })
      resubscribeAll()
    }
    return
  }

  if (msg.type === 'broadcast') {
    const event = parseBroadcast(msg)
    if (!event) return
    Array.from(messageListeners).forEach((listener) => {
      try {
        listener(event)
      } catch {
        // A consumer's handler must never break the shared connection.
      }
    })
  }
  if (msg.type === 'error') {
    // The server surfaces auth failures, subscription denials and protocol
    // errors as error messages. Surface them in the connection status so the
    // LiveToggle/ExecutionTimeline labels are truthful instead of silently
    // showing "Connecting"/"Live" while nothing flows.
    const message = typeof msg.message === 'string' ? msg.message : 'WebSocket error'
    notifyStatus({ error: message })
    return
  }

  // subscription confirmations, cursor_sync, ack, nack: not actionable
  // for the dashboard UI — ignored.
}

function parseBroadcast(msg: ParsedMessage): LiveEvent | null {
  const payloadObj: Record<string, unknown> =
    msg.payload && typeof msg.payload === 'object'
      ? (msg.payload as Record<string, unknown>)
      : {}

  const hasInnerEvent =
    !!payloadObj.event && typeof payloadObj.event === 'object'
  const inner = (hasInnerEvent ? payloadObj.event : payloadObj) as Record<string, unknown>

  const type =
    (typeof inner.type === 'string' && inner.type) ||
    (typeof inner.event_type === 'string' && inner.event_type) ||
    (typeof msg.type === 'string' && msg.type !== 'broadcast' ? msg.type : 'broadcast')

  const cursor =
    typeof inner.cursor === 'number'
      ? inner.cursor
      : typeof msg.cursor === 'number'
        ? msg.cursor
        : 0

  const channel = String(
    inner.channel_id || inner.channel || msg.channel_id || msg.channel || msg.topic || ''
  )

  const eventPayload =
    inner.payload && typeof inner.payload === 'object'
      ? (inner.payload as Record<string, unknown>)
      : hasInnerEvent
        ? {}
        : inner

  return {
    id: typeof inner.id === 'string' ? inner.id : undefined,
    cursor,
    type,
    channel,
    timestamp: new Date(),
    payload: eventPayload,
  }
}

function getAuthToken(): string {
  const stored = localStorage.getItem('actae_auth') || sessionStorage.getItem('actae_auth')
  if (!stored) return ''
  try {
    return (JSON.parse(stored) as { token?: string }).token ?? ''
  } catch {
    return ''
  }
}
