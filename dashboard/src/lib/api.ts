// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { resolveAncestry } from './flow-helpers'
import { filterPayload } from './payload-fields'
import type { DashboardEvent } from './flow-store'

export const API_BASE = process.env.NEXT_PUBLIC_API_BASE || ''

const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_MODE === 'true'

function getToken(): string | null {
  const stored = localStorage.getItem('actae_auth') || sessionStorage.getItem('actae_auth')
  if (!stored) return null
  try {
    return JSON.parse(stored).token
  } catch {
    return null
  }
}

function clearAuth() {
  localStorage.removeItem('actae_auth')
  sessionStorage.removeItem('actae_auth')
}

async function api<T = any>(
  path: string,
  options: RequestInit = {}
): Promise<{ data?: T; error?: string; status: number }> {
  if (MOCK_MODE) {
    return { status: 200, data: {} as T }
  }
  const token = getToken()
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string>),
  }
  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  }

  try {
    const res = await fetch(`${API_BASE}${path}`, {
      ...options,
      headers,
    })

    if (res.status === 401 && typeof window !== 'undefined' && !window.location.pathname.startsWith('/login')) {
      clearAuth()
      window.location.href = '/login/'
      return { status: 401, error: 'Unauthorized' }
    }

    const data = await res.json()
    if (!res.ok) {
      return { status: res.status, error: data.error || `Request failed: ${res.status}` }
    }
    return { data, status: res.status }
  } catch (e: any) {
    return { status: 0, error: e.message || 'Network error' }
  }
}

export function getWsUrl(): string {
  if (API_BASE) {
    // Dev topology: the dashboard runs on a different origin than Actae
    // (e.g. NEXT_PUBLIC_API_BASE=http://localhost:8002 with `npm run dev`
    // on :3000). The WebSocket must follow the API base, not the page
    // origin — the Next dev server cannot upgrade WS connections.
    const clean = API_BASE.trim().replace(/\/+$/, '')
    const match = /^(https?):\/\/([^/?#]+)/.exec(clean)
    if (match) {
      const scheme = match[1] === 'https' ? 'wss' : 'ws'
      return `${scheme}://${match[2]}/ws`
    }
  }
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/ws`
}

export interface ExecutionGroupView { group_id:string; status:string; metadata:Record<string,unknown>|null; created_at:string; updated_at:string }
export interface ExecutionGroupMemberView { group_id:string; member_id:string; channel_id:string; role:string|null; owner_id:string|null; generation:number; lease_until:string|null }
export interface GroupMessageView { message_id:string; group_id:string; from_member_id:string; to_member_id:string; type:string; payload:unknown; source_event_id:string; delivery_event_id:string; status:string; created_at:string }
export interface GroupForkView { fork_group_id:string; source_group_id:string; intervention_member_id:string; intervention_cursor:number; member_policies:Record<string,string>; tool_policies:Record<string,string>; status:string; first_divergence_event_id:string|null; receipt:Record<string,unknown>; created_at:string; updated_at:string }

export function listExecutionGroups(){
  if (MOCK_MODE) return Promise.resolve({ data: { groups: [mockExecutionGroup()] }, status: 200, error: undefined })
  return api<{groups:ExecutionGroupView[]}>('/api/v1/execution-groups')
}
export function listExecutionGroupMembers(groupId:string){
  if (MOCK_MODE) return Promise.resolve({ data: { members: mockGroupMembers(groupId) }, status: 200, error: undefined })
  return api<{members:ExecutionGroupMemberView[]}>(`/api/v1/execution-groups/${encodeURIComponent(groupId)}/members`)
}
export function listExecutionGroupMessages(groupId:string,memberId:string){
  if (MOCK_MODE) return Promise.resolve({ data: { messages: mockGroupMessages(groupId, memberId) }, status: 200, error: undefined })
  return api<{messages:GroupMessageView[]}>(`/api/v1/execution-groups/${encodeURIComponent(groupId)}/members/${encodeURIComponent(memberId)}/messages?limit=500`)
}
export function getExecutionGroupFork(groupId:string){
  if (MOCK_MODE) return Promise.resolve({ status: 200, data: { fork: mockGroupFork(groupId) }, error: undefined })
  return api<{fork:GroupForkView}>(`/api/v1/execution-group-forks/${encodeURIComponent(groupId)}`)
}
export function promoteExecutionGroupMember(groupId:string,_memberId:string){
  if (MOCK_MODE) return Promise.resolve({ status: 200, data: { fork: mockGroupFork(groupId) }, error: undefined })
  return api<{fork:GroupForkView}>(`/api/v1/execution-group-forks/${encodeURIComponent(groupId)}/members/${encodeURIComponent(_memberId)}/promote`,{method:'POST',body:'{}'})
}
export function getCausalGraph(eventId:string,maxNodes=500){
  if (MOCK_MODE) return Promise.resolve({ data: mockCausalGraph(eventId, maxNodes), status: 200, error: undefined })
  return api(`/api/v1/events/${encodeURIComponent(eventId)}/causal-graph?max_nodes=${maxNodes}`)
}

// Auth API
export async function signup(email: string, password: string, name?: string) {
  return api('/api/v1/auth/signup', {
    method: 'POST',
    body: JSON.stringify({ email, password, name }),
  })
}

export async function login(email: string, password: string) {
  return api('/api/v1/auth/login', {
    method: 'POST',
    body: JSON.stringify({ email, password }),
  })
}

/** Exchange a control-plane SSO handoff token for a dashboard session. */
export async function ssoLogin(token: string) {
  return api('/api/v1/auth/sso', {
    method: 'POST',
    body: JSON.stringify({ token }),
  })
}

/** First-run: set the initial admin password with the one-time setup token. */
export async function setupAccount(token: string, password: string, name?: string) {
  return api('/api/v1/auth/setup', {
    method: 'POST',
    body: JSON.stringify({ token, password, name }),
  })
}

/** Server-side auth configuration (which login modes are available). */
export async function getAuthConfig(): Promise<{
  enable_signup: boolean
  sso?: boolean
  setup_required?: boolean
  local_auto_login?: boolean
} | null> {
  if (MOCK_MODE) return { enable_signup: false, sso: false, setup_required: false }
  try {
    const res = await fetch(`${API_BASE}/api/v1/auth/config`)
    if (!res.ok) return null
    return await res.json()
  } catch {
    return null
  }
}

/**
 * Loopback dashboard auto-login: a local self-hosted instance mints an admin
 * session with no credential. Returns the session or null when the instance is
 * not a local standalone one (enrolled/remote ⇒ 403).
 *
 * A direct fetch (not api()) so a refusal is a clean null, never a destructive
 * redirect.
 */
export async function localSession(): Promise<{ token: string; user: any } | null> {
  if (MOCK_MODE) return null
  try {
    const res = await fetch(`${API_BASE}/api/v1/auth/local-session`, { method: 'POST' })
    if (!res.ok) return null
    const data = await res.json()
    if (!data?.token || !data?.user) return null
    return { token: data.token, user: data.user }
  } catch {
    return null
  }
}

export async function getMe() {
  return api('/api/v1/auth/me')
}

export async function logout() {
  return api('/api/v1/auth/logout', { method: 'POST' })
}

/** Revoke every dashboard session for the signed-in user. */
export async function logoutAll() {
  return api('/api/v1/auth/logout-all', { method: 'POST' })
}

// Admin user management (standalone instances; requires an admin session).
export async function listUsers() {
  return api('/api/v1/users')
}

export async function createUser(payload: {
  email: string
  password: string
  name?: string
  role?: string
}) {
  return api('/api/v1/users', { method: 'POST', body: JSON.stringify(payload) })
}

export async function updateUser(id: string, payload: { name?: string; role?: string }) {
  return api(`/api/v1/users/${id}`, { method: 'PATCH', body: JSON.stringify(payload) })
}

export async function deleteUser(id: string) {
  return api(`/api/v1/users/${id}`, { method: 'DELETE' })
}

export async function resetUserPassword(id: string, password: string) {
  return api(`/api/v1/users/${id}/password`, {
    method: 'POST',
    body: JSON.stringify({ password }),
  })
}

// User preferences (per-identity opaque JSON object, shared between the
// dashboard's session JWT and HTTP SDK clients via X-API-Key).
export async function getPreferences() {
  return api('/api/v1/user/preferences')
}

export async function putPreferences(prefs: Record<string, unknown>) {
  return api('/api/v1/user/preferences', {
    method: 'PUT',
    body: JSON.stringify({ prefs }),
  })
}

// Channel export (JSON document for LLM investigation)
export interface ChannelExportEvent {
  id: string | null
  channel_id: string
  type: string
  cursor: number
  timestamp: string | null
  payload: unknown
  is_state?: boolean
  /** True when the event was inherited from an ancestor channel (fork view). */
  inherited?: boolean
}

export interface ChannelExport {
  schema: 'actae.channel_export.v1'
  exported_at: string
  channel_id: string
  display_name: string | null
  parent_channel_id: string | null
  origin_run_id: string | null
  forked_at_cursor: number
  forked_at: string | null
  fork_reason: string | null
  experiment_metadata: Record<string, unknown> | null
  /** Ancestor channel ids, immediate parent → root run. */
  ancestry: string[]
  /** The filters that shaped this export (null = no filter applied). */
  filters: {
    event_types: string[] | null
    payload_fields: string[] | null
  }
  event_count: number
  events: ChannelExportEvent[]
}

export interface ChannelExportOptions {
  /** The exact timeline the canvas renders (the store's `events`). Only
   * these events are exported. */
  events?: DashboardEvent[]
  /** Node-type filter, applied exactly like the canvas does (state
   * snapshots are grouped under the pseudo-type "state"); null = all. */
  visibleEventTypes?: ReadonlySet<string> | null
  /** Payload-field filter, applied exactly like the canvas does; null = all. */
  visibleFields?: ReadonlySet<string> | null
}

// Mirrors `nodeTypeKey` in flow-layout.ts — must stay in sync with it.
function exportTypeKey(e: { isState?: boolean; type: string }): string {
  return e.isState ? 'state' : e.type || 'unknown'
}

function serializeExportEvent(
  e: DashboardEvent,
  visibleFields: ReadonlySet<string> | null
): ChannelExportEvent {
  const isState = e.isState === true
  return {
    id: e.id ?? null,
    channel_id: e.channel,
    type: e.type,
    cursor: e.cursor,
    timestamp: e.timestamp instanceof Date ? e.timestamp.toISOString() : (e.timestamp ?? null),
    payload: filterPayload(isState ? (e.state ?? e.payload) : e.payload, visibleFields),
    is_state: isState || undefined,
    inherited: e.isInherited || undefined,
  }
}

/**
 * Build an export document that mirrors the Flow page's current filtered
 * view: events are taken from the provided timeline (the store's `events` —
 * the exact array the canvas renders), then the same filters the canvas
 * applies are applied: the node-type filter (`visibleEventTypes`, state
 * snapshots grouped under "state") and the payload-field filter
 * (`visibleFields`). The header records which filters were applied, so an
 * LLM knows the export is a filtered view. The result is always valid JSON.
 */
export async function buildChannelExport(
  channelId: string,
  options: ChannelExportOptions = {}
): Promise<{ data?: ChannelExport; error?: string; status: number }> {
  const { events = [], visibleEventTypes = null, visibleFields = null } = options
  try {
    const metaRes = await getChannelMetadata(channelId)
    if (metaRes.status >= 400 || metaRes.error) {
      throw new Error(metaRes.error || `Channel lookup failed with status ${metaRes.status}`)
    }
    const meta = metaRes.data ?? null
    const ancestry = await resolveAncestry(channelId, getChannelMetadata)

    const filtered = visibleEventTypes
      ? events.filter(e => visibleEventTypes.has(exportTypeKey(e)))
      : [...events]

    const data: ChannelExport = {
      schema: 'actae.channel_export.v1',
      exported_at: new Date().toISOString(),
      channel_id: channelId,
      display_name: meta?.display_name ?? null,
      parent_channel_id: meta?.parent_channel_id ?? null,
      origin_run_id: meta?.origin_run_id ?? null,
      forked_at_cursor: meta?.forked_at_cursor ?? 0,
      forked_at: meta?.forked_at ?? null,
      fork_reason: meta?.reason ?? null,
      experiment_metadata: meta?.experiment_metadata ?? null,
      ancestry: ancestry.map(a => a.channelId),
      filters: {
        event_types: visibleEventTypes ? Array.from(visibleEventTypes).sort() : null,
        payload_fields: visibleFields ? Array.from(visibleFields).sort() : null,
      },
      event_count: filtered.length,
      events: filtered.map(e => serializeExportEvent(e, visibleFields)),
    }
    return { status: 200, data }
  } catch (e: any) {
    return { status: 0, error: e.message || 'Failed to export channel' }
  }
}

/** Trigger a browser download of `data` serialized as pretty-printed JSON. */
export function downloadJsonFile(filename: string, data: unknown) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}

/** Copy text to the clipboard, falling back to the legacy execCommand path
 * when the async Clipboard API is unavailable (e.g. non-secure contexts). */
export async function copyToClipboard(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text)
      return
    } catch {
      // fall through to the legacy path (permission denied, etc.)
    }
  }
  const ta = document.createElement('textarea')
  ta.value = text
  ta.style.position = 'fixed'
  ta.style.opacity = '0'
  document.body.appendChild(ta)
  ta.select()
  const ok = document.execCommand('copy')
  ta.remove()
  if (!ok) throw new Error('Clipboard unavailable')
}

// System Metrics
export async function getMetrics() {
  return api<MetricsResponse>('/metrics.json')
}

// Events API
export async function queryEvents(params: { channel_ids?: string[]; event_type?: string; actor?: string; cursor_start?: number; cursor_end?: number; from?: string; to?: string; limit?: number; offset?: number }) {
  if (MOCK_MODE) {
    const { MOCK_EVENTS, MOCK_FORK_EVENTS, MOCK_APPROVAL_EVENTS } = await import('./mock-data')
    const all: Record<string, unknown>[] = []
    for (const [channel, events] of Object.entries(MOCK_EVENTS)) {
      for (const event of events as Record<string, unknown>[]) {
        if (event.isState) continue
        all.push({ ...event, channel_id: event.channel_id ?? channel })
      }
    }
    all.push(...MOCK_FORK_EVENTS, ...MOCK_APPROVAL_EVENTS)
    const filtered = all
      .filter(event => !params.event_type || event.event_type === params.event_type)
      .filter(event => !params.channel_ids || params.channel_ids.length === 0 || params.channel_ids.includes(event.channel_id as string))
    const offset = params.offset ?? 0
    const limit = params.limit ?? 100
    return { data: { events: filtered.slice(offset, offset + limit) }, status: 200, error: undefined }
  }
  return api('/api/v1/events/query', {
    method: 'POST',
    body: JSON.stringify(params),
  })
}

export async function recordEvent(
  channelId: string,
  eventType: string,
  payload: Record<string, unknown>,
  actor?: string
) {
  return api('/api/v1/events/record', {
    method: 'POST',
    body: JSON.stringify({
      channel_id: channelId,
      event_type: eventType,
      payload,
      metadata: { actor: actor || 'dashboard' },
    }),
  })
}

// Persisted wake-ups (durable timers)
export interface WakeupView {
  id: string
  channel_id: string
  run_at: string
  status: string
  payload: Record<string, unknown> | null
  created_at: string
  executed_at: string | null
  attempts: number
  error: string | null
}

export function listWakeups(params: { channel_id?: string; status?: string } = {}) {
  if (MOCK_MODE) {
    const pending: WakeupView[] = [{
      id: 'wk-apr-deploy-gate',
      channel_id: 'data-pipeline-orchestrator',
      run_at: '2026-07-26T04:02:00Z',
      status: 'pending',
      payload: { approval_request_id: 'apr-deploy-gate' },
      created_at: '2026-07-25T04:02:00Z',
      executed_at: null,
      attempts: 0,
      error: null,
    }]
    return Promise.resolve({ data: { wakeups: pending }, status: 200, error: undefined })
  }
  const query: string[] = []
  if (params.channel_id) query.push(`channel_id=${encodeURIComponent(params.channel_id)}`)
  if (params.status) query.push(`status=${encodeURIComponent(params.status)}`)
  const suffix = query.length ? `?${query.join('&')}` : ''
  return api<{ wakeups: WakeupView[] }>(`/api/v1/scheduler/wakeups${suffix}`)
}

export async function getLatestCursor(channelId: string) {
  return api(`/api/v1/events/cursor/${encodeURIComponent(channelId)}`)
}

export async function listChannels() {
  if (MOCK_MODE) {
    const { MOCK_CHANNELS, MOCK_CHANNEL_NAMES } = await import('./mock-data')
    const stats: Record<string, { display_name: string; event_count: number; latest_cursor: number; last_event_at: string }> = {}
    MOCK_CHANNELS.forEach((id, index) => {
      stats[id] = {
        display_name: MOCK_CHANNEL_NAMES[id] ?? id,
        event_count: 5 + (index % 6),
        latest_cursor: 5 + (index % 6),
        last_event_at: new Date(Date.now() - index * 3600_000).toISOString(),
      }
    })
    return { data: { channels: MOCK_CHANNELS, stats }, status: 200, error: undefined }
  }
  return api('/api/v1/channels')
}

export async function replayEvents(channelId: string, cursor?: number, limit?: number) {
  if (MOCK_MODE) {
    const { MOCK_EVENTS } = await import('./mock-data')
    const all = (MOCK_EVENTS[channelId] || []) as any[]
    const from = cursor ?? 0
    const pageLimit = limit ?? 100
    // Match backend semantics: cursor > fromCursor.
    const filtered = all.filter((e: any) => (e.cursor ?? 0) > from)
    const paginated = filtered.slice(0, pageLimit)
    return { data: { events: paginated }, status: 200 }
  }

  const params = new URLSearchParams()
  if (cursor !== undefined) {
    params.append('cursor', String(cursor))
  }
  if (limit !== undefined) {
    params.append('limit', String(limit))
  }
  const query = params.toString() ? `?${params.toString()}` : ''
  return api(`/api/v1/events/replay/${encodeURIComponent(channelId)}${query}`)
}

// State API
export async function saveState(channelId: string, cursor: number, state: any) {
  return api(`/api/v1/state/${encodeURIComponent(channelId)}`, {
    method: 'POST',
    body: JSON.stringify({ channel_id: channelId, cursor, state }),
  })
}

export async function loadState(channelId: string) {
  if (MOCK_MODE) {
    const { MOCK_STATES } = await import('./mock-data')
    const snap = (MOCK_STATES as any)[channelId]
    return {
      data: snap ? { cursor: snap.cursor, state: snap.state } : null,
      status: 200,
    }
  }
  return api(`/api/v1/state/${encodeURIComponent(channelId)}`)
}

// Fork API
export async function forkChannel(request: ForkRequest) {
  if (MOCK_MODE) return { data: { channel_id: request.new_channel_id, status: 'forked' }, status: 201 }
  return api(`/api/v1/channels/fork`, {
    method: 'POST',
    body: JSON.stringify(request),
  })
}

export async function getChannelMetadata(channelId: string) {
  if (MOCK_MODE) {
    const meta: Record<string, ChannelMetadata> = {
      'agent-research-run': { channel_id: 'agent-research-run', parent_channel_id: null, origin_run_id: 'agent-research-run', forked_at_cursor: 0, forked_at: '2026-07-25T09:00:00Z', display_name: 'Research Run', reason: 'Baseline GPT-4', experiment_metadata: { model: 'gpt-4' } },
      'agent-research-run-exp-a': { channel_id: 'agent-research-run-exp-a', parent_channel_id: 'agent-research-run', origin_run_id: 'agent-research-run', forked_at_cursor: 3, forked_at: '2026-07-25T10:00:00Z', display_name: 'Experiment A - GPT-5.5', reason: 'Testing GPT-5.5', experiment_metadata: { model: 'gpt-5.5-flash' } },
      'agent-research-run-exp-b': { channel_id: 'agent-research-run-exp-b', parent_channel_id: 'agent-research-run', origin_run_id: 'agent-research-run', forked_at_cursor: 3, forked_at: '2026-07-25T11:00:00Z', display_name: 'Experiment B - Claude', reason: 'Claude 3.5 comparison', experiment_metadata: { model: 'claude-3.5-sonnet' } },
      'agent-research-run-exp-a-sub': { channel_id: 'agent-research-run-exp-a-sub', parent_channel_id: 'agent-research-run-exp-a', origin_run_id: 'agent-research-run', forked_at_cursor: 2, forked_at: '2026-07-25T12:00:00Z', display_name: 'Sub-experiment A1', reason: 'Higher temperature', experiment_metadata: { temperature: 0.9 } },
    }
    const m = meta[channelId] || null
    return { data: m, status: m ? 200 : 404 }
  }
  return api<ChannelMetadata>(`/api/v1/channels/${encodeURIComponent(channelId)}/metadata`)
}

export async function listForks(channelId: string) {
  if (MOCK_MODE) {
    const forks: Record<string, ChannelMetadata[]> = {
      'agent-research-run': [
        { channel_id: 'agent-research-run-exp-a', parent_channel_id: 'agent-research-run', origin_run_id: 'agent-research-run', forked_at_cursor: 3, forked_at: '2026-07-25T10:00:00Z', display_name: 'Experiment A - GPT-5.5', reason: 'Testing GPT-5.5 prompt', experiment_metadata: { model: 'gpt-5.5-flash' } },
        { channel_id: 'agent-research-run-exp-b', parent_channel_id: 'agent-research-run', origin_run_id: 'agent-research-run', forked_at_cursor: 3, forked_at: '2026-07-25T11:00:00Z', display_name: 'Experiment B - Claude', reason: 'Claude 3.5 comparison', experiment_metadata: { model: 'claude-3.5-sonnet' } },
      ],
      'agent-research-run-exp-a': [
        { channel_id: 'agent-research-run-exp-a-sub', parent_channel_id: 'agent-research-run-exp-a', origin_run_id: 'agent-research-run', forked_at_cursor: 2, forked_at: '2026-07-25T12:00:00Z', display_name: 'Sub-experiment A1', reason: 'Higher temperature', experiment_metadata: { temperature: 0.9 } },
      ],
    }
    const b = forks[channelId] || []
    return { data: { forks: b }, status: 200 }
  }
  return api<{ forks: ChannelMetadata[] }>(`/api/v1/channels/${encodeURIComponent(channelId)}/forks`)
}

export async function getForkTree(rootChannelId: string) {
  if (MOCK_MODE) {
    const tree: ForkInfo = {
      channel_id: 'agent-research-run',
      display_name: 'Research Run',
      reason: 'Baseline GPT-4',
      forked_at_cursor: 0,
      event_count: 5,
      latest_cursor: 5,
      children: [
        {
          channel_id: 'agent-research-run-exp-a',
          display_name: 'Experiment A - GPT-5.5',
          reason: 'Testing GPT-5.5 prompt',
          forked_at_cursor: 3,
          event_count: 4,
          latest_cursor: 4,
          children: [
            {
              channel_id: 'agent-research-run-exp-a-sub',
              display_name: 'Sub-experiment A1',
              reason: 'Higher temperature',
              forked_at_cursor: 2,
              event_count: 2,
              latest_cursor: 2,
              children: [],
            },
          ],
        },
        {
          channel_id: 'agent-research-run-exp-b',
          display_name: 'Experiment B - Claude',
          reason: 'Claude 3.5 comparison',
          forked_at_cursor: 3,
          event_count: 4,
          latest_cursor: 4,
          children: [],
        },
      ],
    }
    return { data: tree, status: 200 }
  }
  return api<ForkInfo>(`/api/v1/channels/${encodeURIComponent(rootChannelId)}/tree`)
}

// Metadata update
export async function updateChannelMetadata(request: {
  channel_id: string
  display_name?: string
  reason?: string
  experiment_metadata?: Record<string, unknown>
}) {
  if (MOCK_MODE) return { data: { display_name: request.display_name, reason: request.reason, channel_id: request.channel_id }, status: 200, error: undefined }
  return api<ChannelMetadata>(`/api/v1/channels/${encodeURIComponent(request.channel_id)}/metadata`, {
    method: 'PUT',
    body: JSON.stringify(request),
  })
}

// Structural diff of two channels' latest saved states.
export async function diffChannelStates(leftChannelId: string, rightChannelId: string) {
  if (MOCK_MODE) return mockStateDiff()
  const qs = new URLSearchParams({ left: leftChannelId, right: rightChannelId })
  return api<StateDiff>(`/api/v1/channels/diff?${qs.toString()}`)
}

// Combined "what changed?" comparison: state diff + manifest diff + tool-call
// diff + output diff + metrics delta.
export async function compareChannels(leftChannelId: string, rightChannelId: string) {
  if (MOCK_MODE) return { data: mockCompareResponse(leftChannelId, rightChannelId), status: 200, error: undefined }
  const qs = new URLSearchParams({ left: leftChannelId, right: rightChannelId })
  return api<CompareResponse>(`/api/v1/channels/compare?${qs.toString()}`)
}

// Decision trail (lineage chain + boundary state + tool-execution ledger).
export async function getDecisionTrail(channelId: string) {
  if (MOCK_MODE) return mockDecisionTrail()
  return api<DecisionTrail>(`/api/v1/channels/${encodeURIComponent(channelId)}/trail`)
}

// Experiment platform
export async function listExperiments() {
  if (MOCK_MODE) return { data: [mockExperimentGroup()], status: 200, error: undefined }
  return api<ExperimentGroup[]>('/api/v1/experiments')
}

export async function getExperiment(groupId: string) {
  if (MOCK_MODE) return { data: mockExperimentGroup(), status: 200, error: undefined }
  return api<ExperimentGroup>(`/api/v1/experiments/${encodeURIComponent(groupId)}`)
}

export async function rankExperiment(groupId: string) {
  if (MOCK_MODE) {
    const group = mockExperimentGroup()
    return { data: { group, ranked: group.members, count: group.members.length }, status: 200, error: undefined }
  }
  return api<ExperimentRanking>(`/api/v1/experiments/${encodeURIComponent(groupId)}/rank`)
}

export async function setChannelOutcome(channelId: string, outcome: ChannelOutcome, score?: number) {
  if (MOCK_MODE) return { data: { channel_id: channelId, outcome, result_score: score ?? null }, status: 200, error: undefined }
  return api<{ channel_id: string; outcome: ChannelOutcome; result_score: number | null }>(
    `/api/v1/channels/${encodeURIComponent(channelId)}/outcome`,
    { method: 'PATCH', body: JSON.stringify({ outcome, score }) },
  )
}

export async function promoteChannel(channelId: string) {
  if (MOCK_MODE) return { data: { status: 'promoted' as const, child_channel_id: channelId, parent_channel_id: 'agent-research-run', parent_cursor: 34 }, status: 200, error: undefined }
  return api<{ status: 'promoted'; child_channel_id: string; parent_channel_id: string; parent_cursor: number }>(
    `/api/v1/channels/${encodeURIComponent(channelId)}/promote`,
    { method: 'POST' },
  )
}

// Types
export interface MetricsResponse {
  status: string
  timestamp: string
  uptime_seconds: number
  mode?: 'dev' | 'production'
  embedded_pg?: boolean
  subscription: SubscriptionStatus
  events?: {
    total: number
    channels: number
    state_snapshots: number
  }
  websocket: {
    connections: number
    topics: number
    topic_stats: Record<string, number>
    messages_sent: number
    messages_received: number
    total_messages: number
    back_pressure: {
      total_consumers: number
      slow_consumers: number
      total_queued_messages: number
      total_dropped_messages: number
      available_permits: number
      max_queue_size: number
    }
  }
}

export interface SubscriptionStatus {
  mode: 'standalone' | 'enrolled'
  status: 'standalone' | 'active' | 'locked' | 'unreachable'
  subscription: string
  last_sync_at?: string | null
  last_attempt_at?: string | null
  last_error?: string | null
  rev: number
  remaining_grace_secs?: number | null
  lock_imminent: boolean
  docs_url: string
}

export interface ForkRequest {
  source_channel_id: string
  new_channel_id: string
  at_cursor: number
  display_name?: string
  reason?: string
}

export interface ChannelMetadata {
  channel_id: string
  parent_channel_id: string | null
  origin_run_id: string
  forked_at_cursor: number
  forked_at: string
  display_name: string | null
  reason: string | null
  experiment_metadata: Record<string, unknown> | null
  requested_at_cursor?: number
  resolved_cursor?: number
  resolved_state_cursor?: number
  resolved_event_id?: string | null
  source_state_version?: number
  source_state_sha256?: string | null
  restorable?: boolean
  manifest?: Record<string, unknown> | null
  reproducibility?: string | null
  outcome?: string | null
  result_score?: number | null
  deleted_at?: string | null
}

export type ChannelOutcome = 'promoted' | 'rejected' | 'inconclusive' | 'crashed'

export interface ExperimentMember {
  channel_id: string
  role: 'baseline' | 'variant'
  declared_delta: Record<string, unknown> | null
  created_at: string
  /** Friendly run name from channel metadata / fork record (falls back to id). */
  display_name?: string | null
  outcome?: ChannelOutcome | null
  result_score?: number | null
}

export interface ExperimentGroup {
  group_id: string
  name: string
  description?: string | null
  baseline_channel_id?: string | null
  status: 'running' | 'completed'
  created_at: string
  updated_at: string
  members: ExperimentMember[]
  /** Dashboard-derived group built from fork.started lineage when the SDK
   * did not register a first-class experiment group. */
  derived?: boolean
}

export interface RankedExperimentMember extends Omit<ExperimentMember, 'created_at'> {}

export interface ExperimentRanking {
  group: ExperimentGroup
  ranked: RankedExperimentMember[]
  count: number
}

export interface ForkInfo {
  channel_id: string
  display_name: string | null
  reason: string | null
  forked_at_cursor: number
  event_count: number
  latest_cursor: number | null
  children: ForkInfo[]
}

export interface StatePoint {
  channel_id: string
  cursor: number
  state: Record<string, unknown>
}

export interface StateDiffEntry {
  path: string[]
  kind: 'added' | 'removed' | 'changed'
  left: unknown
  right: unknown
}

export interface StateDiff {
  left_channel_id: string
  right_channel_id: string
  left: StatePoint
  right: StatePoint
  common: StatePoint | null
  left_diverged_at_cursor: number | null
  right_diverged_at_cursor: number | null
  entries: StateDiffEntry[]
  truncated: boolean
  entry_count_total: number
  max_entries: number
}

export interface CompareResponse {
  left_channel_id: string
  right_channel_id: string
  state_diff: StateDiff
  manifest_diff: StateDiffEntry[]
  manifest_diff_truncated: boolean
  tool_diff: StateDiffEntry[]
  tool_diff_truncated: boolean
  output_left: unknown
  output_right: unknown
  output_changed: boolean
  metrics_delta: StateDiffEntry[]
}

export interface LineageHop {
  channel_id: string
  parent_channel_id: string | null
  forked_at_cursor: number
  display_name: string | null
  reason: string | null
}

export interface ExecutionLedgerEntry {
  id: string
  channel_id: string
  key_name: string
  tool_name: string
  status: string
  params: unknown
  result: unknown
  error: unknown
  started_cursor: number | null
  completed_cursor: number | null
}

export interface DecisionTrail {
  channel_id: string
  origin_run_id: string
  ancestry: LineageHop[]
  boundary: StatePoint | null
  executions: ExecutionLedgerEntry[]
}

function mockStateDiff(): { data: StateDiff; status: number } {
  return {
    data: {
      left_channel_id: 'agent-research-run-exp-a',
      right_channel_id: 'agent-research-run-exp-b',
      left: {
        channel_id: 'agent-research-run-exp-a',
        cursor: 4,
        state: {
          model: 'gpt-5.5-flash',
          messages: [
            { role: 'system', content: 'You are a research assistant.' },
            { role: 'user', content: 'Research the impact of prompt engineering.' },
          ],
          answer: 'GPT-5.5 answered with depth.',
        },
      },
      right: {
        channel_id: 'agent-research-run-exp-b',
        cursor: 4,
        state: {
          model: 'claude-3.5-sonnet',
          messages: [
            { role: 'system', content: 'You are a research assistant.' },
            { role: 'user', content: 'Research the impact of prompt engineering.' },
          ],
          answer: 'Claude answered with brevity.',
        },
      },
      common: {
        channel_id: 'agent-research-run',
        cursor: 3,
        state: { model: 'gpt-4', messages: [{ role: 'system', content: 'You are a research assistant.' }] },
      },
      left_diverged_at_cursor: 3,
      right_diverged_at_cursor: 3,
      truncated: false,
      entry_count_total: 3,
      max_entries: 500,
      entries: [
        { path: ['model'], kind: 'changed', left: 'gpt-5.5-flash', right: 'claude-3.5-sonnet' },
        { path: ['answer'], kind: 'changed', left: 'GPT-5.5 answered with depth.', right: 'Claude answered with brevity.' },
        { path: ['messages', '1', 'content'], kind: 'added', left: null, right: 'Research the impact of prompt engineering.' },
      ],
    },
    status: 200,
  }
}

function mockDecisionTrail(): { data: DecisionTrail; status: number } {
  return {
    data: {
      channel_id: 'agent-research-run-exp-a-sub',
      origin_run_id: 'agent-research-run',
      ancestry: [
        { channel_id: 'agent-research-run-exp-a-sub', parent_channel_id: 'agent-research-run-exp-a', forked_at_cursor: 2, display_name: 'Sub-experiment A1', reason: 'Higher temperature' },
        { channel_id: 'agent-research-run-exp-a', parent_channel_id: 'agent-research-run', forked_at_cursor: 3, display_name: 'Experiment A - GPT-5.5', reason: 'Testing GPT-5.5 prompt' },
        { channel_id: 'agent-research-run', parent_channel_id: null, forked_at_cursor: 0, display_name: 'Research Run', reason: null },
      ],
      boundary: {
        channel_id: 'agent-research-run-exp-a',
        cursor: 2,
        state: { model: 'gpt-5.5-flash', messages: [] },
      },
      executions: [
        { id: 'ex-1', channel_id: 'agent-research-run-exp-a-sub', key_name: 'web_search', tool_name: 'web.search', status: 'completed', params: { query: 'prompt engineering impact' }, result: { results: 10 }, error: null, started_cursor: 2, completed_cursor: 3 },
        { id: 'ex-2', channel_id: 'agent-research-run-exp-a-sub', key_name: 'llm_answer', tool_name: 'llm.chat', status: 'completed', params: { temperature: 0.9 }, result: { answer: '...' }, error: null, started_cursor: 4, completed_cursor: 5 },
      ],
    },
    status: 200,
  }
}

/** First-class experiment group for the demo fork family, so the Forks
 * laboratory shows a scored, ranked, promotable comparison. */
function mockExperimentGroup(): ExperimentGroup {
  return {
    group_id: 'exp-research-fork',
    name: 'Research fork · model comparison',
    description: 'Baseline GPT-4 run vs a GPT-5.5 prompt fork and a Claude 3.5 fork, both forked at cursor 3.',
    baseline_channel_id: 'agent-research-run',
    status: 'completed',
    created_at: '2026-07-25T09:00:00Z',
    updated_at: '2026-07-25T11:05:00Z',
    members: [
      { channel_id: 'agent-research-run', role: 'baseline', declared_delta: null, created_at: '2026-07-25T09:00:00Z', display_name: 'Research run · baseline', outcome: null, result_score: 8 },
      { channel_id: 'agent-research-run-exp-a', role: 'variant', declared_delta: { model: 'gpt-5.5-flash', reason: 'Test GPT-5.5 prompt' }, created_at: '2026-07-25T10:00:00Z', display_name: 'Fork A · GPT-5.5 prompt', outcome: 'promoted', result_score: 9 },
      { channel_id: 'agent-research-run-exp-b', role: 'variant', declared_delta: { model: 'claude-3.5-sonnet', reason: 'Claude 3.5 comparison' }, created_at: '2026-07-25T11:00:00Z', display_name: 'Fork B · Claude 3.5', outcome: 'rejected', result_score: 7 },
    ],
  }
}

/** Authoritative combined comparison for the demo fork pair. */
function mockCompareResponse(leftChannelId: string, rightChannelId: string): CompareResponse {
  return {
    left_channel_id: leftChannelId,
    right_channel_id: rightChannelId,
    state_diff: mockStateDiff().data,
    manifest_diff: [
      { path: ['model'], kind: 'changed', left: 'gpt-4', right: 'gpt-5.5-flash' },
      { path: ['prompt'], kind: 'changed', left: 'summarize briefly', right: 'summarize briefly with citations' },
      { path: ['temperature'], kind: 'changed', left: 0.3, right: 0.2 },
    ],
    manifest_diff_truncated: false,
    tool_diff: [
      { path: ['web_search', 'results_accepted'], kind: 'changed', left: 8, right: 12 },
      { path: ['llm_call', 'tokens'], kind: 'changed', left: 1200, right: 1500 },
      { path: ['llm_call', 'latency_ms'], kind: 'changed', left: 3100, right: 1800 },
    ],
    tool_diff_truncated: false,
    output_left: { model: 'gpt-4', status: 'completed', findings: 15, sources: 8, total_tokens: 4200, cost_usd: 0.18, answer: 'Eight sources accepted; enterprise adoption is the clearest trend.' },
    output_right: { model: 'gpt-5.5-flash', status: 'completed', findings: 21, sources: 12, total_tokens: 4700, cost_usd: 0.11, answer: 'Twelve sources accepted; cost per finding fell 38% with the revised prompt.' },
    output_changed: true,
    metrics_delta: [
      { path: ['findings'], kind: 'changed', left: 15, right: 21 },
      { path: ['sources'], kind: 'changed', left: 8, right: 12 },
      { path: ['total_tokens'], kind: 'changed', left: 4200, right: 4700 },
      { path: ['cost_usd'], kind: 'changed', left: 0.18, right: 0.11 },
      { path: ['duration_ms'], kind: 'changed', left: 25000, right: 21000 },
    ],
  }
}

/** Coordination demo: one execution group with members, causal messages and a
 * completed counterfactual fork so the Coordination page is populated. */
const MOCK_GROUP_ID = 'group-research-crew'
function mockExecutionGroup(): ExecutionGroupView {
  return { group_id: MOCK_GROUP_ID, status: 'running', metadata: { topic: 'multi-agent research' }, created_at: '2026-07-25T09:00:00Z', updated_at: '2026-07-25T09:40:00Z' }
}
function mockGroupMembers(_groupId: string): ExecutionGroupMemberView[] {
  return [
    { group_id: MOCK_GROUP_ID, member_id: 'researcher', channel_id: 'agent-research-run', role: 'producer', owner_id: 'worker-1', generation: 3, lease_until: '2026-07-25T09:41:00Z' },
    { group_id: MOCK_GROUP_ID, member_id: 'analyst', channel_id: 'agent-research-run-exp-a', role: 'worker', owner_id: 'worker-2', generation: 2, lease_until: '2026-07-25T09:41:00Z' },
    { group_id: MOCK_GROUP_ID, member_id: 'writer', channel_id: 'agent-research-run-exp-b', role: 'worker', owner_id: null, generation: 1, lease_until: null },
  ]
}
function mockGroupMessages(groupId: string, memberId: string): GroupMessageView[] {
  const messages: GroupMessageView[] = [
    { message_id: 'msg-1', group_id: groupId, from_member_id: 'researcher', to_member_id: 'analyst', type: 'task.claim', payload: { task: 'cross_reference_sources' }, source_event_id: 'evt-a', delivery_event_id: 'del-a', status: 'acked', created_at: '2026-07-25T09:01:00Z' },
    { message_id: 'msg-2', group_id: groupId, from_member_id: 'analyst', to_member_id: 'writer', type: 'result.ready', payload: { trends: 3 }, source_event_id: 'evt-b', delivery_event_id: 'del-b', status: 'acked', created_at: '2026-07-25T09:02:00Z' },
    { message_id: 'msg-3', group_id: groupId, from_member_id: 'writer', to_member_id: 'researcher', type: 'review.request', payload: { draft: 'executive_summary_v2' }, source_event_id: 'evt-c', delivery_event_id: 'del-c', status: 'delivered', created_at: '2026-07-25T09:03:00Z' },
  ]
  // The messages endpoint is per-member; filter so each member's tab is coherent.
  return messages.filter(message => message.from_member_id === memberId || message.to_member_id === memberId)
}
function mockGroupFork(groupId: string): GroupForkView {
  return { fork_group_id: `${groupId}-fork`, source_group_id: groupId, intervention_member_id: 'writer', intervention_cursor: 34, member_policies: { writer: 'frozen', analyst: 'production' }, tool_policies: {}, status: 'completed', first_divergence_event_id: 'evt-fork-c', receipt: {}, created_at: '2026-07-25T09:30:00Z', updated_at: '2026-07-25T09:40:00Z' }
}
function mockCausalGraph(eventId: string, _maxNodes: number): { events: unknown[]; dependencies: unknown[]; truncated: boolean } {
  return {
    events: [
      { id: 'evt-a', type: 'task.claim', cursor: 5 },
      { id: 'evt-b', type: 'result.ready', cursor: 6 },
      { id: eventId || 'evt-c', type: 'review.request', cursor: 7 },
    ],
    dependencies: [
      { from: 'evt-a', to: 'evt-b' },
      { from: 'evt-b', to: 'evt-c' },
    ],
    truncated: false,
  }
}
