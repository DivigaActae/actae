// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

function stubLocation() {
  const loc = {
    href: 'http://localhost/dashboard/',
    pathname: '/dashboard/',
    origin: 'http://localhost',
    protocol: 'http:',
    host: 'localhost',
  }
  Object.defineProperty(window, 'location', {
    value: loc,
    writable: true,
    configurable: true,
  })
}

function mockFetch(response: unknown, status = 200): any {
  return vi.fn(async () => ({
    ok: status >= 200 && status < 300,
    status,
    json: async () => response,
  }))
}

describe('api client', () => {
  let api: typeof import('./api')
  let fetchMock: any

  beforeEach(() => {
    stubLocation()
    localStorage.clear()
    sessionStorage.clear()
    fetchMock = mockFetch({})
    vi.stubGlobal('fetch', fetchMock)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.unstubAllEnvs()
    vi.resetModules()
  })

  async function loadApi() {
    api = await import('./api')
    return api
  }

  it('sends Authorization Bearer token from localStorage', async () => {
    localStorage.setItem('actae_auth', JSON.stringify({ token: 'tok-123' }))
    await loadApi()
    await api.login('a@b.com', 'pw')
    expect(fetchMock).toHaveBeenCalled()
    const [url, opts] = fetchMock.mock.calls[0]
    expect(url).toContain('/api/v1/auth/login')
    expect((opts.headers as any)['Authorization']).toBe('Bearer tok-123')
  })

  it('reads token from sessionStorage when localStorage is empty', async () => {
    sessionStorage.setItem('actae_auth', JSON.stringify({ token: 'tok-sess' }))
    await loadApi()
    await api.getMe()
    expect((fetchMock.mock.calls[0][1].headers as any)['Authorization']).toBe('Bearer tok-sess')
  })

  it('omits Authorization header when no token stored', async () => {
    await loadApi()
    await api.getMe()
    expect((fetchMock.mock.calls[0][1].headers as any)['Authorization']).toBeUndefined()
  })

  it('returns parsed data on success', async () => {
    fetchMock = mockFetch({ status: 'ok', events: [{ id: 'e1' }] })
    vi.stubGlobal('fetch', fetchMock)
    await loadApi()
    const res = await api.queryEvents({})
    expect(res.status).toBe(200)
    expect(res.data).toEqual({ status: 'ok', events: [{ id: 'e1' }] })
    expect(res.error).toBeUndefined()
  })

  it('returns server error message on non-ok response', async () => {
    fetchMock = mockFetch({ error: 'No such channel' }, 404)
    vi.stubGlobal('fetch', fetchMock)
    await loadApi()
    const res = await api.listChannels()
    expect(res.status).toBe(404)
    expect(res.error).toBe('No such channel')
    expect(res.data).toBeUndefined()
  })

  it('falls back to status text when error payload has no message', async () => {
    fetchMock = mockFetch({}, 500)
    vi.stubGlobal('fetch', fetchMock)
    await loadApi()
    const res = await api.replayEvents('ch')
    expect(res.status).toBe(500)
    expect(res.error).toContain('Request failed: 500')
  })

  it('returns network error when fetch rejects', async () => {
    fetchMock = vi.fn(async () => { throw new Error('boom') })
    vi.stubGlobal('fetch', fetchMock)
    await loadApi()
    const res = await api.getMetrics()
    expect(res.status).toBe(0)
    expect(res.error).toContain('boom')
  })

  it('clears auth and redirects to login on 401', async () => {
    localStorage.setItem('actae_auth', JSON.stringify({ token: 'tok' }))
    fetchMock = mockFetch({ error: 'Unauthorized' }, 401)
    vi.stubGlobal('fetch', fetchMock)
    await loadApi()
    await api.getMe()
    expect(localStorage.getItem('actae_auth')).toBeNull()
    expect(sessionStorage.getItem('actae_auth')).toBeNull()
    expect(window.location.href).toBe('/login/')
  })

  it('does not redirect when already on the login page', async () => {
    const loc = {
      href: 'http://localhost/login/',
      pathname: '/login/',
      origin: 'http://localhost',
      protocol: 'http:',
      host: 'localhost',
    }
    Object.defineProperty(window, 'location', { value: loc, writable: true, configurable: true })
    fetchMock = mockFetch({ error: 'Unauthorized' }, 401)
    vi.stubGlobal('fetch', fetchMock)
    await loadApi()
    await api.getMe()
    expect(window.location.href).toBe('http://localhost/login/')
  })

  describe('auth endpoints', () => {
    it('signup POSTs email/password/name', async () => {
      fetchMock = mockFetch({ token: 't', user: { id: 'u' } }, 201)
      vi.stubGlobal('fetch', fetchMock)
      await loadApi()
      await api.signup('a@b.com', 'pw', 'Alice')
      const [url, opts] = fetchMock.mock.calls[0]
      expect(url).toContain('/api/v1/auth/signup')
      expect(opts.method).toBe('POST')
      expect(JSON.parse(opts.body)).toEqual({ email: 'a@b.com', password: 'pw', name: 'Alice' })
    })

    it('login POSTs credentials', async () => {
      await loadApi()
      await api.login('a@b.com', 'pw')
      const [url, opts] = fetchMock.mock.calls[0]
      expect(url).toContain('/api/v1/auth/login')
      expect(JSON.parse(opts.body)).toEqual({ email: 'a@b.com', password: 'pw' })
    })

    it('logout POSTs to /logout', async () => {
      await loadApi()
      await api.logout()
      expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/auth/logout')
      expect(fetchMock.mock.calls[0][1].method).toBe('POST')
    })
  })

  describe('user preferences', () => {
    it('getPreferences GETs the preferences endpoint', async () => {
      fetchMock = mockFetch({ prefs: { flow: { layoutMode: 'grid' } } })
      vi.stubGlobal('fetch', fetchMock)
      await loadApi()
      const res = await api.getPreferences()
      expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/user/preferences')
      expect(res.data?.prefs.flow.layoutMode).toBe('grid')
    })

    it('putPreferences PUTs the flow prefs envelope', async () => {
      await loadApi()
      await api.putPreferences({ flow: { layoutMode: 'compact', visibleEventTypes: ['agent.started'], visibleFields: [] } })
      const [url, opts] = fetchMock.mock.calls[0]
      expect(url).toContain('/api/v1/user/preferences')
      expect(opts.method).toBe('PUT')
      expect(JSON.parse(opts.body)).toEqual({
        prefs: { flow: { layoutMode: 'compact', visibleEventTypes: ['agent.started'], visibleFields: [] } },
      })
    })
  })

  describe('metrics', () => {
    it('getMetrics GETs /metrics.json', async () => {
      fetchMock = mockFetch({ status: 'ok', websocket: { connections: 3 } })
      vi.stubGlobal('fetch', fetchMock)
      await loadApi()
      const res = await api.getMetrics()
      expect(fetchMock.mock.calls[0][0]).toContain('/metrics.json')
      expect(res.data?.websocket.connections).toBe(3)
    })
  })

  describe('events API', () => {
    it('queryEvents POSTs channel_ids and filters', async () => {
      await loadApi()
      await api.queryEvents({ channel_ids: ['a', 'b'], event_type: 'agent.started', cursor_start: 5, cursor_end: 10, limit: 50, offset: 25 })
      const [url, opts] = fetchMock.mock.calls[0]
      expect(url).toContain('/api/v1/events/query')
      expect(opts.method).toBe('POST')
      expect(JSON.parse(opts.body)).toEqual({
        channel_ids: ['a', 'b'],
        event_type: 'agent.started',
        cursor_start: 5,
        cursor_end: 10,
        limit: 50,
        offset: 25,
      })
    })

    it('getLatestCursor GETs the channel cursor endpoint', async () => {
      fetchMock = mockFetch({ status: 'ok', channel_id: 'ch', latest_cursor: 42 })
      vi.stubGlobal('fetch', fetchMock)
      await loadApi()
      const res = await api.getLatestCursor('ch/with/slash')
      expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/events/cursor/ch%2Fwith%2Fslash')
      expect(res.data?.latest_cursor).toBe(42)
    })

    it('listChannels GETs /api/v1/channels', async () => {
      await loadApi()
      await api.listChannels()
      expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/channels')
    })

    it('replayEvents appends cursor and limit query params', async () => {
      await loadApi()
      await api.replayEvents('ch', 7, 200)
      const [url] = fetchMock.mock.calls[0]
      expect(url).toContain('/api/v1/events/replay/ch?cursor=7&limit=200')
    })

    it('replayEvents omits params when not provided', async () => {
      await loadApi()
      await api.replayEvents('ch')
      expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/events/replay/ch')
      expect(fetchMock.mock.calls[0][0]).not.toContain('?')
    })
  })

  describe('state API', () => {
    it('saveState POSTs channel/cursor/state', async () => {
      await loadApi()
      await api.saveState('ch', 3, { agent: 'x' })
      const [url, opts] = fetchMock.mock.calls[0]
      expect(url).toContain('/api/v1/state/ch')
      expect(JSON.parse(opts.body)).toEqual({ channel_id: 'ch', cursor: 3, state: { agent: 'x' } })
    })

    it('loadState GETs the state endpoint', async () => {
      await loadApi()
      await api.loadState('ch')
      expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/state/ch')
    })
  })

  describe('fork & channel metadata API', () => {
    it('forkChannel POSTs fork request', async () => {
      fetchMock = mockFetch({ channel_id: 'new-ch', status: 'forked' }, 201)
      vi.stubGlobal('fetch', fetchMock)
      await loadApi()
      const res = await api.forkChannel({ source_channel_id: 'src', new_channel_id: 'new-ch', at_cursor: 3, display_name: 'Exp', reason: 'why' })
      expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
        source_channel_id: 'src',
        new_channel_id: 'new-ch',
        at_cursor: 3,
        display_name: 'Exp',
        reason: 'why',
      })
      expect(res.status).toBe(201)
    })

    it('getChannelMetadata GETs metadata endpoint', async () => {
      await loadApi()
      await api.getChannelMetadata('ch')
      expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/channels/ch/metadata')
    })

    it('listForks GETs forks endpoint', async () => {
      await loadApi()
      await api.listForks('ch')
      expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/channels/ch/forks')
    })

    it('getForkTree GETs tree endpoint', async () => {
      await loadApi()
      await api.getForkTree('root')
      expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/channels/root/tree')
    })

    it('updateChannelMetadata PUTs fields', async () => {
      await loadApi()
      await api.updateChannelMetadata({ channel_id: 'ch', display_name: 'New', reason: 'R' })
      const [url, opts] = fetchMock.mock.calls[0]
      expect(url).toContain('/api/v1/channels/ch/metadata')
      expect(opts.method).toBe('PUT')
      expect(JSON.parse(opts.body)).toEqual({ channel_id: 'ch', display_name: 'New', reason: 'R' })
    })

    it('diffChannelStates GETs diff with left/right query params', async () => {
      fetchMock = mockFetch({ left_channel_id: 'a', right_channel_id: 'b', left: {}, right: {}, entries: [] })
      vi.stubGlobal('fetch', fetchMock)
      await loadApi()
      await api.diffChannelStates('fix-a', 'fix b')
      const url = fetchMock.mock.calls[0][0]
      expect(url).toContain('/api/v1/channels/diff')
      expect(url).toContain('left=fix-a')
      expect(url).toContain('right=fix+b')
    })

    it('getDecisionTrail GETs trail endpoint', async () => {
      fetchMock = mockFetch({ channel_id: 'c', origin_run_id: 'root', ancestry: [], executions: [] })
      vi.stubGlobal('fetch', fetchMock)
      await loadApi()
      await api.getDecisionTrail('fork/c')
      expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/channels/fork%2Fc/trail')
    })
  })

  describe('getWsUrl', () => {
    it('builds ws:// URL from window location', async () => {
      await loadApi()
      expect(api.getWsUrl()).toBe('ws://localhost/ws')
    })

    it('builds wss:// URL for https page origins', async () => {
      stubLocation()
      const loc = {
        href: 'https://actae.example.com/dashboard/',
        pathname: '/dashboard/',
        origin: 'https://actae.example.com',
        protocol: 'https:',
        host: 'actae.example.com',
      }
      Object.defineProperty(window, 'location', {
        value: loc,
        writable: true,
        configurable: true,
      })
      await loadApi()
      expect(api.getWsUrl()).toBe('wss://actae.example.com/ws')
    })

    it('follows NEXT_PUBLIC_API_BASE when set (dev topology)', async () => {
      vi.stubEnv('NEXT_PUBLIC_API_BASE', 'http://localhost:8002')
      const mod = await import('./api')
      expect(mod.getWsUrl()).toBe('ws://localhost:8002/ws')
      vi.unstubAllEnvs()
    })

    it('maps https API_BASE to wss and strips trailing slashes and paths', async () => {
      vi.stubEnv('NEXT_PUBLIC_API_BASE', 'https://actae.example.com/api/')
      const mod = await import('./api')
      expect(mod.getWsUrl()).toBe('wss://actae.example.com/ws')
      vi.unstubAllEnvs()
    })

    it('falls back to the page origin when API_BASE is not a valid URL', async () => {
      vi.stubEnv('NEXT_PUBLIC_API_BASE', 'not-a-url')
      const mod = await import('./api')
      expect(mod.getWsUrl()).toBe('ws://localhost/ws')
      vi.unstubAllEnvs()
    })
  })

  describe('MOCK_MODE forks', () => {
    async function loadMockApi() {
      vi.stubEnv('NEXT_PUBLIC_MOCK_MODE', 'true')
      api = await import('./api')
      return api
    }

    it('generic api() short-circuits to empty data in mock mode', async () => {
      await loadMockApi()
      const res = await api.login('a', 'b')
      expect(res).toEqual({ status: 200, data: {} })
      expect(fetchMock).not.toHaveBeenCalled()
    })

    it('replayEvents filters MOCK_EVENTS by cursor > from', async () => {
      await loadMockApi()
      const res = await api.replayEvents('agent-research-run', 3)
      expect(res.data.events.every((e: any) => e.cursor > 3)).toBe(true)
    })

    it('loadState returns MOCK_STATES snapshot for known channel', async () => {
      await loadMockApi()
      const res = await api.loadState('agent-research-run')
      expect(res.data).not.toBeNull()
      expect(res.data.state).toBeTruthy()
    })

    it('forkChannel returns a forked response', async () => {
      await loadMockApi()
      const res = await api.forkChannel({ source_channel_id: 's', new_channel_id: 'n', at_cursor: 0 })
      expect(res.status).toBe(201)
      expect(res.data?.channel_id).toBe('n')
    })

    it('getChannelMetadata returns metadata for known channels and null for unknown', async () => {
      await loadMockApi()
      const known = await api.getChannelMetadata('agent-research-run-exp-a')
      expect(known.data?.parent_channel_id).toBe('agent-research-run')
      const unknown = await api.getChannelMetadata('nope')
      expect(unknown.status).toBe(404)
    })

    it('listForks returns forks for known channel', async () => {
      await loadMockApi()
      const res = await api.listForks('agent-research-run')
      expect(res.data?.forks.length).toBe(2)
    })

    it('getForkTree returns the mock tree', async () => {
      await loadMockApi()
      const res = await api.getForkTree('agent-research-run')
      expect(res.data?.channel_id).toBe('agent-research-run')
    })

    it('queryEvents surfaces approval + fork events for the demo pages', async () => {
      await loadMockApi()
      const approvals = await api.queryEvents({ event_type: 'approval.requested', limit: 100 })
      expect((approvals.data as any).events.length).toBeGreaterThanOrEqual(3)
      const forks = await api.queryEvents({ event_type: 'fork.started', limit: 100 })
      expect((forks.data as any).events.some((e: any) => e.payload?.source_channel_id)).toBe(true)
    })

    it('listWakeups returns a pending approval deadline', async () => {
      await loadMockApi()
      const res = await api.listWakeups({ status: 'pending' })
      expect(res.data?.wakeups.some(w => (w.payload as any)?.approval_request_id)).toBe(true)
    })

    it('coordination mock returns a group, members, messages, fork and graph', async () => {
      await loadMockApi()
      const groups = await api.listExecutionGroups()
      const groupId = groups.data!.groups[0]!.group_id
      expect(groups.data!.groups.length).toBeGreaterThan(0)
      const members = await api.listExecutionGroupMembers(groupId)
      expect(members.data!.members.length).toBeGreaterThan(0)
      const messages = await api.listExecutionGroupMessages(groupId, members.data!.members[0]!.member_id)
      expect(messages.data!.messages.length).toBeGreaterThan(0)
      const fork = await api.getExecutionGroupFork(groupId)
      expect(fork.data!.fork.source_group_id).toBe(groupId)
      const graph = await api.getCausalGraph('evt-a')
      expect((graph.data as any).events.length).toBeGreaterThan(0)
    })

    it('listExperiments returns a scored fork group', async () => {
      await loadMockApi()
      const res = await api.listExperiments()
      expect((res.data as any[])[0]?.members.some((m: any) => m.result_score != null)).toBe(true)
    })

    it('updateChannelMetadata echoes the request', async () => {
      await loadMockApi()
      const res = await api.updateChannelMetadata({ channel_id: 'ch', display_name: 'X' })
      expect(res.data?.display_name).toBe('X')
    })

    it('buildChannelExport assembles the MOCK_EVENTS timeline', async () => {
      await loadMockApi()
      const { MOCK_EVENTS } = await import('./mock-data')
      // The store's timeline is the paginateReplay normalization of MOCK_EVENTS.
      const timeline = (MOCK_EVENTS['agent-research-run'] as any[]).map((e: any) => ({
        id: e.id,
        cursor: e.cursor,
        type: e.event_type || e.type || 'unknown',
        channel: e.channel_id,
        timestamp: e.timestamp,
        payload: e.payload,
        isState: e.isState,
        state: e.state,
      }))
      const res = await api.buildChannelExport('agent-research-run', { events: timeline })
      expect(res.status).toBe(200)
      expect(res.data?.channel_id).toBe('agent-research-run')
      expect(res.data?.event_count).toBeGreaterThan(0)
      expect(res.data?.events[0].cursor).toBe(1)
      // The trailing state snapshot is preserved with its is_state marker.
      const last = res.data?.events[res.data!.events.length - 1]
      expect(last?.is_state).toBe(true)
    })
  })

  describe('buildChannelExport', () => {
    function mockMetadata(extra: Record<string, unknown> = {}) {
      fetchMock = vi.fn(async () => ({
        ok: true,
        status: 200,
        json: async () => ({
          channel_id: 'ch',
          parent_channel_id: null,
          origin_run_id: 'ch',
          forked_at_cursor: 0,
          forked_at: null,
          display_name: 'My Run',
          reason: null,
          experiment_metadata: { model: 'gpt-4' },
          ...extra,
        }),
      }))
      vi.stubGlobal('fetch', fetchMock)
    }

    it('builds a valid JSON document from the given timeline and metadata', async () => {
      mockMetadata()
      await loadApi()
      const res = await api.buildChannelExport('ch', {
        events: [
          { id: 'e1', cursor: 1, type: 'agent.started', channel: 'ch', timestamp: '2026-01-01T00:00:00Z', payload: { model: 'gpt-4' } },
          { id: 'e2', cursor: 2, type: 'tool.llm', channel: 'ch', timestamp: new Date('2026-01-01T00:00:01Z'), payload: { model: 'gpt-4', tokens: 100 } },
        ],
      })
      expect(res.status).toBe(200)
      const doc = res.data!
      expect(doc.schema).toBe('actae.channel_export.v1')
      expect(doc.channel_id).toBe('ch')
      expect(doc.display_name).toBe('My Run')
      expect(doc.experiment_metadata).toEqual({ model: 'gpt-4' })
      expect(doc.ancestry).toEqual([])
      expect(doc.filters).toEqual({ event_types: null, payload_fields: null })
      expect(doc.event_count).toBe(2)
      // Date timestamps are serialized to ISO strings; the export is valid JSON.
      expect(doc.events[1].timestamp).toBe('2026-01-01T00:00:01.000Z')
      expect(() => JSON.stringify(doc)).not.toThrow()
    })

    it('applies the type filter exactly like the canvas (state snapshots grouped as "state")', async () => {
      mockMetadata()
      await loadApi()
      const res = await api.buildChannelExport('ch', {
        events: [
          { id: 'e1', cursor: 1, type: 'agent.started', channel: 'ch', timestamp: '2026-01-01T00:00:00Z', payload: {} },
          { id: 'e2', cursor: 2, type: 'tool.llm', channel: 'ch', timestamp: '2026-01-01T00:00:01Z', payload: {} },
          { id: 'e3', cursor: 3, type: 'tool.llm', channel: 'ch', timestamp: '2026-01-01T00:00:02Z', payload: {} },
          { cursor: 4, type: 'state.transition', channel: 'ch', timestamp: '2026-01-01T00:00:03Z', payload: {}, isState: true, state: { a: 1 } },
        ],
        visibleEventTypes: new Set(['tool.llm']),
      })
      const doc = res.data!
      expect(doc.event_count).toBe(2)
      expect(doc.events.map(e => e.type)).toEqual(['tool.llm', 'tool.llm'])
      // The state snapshot is keyed "state", so it is excluded by the filter.
      expect(doc.events.some(e => e.is_state)).toBe(false)
      expect(doc.filters.event_types).toEqual(['tool.llm'])
    })

    it('keeps state events when the "state" type is selected', async () => {
      mockMetadata()
      await loadApi()
      const res = await api.buildChannelExport('ch', {
        events: [
          { cursor: 4, type: 'state.transition', channel: 'ch', timestamp: '2026-01-01T00:00:03Z', payload: {}, isState: true, state: { a: 1 } },
          { id: 'e2', cursor: 2, type: 'tool.llm', channel: 'ch', timestamp: '2026-01-01T00:00:01Z', payload: {} },
        ],
        visibleEventTypes: new Set(['state']),
      })
      const doc = res.data!
      expect(doc.event_count).toBe(1)
      const stateEvent = doc.events[0]
      expect(stateEvent.is_state).toBe(true)
      // State payloads come from the state field, trimmed by the field filter.
      expect(stateEvent.payload).toEqual({ a: 1 })
    })

    it('trims payloads with the field filter and reports it in the header', async () => {
      mockMetadata()
      await loadApi()
      const res = await api.buildChannelExport('ch', {
        events: [
          { id: 'e1', cursor: 1, type: 'agent.started', channel: 'ch', timestamp: '2026-01-01T00:00:00Z', payload: { model: 'gpt-4', tokens: 100, secret: 'x' } },
          { id: 'e2', cursor: 2, type: 'agent.started', channel: 'ch', timestamp: '2026-01-01T00:00:01Z', payload: { model: 'claude', secret: 'y' } },
        ],
        visibleFields: new Set(['model', 'tokens']),
      })
      const doc = res.data!
      expect(doc.event_count).toBe(2)
      expect(doc.events.map(e => e.payload)).toEqual([{ model: 'gpt-4', tokens: 100 }, { model: 'claude' }])
      expect(doc.filters.payload_fields).toEqual(['model', 'tokens'])
    })

    it('keeps inherited markers on events from ancestor channels', async () => {
      mockMetadata()
      await loadApi()
      const res = await api.buildChannelExport('ch', {
        events: [
          { id: 'p1', cursor: 1, type: 'step', channel: 'parent', timestamp: '2026-01-01T00:00:00Z', payload: {}, isInherited: true },
          { id: 'c1', cursor: 1, type: 'step', channel: 'ch', timestamp: '2026-01-01T00:00:01Z', payload: {} },
        ],
      })
      const doc = res.data!
      expect(doc.events.map(e => e.channel_id)).toEqual(['parent', 'ch'])
      expect(doc.events[0].inherited).toBe(true)
      expect(doc.events[1].inherited).toBeUndefined()
    })

    it('records the ancestry chain from the metadata parent walk', async () => {
      fetchMock = vi.fn(async (url: string) => {
        const id = url.includes('/channels/parent/') ? 'parent' : 'child'
        const m = id === 'parent'
          ? { channel_id: 'parent', parent_channel_id: null, origin_run_id: 'parent', forked_at_cursor: 0, forked_at: null, display_name: null, reason: null, experiment_metadata: null }
          : { channel_id: 'child', parent_channel_id: 'parent', origin_run_id: 'parent', forked_at_cursor: 2, forked_at: null, display_name: 'Child', reason: null, experiment_metadata: null }
        return { ok: true, status: 200, json: async () => m }
      })
      vi.stubGlobal('fetch', fetchMock)
      await loadApi()
      const res = await api.buildChannelExport('child', { events: [] })
      expect(res.status).toBe(200)
      expect(res.data!.ancestry).toEqual(['parent'])
      expect(res.data!.parent_channel_id).toBe('parent')
    })

    it('returns an error when the channel metadata lookup fails', async () => {
      fetchMock = vi.fn(async () => ({ ok: false, status: 404, json: async () => ({ error: 'No such channel' }) }))
      vi.stubGlobal('fetch', fetchMock)
      await loadApi()
      const res = await api.buildChannelExport('nope', { events: [] })
      expect(res.status).toBe(0)
      expect(res.error).toContain('No such channel')
    })

    it('downloadJsonFile triggers a JSON blob download', async () => {
      await loadApi()
      const create = vi.fn(() => 'blob:mock')
      const revoke = vi.fn()
      vi.stubGlobal('URL', { ...URL, createObjectURL: create, revokeObjectURL: revoke })
      try {
        const click = vi.fn()
        const remove = vi.fn()
        vi.spyOn(document, 'createElement').mockReturnValue({ click, remove, href: '', download: '' } as unknown as HTMLAnchorElement)
        vi.spyOn(document.body, 'appendChild').mockImplementation(() => ({} as any))
        api.downloadJsonFile('ch.json', { a: 1 })
        expect(create).toHaveBeenCalled()
        expect(click).toHaveBeenCalled()
        expect(revoke).toHaveBeenCalled()
        expect(remove).toHaveBeenCalled()
      } finally {
        vi.restoreAllMocks()
      }
    })

    it('copyToClipboard uses the async Clipboard API when available', async () => {
      await loadApi()
      const writeText = vi.fn(async () => {})
      vi.stubGlobal('navigator', { ...navigator, clipboard: { writeText } })
      await api.copyToClipboard('{ "a": 1 }')
      expect(writeText).toHaveBeenCalledWith('{ "a": 1 }')
    })

    it('copyToClipboard falls back to execCommand when the Clipboard API is unavailable', async () => {
      await loadApi()
      vi.stubGlobal('navigator', { ...navigator, clipboard: undefined })
      const execCommand = vi.fn(() => true)
      const original = (document as any).execCommand
      ;(document as any).execCommand = execCommand
      try {
        vi.spyOn(document, 'createElement').mockReturnValue({
          value: '',
          select: vi.fn(),
          remove: vi.fn(),
          style: {},
        } as unknown as HTMLTextAreaElement)
        vi.spyOn(document.body, 'appendChild').mockImplementation(() => ({} as any))
        await api.copyToClipboard('text')
        expect(execCommand).toHaveBeenCalledWith('copy')
      } finally {
        vi.restoreAllMocks()
        if (original === undefined) {
          delete (document as any).execCommand
        } else {
          ;(document as any).execCommand = original
        }
      }
    })
  })
})
