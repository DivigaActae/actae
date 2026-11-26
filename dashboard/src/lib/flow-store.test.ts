// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { useFlowStore } from './flow-store'
import { resetFlowStore } from '@/test/utils'

const api = vi.hoisted(() => ({
  listChannels: vi.fn(),
  replayEvents: vi.fn(),
  loadState: vi.fn(),
  listForks: vi.fn(),
  getChannelMetadata: vi.fn(),
  getPreferences: vi.fn(),
  putPreferences: vi.fn(),
}))

vi.mock('@/lib/api', () => ({
  listChannels: api.listChannels,
  replayEvents: api.replayEvents,
  loadState: api.loadState,
  listForks: api.listForks,
  getChannelMetadata: api.getChannelMetadata,
  getPreferences: api.getPreferences,
  putPreferences: api.putPreferences,
}))

function mockChannelList(channels: string[]) {
  api.listChannels.mockResolvedValue({ data: { channels }, status: 200 })
}

let replayRegistry: Record<string, any[]> = {}

function mockReplayFor(channel: string, events: any[]) {
  replayRegistry[channel] = events
  api.replayEvents.mockImplementation(async (id: string, cursor = 0, limit = 1000) => {
    const filtered = (replayRegistry[id] ?? []).filter(e => (e.cursor ?? 0) > cursor)
    return { data: { events: filtered.slice(0, limit) }, status: 200 }
  })
}

function mockState(cursor: number | null, state: any = {}) {
  api.loadState.mockResolvedValue(
    cursor === null
      ? { data: null, status: 404 }
      : { data: { cursor, state }, status: 200 }
  )
}

beforeEach(() => {
  resetFlowStore()
  replayRegistry = {}
  vi.clearAllMocks()
  api.listChannels.mockResolvedValue({ data: { channels: [] }, status: 200 })
  api.replayEvents.mockResolvedValue({ data: { events: [] }, status: 200 })
  api.loadState.mockResolvedValue({ data: null, status: 404 })
  api.listForks.mockResolvedValue({ data: { forks: [] }, status: 200 })
  api.getChannelMetadata.mockResolvedValue({ data: null, status: 404 })
  api.getPreferences.mockResolvedValue({ data: { prefs: {} }, status: 200 })
  api.putPreferences.mockResolvedValue({ status: 200 })
})

describe('loadChannels', () => {
  it('maps { channels: [...] } responses into the channels list', async () => {
    mockChannelList(['a', 'b'])
    await useFlowStore.getState().loadChannels()
    expect(useFlowStore.getState().channels).toEqual(['a', 'b'])
  })

  it('maps raw array responses', async () => {
    api.listChannels.mockResolvedValue({ data: ['x', 'y'], status: 200 })
    await useFlowStore.getState().loadChannels()
    expect(useFlowStore.getState().channels).toEqual(['x', 'y'])
  })

  it('maps object channel entries by id', async () => {
    api.listChannels.mockResolvedValue({ data: [{ channel_id: 'c1' }, { id: 'c2' }], status: 200 })
    await useFlowStore.getState().loadChannels()
    expect(useFlowStore.getState().channels).toEqual(['c1', 'c2'])
  })

  it('auto-selects the most recently active channel when none is active', async () => {
    api.listChannels.mockResolvedValue({ data: { channels: ['fresh-run', 'old-run'] }, status: 200 })
    await useFlowStore.getState().loadChannels()
    expect(useFlowStore.getState().activeChannel).toBe('fresh-run')
    expect(useFlowStore.getState().events).toEqual([])
  })

  it('does not override an already active channel on loadChannels', async () => {
    api.listChannels.mockResolvedValue({ data: { channels: ['fresh-run', 'old-run'] }, status: 200 })
    useFlowStore.setState({ activeChannel: 'old-run' })
    await useFlowStore.getState().loadChannels()
    expect(useFlowStore.getState().activeChannel).toBe('old-run')
  })
})

describe('channel polling (auto-refresh)', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    useFlowStore.getState().stopChannelPolling()
    vi.useRealTimers()
  })

  it('polls loadChannels on an interval', async () => {
    api.listChannels.mockResolvedValue({ data: { channels: ['a'] }, status: 200 })
    useFlowStore.getState().startChannelPolling(5000)
    await vi.advanceTimersByTimeAsync(5000)
    expect(api.listChannels).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(5000)
    expect(api.listChannels).toHaveBeenCalledTimes(2)
  })

  it('stopChannelPolling halts the interval', async () => {
    useFlowStore.getState().startChannelPolling(5000)
    useFlowStore.getState().stopChannelPolling()
    await vi.advanceTimersByTimeAsync(20000)
    expect(api.listChannels).not.toHaveBeenCalled()
  })

  it('startChannelPolling is idempotent (no duplicate intervals)', async () => {
    useFlowStore.getState().startChannelPolling(5000)
    useFlowStore.getState().startChannelPolling(5000)
    await vi.advanceTimersByTimeAsync(5000)
    expect(api.listChannels).toHaveBeenCalledTimes(1)
  })

  it('the poll never resets the active channel, events or selection', async () => {
    api.getChannelMetadata.mockResolvedValue({ data: { parent_channel_id: null }, status: 200 })
    mockReplayFor('ch', [
      { id: '1', cursor: 1, event_type: 'agent.started', payload: {}, timestamp: '2026-01-01T00:00:00Z' },
    ])
    await useFlowStore.getState().setActiveChannel('ch')
    useFlowStore.getState().setSelectedNode({ type: 'agent.started', cursor: 1, payload: {} })
    expect(useFlowStore.getState().selectedNode).not.toBeNull()

    mockChannelList(['ch', 'code-created-fork'])
    useFlowStore.getState().startChannelPolling(5000)
    await vi.advanceTimersByTimeAsync(5000)

    const s = useFlowStore.getState()
    expect(s.channels).toEqual(['ch', 'code-created-fork'])
    expect(s.activeChannel).toBe('ch')
    expect(s.events).toHaveLength(1)
    expect(s.selectedNode).not.toBeNull()
  })

  it('surfaces newly created channels from code forks', async () => {
    api.getChannelMetadata.mockResolvedValue({ data: { parent_channel_id: null }, status: 200 })
    mockChannelList(['ch'])
    await useFlowStore.getState().loadChannels()
    expect(useFlowStore.getState().channels).toEqual(['ch'])

    mockChannelList(['ch', 'exp/refactor-v2'])
    useFlowStore.getState().startChannelPolling(5000)
    await vi.advanceTimersByTimeAsync(5000)
    expect(useFlowStore.getState().channels).toEqual(['ch', 'exp/refactor-v2'])
  })
})

describe('setActiveChannel', () => {
  it('loads events, state and forks for the channel', async () => {
    mockChannelList(['ch'])
    mockReplayFor('ch', [
      { id: '1', cursor: 1, event_type: 'agent.started', payload: { a: 1 }, timestamp: '2026-01-01T00:00:00Z' },
      { id: '2', cursor: 2, event_type: 'agent.completed', payload: {}, timestamp: '2026-01-01T00:00:01Z' },
    ])
    mockState(2, { status: 'done' })
    api.listForks.mockResolvedValue({ data: { forks: [{ channel_id: 'fork' }] }, status: 200 })

    await useFlowStore.getState().setActiveChannel('ch')

    const s = useFlowStore.getState()
    expect(s.activeChannel).toBe('ch')
    expect(s.events).toHaveLength(2)
    expect(s.events[0].type).toBe('agent.started')
    expect(s.events[0].channel).toBe('ch')
    expect(s.stateSnapshot).toEqual({ cursor: 2, state: { status: 'done' } })
    expect(s.forks).toEqual([{ channel_id: 'fork' }])
    expect(s.loading).toBe(false)
  })

  it('resets previous channel state when switching', async () => {
    mockReplayFor('ch1', [{ id: '1', cursor: 1, event_type: 'agent.started', payload: {}, timestamp: '2026-01-01T00:00:00Z' }])
    mockReplayFor('ch2', [])
    await useFlowStore.getState().setActiveChannel('ch1')
    useFlowStore.getState().setSelectedNode({ type: 'agent.started', cursor: 1, payload: {} })
    expect(useFlowStore.getState().selectedNode).not.toBeNull()

    await useFlowStore.getState().setActiveChannel('ch2')
    const s = useFlowStore.getState()
    expect(s.events).toEqual([])
    expect(s.selectedNode).toBeNull()
    expect(s.replayFromCursor).toBeNull()
    expect(s.liveEvents).toEqual([])
    expect(s.forks).toEqual([])
  })

  it('sets forkedAtCursor when fork info is supplied', async () => {
    mockReplayFor('new', [])
    await useFlowStore.getState().setActiveChannel('new', { parentChannelId: 'old', forkCursor: 7 })
    expect(useFlowStore.getState().forkedAtCursor).toBe(7)
  })

  it('inherits parent events up to the fork cursor', async () => {
    api.getChannelMetadata.mockResolvedValue({
      data: {
        channel_id: 'fork',
        parent_channel_id: 'root',
        origin_run_id: 'root',
        forked_at_cursor: 2,
        forked_at: '2026-01-01T00:00:00Z',
        display_name: null,
        reason: null,
        experiment_metadata: null,
      },
      status: 200,
    })
    mockReplayFor('root', [
      { id: 'r1', cursor: 1, event_type: 'agent.started', payload: {}, timestamp: '2026-01-01T00:00:00Z' },
      { id: 'r2', cursor: 2, event_type: 'tool.search', payload: {}, timestamp: '2026-01-01T00:00:01Z' },
      { id: 'r3', cursor: 3, event_type: 'agent.completed', payload: {}, timestamp: '2026-01-01T00:00:02Z' },
    ])
    mockReplayFor('fork', [{ id: 'f1', cursor: 1, event_type: 'experiment.started', payload: {}, timestamp: '2026-01-01T00:01:00Z' }])

    await useFlowStore.getState().setActiveChannel('fork')

    const types = useFlowStore.getState().events.map(e => e.type)
    // Parent events up to fork cursor (1,2) inherited, own fork events appended.
    expect(types).toEqual(['agent.started', 'tool.search', 'experiment.started'])
    const inherited = useFlowStore.getState().events.filter(e => e.isInherited)
    expect(inherited).toHaveLength(2)
  })

  it('dedupes events shared between parent and fork', async () => {
    api.getChannelMetadata.mockResolvedValue({
      data: {
        channel_id: 'fork',
        parent_channel_id: 'root',
        origin_run_id: 'root',
        forked_at_cursor: 1,
        forked_at: '2026-01-01T00:00:00Z',
        display_name: null,
        reason: null,
        experiment_metadata: null,
      },
      status: 200,
    })
    const shared = { id: 'e1', cursor: 1, event_type: 'agent.started', payload: {}, timestamp: '2026-01-01T00:00:00Z' }
    mockReplayFor('root', [shared])
    mockReplayFor('fork', [{ ...shared }])
    await useFlowStore.getState().setActiveChannel('fork')
    expect(useFlowStore.getState().events).toHaveLength(1)
  })

  it('surfaces load errors', async () => {
    api.replayEvents.mockRejectedValue(new Error('pg is down'))
    await useFlowStore.getState().setActiveChannel('ch')
    expect(useFlowStore.getState().error).toContain('pg is down')
    expect(useFlowStore.getState().loading).toBe(false)
  })
})

describe('replay cursor', () => {
  it('setReplayCursor reloads events', async () => {
    mockReplayFor('ch', [{ id: '1', cursor: 1, event_type: 'agent.started', payload: {}, timestamp: '2026-01-01T00:00:00Z' }])
    await useFlowStore.getState().setActiveChannel('ch')
    await useFlowStore.getState().setReplayCursor(5)
    expect(useFlowStore.getState().replayFromCursor).toBe(5)
    // reload happened (replay called again)
    const calls = api.replayEvents.mock.calls.length
    expect(calls).toBeGreaterThanOrEqual(2)
  })
})

describe('live mode', () => {
  it('setIsLive(true) enables live event collection', () => {
    useFlowStore.getState().setIsLive(true)
    useFlowStore.getState().addLiveEvent({
      id: 'e1', cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {},
    })
    expect(useFlowStore.getState().liveEvents).toHaveLength(1)
  })

  it('ignores events when not live', () => {
    useFlowStore.getState().addLiveEvent({
      id: 'e1', cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {},
    })
    expect(useFlowStore.getState().liveEvents).toHaveLength(0)
  })

  it('dedupes by id', () => {
    useFlowStore.getState().setIsLive(true)
    const evt = {
      id: 'e1', cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {},
    }
    const s = useFlowStore.getState()
    s.addLiveEvent(evt)
    s.addLiveEvent(evt)
    expect(useFlowStore.getState().liveEvents).toHaveLength(1)
  })

  it('skips live events at or below the replay cursor', () => {
    useFlowStore.getState().setIsLive(true)
    useFlowStore.setState({ replayFromCursor: 5 })
    const s = useFlowStore.getState()
    s.addLiveEvent({ id: 'old', cursor: 3, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} })
    expect(useFlowStore.getState().liveEvents).toHaveLength(0)
    expect(useFlowStore.getState().events).toHaveLength(0)
    s.addLiveEvent({ id: 'new', cursor: 7, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} })
    expect(useFlowStore.getState().liveEvents).toHaveLength(1)
    expect(useFlowStore.getState().events).toHaveLength(1)
  })

  it('caps the live buffer at 200 events, newest first', () => {
    useFlowStore.getState().setIsLive(true)
    const s = useFlowStore.getState()
    for (let i = 0; i < 250; i++) {
      s.addLiveEvent({ id: `e${i}`, cursor: i, type: 't', channel: 'ch', timestamp: new Date(), payload: {} })
    }
    expect(useFlowStore.getState().liveEvents).toHaveLength(200)
    expect(useFlowStore.getState().liveEvents[0].id).toBe('e249')
  })

  it('merges live events into the canvas timeline, sorted by cursor', () => {
    useFlowStore.getState().setIsLive(true)
    useFlowStore.setState({
      events: [{ id: 'r1', cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} }],
    })
    useFlowStore.getState().addLiveEvent({
      id: 'live-2', cursor: 2, type: 'tool.search', channel: 'ch', timestamp: new Date(), payload: {},
    })
    useFlowStore.getState().addLiveEvent({
      id: 'live-3', cursor: 3, type: 'agent.completed', channel: 'ch', timestamp: new Date(), payload: {},
    })
    expect(useFlowStore.getState().events.map(e => e.type)).toEqual([
      'agent.started', 'tool.search', 'agent.completed',
    ])
  })

  it('does not duplicate a live event already present by channel+cursor', () => {
    useFlowStore.getState().setIsLive(true)
    useFlowStore.setState({
      events: [{ id: 'r1', cursor: 5, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {} }],
    })
    useFlowStore.getState().addLiveEvent({
      id: 'live-1', cursor: 5, type: 'agent.started', channel: 'ch', timestamp: new Date(), payload: {},
    })
    expect(useFlowStore.getState().events).toHaveLength(1)
    expect(useFlowStore.getState().liveEvents).toHaveLength(1)
  })

  it('clears live events when live is disabled', () => {
    useFlowStore.getState().setIsLive(true)
    useFlowStore.getState().addLiveEvent({
      id: 'e1', cursor: 1, type: 't', channel: 'ch', timestamp: new Date(), payload: {},
    })
    useFlowStore.getState().setIsLive(false)
    expect(useFlowStore.getState().liveEvents).toHaveLength(0)
    expect(useFlowStore.getState().isLive).toBe(false)
  })
})

describe('connection status', () => {
  it('merges partial connection state', () => {
    useFlowStore.getState().setConnectionStatus({ connected: true })
    expect(useFlowStore.getState().connection).toMatchObject({ connected: true, authenticated: false })
    useFlowStore.getState().setConnectionStatus({ authenticated: true, error: null })
    expect(useFlowStore.getState().connection).toMatchObject({ connected: true, authenticated: true })
  })
})

describe('selected node', () => {
  it('sets inspectorOpen when a node is selected', () => {
    useFlowStore.getState().setSelectedNode({ type: 't', cursor: 1, payload: { x: 1 } })
    const s = useFlowStore.getState()
    expect(s.selectedNode).toMatchObject({ type: 't', cursor: 1 })
    expect(s.inspectorOpen).toBe(true)
  })

  it('reconstructs cumulative context from prior events', async () => {
    mockReplayFor('ch', [
      { id: '1', cursor: 1, event_type: 'step.a', payload: { context: { topic: 'AI' } }, timestamp: '2026-01-01T00:00:00Z' },
      { id: '2', cursor: 2, event_type: 'step.b', payload: { context: { depth: 3 } }, timestamp: '2026-01-01T00:00:01Z' },
    ])
    await useFlowStore.getState().setActiveChannel('ch')
    useFlowStore.getState().setSelectedNode({ type: 'step.b', cursor: 2, payload: { context: { depth: 3 } } })
    expect(useFlowStore.getState().selectedNode?.cumulativeContext).toEqual({ topic: 'AI', depth: 3 })
  })

  it('closes the inspector when selecting null', () => {
    useFlowStore.getState().setSelectedNode({ type: 't', cursor: 1, payload: {} })
    useFlowStore.getState().setSelectedNode(null)
    expect(useFlowStore.getState().inspectorOpen).toBe(false)
  })
})

describe('inspector controls', () => {
  it('setInspectorOpen(null node) clears selection', () => {
    useFlowStore.getState().setSelectedNode({ type: 't', cursor: 1, payload: {} })
    useFlowStore.getState().setInspectorOpen(false)
    expect(useFlowStore.getState().selectedNode).toBeNull()
  })

  it('clamps inspector width to 240..600', () => {
    useFlowStore.getState().setInspectorWidth(100)
    expect(useFlowStore.getState().inspectorWidth).toBe(240)
    useFlowStore.getState().setInspectorWidth(5000)
    expect(useFlowStore.getState().inspectorWidth).toBe(600)
    useFlowStore.getState().setInspectorWidth(400)
    expect(useFlowStore.getState().inspectorWidth).toBe(400)
  })
})

describe('layout mode', () => {
  it('switches layout mode', () => {
    useFlowStore.getState().setLayoutMode('compact')
    expect(useFlowStore.getState().layoutMode).toBe('compact')
    useFlowStore.getState().setLayoutMode('horizontal')
    expect(useFlowStore.getState().layoutMode).toBe('horizontal')
  })
})

describe('canvas controller', () => {
  it('stores and clears the canvas controller', () => {
    const ctrl = { zoomIn: () => {}, zoomOut: () => {}, fitView: () => {} }
    useFlowStore.getState().setCanvasController(ctrl)
    expect(useFlowStore.getState().canvasController).toBe(ctrl)
    useFlowStore.getState().setCanvasController(null)
    expect(useFlowStore.getState().canvasController).toBeNull()
  })
})

describe('visibleEventTypes filter', () => {
  const evts = (types: string[]) =>
    types.map((t, i) => ({
      cursor: i + 1,
      type: t,
      channel: 'ch',
      timestamp: new Date().toISOString(),
      payload: {},
    }))

  it('defaults to null (show everything)', () => {
    expect(useFlowStore.getState().visibleEventTypes).toBeNull()
  })

  it('hides framework-internal types by default when business events exist', async () => {
    mockChannelList(['ch'])
    mockReplayFor('ch', [
      { id: '1', cursor: 1, event_type: 'langgraph.checkpoint', payload: {}, timestamp: '2026-01-01T00:00:00Z' },
      { id: '2', cursor: 2, event_type: 'filter_source', payload: {}, timestamp: '2026-01-01T00:00:01Z' },
      { id: '3', cursor: 3, event_type: 'write_report', payload: {}, timestamp: '2026-01-01T00:00:02Z' },
    ])
    mockState(3, {})
    await useFlowStore.getState().setActiveChannel('ch')
    expect(useFlowStore.getState().visibleEventTypes).toEqual(new Set(['filter_source', 'write_report']))
  })

  it('shows everything when the run has only framework-internal events', async () => {
    mockChannelList(['ch'])
    mockReplayFor('ch', [
      { id: '1', cursor: 1, event_type: 'langgraph.checkpoint', payload: {}, timestamp: '2026-01-01T00:00:00Z' },
    ])
    mockState(1, {})
    await useFlowStore.getState().setActiveChannel('ch')
    expect(useFlowStore.getState().visibleEventTypes).toBeNull()
  })

  it('setVisibleEventTypes stores the set', () => {
    const set = new Set(['agent.started'])
    useFlowStore.getState().setVisibleEventTypes(set)
    expect(useFlowStore.getState().visibleEventTypes).toBe(set)
  })

  it('resetVisibleEventTypes restores show-everything', () => {
    useFlowStore.getState().setVisibleEventTypes(new Set(['tool.llm']))
    useFlowStore.getState().resetVisibleEventTypes()
    expect(useFlowStore.getState().visibleEventTypes).toBeNull()
  })

  it('first toggle hides only that type (starting from show-all)', () => {
    useFlowStore.setState({ events: evts(['agent.started', 'tool.llm', 'agent.completed']) })
    useFlowStore.getState().toggleVisibleEventType('tool.llm')
    expect(useFlowStore.getState().visibleEventTypes).toEqual(new Set(['agent.started', 'agent.completed']))
  })

  it('toggling the last hidden type back on restores show-all', () => {
    useFlowStore.setState({ events: evts(['agent.started', 'tool.llm']) })
    useFlowStore.setState({ visibleEventTypes: new Set(['agent.started']) })
    useFlowStore.getState().toggleVisibleEventType('tool.llm')
    expect(useFlowStore.getState().visibleEventTypes).toBeNull()
  })

  it('toggling a shown type off removes it from the selection', () => {
    useFlowStore.setState({ events: evts(['agent.started', 'tool.llm']) })
    useFlowStore.setState({ visibleEventTypes: new Set(['agent.started', 'tool.llm']) })
    useFlowStore.getState().toggleVisibleEventType('agent.started')
    expect(useFlowStore.getState().visibleEventTypes).toEqual(new Set(['tool.llm']))
  })

  it('keys state snapshots under the pseudo-type "state"', () => {
    useFlowStore.setState({
      events: [
        { cursor: 1, type: 'agent.started', channel: 'ch', timestamp: new Date().toISOString(), payload: {} },
        { cursor: 2, type: 'state', channel: 'ch', timestamp: new Date().toISOString(), payload: {}, isState: true, state: {} },
      ],
    })
    useFlowStore.getState().toggleVisibleEventType('state')
    expect(useFlowStore.getState().visibleEventTypes).toEqual(new Set(['agent.started']))
  })

  it('resets the filter when switching channels', async () => {
    useFlowStore.setState({ activeChannel: 'a', events: evts(['agent.started']) })
    useFlowStore.getState().setVisibleEventTypes(new Set(['agent.started']))
    await useFlowStore.getState().setActiveChannel('b')
    expect(useFlowStore.getState().visibleEventTypes).toBeNull()
  })
})

describe('visibleFields filter', () => {
  const evts = (payloads: Array<Record<string, unknown>>) =>
    payloads.map((payload, i) => ({
      cursor: i + 1,
      type: 'agent.started',
      channel: 'ch',
      timestamp: new Date().toISOString(),
      payload,
    }))

  it('defaults to null (show everything)', () => {
    expect(useFlowStore.getState().visibleFields).toBeNull()
  })

  it('setVisibleFields stores the set', () => {
    const set = new Set(['model'])
    useFlowStore.getState().setVisibleFields(set)
    expect(useFlowStore.getState().visibleFields).toBe(set)
  })

  it('resetVisibleFields restores show-everything', () => {
    useFlowStore.getState().setVisibleFields(new Set(['model']))
    useFlowStore.getState().resetVisibleFields()
    expect(useFlowStore.getState().visibleFields).toBeNull()
  })

  it('first toggle hides only that field (starting from show-all)', () => {
    useFlowStore.setState({
      events: evts([{ model: 'gpt-4', tokens: 1 }, { model: 'gpt-4', status: 'ok' }]),
    })
    useFlowStore.getState().toggleVisibleField('tokens')
    expect(useFlowStore.getState().visibleFields).toEqual(new Set(['model', 'status']))
  })

  it('toggling the last hidden field back on restores show-all', () => {
    useFlowStore.setState({ events: evts([{ model: 'gpt-4', tokens: 1 }]) })
    useFlowStore.setState({ visibleFields: new Set(['model']) })
    useFlowStore.getState().toggleVisibleField('tokens')
    expect(useFlowStore.getState().visibleFields).toBeNull()
  })

  it('toggling a shown field off removes it from the selection', () => {
    useFlowStore.setState({ events: evts([{ model: 'gpt-4', tokens: 1 }]) })
    useFlowStore.setState({ visibleFields: new Set(['model', 'tokens']) })
    useFlowStore.getState().toggleVisibleField('model')
    expect(useFlowStore.getState().visibleFields).toEqual(new Set(['tokens']))
  })

  it('resets the filter when switching channels', async () => {
    useFlowStore.setState({
      activeChannel: 'a',
      events: evts([{ model: 'gpt-4' }]),
    })
    useFlowStore.getState().setVisibleFields(new Set(['model']))
    await useFlowStore.getState().setActiveChannel('b')
    expect(useFlowStore.getState().visibleFields).toBeNull()
  })
})

describe('preferences persistence', () => {
  it('loadPreferences applies stored layout + filters', async () => {
    api.getPreferences.mockResolvedValue({
      data: {
        prefs: {
          flow: {
            visualizationMode: 'graph',
            layoutMode: 'compact',
            visibleEventTypes: ['agent.started', 'agent.completed'],
            visibleFields: ['model'],
          },
        },
      },
      status: 200,
    })
    await useFlowStore.getState().loadPreferences()
    expect(useFlowStore.getState().layoutMode).toBe('compact')
    expect(useFlowStore.getState().visualizationMode).toBe('graph')
    expect(useFlowStore.getState().visibleEventTypes).toEqual(new Set(['agent.started', 'agent.completed']))
    expect(useFlowStore.getState().visibleFields).toEqual(new Set(['model']))
  })

  it('loadPreferences treats empty arrays as show-all (null)', async () => {
    api.getPreferences.mockResolvedValue({
      data: {
        prefs: { flow: { layoutMode: 'grid', visibleEventTypes: [], visibleFields: [] } },
      },
      status: 200,
    })
    await useFlowStore.getState().loadPreferences()
    expect(useFlowStore.getState().layoutMode).toBe('grid')
    expect(useFlowStore.getState().visibleEventTypes).toBeNull()
    expect(useFlowStore.getState().visibleFields).toBeNull()
  })

  it('loadPreferences is a no-op on failure (never blocks the view)', async () => {
    api.getPreferences.mockResolvedValue({ status: 0, error: 'network' })
    await expect(useFlowStore.getState().loadPreferences()).resolves.toBeUndefined()
    expect(useFlowStore.getState().visibleEventTypes).toBeNull()
  })

  it('saves the flow view as a flow preference object', async () => {
    vi.useFakeTimers()
    try {
      useFlowStore.getState().setVisibleEventTypes(new Set(['agent.started']))
      useFlowStore.getState().setLayoutMode('horizontal')
      expect(api.putPreferences).not.toHaveBeenCalled()
      await vi.advanceTimersByTimeAsync(300)
      expect(api.putPreferences).toHaveBeenCalledTimes(1)
      const flow = api.putPreferences.mock.calls[0][0].flow
      expect(flow).toEqual({
        layoutMode: 'horizontal',
        visualizationMode: 'timeline',
        visibleEventTypes: ['agent.started'],
        visibleFields: [],
      })
    } finally {
      vi.useRealTimers()
    }
  })

  it('toggle + reset persist through savePreferences (debounced)', async () => {
    const evts = (types: string[]) =>
      types.map((type, i) => ({
        cursor: i + 1,
        type,
        channel: 'ch',
        timestamp: new Date().toISOString(),
        payload: {},
      }))
    vi.useFakeTimers()
    try {
      useFlowStore.setState({ events: evts(['agent.started', 'tool.llm']) })
      useFlowStore.getState().toggleVisibleEventType('tool.llm')
      useFlowStore.getState().resetVisibleEventTypes()
      // Two mutations coalesce into a single PUT carrying the final state.
      await vi.advanceTimersByTimeAsync(300)
      expect(api.putPreferences).toHaveBeenCalledTimes(1)
      expect(api.putPreferences.mock.calls[0][0].flow.visibleEventTypes).toEqual([])
    } finally {
      vi.useRealTimers()
    }
  })
})

describe('demo replay injection', () => {
  const ev = (over: Partial<{ id: string; channel: string; cursor: number; type: string }> = {}) => ({
    id: over.id ?? 'demo-1',
    channel: over.channel ?? 'demo-live-run',
    cursor: over.cursor ?? 1,
    type: over.type ?? 'agent.started',
    timestamp: '2026-07-25T09:00:00Z',
    payload: {},
  })

  it('injects without a live connection and flags demoLive', () => {
    const store = useFlowStore.getState()
    expect(store.isLive).toBe(false)
    store.injectDemoEvent(ev())
    const after = useFlowStore.getState()
    expect(after.demoLive).toBe(true)
    expect(after.events).toHaveLength(1)
    expect(after.liveEvents).toHaveLength(1)
  })

  it('dedupes by channel + cursor and by live id', () => {
    const store = useFlowStore.getState()
    store.injectDemoEvent(ev())
    store.injectDemoEvent(ev())
    const after = useFlowStore.getState()
    expect(after.events).toHaveLength(1)
    expect(after.liveEvents).toHaveLength(1)
  })

  it('resetDemoReplay drops the demo channels and clears the live feed', () => {
    const store = useFlowStore.getState()
    store.injectDemoEvent(ev({ channel: 'demo-live-run', cursor: 1 }))
    store.injectDemoEvent(ev({ id: 'demo-2', channel: 'demo-live-run-fork', cursor: 1 }))
    store.resetDemoReplay(['demo-live-run', 'demo-live-run-fork'])
    const after = useFlowStore.getState()
    expect(after.events).toHaveLength(0)
    expect(after.liveEvents).toHaveLength(0)
    expect(after.demoLive).toBe(false)
  })
})
