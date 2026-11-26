// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { create } from 'zustand'
import {
  listChannels,
  replayEvents,
  loadState,
  listForks,
  getChannelMetadata,
  getPreferences,
  putPreferences,
  type ChannelMetadata,
} from './api'
import {
  resolveAncestry,
  loadInheritedTimeline,
  paginateReplay,
  mergeEvents,
  reconstructContext,
} from './flow-helpers'
import { isFrameworkInternalType } from './execution-timeline'

const MOCK_MODE = process.env.NEXT_PUBLIC_MOCK_MODE === 'true'

export interface DashboardEvent {
  id?: string
  cursor: number
  type: string
  channel: string
  timestamp: Date | string
  payload: Record<string, unknown>
  isState?: boolean
  state?: Record<string, unknown>
  isInherited?: boolean
  forkedFrom?: string | null
}

interface FlowState {
  channels: string[]
  activeChannel: string | null
  events: DashboardEvent[]
  stateSnapshot: { cursor: number; state: Record<string, unknown> } | null
  loading: boolean
  error: string | null
  isLive: boolean
  liveEvents: DashboardEvent[]
  /** Mock-mode demo replay is streaming (no WebSocket). */
  demoLive: boolean
  connection: {
    connected: boolean
    authenticated: boolean
    error: string | null
  }
  selectedNode: { type: string; cursor: number; channel?: string | null; payload?: any; state?: any; cumulativeContext?: Record<string, unknown> } | null
  replayFromCursor: number | null
  inspectorOpen: boolean
  inspectorWidth: number
  visualizationMode: 'timeline' | 'graph'
  layoutMode: 'horizontal' | 'grid' | 'compact'
  forks: ChannelMetadata[]
  forkedAtCursor: number | null
  canvasController: {
    zoomIn: () => void
    zoomOut: () => void
    fitView: () => void
  } | null
  /** Event-type filter for the flow canvas. `null` = show everything. */
  visibleEventTypes: Set<string> | null
  /** Payload-field filter (which payload keys to display). `null` = all. */
  visibleFields: Set<string> | null

  // Actions
  /** Fetch the current identity's stored preferences and apply the flow
   * view state (layout + filters). Server round-trip; ignores failures. */
  loadPreferences: () => Promise<void>
  /** Persist the flow view state to the user's preferences. Debounced. */
  savePreferences: () => void
  loadChannels: () => Promise<void>
  /** Poll `loadChannels()` every `intervalMs` (default 15s) so channels and
   * forks created by SDK code appear without a page reload. */
  startChannelPolling: (intervalMs?: number) => void
  stopChannelPolling: () => void
  setActiveChannel: (channel: string | null, forkInfo?: { parentChannelId: string; forkCursor: number }) => Promise<void>
  loadEvents: (forkInfo?: { parentChannelId: string; forkCursor: number }) => Promise<void>
  setReplayCursor: (cursor: number | null) => Promise<void>
  setIsLive: (live: boolean) => void
  addLiveEvent: (event: DashboardEvent) => void
  /** Inject a demo-replay event regardless of `isLive` (no WebSocket needed):
   * merges it into the timeline and the live feed, and flags `demoLive`. */
  injectDemoEvent: (event: DashboardEvent) => void
  /** Clear the demo replay: drop the demo channels' events + the live feed. */
  resetDemoReplay: (channels: string[]) => void
  setConnectionStatus: (status: Partial<FlowState['connection']>) => void
  setError: (error: string | null) => void
  setSelectedNode: (node: FlowState['selectedNode']) => void
  setInspectorOpen: (open: boolean) => void
  setInspectorWidth: (width: number) => void
  setVisualizationMode: (mode: 'timeline' | 'graph') => void
  setLayoutMode: (mode: 'horizontal' | 'grid' | 'compact') => void
  loadForks: () => Promise<void>
  setCanvasController: (controller: FlowState['canvasController']) => void
  setVisibleEventTypes: (types: Set<string> | null) => void
  toggleVisibleEventType: (type: string) => void
  resetVisibleEventTypes: () => void
  setVisibleFields: (fields: Set<string> | null) => void
  toggleVisibleField: (field: string) => void
  resetVisibleFields: () => void
}

export const useFlowStore = create<FlowState>((set, get) => {
  let pollTimer: ReturnType<typeof setInterval> | null = null
  let prefsTimer: ReturnType<typeof setTimeout> | null = null
  return {
  channels: [],
  activeChannel: null,
  events: [],
  stateSnapshot: null,
  loading: false,
  error: null,
  isLive: true,
  liveEvents: [],
  demoLive: false,
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

  loadChannels: async () => {
    if (MOCK_MODE) {
      const { MOCK_CHANNELS } = await import('./mock-data')
      set({ channels: [...MOCK_CHANNELS] })
      if (!get().activeChannel && MOCK_CHANNELS.length > 0) {
        await get().setActiveChannel(MOCK_CHANNELS[0])
      }
      return
    }
    const { data } = await listChannels()
    const channelList = (data as any)?.channels ?? data
    let normalized: string[] = []
    if (Array.isArray(channelList)) {
      normalized = channelList.map((c: any) => typeof c === 'string' ? c : c.id || c.channel_id)
    } else if (Array.isArray(data)) {
      normalized = data.map((c: any) => typeof c === 'string' ? c : c.id || c.channel_id)
    }
    set({ channels: normalized })
    // Auto-select the most recently active channel (server returns them
    // ordered by last activity) so the flow page shows data immediately.
    if (!get().activeChannel && normalized.length > 0) {
      await get().setActiveChannel(normalized[0])
    }
  },

  startChannelPolling: (intervalMs = 15000) => {
    if (pollTimer) return
    pollTimer = setInterval(() => {
      // Quiet refresh: loadChannels only updates the list and auto-selects
      // when nothing is active — it never resets the current channel,
      // events or selection.
      void get().loadChannels()
    }, intervalMs)
  },

  stopChannelPolling: () => {
    if (pollTimer) {
      clearInterval(pollTimer)
      pollTimer = null
    }
  },

  setActiveChannel: async (channel, forkInfo?) => {
    const state = get()
    if (channel === state.activeChannel) return
    set({
      activeChannel: channel,
      events: [],
      stateSnapshot: null,
      selectedNode: null,
      replayFromCursor: null,
      liveEvents: [],
      error: null,
      forks: [],
      forkedAtCursor: forkInfo?.forkCursor ?? null,
      visibleEventTypes: null,
      visibleFields: null,
    })
    if (channel) {
      await get().loadEvents(forkInfo)
      await get().loadForks()
    }
  },

  loadEvents: async (forkInfo?) => {
    const { activeChannel, channels, replayFromCursor } = get()
    if (!activeChannel) return
    set({ loading: true, error: null, forkedAtCursor: null })

    try {
      // Build the FULL ancestry chain (immediate → root) in one pass. forkInfo
      // supplies the immediate parent directly after a fork to avoid a
      // metadata lookup race; the rest of the chain is resolved from the
      // parent so nested forks keep their complete chronology.
      const ancestry = forkInfo
        ? [
            { channelId: forkInfo.parentChannelId, forkCursor: forkInfo.forkCursor },
            ...(await resolveAncestry(forkInfo.parentChannelId, getChannelMetadata)),
          ]
        : await resolveAncestry(activeChannel, getChannelMetadata)

      const forkCursor = ancestry[0]?.forkCursor ?? null

      // Single-pass inherited timeline: each ancestor replayed exactly once
      // for the segment it contributes, oldest first.
      const inheritedEvents = await loadInheritedTimeline(
        ancestry,
        replayEvents,
        getChannelMetadata
      )

      // Load the channel's own events. For a brand-new fork this will be empty;
      // for an active experiment it contains the divergent path.
      const ownEvents = await paginateReplay(activeChannel, replayEvents, {
        fromCursor: replayFromCursor ?? 0,
        limit: 1000,
      })

      const merged = mergeEvents(inheritedEvents, ownEvents)

      // Load the latest state snapshot for the active channel.
      const { data: stateData } = await loadState(activeChannel)
      const snap = stateData?.state
        ? { cursor: stateData.cursor, state: stateData.state }
        : null

      set({
        events: merged,
        stateSnapshot: snap,
        loading: false,
        forkedAtCursor: forkCursor,
        channels: MOCK_MODE && channels.length === 0 ? (await import('./mock-data')).MOCK_CHANNELS : channels,
      })

      // Readability default: when the run has business events AND framework
      // internals (e.g. LangGraph checkpoints), open with the internals hidden
      // so the timeline reads as the agent's steps. Never applied when the user
      // already chose a filter, and never leaves the timeline empty.
      if (get().visibleEventTypes === null) {
        const present = new Set(merged.map(e => (e.isState ? 'state' : e.type || 'unknown')))
        const business = Array.from(present).filter(t => !isFrameworkInternalType(t))
        if (business.length > 0 && business.length < present.size) {
          set({ visibleEventTypes: new Set(business) })
        }
      }
    } catch (e: any) {
      set({ error: e.message || 'Failed to load events', loading: false })
    }
  },

  loadForks: async () => {
    const { activeChannel } = get()
    if (!activeChannel) return
    try {
      const res = await listForks(activeChannel)
      const forks = res.data?.forks || []
      set({ forks })
    } catch (e: any) {
      console.warn('Failed to load forks:', e.message)
    }
  },

  setReplayCursor: async (cursor) => {
    set({ replayFromCursor: cursor })
    if (get().activeChannel) {
      await get().loadEvents()
    }
  },

  setIsLive: (live) => {
    set({ isLive: live })
    if (!live) {
      set({ liveEvents: [] })
    }
  },

  addLiveEvent: (event) => {
    const state = get()
    if (!state.isLive) return
    // A replay-filtered timeline hides events at or below the replay cursor;
    // a live event inside that hidden window (only possible if the WS ever
    // replays history) must not resurface in the canvas or the live bar.
    if (state.replayFromCursor !== null && event.cursor <= state.replayFromCursor) return
    // Deduplicate the live feed (by id)
    const exists = state.liveEvents.some(e => e.id === event.id)
    const feed = exists
      ? state.liveEvents
      : [event, ...state.liveEvents].slice(0, 200)
    set({ liveEvents: feed })

    // Merge into the canvas timeline too, so the flow updates without a
    // refresh. Skip if the event's channel+cursor is already present
    // (matches mergeEvents' fallback key for replayed events).
    const alreadyPresent = state.events.some(
      e => e.channel === event.channel && e.cursor === event.cursor
    )
    if (alreadyPresent) return
    const merged = [...state.events, event].sort((a, b) => a.cursor - b.cursor)
    set({ events: merged })
  },

  injectDemoEvent: (event) => {
    const state = get()
    // Intentionally independent of `isLive`: the demo replay is a scripted
    // stream, not a WebSocket connection. Mirror addLiveEvent's merge/dedupe.
    const feed = state.liveEvents.some(e => e.id === event.id)
      ? state.liveEvents
      : [event, ...state.liveEvents].slice(0, 200)
    const alreadyPresent = state.events.some(
      e => e.channel === event.channel && e.cursor === event.cursor
    )
    const events = alreadyPresent ? state.events : [...state.events, event]
    set({ liveEvents: feed, events, demoLive: true })
  },

  resetDemoReplay: (channels) => {
    const demo = new Set(channels)
    set(state => ({
      events: state.events.filter(e => !demo.has(e.channel)),
      liveEvents: [],
      demoLive: false,
    }))
  },

  setConnectionStatus: (status) => {
    set((s) => ({ connection: { ...s.connection, ...status } }))
  },

  setError: (error) => {
    set({ error })
  },

  setSelectedNode: (node) => {
    if (node && node.payload?.context) {
      const cumulative = reconstructContext(get().events, node.cursor)
      set({ selectedNode: { ...node, cumulativeContext: cumulative }, inspectorOpen: true })
    } else if (node && node.state) {
      set({ selectedNode: node, inspectorOpen: true })
    } else {
      set({ selectedNode: node, inspectorOpen: node !== null })
    }
  },

  setInspectorOpen: (open) => {
    set({ inspectorOpen: open })
    if (!open) set({ selectedNode: null })
  },

  setInspectorWidth: (width) => {
    set({ inspectorWidth: Math.max(240, Math.min(600, width)) })
  },

  setVisualizationMode: (mode) => {
    set({ visualizationMode: mode })
    get().savePreferences()
  },

  setLayoutMode: (mode) => {
    set({ layoutMode: mode })
    get().savePreferences()
  },

  setCanvasController: (controller) => {
    set({ canvasController: controller })
  },

  loadPreferences: async () => {
    if (MOCK_MODE) return
    try {
      const { data } = await getPreferences()
      const prefs = (data as any)?.prefs ?? {}
      const flow = prefs?.flow ?? {}
      const next: Partial<FlowState> = {}
      if (flow.visualizationMode === 'timeline' || flow.visualizationMode === 'graph') {
        next.visualizationMode = flow.visualizationMode
      }
      if (flow.layoutMode === 'horizontal' || flow.layoutMode === 'grid' || flow.layoutMode === 'compact') {
        next.layoutMode = flow.layoutMode
      }
      if (Array.isArray(flow.visibleEventTypes)) {
        next.visibleEventTypes = (flow.visibleEventTypes as string[]).length > 0
          ? new Set(flow.visibleEventTypes as string[])
          : null
      }
      if (Array.isArray(flow.visibleFields)) {
        next.visibleFields = (flow.visibleFields as string[]).length > 0
          ? new Set(flow.visibleFields as string[])
          : null
      }
      if (Object.keys(next).length > 0) set(next)
    } catch {
      // Never block the flow view on a preference fetch failure.
    }
  },

  savePreferences: () => {
    if (MOCK_MODE) return
    if (prefsTimer) clearTimeout(prefsTimer)
    // Debounce: coalesce rapid filter/layout changes into a single PUT and
    // only snapshot the state when the user pauses (avoids out-of-order
    // PUT completions persisting stale state).
    prefsTimer = setTimeout(() => {
      prefsTimer = null
      const { visibleEventTypes, visibleFields, layoutMode, visualizationMode } = get()
      const flow: Record<string, unknown> = { layoutMode, visualizationMode }
      flow.visibleEventTypes = visibleEventTypes ? Array.from(visibleEventTypes) : []
      flow.visibleFields = visibleFields ? Array.from(visibleFields) : []
      putPreferences({ flow }).catch(() => {
        // Best-effort persistence — a failed save is non-fatal.
      })
    }, 300)
  },

  setVisibleEventTypes: (types) => {
    set({ visibleEventTypes: types })
    get().savePreferences()
  },

  toggleVisibleEventType: (type) => {
    const all = new Set(get().events.map(e => (e.isState ? 'state' : e.type || 'unknown')))
    const current = get().visibleEventTypes
    if (current === null) {
      // First toggle starts from "all shown": hide only this type.
      all.delete(type)
      set({ visibleEventTypes: all.size === 0 ? null : all })
      get().savePreferences()
      return
    }
    const next = new Set(current)
    if (next.has(type)) next.delete(type)
    else next.add(type)
    // Re-adding the last hidden type restores "show everything".
    const showsAll = next.size === all.size && Array.from(all).every(t => next.has(t))
    set({ visibleEventTypes: showsAll ? null : next })
    get().savePreferences()
  },

  resetVisibleEventTypes: () => {
    set({ visibleEventTypes: null })
    get().savePreferences()
  },

  setVisibleFields: (fields) => {
    set({ visibleFields: fields })
    get().savePreferences()
  },

  toggleVisibleField: (field) => {
    const all = new Set(
      get().events.flatMap(e => Object.keys(e.payload ?? {}))
    )
    const current = get().visibleFields
    if (current === null) {
      // First toggle starts from "all shown": hide only this field.
      all.delete(field)
      set({ visibleFields: all.size === 0 ? null : all })
      get().savePreferences()
      return
    }
    const next = new Set(current)
    if (next.has(field)) next.delete(field)
    else next.add(field)
    // Re-adding the last hidden field restores "show everything".
    const showsAll = next.size === all.size && Array.from(all).every(f => next.has(f))
    set({ visibleFields: showsAll ? null : next })
    get().savePreferences()
  },

  resetVisibleFields: () => {
    set({ visibleFields: null })
    get().savePreferences()
  },
  }
})
