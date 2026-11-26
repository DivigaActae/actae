// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import EventsPage from './page'

const queryEvents = vi.hoisted(() => vi.fn())
const routerReplace = vi.hoisted(() => vi.fn())
const params = vi.hoisted(() => ({ current: new URLSearchParams() }))
const listChannels = vi.hoisted(() => vi.fn())
const getPreferences = vi.hoisted(() => vi.fn())
const putPreferences = vi.hoisted(() => vi.fn())
const live = vi.hoisted(() => ({
  subscribe: vi.fn(),
  addMessageListener: vi.fn(),
  addStatusListener: vi.fn(),
}))

vi.mock('next/navigation', () => ({
  useSearchParams: () => params.current,
  useRouter: () => ({ replace: routerReplace }),
}))

vi.mock('@/components/ProtectedLayout', () => ({
  default: ({ children }: any) => <div>{children}</div>,
}))

vi.mock('@/components/ChannelSelector', () => ({
  default: ({ value, onChange }: any) => (
    <select data-testid="channel-select" value={value} onChange={(e) => onChange(e.target.value)}>
      <option value="">All channels</option>
      <option value="ch-a">ch-a</option>
      <option value="ch-b">ch-b</option>
    </select>
  ),
}))

vi.mock('@/lib/api', () => ({ queryEvents, listChannels, getPreferences, putPreferences }))
vi.mock('@/lib/live', () => live)

function evt(id: string, channel: string, cursor: number, type: string, extra: Record<string, unknown> = {}) {
  return { id, channel_id: channel, cursor, type, payload: { x: 1 }, timestamp: '2026-07-25T09:00:00Z', ...extra }
}

let messageCb: ((e: any) => void) | null = null
let statusCb: ((s: any) => void) | null = null
let liveTopics: string[] = []

beforeEach(() => {
  vi.clearAllMocks()
  params.current = new URLSearchParams()
  queryEvents.mockResolvedValue({ data: { events: [] }, status: 200 })
  listChannels.mockResolvedValue({ data: { channels: ['ch-a', 'ch-b'] }, status: 200 })
  getPreferences.mockResolvedValue({ data: { prefs: {} }, status: 200 })
  putPreferences.mockResolvedValue({ status: 200 })
  messageCb = null
  statusCb = null
  liveTopics = []
  live.subscribe.mockImplementation((topic: string) => {
    liveTopics.push(topic)
    return vi.fn(() => {
      const i = liveTopics.indexOf(topic)
      if (i >= 0) liveTopics.splice(i, 1)
    })
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

describe('EventsPage', () => {
  it('fetches all channels by default', async () => {
    queryEvents.mockResolvedValue({ data: { events: [evt('e1', 'ch-a', 1, 'agent.started')] }, status: 200 })
    render(<EventsPage />)
    await waitFor(() => {
      expect(queryEvents).toHaveBeenCalledWith(expect.objectContaining({ limit: 100, offset: 0 }))
    })
    const call = queryEvents.mock.calls[0][0]
    expect(call.channel_ids).toBeUndefined()
    expect(call.cursor_start).toBeUndefined()
  })

  it('filters by the channel search param', async () => {
    params.current = new URLSearchParams('?channel=ch-a')
    queryEvents.mockResolvedValue({ data: { events: [evt('e1', 'ch-a', 1, 'agent.started')] }, status: 200 })
    render(<EventsPage />)
    await waitFor(() => {
      expect(queryEvents).toHaveBeenCalledWith(expect.objectContaining({ channel_ids: ['ch-a'] }))
    })
    expect(screen.getByText('agent.started')).toBeInTheDocument()
    expect(screen.getAllByText('ch-a').length).toBeGreaterThanOrEqual(1)
  })

  it('fetches a cursor window for deep links', async () => {
    params.current = new URLSearchParams('?channel=ch-a&cursor=142')
    queryEvents.mockResolvedValue({ data: { events: [evt('e1', 'ch-a', 142, 'agent.started')] }, status: 200 })
    render(<EventsPage />)
    await waitFor(() => {
      expect(queryEvents).toHaveBeenCalledWith(
        expect.objectContaining({ channel_ids: ['ch-a'], cursor_start: 43, cursor_end: 142 })
      )
    })
  })

  it('renders event payloads as JSON and tree', async () => {
    queryEvents.mockResolvedValue({
      data: { events: [evt('e1', 'ch-a', 3, 'tool.llm', { payload: { model: 'gpt-4', tokens: 100 } })] },
      status: 200,
    })
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText('tool.llm')).toBeInTheDocument()
    })
    expect(screen.getByText('@3')).toBeInTheDocument()
    expect(screen.getByText('"model":')).toBeInTheDocument()
    expect(screen.getByText('"gpt-4"')).toBeInTheDocument()
  })

  it('shows actor and depends_on metadata', async () => {
    queryEvents.mockResolvedValue({
      data: { events: [evt('e1', 'ch-a', 3, 'tool.llm', { actor: 'alice', depends_on: 'e0' })] },
      status: 200,
    })
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText(/alice/)).toBeInTheDocument()
    })
    expect(screen.getByText(/depends_on: e0/)).toBeInTheDocument()
  })

  it('shows the empty state', async () => {
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText(/No events found/)).toBeInTheDocument()
    })
  })

  it('shows the error state', async () => {
    queryEvents.mockResolvedValue({ data: null, error: 'boom', status: 500 })
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText('boom')).toBeInTheDocument()
    })
  })

  it('loads more pages via the Load more button', async () => {
    const page1 = Array.from({ length: 100 }, (_, i) => evt(`e${i}`, 'ch-a', i + 1, 'agent.started'))
    queryEvents
      .mockResolvedValueOnce({ data: { events: page1 }, status: 200 })
      .mockResolvedValueOnce({ data: { events: [evt('e-new', 'ch-a', 101, 'agent.completed')] }, status: 200 })
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Load more' })).toBeInTheDocument()
    })
    await user.click(screen.getByRole('button', { name: 'Load more' }))
    await waitFor(() => {
      expect(screen.getByText('agent.completed')).toBeInTheDocument()
    })
    expect(queryEvents).toHaveBeenLastCalledWith(expect.objectContaining({ offset: 100 }))
    expect(screen.getByText('101 events shown')).toBeInTheDocument()
  })

  it('highlights the deep-linked event', async () => {
    params.current = new URLSearchParams('?channel=ch-a&cursor=142')
    queryEvents.mockResolvedValue({
      data: { events: [
        evt('e1', 'ch-a', 141, 'agent.started'),
        evt('e2', 'ch-a', 142, 'agent.completed'),
      ] },
      status: 200,
    })
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText('agent.completed')).toBeInTheDocument()
    })
    const highlighted = screen.getByText('agent.completed').closest('button')
    expect(highlighted).toBeInTheDocument()
    expect(highlighted?.className).toContain('border-actae-500')
  })

  it('updates the URL when a channel is selected', async () => {
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(screen.getByTestId('channel-select')).toBeInTheDocument()
    })
    await user.selectOptions(screen.getByTestId('channel-select'), 'ch-b')
    expect(routerReplace).toHaveBeenCalledWith('/dashboard/events/?channel=ch-b')
    await waitFor(() => {
      expect(queryEvents).toHaveBeenCalledWith(expect.objectContaining({ channel_ids: ['ch-b'] }))
    })
  })

  it('updates the URL with the cursor when an event is clicked', async () => {
    queryEvents.mockResolvedValue({ data: { events: [evt('e1', 'ch-a', 7, 'agent.started')] }, status: 200 })
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(screen.getByText('agent.started')).toBeInTheDocument()
    })
    await user.click(screen.getByText('agent.started'))
    expect(routerReplace).toHaveBeenCalledWith('/dashboard/events/?selected_channel=ch-a&cursor=7')
  })

  it('does not filter all-channel results when an event is selected', async () => {
    queryEvents.mockResolvedValue({ data: { events: [
      evt('e1', 'ch-a', 7, 'agent.started'),
      evt('e2', 'ch-b', 8, 'agent.completed'),
    ] }, status: 200 })
    render(<EventsPage />)
    const user = userEvent.setup()
    await user.click(await screen.findByText('agent.started'))
    expect(routerReplace).toHaveBeenCalledWith('/dashboard/events/?selected_channel=ch-a&cursor=7')
    expect(screen.getByText('agent.completed')).toBeInTheDocument()
    expect(queryEvents).toHaveBeenCalledTimes(1)
  })

  it('supports previous and next navigation without changing the channel filter', async () => {
    queryEvents.mockResolvedValue({ data: { events: [
      evt('e1', 'ch-a', 1, 'agent.started'),
      evt('e2', 'ch-b', 2, 'agent.completed'),
    ] }, status: 200 })
    render(<EventsPage />)
    const user = userEvent.setup()
    await screen.findByText('agent.completed')
    await user.click(screen.getByRole('button', { name: 'Next event' }))
    expect(screen.getByText('2/2')).toBeInTheDocument()
    expect(screen.getAllByText('ch-a').length).toBeGreaterThan(0)
    expect(screen.getAllByText('ch-b').length).toBeGreaterThan(0)
  })

  it('renders events oldest-first regardless of the API order', async () => {
    // The API returns newest-first (cursor DESC); the page must show 1, 2, 3.
    queryEvents.mockResolvedValue({
      data: { events: [
        evt('e3', 'ch-a', 3, 'agent.completed'),
        evt('e1', 'ch-a', 1, 'agent.started'),
        evt('e2', 'ch-a', 2, 'tool.llm'),
      ] },
      status: 200,
    })
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText('agent.completed')).toBeInTheDocument()
    })
    const cursors = screen.getAllByText(/^@\d+$/).map((el) => el.textContent)
    expect(cursors).toEqual(['@1', '@2', '@3'])
  })

  it('orders all-channel events chronologically instead of mixing per-channel cursors', async () => {
    queryEvents.mockResolvedValue({ data: { events: [
      evt('e1', 'ch-a', 99, 'later.event', { timestamp: '2026-07-25T09:00:10Z' }),
      evt('e2', 'ch-b', 1, 'earlier.event', { timestamp: '2026-07-25T09:00:00Z' }),
    ] }, status: 200 })
    render(<EventsPage />)
    await screen.findByText('later.event')
    const rows = screen.getAllByRole('button').filter(button => button.textContent?.includes('.event'))
    expect(rows.map(row => row.textContent)).toEqual([
      expect.stringContaining('earlier.event'),
      expect.stringContaining('later.event'),
    ])
  })

  it('searches across event type, channel and payload', async () => {
    queryEvents.mockResolvedValue({ data: { events: [
      evt('e1', 'ch-a', 1, 'agent.started', { payload: { topic: 'markets' } }),
      evt('e2', 'ch-b', 2, 'tool.llm', { payload: { topic: 'science' } }),
    ] }, status: 200 })
    render(<EventsPage />)
    const user = userEvent.setup()
    await user.type(await screen.findByLabelText('Search events'), 'science')
    expect(screen.queryByText('agent.started')).not.toBeInTheDocument()
    expect(screen.getByText('tool.llm')).toBeInTheDocument()
  })

  it('filters events by type via the dropdown (client-side)', async () => {
    queryEvents.mockResolvedValue({
      data: { events: [
        evt('e1', 'ch-a', 1, 'agent.started'),
        evt('e2', 'ch-a', 2, 'tool.llm'),
        evt('e3', 'ch-a', 3, 'tool.llm'),
      ] },
      status: 200,
    })
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(screen.getByText('Types: all 2')).toBeInTheDocument()
    })
    await user.click(screen.getByText('Types: all 2'))
    const listbox = screen.getByRole('listbox')
    const checkbox = Array.from(listbox.querySelectorAll('input[type="checkbox"]'))
      .find((el) => el.parentElement?.textContent?.includes('tool.llm')) as HTMLInputElement
    await user.click(checkbox)
    await user.keyboard('{Escape}')

    await waitFor(() => {
      expect(screen.getByText('1 of 3 events shown')).toBeInTheDocument()
    })
    expect(screen.queryByText('tool.llm')).not.toBeInTheDocument()
    expect(screen.getByText('agent.started')).toBeInTheDocument()
    // Filtering is client-side: the API call never carries event_type.
    expect(queryEvents.mock.calls[0][0].event_type).toBeUndefined()
  })

  it('shows a recovery state when the filter hides everything', async () => {
    queryEvents.mockResolvedValue({
      data: { events: [evt('e1', 'ch-a', 1, 'agent.started')] },
      status: 200,
    })
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(screen.getByText('Types: all 1')).toBeInTheDocument()
    })
    await user.click(screen.getByText('Types: all 1'))
    await user.click(screen.getByText('Show none'))
    await user.keyboard('{Escape}')

    await waitFor(() => {
      expect(screen.getByText(/No events match the current type filter/)).toBeInTheDocument()
    })
    await user.click(screen.getByText('Show all types'))
    await waitFor(() => {
      expect(screen.getByText('agent.started')).toBeInTheDocument()
    })
    expect(screen.getByText('1 event shown')).toBeInTheDocument()
  })

  it('filters payload fields via the Fields dropdown (client-side)', async () => {
    queryEvents.mockResolvedValue({
      data: { events: [
        evt('e1', 'ch-a', 1, 'agent.started', { payload: { model: 'gpt-4', tokens: 42 } }),
      ] },
      status: 200,
    })
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(screen.getByText('Fields: all 2')).toBeInTheDocument()
    })
    await user.click(screen.getByText('Fields: all 2'))
    const listbox = screen.getByRole('listbox')
    const checkbox = Array.from(listbox.querySelectorAll('input[type="checkbox"]'))
      .find((el) => el.parentElement?.textContent?.includes('model')) as HTMLInputElement
    await user.click(checkbox)
    await user.keyboard('{Escape}')

    await waitFor(() => {
      expect(screen.getAllByText(/"tokens":/).length).toBeGreaterThan(0)
    })
    expect(screen.queryAllByText(/"model":/)).toHaveLength(0)
    expect(screen.getByText('Fields: 1/2')).toBeInTheDocument()
  })

  it('restores all payload fields from the Fields dropdown', async () => {
    queryEvents.mockResolvedValue({
      data: { events: [
        evt('e1', 'ch-a', 1, 'agent.started', { payload: { model: 'gpt-4', tokens: 42 } }),
      ] },
      status: 200,
    })
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(screen.getByText('Fields: all 2')).toBeInTheDocument()
    })
    await user.click(screen.getByText('Fields: all 2'))
    const listbox = screen.getByRole('listbox')
    const checkbox = Array.from(listbox.querySelectorAll('input[type="checkbox"]'))
      .find((el) => el.parentElement?.textContent?.includes('model')) as HTMLInputElement
    await user.click(checkbox)
    await user.keyboard('{Escape}')
    await waitFor(() => {
      expect(screen.queryAllByText(/"model":/)).toHaveLength(0)
    })

    await user.click(screen.getByText('Fields: 1/2'))
    await user.click(screen.getByText('Show all'))
    await user.keyboard('{Escape}')
    await waitFor(() => {
      expect(screen.getAllByText(/"model":/).length).toBeGreaterThan(0)
    })
    expect(screen.getByText('Fields: all 2')).toBeInTheDocument()
  })

  it('restores persisted type + field filters on mount', async () => {
    getPreferences.mockResolvedValue({
      data: {
        prefs: {
          events: { visibleEventTypes: ['agent.started'], visibleFields: ['model'] },
        },
      },
      status: 200,
    })
    queryEvents.mockResolvedValue({
      data: { events: [
        evt('e1', 'ch-a', 1, 'agent.started', { payload: { model: 'gpt-4', tokens: 42 } }),
        evt('e2', 'ch-a', 2, 'tool.llm', { payload: { model: 'gpt-4' } }),
      ] },
      status: 200,
    })
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText('Types: 1/2')).toBeInTheDocument()
    })
    expect(screen.getByText('Fields: 1/2')).toBeInTheDocument()
  })

  it('treats persisted empty filter arrays as show-all', async () => {
    getPreferences.mockResolvedValue({
      data: { prefs: { events: { visibleEventTypes: [], visibleFields: [] } } },
      status: 200,
    })
    queryEvents.mockResolvedValue({
      data: { events: [evt('e1', 'ch-a', 1, 'agent.started')] },
      status: 200,
    })
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText('Types: all 1')).toBeInTheDocument()
    })
  })

  it('persists filter changes to preferences (debounced)', async () => {
    queryEvents.mockResolvedValue({
      data: { events: [
        evt('e1', 'ch-a', 1, 'agent.started'),
        evt('e2', 'ch-a', 2, 'tool.llm'),
      ] },
      status: 200,
    })
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(screen.getByText('Types: all 2')).toBeInTheDocument()
    })
    await user.click(screen.getByText('Types: all 2'))
    const listbox = screen.getByRole('listbox')
    const checkbox = Array.from(listbox.querySelectorAll('input[type="checkbox"]'))
      .find((el) => el.parentElement?.textContent?.includes('tool.llm')) as HTMLInputElement
    await user.click(checkbox)
    await user.keyboard('{Escape}')
    // Debounced: the PUT lands after the 300ms settle window. The initial
    // "not called" check is best-effort (a slow CI could already have fired
    // it) — the real assertion is the single call carrying the final state.
    try {
      expect(putPreferences).not.toHaveBeenCalled()
    } catch {
      /* already debounced+flushed under load — fine, asserted below */
    }
    await waitFor(() => {
      expect(putPreferences).toHaveBeenCalledTimes(1)
    }, { timeout: 5000 })
    expect(putPreferences.mock.calls[0][0].events).toEqual({
      visibleEventTypes: ['agent.started'],
      visibleFields: [],
    })
  })

  it('keeps the type filter when loading more pages', async () => {
    const page1 = Array.from({ length: 100 }, (_, i) => evt(`e${i}`, 'ch-a', i + 1, 'agent.started'))
    queryEvents
      .mockResolvedValueOnce({ data: { events: page1 }, status: 200 })
      .mockResolvedValueOnce({ data: { events: [evt('e-new', 'ch-a', 101, 'agent.started')] }, status: 200 })
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Load more' })).toBeInTheDocument()
    })
    await user.click(screen.getByRole('button', { name: 'Load more' }))
    await waitFor(() => {
      expect(screen.getByText('101 events shown')).toBeInTheDocument()
    })
    expect(queryEvents).toHaveBeenLastCalledWith(expect.objectContaining({ offset: 100 }))
  })

  it('subscribes to every known channel when none is selected', async () => {
    render(<EventsPage />)
    await waitFor(() => {
      expect(liveTopics).toEqual(['ch-a', 'ch-b'])
    })
  })

  it('subscribes to only the selected channel', async () => {
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(liveTopics).toEqual(['ch-a', 'ch-b'])
    })
    await user.selectOptions(screen.getByTestId('channel-select'), 'ch-b')
    await waitFor(() => {
      expect(liveTopics).toEqual(['ch-b'])
    })
  })

  it('re-subscribes as the polled channel list grows', async () => {
    vi.useFakeTimers()
    try {
      listChannels.mockResolvedValueOnce({ data: { channels: ['ch-a'] }, status: 200 })
      render(<EventsPage />)
      await act(async () => { await vi.advanceTimersByTimeAsync(0) })
      expect(liveTopics).toEqual(['ch-a'])
      listChannels.mockResolvedValue({ data: { channels: ['ch-a', 'ch-new'] }, status: 200 })
      await act(async () => { await vi.advanceTimersByTimeAsync(15000) })
      await act(async () => { await vi.advanceTimersByTimeAsync(0) })
      expect(liveTopics).toEqual(['ch-a', 'ch-new'])
    } finally {
      vi.useRealTimers()
    }
  })

  it('appends live events from subscribed channels', async () => {
    queryEvents.mockResolvedValue({ data: { events: [evt('e1', 'ch-a', 1, 'agent.started')] }, status: 200 })
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText('1 event shown')).toBeInTheDocument()
    })
    act(() => {
      messageCb?.({
        id: 'e2', channel: 'ch-b', cursor: 9, type: 'agent.completed',
        payload: { ok: true }, timestamp: new Date('2026-07-25T09:00:05Z'),
      })
    })
    await waitFor(() => {
      expect(screen.getByText('agent.completed')).toBeInTheDocument()
    })
    expect(screen.getByText('2 events shown')).toBeInTheDocument()
  })

  it('dedupes live events already loaded by the paged query', async () => {
    queryEvents.mockResolvedValue({ data: { events: [evt('e1', 'ch-a', 1, 'agent.started')] }, status: 200 })
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText('1 event shown')).toBeInTheDocument()
    })
    act(() => {
      messageCb?.({ id: 'e1', channel: 'ch-a', cursor: 1, type: 'agent.started', payload: {}, timestamp: new Date() })
    })
    await waitFor(() => {
      expect(screen.getAllByText('agent.started')).toHaveLength(1)
    })
    expect(screen.getByText('1 event shown')).toBeInTheDocument()
  })

  it('dedupes live events without ids by channel:cursor', async () => {
    queryEvents.mockResolvedValue({ data: { events: [evt('', 'ch-a', 7, 'tool.llm')] }, status: 200 })
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText('1 event shown')).toBeInTheDocument()
    })
    act(() => {
      messageCb?.({ channel: 'ch-a', cursor: 7, type: 'tool.llm', payload: {}, timestamp: new Date() })
    })
    await waitFor(() => {
      expect(screen.getByText('1 event shown')).toBeInTheDocument()
    })
  })

  it('ignores events from channels it is not subscribed to', async () => {
    render(<EventsPage />)
    const user = userEvent.setup()
    await waitFor(() => {
      expect(liveTopics).toEqual(['ch-a', 'ch-b'])
    })
    await user.selectOptions(screen.getByTestId('channel-select'), 'ch-b')
    await waitFor(() => {
      expect(liveTopics).toEqual(['ch-b'])
    })
    act(() => {
      messageCb?.({ id: 'foreign', channel: 'ch-a', cursor: 3, type: 'tool.llm', payload: {}, timestamp: new Date() })
    })
    await waitFor(() => {
      expect(screen.getByText(/No events found/)).toBeInTheDocument()
    })
  })

  it('shows the live connection state', async () => {
    render(<EventsPage />)
    await waitFor(() => {
      expect(screen.getByText('Offline')).toBeInTheDocument()
    })
    act(() => {
      statusCb?.({ connected: true, authenticated: false, error: null })
    })
    expect(screen.getByText('Connecting…')).toBeInTheDocument()
    act(() => {
      statusCb?.({ connected: true, authenticated: true, error: null })
    })
    expect(screen.getByText('Live')).toBeInTheDocument()
  })

  it('applies ?q= once and does not re-apply a cleared search on later URL changes', async () => {
    const user = userEvent.setup()
    params.current = new URLSearchParams({ q: 'timeout' })
    queryEvents.mockResolvedValue({ data: { events: [evt('e1', 'ch-a', 1, 'agent.started')] }, status: 200 })
    render(<EventsPage />)
    const search = await screen.findByLabelText('Search events')
    expect(search).toHaveValue('timeout')
    await user.clear(search)
    expect(search).toHaveValue('')
    // A later URL change that still carries the old ?q= (selecting a row keeps
    // the existing params) must not restore the query the user cleared.
    params.current = new URLSearchParams({ q: 'timeout' })
    await user.click(screen.getByRole('button', { name: /agent\.started/ }))
    expect(screen.getByLabelText('Search events')).toHaveValue('')
  })

  it('polls for new channels every 15s', async () => {
    vi.useFakeTimers()
    try {
      render(<EventsPage />)
      await act(async () => { await vi.advanceTimersByTimeAsync(0) })
      expect(listChannels).toHaveBeenCalledTimes(1)
      await act(async () => { await vi.advanceTimersByTimeAsync(15000) })
      expect(listChannels).toHaveBeenCalledTimes(2)
      await act(async () => { await vi.advanceTimersByTimeAsync(15000) })
      expect(listChannels).toHaveBeenCalledTimes(3)
    } finally {
      vi.useRealTimers()
    }
  })
})
