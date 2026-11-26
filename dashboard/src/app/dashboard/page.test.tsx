// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import OverviewPage from './page'

const getMetrics = vi.hoisted(() => vi.fn())
const listChannels = vi.hoisted(() => vi.fn())
const getLatestCursor = vi.hoisted(() => vi.fn())
const getPreferences = vi.hoisted(() => vi.fn())
const putPreferences = vi.hoisted(() => vi.fn())

vi.mock('@/lib/api', () => ({ getMetrics, listChannels, getLatestCursor, getPreferences, putPreferences }))
vi.mock('@/components/ProtectedLayout', () => ({
  default: ({ children }: any) => <div>{children}</div>,
}))

vi.mock('recharts', () => {
  const Fake = ({ children, title }: any) => <div data-testid="chart">{title || children}</div>
  return {
    ResponsiveContainer: ({ children }: any) => <div data-testid="rc">{children}</div>,
    AreaChart: Fake,
    LineChart: Fake,
    BarChart: Fake,
    Area: () => null,
    Line: () => null,
    Bar: () => null,
    Cell: () => null,
    XAxis: () => null,
    YAxis: () => null,
    Tooltip: () => null,
  }
})

const metrics = {
  status: 'ok',
  timestamp: '2026-07-25T09:00:00Z',
  uptime_seconds: 90061,
  mode: 'dev',
  embedded_pg: true,
  events: {
    total: 1240,
    channels: 2,
    state_snapshots: 8,
  },
  websocket: {
    connections: 42,
    topics: 5,
    topic_stats: { 'langgraph-agent-prod': 3, 'crewai-research-crew': 2 },
    messages_sent: 5000,
    messages_received: 1000,
    total_messages: 6000,
    back_pressure: {
      total_consumers: 6,
      slow_consumers: 1,
      total_queued_messages: 4,
      total_dropped_messages: 1,
      available_permits: 800,
      max_queue_size: 10000,
    },
  },
}

beforeEach(() => {
  vi.clearAllMocks()
  getMetrics.mockResolvedValue({ data: metrics, status: 200 })
  listChannels.mockResolvedValue({ data: { channels: ['ch-a', 'ch-b'] }, status: 200 })
  getLatestCursor.mockResolvedValue({ data: { latest_cursor: 42 }, status: 200 })
  getPreferences.mockResolvedValue({ status: 200, data: { prefs: {} } })
  putPreferences.mockResolvedValue({ status: 200 })
})

describe('OverviewPage', () => {
  it('renders status pills with correct states', async () => {
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('System')).toBeInTheDocument()
    })
    expect(screen.getByText('Mode')).toBeInTheDocument()
    expect(screen.getByText('Dev · embedded PG')).toBeInTheDocument()
    expect(screen.queryByText('Replication')).not.toBeInTheDocument()
    expect(screen.queryByText('Resources')).not.toBeInTheDocument()
  })

  it('formats the uptime', async () => {
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('1d 1h 1m 1s')).toBeInTheDocument()
    })
  })

  it('renders stat cards with metric values', async () => {
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('Connections')).toBeInTheDocument()
    })
    expect(screen.getByText('42')).toBeInTheDocument()
    expect(screen.getAllByText('Channels').length).toBeGreaterThanOrEqual(1)
    expect(screen.getByText('1,240')).toBeInTheDocument()
    expect(screen.getByText('Live Topics')).toBeInTheDocument()
  })

  it('renders the topic subscription table behind system details', async () => {
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText(/System details/)).toBeInTheDocument()
    })
    fireEvent.click(screen.getByRole('button', { name: 'Show' }))
    expect(screen.getByText('langgraph-agent-prod')).toBeInTheDocument()
    expect(screen.getByText('crewai-research-crew')).toBeInTheDocument()
    expect(screen.getByText('Topic Subscriptions')).toBeInTheDocument()
  })

  it('shows the getting-started panel until dismissed, persisting the dismissal', async () => {
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('Getting started')).toBeInTheDocument()
    })
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss getting started' }))
    expect(screen.queryByText('Getting started')).not.toBeInTheDocument()
    expect(putPreferences).toHaveBeenCalledWith({ onboarding: { seen: true } })
  })

  it('hides the getting-started panel when previously dismissed', async () => {
    getPreferences.mockResolvedValue({ status: 200, data: { prefs: { onboarding: { seen: true } } } })
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('System')).toBeInTheDocument()
    })
    expect(screen.queryByText('Getting started')).not.toBeInTheDocument()
  })

  it('lists channels with event counts and cursors from the server stats', async () => {
    listChannels.mockResolvedValue({
      data: {
        channels: ['ch-a', 'ch-b'],
        stats: {
          'ch-a': { event_count: 1200, latest_cursor: 42, last_event_at: '2026-07-25T09:00:00Z' },
          'ch-b': { event_count: 7, latest_cursor: 7, last_event_at: '2026-07-25T10:00:00Z' },
        },
      },
      status: 200,
    })
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('ch-a')).toBeInTheDocument()
    })
    expect(screen.getByText('ch-b')).toBeInTheDocument()
    expect(screen.getByText('1,200')).toBeInTheDocument()
    expect(screen.getByText('@42')).toBeInTheDocument()
    expect(screen.getByText('@7')).toBeInTheDocument()
    // Stats come in a single round-trip — no per-channel cursor calls.
    expect(getLatestCursor).not.toHaveBeenCalled()
    const flowLinks = screen.getAllByRole('link', { name: 'Open run' })
    const eventsLinks = screen.getAllByRole('link', { name: 'Events' })
    expect(flowLinks[0]).toHaveAttribute('href', '/dashboard/flow/?channel=ch-a')
    expect(eventsLinks[0]).toHaveAttribute('href', '/dashboard/events/?channel=ch-a')
  })

  it('falls back to per-channel cursor lookups when stats are absent', async () => {
    getLatestCursor.mockImplementation(async (id: string) => ({
      data: { latest_cursor: id === 'ch-a' ? 42 : 7 }, status: 200,
    }))
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('ch-a')).toBeInTheDocument()
    })
    expect(getLatestCursor).toHaveBeenCalledTimes(2)
    expect(screen.getByText('@42')).toBeInTheDocument()
    expect(screen.getByText('@7')).toBeInTheDocument()
  })

  it('shows an error banner when metrics fail', async () => {
    getMetrics.mockResolvedValue({ data: null, error: 'metrics down', status: 500 })
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('metrics down')).toBeInTheDocument()
    })
  })

  it('marks channels with unknown cursors as —', async () => {
    listChannels.mockResolvedValue({ data: { channels: ['ch-x'] }, status: 200 })
    getLatestCursor.mockRejectedValue(new Error('nope'))
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('ch-x')).toBeInTheDocument()
    })
    // Events and Latest Cursor both fall back to — in the fallback path.
    expect(screen.getAllByText('—').length).toBeGreaterThanOrEqual(2)
  })

  it('shows the empty channels state', async () => {
    listChannels.mockResolvedValue({ data: { channels: [] }, status: 200 })
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('No runs yet.')).toBeInTheDocument()
    })
  })

  it('hides the subscription banner in standalone mode', async () => {
    getMetrics.mockResolvedValue({
      data: { ...metrics, subscription: { mode: 'standalone', status: 'standalone', subscription: 'unknown', rev: 0, lock_imminent: false, docs_url: 'https://docs.diviga.com' } },
      status: 200,
    })
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('System')).toBeInTheDocument()
    })
    expect(screen.queryByText(/Subscription inactive/)).not.toBeInTheDocument()
  })

  it('shows a locked banner when the subscription is inactive', async () => {
    getMetrics.mockResolvedValue({
      data: { ...metrics, subscription: { mode: 'enrolled', status: 'locked', subscription: 'cancelled', rev: 5, lock_imminent: true, remaining_grace_secs: null, last_sync_at: '2026-08-03T09:00:00Z', docs_url: 'https://docs.diviga.com' } },
      status: 200,
    })
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText(/Subscription inactive — requests are being blocked/)).toBeInTheDocument()
    })
    expect(screen.getByRole('alert')).toBeInTheDocument()
  })

  it('shows a grace banner when the control plane is unreachable', async () => {
    getMetrics.mockResolvedValue({
      data: { ...metrics, subscription: { mode: 'enrolled', status: 'unreachable', subscription: 'active', rev: 5, lock_imminent: true, remaining_grace_secs: 3600, last_sync_at: '2026-08-03T09:00:00Z', docs_url: 'https://docs.diviga.com' } },
      status: 200,
    })
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText(/Control plane unreachable/)).toBeInTheDocument()
    })
    expect(screen.getByText(/1h 0m/)).toBeInTheDocument()
  })

  it('shows an imminent banner when grace is almost expired', async () => {
    getMetrics.mockResolvedValue({
      data: { ...metrics, subscription: { mode: 'enrolled', status: 'active', subscription: 'active', rev: 5, lock_imminent: true, remaining_grace_secs: 70000, last_sync_at: '2026-08-03T09:00:00Z', docs_url: 'https://docs.diviga.com' } },
      status: 200,
    })
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText(/Subscription grace period expiring soon/)).toBeInTheDocument()
    })
  })

  it('shows a sync-failing banner when the control plane is unreachable from the start', async () => {
    getMetrics.mockResolvedValue({
      data: { ...metrics, subscription: { mode: 'enrolled', status: 'active', subscription: 'unknown', rev: 0, lock_imminent: false, last_sync_at: null, last_error: 'TLS error: invalid peer certificate', docs_url: 'https://docs.diviga.com' } },
      status: 200,
    })
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText(/Control plane sync failing/)).toBeInTheDocument()
    })
    expect(screen.getByText(/TLS error: invalid peer certificate/)).toBeInTheDocument()
  })

  it('hides the sync-failing banner when there is no error', async () => {
    getMetrics.mockResolvedValue({
      data: { ...metrics, subscription: { mode: 'enrolled', status: 'active', subscription: 'active', rev: 5, lock_imminent: false, last_sync_at: '2026-08-03T09:00:00Z', last_error: null, docs_url: 'https://docs.diviga.com' } },
      status: 200,
    })
    render(<OverviewPage />)
    await waitFor(() => {
      expect(screen.getByText('System')).toBeInTheDocument()
    })
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })
})
