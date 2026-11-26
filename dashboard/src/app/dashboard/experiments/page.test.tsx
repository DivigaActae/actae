// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import ExperimentsPage from './page'
import { deriveForkGroups } from '@/lib/fork-families'

const listExperiments = vi.hoisted(() => vi.fn())
const getExperiment = vi.hoisted(() => vi.fn())
const rankExperiment = vi.hoisted(() => vi.fn())
const setChannelOutcome = vi.hoisted(() => vi.fn())
const promoteChannel = vi.hoisted(() => vi.fn())
const queryEvents = vi.hoisted(() => vi.fn())
const compareChannels = vi.hoisted(() => vi.fn())
const listChannels = vi.hoisted(() => vi.fn())

vi.mock('@/components/ProtectedLayout', () => ({ default: ({ children }: { children: React.ReactNode }) => <>{children}</> }))
vi.mock('@/lib/api', () => ({ listExperiments, getExperiment, rankExperiment, setChannelOutcome, promoteChannel, queryEvents, compareChannels, listChannels }))

const group = {
  group_id: 'exp-1', name: 'Prompt quality', description: 'Compare prompts', baseline_channel_id: 'base', status: 'running',
  created_at: '2026-08-10T10:00:00Z', updated_at: '2026-08-10T10:00:00Z',
  members: [
    { channel_id: 'base', role: 'baseline', declared_delta: null, created_at: '2026-08-10T10:00:00Z', result_score: 0.7 },
    { channel_id: 'variant-a', role: 'variant', declared_delta: { model: 'gpt-5' }, created_at: '2026-08-10T10:00:00Z', result_score: 0.9 },
  ],
}

beforeEach(() => {
  vi.clearAllMocks()
  listExperiments.mockResolvedValue({ data: [group], status: 200 })
  getExperiment.mockResolvedValue({ data: group, status: 200 })
  rankExperiment.mockResolvedValue({ data: { group, ranked: [group.members[1], group.members[0]], count: 2 }, status: 200 })
  setChannelOutcome.mockResolvedValue({ data: { channel_id: 'variant-a', outcome: 'rejected' }, status: 200 })
  promoteChannel.mockResolvedValue({ data: { status: 'promoted' }, status: 200 })
  queryEvents.mockResolvedValue({ data: { events: [] }, status: 200 })
  listChannels.mockResolvedValue({
    data: {
      channels: ['base', 'variant-a'],
      stats: {
        base: { display_name: 'Research baseline', parent_channel_id: null },
        'variant-a': { display_name: 'Fork A · +fact-check', parent_channel_id: 'base' },
      },
    },
    status: 200,
  })
  compareChannels.mockResolvedValue({ data: {
    left_channel_id: 'base', right_channel_id: 'variant-a',
    state_diff: { left_channel_id: 'base', right_channel_id: 'variant-a', left: { channel_id: 'base', cursor: 4, state: {} }, right: { channel_id: 'variant-a', cursor: 4, state: {} }, common: null, left_diverged_at_cursor: null, right_diverged_at_cursor: 2, entries: [{ path: ['answer'], kind: 'changed', left: 'old answer', right: 'new answer' }], truncated: false, entry_count_total: 1, max_entries: 100 },
    manifest_diff: [], manifest_diff_truncated: false, tool_diff: [], tool_diff_truncated: false,
    output_left: { result: 'old' }, output_right: { result: 'new' }, output_changed: true, metrics_delta: [],
  }, status: 200 })
})

describe('ExperimentsPage', () => {
  it('renders ranked experiment members and declared deltas', async () => {
    render(<ExperimentsPage />)
    expect(await screen.findByText('Prompt quality')).toBeInTheDocument()
    expect(await screen.findByText('model=gpt-5')).toBeInTheDocument()
    expect(screen.getByText('0.9')).toBeInTheDocument()
    expect(screen.getAllByRole('link', { name: 'Inspect' })[0]).toHaveAttribute('href', '/dashboard/flow/?channel=variant-a')
    expect(await screen.findByRole('region', { name: 'Fork comparison' })).toBeInTheDocument()
    await waitFor(() => expect(compareChannels).toHaveBeenCalledWith('base', 'variant-a'))
    expect(await screen.findByText(/old answer/)).toBeInTheDocument()
    expect(screen.getByText(/new answer/)).toBeInTheDocument()
  })

  it('records a rejected outcome and refreshes the ranking', async () => {
    const user = userEvent.setup()
    render(<ExperimentsPage />)
    await user.click(await screen.findByRole('button', { name: 'Reject' }))
    await waitFor(() => expect(setChannelOutcome).toHaveBeenCalledWith('variant-a', 'rejected'))
    expect(getExperiment).toHaveBeenCalledTimes(2)
  })

  it('updates derived fork outcomes locally after reject', async () => {
    listExperiments.mockResolvedValue({ data: [], status: 200 })
    queryEvents.mockResolvedValue({ data: { events: [
      { channel_id: 'run-fork', timestamp: '2026-08-10T12:00:00Z', payload: { source_channel_id: 'run' } },
    ] }, status: 200 })
    const user = userEvent.setup()
    render(<ExperimentsPage />)
    await user.click(await screen.findByRole('button', { name: 'Reject' }))
    await waitFor(() => expect(setChannelOutcome).toHaveBeenCalledWith('run-fork', 'rejected'))
    expect(screen.getByText('rejected')).toBeInTheDocument()
    expect(getExperiment).not.toHaveBeenCalled()
  })

  it('promotes a fork and reflects the outcome', async () => {
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    const user = userEvent.setup()
    render(<ExperimentsPage />)
    await user.click(await screen.findByRole('button', { name: 'Promote' }))
    await waitFor(() => expect(promoteChannel).toHaveBeenCalledWith('variant-a'))
    expect(screen.getByText('promoted')).toBeInTheDocument()
  })

  it('shows the empty state', async () => {
    listExperiments.mockResolvedValue({ data: [], status: 200 })
    render(<ExperimentsPage />)
    expect(await screen.findByText('No forks or experiment groups yet')).toBeInTheDocument()
  })

  it('derives nested fork families without explicit experiment registration', () => {
    const groups = deriveForkGroups([
      { channel_id: 'root-a', timestamp: '2026-08-10T10:00:00Z', payload: { source_channel_id: 'root', reason: 'A', resolved_cursor: 4 } },
      { channel_id: 'root-a-1', timestamp: '2026-08-10T11:00:00Z', payload: { source_channel_id: 'root-a', reason: 'nested' } },
      { channel_id: 'root-b', timestamp: '2026-08-10T12:00:00Z', payload: { source_channel_id: 'root' } },
    ])
    expect(groups).toHaveLength(1)
    expect(groups[0].baseline_channel_id).toBe('root')
    expect(groups[0].members.map(member => member.channel_id)).toEqual(['root', 'root-a', 'root-a-1', 'root-b'])
    expect(groups[0].members[1].declared_delta).toMatchObject({ reason: 'A', fork_cursor: 4 })
  })

  it('shows ordinary forks when no explicit experiment group exists', async () => {
    listExperiments.mockResolvedValue({ data: [], status: 200 })
    queryEvents.mockResolvedValue({ data: { events: [
      { channel_id: 'run-fork', timestamp: '2026-08-10T12:00:00Z', payload: { source_channel_id: 'run', reason: 'fidelity check', reproducibility: 'state_exact' } },
    ] }, status: 200 })
    render(<ExperimentsPage />)
    expect(await screen.findByText('Fork family · run')).toBeInTheDocument()
    expect(await screen.findByText('reason=fidelity check · reproducibility=state_exact')).toBeInTheDocument()
    expect(getExperiment).not.toHaveBeenCalled()
  })
})
