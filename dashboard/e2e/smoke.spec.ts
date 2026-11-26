// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { test, expect, type Page } from '@playwright/test'

/**
 * Deterministic smoke for the statically-exported dashboard. API calls are
 * intercepted so no Actae server is needed. Covers the shell navigation, the
 * new Runs list, the command palette, and the help dialog — and captures
 * screenshots into `e2e/__screenshots__/`.
 */

const NOW = new Date('2026-07-25T09:00:20Z').toISOString()

const CHANNELS = ['alpha-run', 'beta-run']
const STATS = {
  'alpha-run': { event_count: 12, latest_cursor: 12, last_event_at: NOW },
  'beta-run': { event_count: 4, latest_cursor: 4, last_event_at: '2026-07-25T08:00:00Z' },
}
const METADATA: Record<string, unknown> = {
  'alpha-run': { channel_id: 'alpha-run', display_name: 'Alpha Run', parent_channel_id: null, origin_run_id: 'alpha-run', forked_at_cursor: 0, forked_at: NOW, reason: null, experiment_metadata: null },
  'beta-run': { channel_id: 'beta-run', display_name: 'Beta Run', parent_channel_id: 'alpha-run', origin_run_id: 'alpha-run', forked_at_cursor: 3, forked_at: NOW, reason: 'alt model', experiment_metadata: null },
}
const METRICS = {
  status: 'ok',
  timestamp: NOW,
  uptime_seconds: 90061,
  mode: 'production',
  embedded_pg: false,
  events: { total: 16, channels: 2, state_snapshots: 3 },
  subscription: { mode: 'standalone', status: 'standalone', subscription: 'unknown', rev: 0, lock_imminent: false, docs_url: '#' },
  websocket: {
    connections: 2,
    topics: 1,
    topic_stats: { 'alpha-run': 1 },
    messages_sent: 10,
    messages_received: 5,
    total_messages: 15,
    back_pressure: { total_consumers: 1, slow_consumers: 0, total_queued_messages: 0, total_dropped_messages: 0, available_permits: 1000, max_queue_size: 10000 },
  },
}
const REPLAY = {
  events: [
    { id: 'e1', type: 'agent.started', cursor: 1, channel_id: 'alpha-run', timestamp: NOW, payload: { step: 'start' } },
    { id: 'e2', type: 'tool.web_search', cursor: 2, channel_id: 'alpha-run', timestamp: NOW, payload: { query: 'ai', results: 5 } },
    { id: 'e3', type: 'agent.completed', cursor: 3, channel_id: 'alpha-run', timestamp: NOW, payload: { tokens_total: 4200 } },
  ],
}
const FORK = { channel_id: 'beta-run', display_name: 'Beta Run', parent_channel_id: 'alpha-run', origin_run_id: 'alpha-run', forked_at_cursor: 3, forked_at: NOW, reason: 'alt model', experiment_metadata: null }
const COMPARE = {
  left_channel_id: 'alpha-run',
  right_channel_id: 'beta-run',
  state_diff: {
    left_channel_id: 'alpha-run', right_channel_id: 'beta-run',
    left: { channel_id: 'alpha-run', cursor: 3, state: { model: 'gpt-4' } },
    right: { channel_id: 'beta-run', cursor: 4, state: { model: 'claude' } },
    common: null, left_diverged_at_cursor: 3, right_diverged_at_cursor: 3,
    entries: [{ path: ['model'], kind: 'changed', left: 'gpt-4', right: 'claude' }],
    truncated: false, entry_count_total: 1, max_entries: 500,
  },
  manifest_diff: [], manifest_diff_truncated: false,
  tool_diff: [], tool_diff_truncated: false,
  output_left: 'answer A', output_right: 'answer B', output_changed: true,
  metrics_delta: [],
}

async function mockApi(page: Page) {
  await page.route('**/metrics.json', route => route.fulfill({ json: METRICS }))
  await page.route('**/api/**', route => {
    const { pathname } = new URL(route.request().url())
    const json = (body: unknown) => route.fulfill({ json: body })

    if (pathname === '/api/v1/auth/me') return json({ user: { id: 'u1', email: 'e2e@actae.local', name: 'E2E' } })
    if (pathname === '/api/v1/auth/config') return json({ enable_signup: false, sso: false })
    if (pathname === '/api/v1/channels') return json({ channels: CHANNELS, stats: STATS })
    if (pathname === '/api/v1/channels/compare') return json(COMPARE)
    if (pathname === '/api/v1/user/preferences') return json({ prefs: {} })
    if (pathname === '/api/v1/events/query') return json({ events: [] })
    if (pathname === '/api/v1/experiments') return json([])
    if (pathname.startsWith('/api/v1/scheduler/wakeups')) return json({ wakeups: [] })
    if (pathname.startsWith('/api/v1/events/replay/')) return json(REPLAY)
    if (pathname.startsWith('/api/v1/events/cursor/')) return json({ latest_cursor: 12 })
    if (pathname.startsWith('/api/v1/state/')) return json({ state: null })
    const meta = pathname.match(/^\/api\/v1\/channels\/([^/]+)\/metadata$/)
    if (meta) return json(METADATA[meta[1]] ?? null)
    const forks = pathname.match(/^\/api\/v1\/channels\/([^/]+)\/forks$/)
    if (forks) return json({ forks: forks[1] === 'alpha-run' ? [FORK] : [] })
    if (/^\/api\/v1\/channels\/[^/]+\/trail$/.test(pathname)) {
      return json({ channel_id: 'alpha-run', origin_run_id: 'alpha-run', ancestry: [], boundary: null, executions: [] })
    }
    return json({})
  })
}

async function seedAuth(page: Page) {
  await page.addInitScript(() => {
    localStorage.setItem('actae_auth', JSON.stringify({ token: 'e2e-token' }))
  })
}

test('login page renders', async ({ page }) => {
  await page.goto('/login/')
  await expect(page.getByPlaceholder('you@example.com')).toBeVisible()
  await expect(page.getByRole('button', { name: /sign in/i })).toBeVisible()
  await page.screenshot({ path: 'e2e/__screenshots__/login.png', fullPage: true })
})

test.describe('authenticated shell', () => {
  test.beforeEach(async ({ page }) => {
    await seedAuth(page)
    await mockApi(page)
  })

  test('overview shows grouped nav, getting started, and recent runs', async ({ page }) => {
    await page.goto('/dashboard/')
    await expect(page.getByRole('heading', { name: 'Agent runs at a glance' })).toBeVisible()
    // The brand icon lives in the top-left and links back to the Actae site.
    const brand = page.getByRole('link', { name: 'Actae home' })
    await expect(brand).toHaveAttribute('href', 'https://actae.dev')
    await expect(brand.locator('img')).toBeVisible()
    const nav = page.locator('.actae-sidebar nav')
    for (const label of ['Overview', 'Runs', 'Events', 'Forks', 'Approvals']) {
      await expect(nav.getByRole('link', { name: new RegExp(`^${label}\\b`) })).toBeVisible()
    }
    await expect(page.getByText('Getting started')).toBeVisible()
    await expect(page.getByRole('heading', { name: 'Recent runs' })).toBeVisible()
    await expect(page.getByRole('link', { name: 'Open run' }).first()).toBeVisible()
    await page.screenshot({ path: 'e2e/__screenshots__/overview.png', fullPage: true })
  })

  test('runs list renders rows with a fork badge and open link', async ({ page }) => {
    await page.goto('/dashboard/runs/')
    await expect(page.getByRole('heading', { name: 'Agent runs' })).toBeVisible()
    await expect(page.getByText('Alpha Run')).toBeVisible()
    await expect(page.getByText('Beta Run')).toBeVisible()
    await expect(page.getByText('fork', { exact: true })).toBeVisible()
    await page.screenshot({ path: 'e2e/__screenshots__/runs.png', fullPage: true })
  })

  test('run detail renders the breadcrumb and title', async ({ page }) => {
    await page.goto('/dashboard/flow/?channel=alpha-run')
    await expect(page.getByRole('link', { name: 'Runs', exact: true })).toBeVisible()
    await expect(page.getByRole('heading', { name: 'Alpha Run', exact: true })).toBeVisible()
    await page.screenshot({ path: 'e2e/__screenshots__/run-detail.png', fullPage: true })
  })

  test('opening a step reveals the inspector on demand', async ({ page }) => {
    await page.goto('/dashboard/flow/?channel=alpha-run')
    // The inspector is closed until a step is selected (progressive disclosure).
    await expect(page.getByText('Run details')).toHaveCount(0)
    await page.getByRole('button', { name: /cursor 1/ }).first().click()
    await expect(page.getByText('Selected Event @1')).toBeVisible()
    await expect(page.getByText('Run details')).toBeVisible()
    await page.screenshot({ path: 'e2e/__screenshots__/run-detail-inspector.png', fullPage: true })
  })

  test('compare tab renders the server fork diff', async ({ page }) => {
    await page.goto('/dashboard/flow/?channel=alpha-run')
    // The activity panel is collapsed by default (progressive disclosure).
    await page.getByRole('button', { name: 'Show activity' }).click()
    await page.getByRole('button', { name: 'Compare', exact: true }).click()
    await expect(page.getByText('Compare with another run')).toBeVisible()
    await expect(page.getByRole('heading', { name: 'What changed?' })).toBeVisible()
    // Run the comparison against the auto-selected sibling fork.
    await page.getByRole('button', { name: 'Compare', exact: true }).last().click()
    await expect(page.getByText('Authoritative server diff')).toBeVisible()
    await page.screenshot({ path: 'e2e/__screenshots__/run-compare.png', fullPage: true })
  })

  test('command palette opens from the sidebar and via Ctrl+K, closing on Escape', async ({ page }) => {
    await page.goto('/dashboard/')
    const palette = page.getByRole('dialog', { name: 'Command palette' })

    await page.getByTitle('Search runs and events').click()
    await expect(palette).toBeVisible()
    await expect(palette.getByText('Browse all runs')).toBeVisible()
    await expect(palette.getByText('alpha-run')).toBeVisible()
    await page.screenshot({ path: 'e2e/__screenshots__/command-palette.png' })

    await page.keyboard.press('Escape')
    await expect(palette).toBeHidden()

    // The global shortcut works once the client has hydrated the listener.
    await page.keyboard.press('Control+k')
    await expect(palette).toBeVisible()
  })

  test('help dialog shows the glossary and closes on Escape', async ({ page }) => {
    await page.goto('/dashboard/')
    await page.getByRole('button', { name: 'Help and glossary' }).click()
    const dialog = page.getByRole('dialog', { name: 'Help and glossary' })
    await expect(dialog).toBeVisible()
    await expect(dialog.getByText('Fork', { exact: true })).toBeVisible()
    await page.screenshot({ path: 'e2e/__screenshots__/help.png' })
    await page.keyboard.press('Escape')
    await expect(dialog).toBeHidden()
  })
})
