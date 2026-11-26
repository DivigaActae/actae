// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { defineConfig, devices } from '@playwright/test'

const PORT = Number(process.env.E2E_PORT || 4173)
const baseURL = `http://127.0.0.1:${PORT}`

/**
 * Opt-in visual/behavioural smoke for the statically-exported dashboard.
 * Not part of `npm test` (which is jsdom-only). Run with:
 *
 *   npm run e2e            # builds `out/`, serves it, runs Chromium
 *   npm run e2e:install    # first time: download the Chromium browser
 *
 * API calls are intercepted in the specs, so no Actae server is required.
 */
export default defineConfig({
  testDir: './e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: [['list']],
  use: {
    baseURL,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    command: `npm run build && node e2e/static-server.mjs`,
    url: `${baseURL}/login/`,
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
    env: { PORT: String(PORT) },
  },
})
