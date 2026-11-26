// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';

// These tests import the SDK source directly (`../../typescript/src/...`) and
// read monorepo fixtures/spec, so they live outside the publishable SDK.
export default defineConfig({
  test: {
    environment: 'node',
    include: ['*.test.ts'],
    testTimeout: 30000,
  },
});
