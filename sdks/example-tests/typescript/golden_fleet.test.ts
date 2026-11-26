// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { parseJson, unwrapSonic } from '../../typescript/src/json.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const specPath = join(__dirname, '..', '..', '..', 'spec', 'api-parity', 'v1', 'golden_fleet_vectors.json');

function vectors() {
  return JSON.parse(readFileSync(specPath, 'utf8'))['fleet_wire'];
}

describe('cross-SDK golden fleet wire corpus', () => {
  it('parseJson preserves arbitrary-precision numbers as bigint', () => {
    const v = vectors()['arbitrary_precision'];
    const got = parseJson(JSON.stringify(v.input)) as any;
    expect(got.big).toBe(9007199254740993n); // > Number.MAX_SAFE_INTEGER; must stay bigint
    expect(got.negative).toBe(-9007199254740993n);
    expect(got.float).toBe(3.5);
    expect(got.nested.a).toBe(123);
    expect(got.list[0]).toBe(42);
    // typeof bigint — never a silently-rounded Number.
    expect(typeof got.big).toBe('bigint');
  });

  it('unwrapSonic handles the float and nested marker forms', () => {
    const v = vectors()['arbitrary_precision'];
    const got = unwrapSonic(v.input) as any;
    expect(got.float).toBe(3.5);
    expect(got.nested.a).toBe(123);
    expect(got.list[0]).toBe(42);
  });

  it('preserves the fleet HTTP envelope data object', () => {
    const v = vectors()['http_envelope'];
    const got = unwrapSonic(v.input) as any;
    expect(got.data.ok).toBe(true);
    expect(got.data.cursor).toBe(5);
    expect(got).toEqual(v.input);
  });

  it('keeps pagination tokens as an opaque per-instance map', () => {
    const v = vectors()['pagination'];
    expect(v.page_tokens).toEqual({ ins_a: 'tok-a', ins_b: 'tok-b' });
    // Parsing preserves both keys (never collapses to one token).
    const parsed = parseJson(JSON.stringify(v.page_tokens)) as Record<string, string>;
    expect(Object.keys(parsed)).toHaveLength(2);
    expect(parsed.ins_a).toBe('tok-a');
    expect(parsed.ins_b).toBe('tok-b');
  });

  it('maps the PR-021 error envelope fields', () => {
    const v = vectors()['error_envelope'];
    expect(v.input.code).toBe('idempotency_conflict');
    expect(v.status).toBe(409);
    expect(v.input.message).toBe(v.message);
    expect(v.input.retryable).toBe(false);
    expect(v.input.request_id).toBe('rq-1');
  });

  it('keeps fork-receipt provenance fields', () => {
    const v = vectors()['fork_receipt'];
    expect(v.input.child_channel_id).toBe(v.child_channel_id);
    expect(v.input.source_channel_id).toBe(v.source_channel_id);
    expect(v.input.source_state_version).toBe(v.source_state_version);
    expect(v.input.restorable).toBe(true);
    expect(v.input.manifest).toEqual({ prompt: 'p' });
  });

  it('treats both WS broadcast shapes as events', () => {
    const v = vectors()['ws_broadcast'];
    for (const shape of [v.legacy, v.type_tagged]) {
      const payload = 'Broadcast' in shape ? shape.Broadcast.payload : shape.payload;
      expect(payload.event.channel_id).toBe('ch');
    }
  });
});