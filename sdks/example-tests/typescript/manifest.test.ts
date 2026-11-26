// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { MANIFEST_OPERATIONS, manifestOperation } from '../../typescript/src/manifest.js';
import { canonicalStepContent, deterministicOperationKey } from '../../typescript/src/deterministic.js';

const spec = (name: string) => JSON.parse(readFileSync(new URL(`../../../spec/api-parity/v1/${name}`, import.meta.url), 'utf8')) as any;

describe('generated API manifest bindings', () => {
  it('matches every source operation', () => {
    const source = spec('manifest.json').operations as Array<any>;
    expect(MANIFEST_OPERATIONS).toHaveLength(76);
    expect(MANIFEST_OPERATIONS.map((x) => x.id)).toEqual(source.map((x) => x.id));
    for (const op of source) expect(manifestOperation(op.id)).toMatchObject({ method: op.method, path: op.path, action: op.action });
  });

  it('uses the shared cross-SDK golden vectors', () => {
    const vectors = spec('golden_vectors.json').deterministic_operation_keys as Array<any>;
    for (const vector of vectors) {
      expect(canonicalStepContent(vector.payload, vector.metadata)).toBe(vector.canonical);
      expect(deterministicOperationKey(vector.scope, vector.type, vector.channel, vector.step, vector.canonical)).toBe(vector.operation_id);
    }
  });

  it('routes every generic operation through the typed gateway dispatch', async () => {
    const seen: string[] = [];
    const client = new (await import('../../typescript/src/fleet.js')).FleetClient({ endpoint: 'https://fleet.example', organizationToken: 'org', organizationId: 'org-1', fetch: (async (url, init) => {
      seen.push(`${init?.method} ${new URL(String(url)).pathname}`);
      return new Response(JSON.stringify({ access_token: 'fleet', ok: true }), { status: 200 });
    }) as typeof fetch });
    await client.callOperation('actae.api.v1.events.replay', { instanceId: 'inst-1', pathParams: { channel_id: 'run' }, query: { limit: 2 } });
    expect(seen.at(-1)).toBe('POST /v1/organizations/org-1/instances/inst-1/ops/actae.api.v1.events.replay');
  });

  it('binds every manifest operation to the fleet dispatch route', async () => {
    const seen: string[] = [];
    const client = new (await import('../../typescript/src/fleet.js')).FleetClient({
      endpoint: 'https://fleet.example', organizationToken: 'org', organizationId: 'org-1',
      fetch: (async (url, init) => {
        seen.push(`${init?.method} ${new URL(String(url)).pathname}`);
        if (new URL(String(url)).pathname === '/api/v1/fleet/token') {
          return new Response(JSON.stringify({ access_token: 'fleet' }), { status: 200 });
        }
        return new Response(JSON.stringify({ data: {} }), { status: 200 });
      }) as typeof fetch,
    });
    const fleetSupported = MANIFEST_OPERATIONS.filter((op) => op.fleetSupported);
    const unsupported = MANIFEST_OPERATIONS.filter((op) => !op.fleetSupported);
    for (const op of MANIFEST_OPERATIONS) {
      const names = [...op.path.matchAll(/\{([^}]+)\}/g)].map((m) => m[1]!);
      const pathParams = Object.fromEntries(names.map((name, i) => [name, `value-${i}`]));
      if (!op.fleetSupported) {
        // PR-016: auth/session/dashboard ops fail LOCALLY (426) before I/O.
        await expect(client.callOperation(op.id, {
          instanceId: 'inst-1', pathParams, body: {},
          confirmTarget: op.confirmation ? Object.values(pathParams)[0] : undefined,
        })).rejects.toThrow('unsupported API operation');
        continue;
      }
      await client.callOperation(op.id, {
        instanceId: 'inst-1', pathParams, body: {}, idempotencyKey: 'stable-key',
        confirmTarget: op.confirmation ? Object.values(pathParams)[0] : undefined,
      });
    }
    // Token exchange + only the fleet-supported ops reach the gateway.
    expect(seen).toHaveLength(fleetSupported.length + 1);
    expect(unsupported.length).toBeGreaterThan(0); // auth surface is excluded
    expect(seen.slice(1).every((line) => line.startsWith('POST /v1/organizations/org-1/instances/inst-1/ops/'))).toBe(true);
  });

  it('sends the canonical token-exchange body with requested scopes (PR-006)', async () => {
    const calls: Array<{ body?: string; method?: string; path?: string; headers?: Record<string, string> }> = [];
    const client = new (await import('../../typescript/src/fleet.js')).FleetClient({
      endpoint: 'https://fleet.example', organizationToken: 'org', organizationId: 'org-1',
      requestedScopes: ['events:read'],
      fetch: (async (url, init) => {
        calls.push({ body: String(init?.body), method: init?.method, path: new URL(String(url)).pathname, headers: init?.headers as Record<string, string> });
        return new Response(JSON.stringify({ token: 'fleet-tok', expires_in: 300 }), { status: 200 });
      }) as typeof fetch,
    });
    await client.instances();
    const exchange = calls.find((c) => c.path === '/api/v1/fleet/token')!;
    expect(exchange).toBeDefined();
    expect(exchange.method).toBe('POST');
    expect(JSON.parse(exchange.body!)).toEqual({ purpose: 'fleet_access', organization_id: 'org-1', scopes: ['events:read'] });
    expect(exchange.headers!['authorization'] ?? exchange.headers!['Authorization']).toBe('Bearer org');
  });

  it('defaults to least-privilege scopes (PR-006)', async () => {
    let body: any;
    const client = new (await import('../../typescript/src/fleet.js')).FleetClient({
      endpoint: 'https://fleet.example', organizationToken: 'org', organizationId: 'org-1',
      fetch: (async (url, init) => {
        if (new URL(String(url)).pathname === '/api/v1/fleet/token') body = JSON.parse(String(init?.body));
        return new Response(JSON.stringify({ token: 't', expires_in: 300 }), { status: 200 });
      }) as typeof fetch,
    });
    await client.instances();
    expect(Array.isArray(body.scopes)).toBe(true);
    expect(body.scopes.length).toBeGreaterThan(0);
  });

  it('sends per-instance continuation as top-level page_tokens (PR-008)', async () => {
    let body: any;
    const client = new (await import('../../typescript/src/fleet.js')).FleetClient({
      endpoint: 'https://fleet.example', organizationToken: 'org', organizationId: 'org-1',
      fetch: (async (url, init) => {
        if (new URL(String(url)).pathname === '/api/v1/fleet/token') {
          return new Response(JSON.stringify({ token: 't', expires_in: 300 }), { status: 200 });
        }
        body = JSON.parse(String(init?.body));
        return new Response(JSON.stringify({
          events: [{ id: 'e1' }],
          page: { next_page_tokens: { 'ins-a': 'tok-a', 'ins-b': 'tok-b' }, has_more: true },
        }), { status: 200 });
      }) as typeof fetch,
    });
    const result = await client.queryMany(['ins-a', 'ins-b'], { limit: 25 }, {}, { 'ins-a': 'prev-a', 'ins-b': 'prev-b' });
    expect(body.instances).toEqual(['ins-a', 'ins-b']);
    expect(body.query.limit).toBe(25);
    // Continuation is top-level, never inside query.
    expect(body.page_tokens).toEqual({ 'ins-a': 'prev-a', 'ins-b': 'prev-b' });
    expect(body.query.page_token).toBeUndefined();
    // Full per-instance map preserved.
    expect(result.pageTokens).toEqual({ 'ins-a': 'tok-a', 'ins-b': 'tok-b' });
  });
});
