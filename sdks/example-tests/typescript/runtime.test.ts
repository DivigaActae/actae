// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';

import {
  Actae,
  channelForRun,
  deterministicOperationKey,
  parseCarrier,
  type JsonValue,
} from '../../typescript/src/index.js';


function claim(status = 'claimed', result?: JsonValue) {
  return {
    status,
    result,
    claimToken: status === 'in_progress' ? undefined : 'token-1',
    execution: { id: 'execution-1' },
  };
}

function client() {
  return {
    record: vi.fn().mockResolvedValue({ id: 'event-1', cursor: 1 }),
    claimExecution: vi.fn().mockResolvedValue(claim()),
    completeExecution: vi.fn().mockResolvedValue({}),
    failExecution: vi.fn().mockResolvedValue({}),
    heartbeatExecution: vi.fn().mockResolvedValue({}),
    cancelExecution: vi.fn().mockResolvedValue({}),
  };
}

describe('Actae ergonomic runtime', () => {
  it('builds stable readable channels without sanitized collisions', () => {
    expect(channelForRun('ticket-42', 'langgraph')).toBe('langgraph:ticket-42');
    const first = channelForRun('tenant/a workflow', 'temporal');
    expect(first).toBe(channelForRun('tenant/a workflow', 'temporal'));
    expect(first).toMatch(/^temporal:tenant-a-workflow:/);
    expect(first).not.toBe(channelForRun('tenant:a workflow', 'temporal'));
    expect(Buffer.byteLength(channelForRun('x'.repeat(1000)), 'utf8')).toBeLessThanOrEqual(256);
  });

  it('matches the Python/Go operation-key vector', () => {
    expect(deterministicOperationKey('agent-session', 'tool', 'channel-1', '7', 'payload'))
      .toBe('f059dcde-a40f-5156-9ba8-ad9246cf929d');
  });

  it('restores carrier lineage in activities and records attempt metadata', async () => {
    const fake = client();
    const actae = new Actae(fake as never);
    const temporal = actae.orchestrator('temporal');
    await temporal.activity({ id: 'agent-0', parent: temporal.carrier('wf/42'), attempt: 2 }, async (activity) => {
      expect(actae.current()).toBe(activity);
      expect(activity.workflowId).toBe('wf/42');
      await activity.child('agent-1', async (child) => {
        expect(child.parentRunId).toBe('agent-0');
        expect(actae.current()).toBe(child);
      });
      expect(actae.current()).toBe(activity);
    });
    expect(actae.current(false)).toBeUndefined();
    expect(fake.record.mock.calls.map((call) => call[1])).toEqual([
      'framework.run.started',
      'framework.run.started',
      'framework.run.completed',
      'framework.run.completed',
    ]);
    expect(fake.record.mock.calls[0]?.[2].data.attempt).toBe(2);
    expect(fake.record.mock.calls[0]?.[3].operationId).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('accepts only the shared credential-free carrier schema', () => {
    const fixture = JSON.parse(readFileSync(new URL('../../../tests/fixtures/run_carrier_v1.json', import.meta.url), 'utf8'));
    expect(parseCarrier(fixture)).toMatchObject({ run_id: 'workflow-42', workflow_id: 'workflow-42' });
    expect(() => parseCarrier({ ...fixture, api_key: 'never-accepted' })).toThrow('unsupported fields');
  });

  it('preserves application errors when recording failure also fails', async () => {
    const fake = client();
    fake.record
      .mockResolvedValueOnce({ id: 'started' })
      .mockRejectedValueOnce(new Error('storage down'));
    const actae = new Actae(fake as never);
    await expect(actae.run('r1', async () => {
      throw new Error('application failed');
    })).rejects.toThrow('application failed');
  });

  it('records cancellation distinctly from application failure', async () => {
    const fake = client();
    const actae = new Actae(fake as never);
    const cancelled = new Error('caller cancelled');
    cancelled.name = 'AbortError';
    await expect(actae.run('r1', async () => { throw cancelled; })).rejects.toBe(cancelled);
    expect(fake.record.mock.calls.map((call) => call[1])).toEqual([
      'framework.run.started', 'framework.run.cancelled',
    ]);
  });

  it('supports fail-open lifecycle telemetry', async () => {
    const fake = client();
    fake.record.mockRejectedValue(new Error('offline'));
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const actae = new Actae(fake as never, 'warn');
    await expect(actae.run('r1', async () => 42)).resolves.toBe(42);
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });

  it('claims an effect once and replays without invoking application code', async () => {
    const fake = client();
    const actae = new Actae(fake as never);
    const invoke = vi.fn(async (input: { orderId: string; amount: number }) => ({
      receipt: 'r-1', amount: input.amount,
    }));
    const charge = actae.effect({ key: 'order:{orderId}:charge', name: 'charge' }, invoke);

    await actae.run('checkout-1', async () => {
      await expect(charge({ orderId: 'o-1', amount: 25 })).resolves.toEqual({ receipt: 'r-1', amount: 25 });
      fake.claimExecution.mockResolvedValue(claim('replayed', { receipt: 'r-1', amount: 25 }));
      await expect(charge({ orderId: 'o-1', amount: 25 })).resolves.toEqual({ receipt: 'r-1', amount: 25 });
    });

    expect(invoke).toHaveBeenCalledTimes(1);
    expect(fake.claimExecution.mock.calls[0]?.slice(0, 4)).toEqual([
      'custom:checkout-1',
      'order:o-1:charge',
      'charge',
      { orderId: 'o-1', amount: 25 },
    ]);
  });

  it('isolates ambient context between concurrent runs', async () => {
    const fake = client();
    const actae = new Actae(fake as never);
    const seen: string[] = [];
    await Promise.all(['a', 'b'].map((id) => actae.run(id, async () => {
      await Promise.resolve();
      seen.push(actae.current()!.runId);
    })));
    expect(seen.sort()).toEqual(['a', 'b']);
  });
});
