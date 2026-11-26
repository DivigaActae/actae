// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';
import {
  CHECKPOINT_METADATA_KEY,
  FULL_FEATURE_SET,
  ActaeFeature,
  ActaeRunContext,
  ActaeToolExecutor,
  AdapterCapabilities,
  AdapterContractError,
  CheckpointEnvelope,
  ToolExecutionInProgressError,
  embedCheckpointMetadata,
  extractCheckpointEnvelope,
  frameworkEvent,
  stripCheckpointMetadata,
} from '../../typescript/src/adapters/contract.js';
import {
  ADAPTER_CAPABILITIES,
  commonSurfaceCapabilities,
  getAdapterCapabilities,
} from '../../typescript/src/adapters/profiles.js';

function envelope() {
  return new CheckpointEnvelope({
    framework: 'test',
    adapterVersion: '1',
    channelId: 'run:test',
    portableState: { messages: [{ role: 'user', content: 'hello' }] },
    nativeCheckpoint: { checkpoint_id: 'cp-1' },
    applicationState: { customer: 42 },
    pendingWork: { tool: 'lookup' },
    manifest: { model: 'test-model' },
    eventCursor: 7,
    eventId: 'evt-7',
  });
}

function claim(status: string, result?: unknown) {
  return {
    status,
    result,
    claimToken: status === 'in_progress' ? undefined : 'token-1',
    execution: { id: 'execution-1' },
  };
}

function client(status = 'claimed', result?: unknown) {
  return {
    claimExecution: vi.fn().mockResolvedValue(claim(status, result)),
    completeExecution: vi.fn().mockResolvedValue({}),
    failExecution: vi.fn().mockResolvedValue({}),
    heartbeatExecution: vi.fn().mockResolvedValue({}),
    cancelExecution: vi.fn().mockResolvedValue({}),
    record: vi.fn().mockResolvedValue({ id: 'event-1' }),
    latestCursor: vi.fn().mockResolvedValue(7),
    saveState: vi.fn().mockResolvedValue(11),
    latestState: vi.fn().mockResolvedValue({ state: envelope().toJSON() }),
    replay: vi.fn().mockResolvedValue([]),
    fork: vi.fn().mockResolvedValue({ childChannelId: 'run:fork' }),
    createExperiment: vi.fn().mockResolvedValue({ id: 'experiment-1' }),
    addExperimentMember: vi.fn().mockResolvedValue({ ok: true }),
    createExecutionGroup: vi.fn().mockResolvedValue({ groupId: 'group-1' }),
    scheduleWakeup: vi.fn().mockResolvedValue({ id: 'wakeup-1' }),
  };
}

describe('framework adapter contract', () => {
  it('validates and serializes capability claims', () => {
    const capabilities = new AdapterCapabilities({
      framework: 'test',
      adapterVersion: '1',
      supportLevel: 'certified',
      resumeFidelity: 'checkpoint_exact',
      features: FULL_FEATURE_SET,
      nativeCheckpoint: true,
    });
    expect(capabilities.fullParity).toBe(true);
    expect(capabilities.supports(ActaeFeature.ToolExecutions)).toBe(true);
    expect(capabilities.toJSON()['features']).toEqual([...FULL_FEATURE_SET].sort());
    expect(() => new AdapterCapabilities({
      framework: 'bad', adapterVersion: '1', supportLevel: 'preview',
      resumeFidelity: 'context_seeded', features: [ActaeFeature.Resume], nativeCheckpoint: false,
    })).toThrow(/resume support requires/);
  });

  it('publishes native and full common-surface profiles for every JS adapter', () => {
    expect([...ADAPTER_CAPABILITIES.keys()].sort()).toEqual([
      'claude-agent-sdk', 'codex', 'github-copilot-sdk', 'langchain', 'langgraph', 'openai-agents',
    ]);
    expect(getAdapterCapabilities('langgraph').resumeFidelity).toBe('checkpoint_exact');
    expect(getAdapterCapabilities('codex').resumeFidelity).toBe('observe_only');
    for (const framework of ADAPTER_CAPABILITIES.keys()) {
      expect(commonSurfaceCapabilities(framework).fullParity).toBe(true);
    }
    expect(() => getAdapterCapabilities('unknown')).toThrow(/available/);
  });

  it('round-trips envelopes without changing framework state', () => {
    const original = envelope();
    const restored = CheckpointEnvelope.fromJSON(original.toJSON());
    expect(restored.toJSON()).toEqual(original.toJSON());
    const state = embedCheckpointMetadata({ owned: [1, 2] }, original);
    expect(state['owned']).toEqual([1, 2]);
    expect(state[CHECKPOINT_METADATA_KEY]).toBeDefined();
    expect(extractCheckpointEnvelope(state)?.toJSON()).toEqual(original.toJSON());
    expect(stripCheckpointMetadata(state)).toEqual({ owned: [1, 2] });
  });

  it('matches the cross-SDK checkpoint fixture', () => {
    const path = new URL('../../../tests/fixtures/framework_checkpoint_v1.json', import.meta.url);
    const value = JSON.parse(readFileSync(path, 'utf8'));
    expect(CheckpointEnvelope.fromJSON(value).toJSON()).toEqual(value);
  });

  it('rejects incompatible schemas and non-JSON values', () => {
    expect(() => CheckpointEnvelope.fromJSON({ ...envelope().toJSON(), schema: 'future/v9' }))
      .toThrow(/unsupported checkpoint schema/);
    expect(() => frameworkEvent({ kind: 'step', framework: 'test', runId: 'r', payload: { score: Number.NaN } }))
      .toThrow(AdapterContractError);
  });

  it('claims, completes and replays tool effects', async () => {
    const fake = client();
    const executor = new ActaeToolExecutor(fake as never, 'run:test');
    const invoke = vi.fn().mockResolvedValue({ ok: true });
    await expect(executor.execute('step-1:email', 'send_email', { to: 'a@example.com' }, invoke))
      .resolves.toEqual({ ok: true });
    expect(fake.completeExecution).toHaveBeenCalledWith('execution-1', 'token-1', { ok: true });

    fake.claimExecution.mockResolvedValue(claim('replayed', { ok: true }));
    await expect(executor.execute('step-1:email', 'send_email', {}, invoke)).resolves.toEqual({ ok: true });
    expect(invoke).toHaveBeenCalledTimes(1);
  });

  it('heartbeats long tools and persists failures', async () => {
    const fake = client();
    const executor = new ActaeToolExecutor(fake as never, 'run:test');
    await executor.execute('slow-1', 'slow', {}, async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      return 'done';
    }, { heartbeatIntervalMs: 5 });
    expect(fake.heartbeatExecution.mock.calls.length).toBeGreaterThan(0);

    fake.claimExecution.mockResolvedValue(claim('claimed'));
    await expect(executor.execute('fail-1', 'fail', {}, async () => {
      throw new Error('boom');
    })).rejects.toThrow('boom');
    expect(fake.failExecution).toHaveBeenCalledWith('execution-1', 'boom', expect.objectContaining({
      claimToken: 'token-1', errorType: 'Error',
    }));
  });

  it('returns promptly on heartbeat lease loss even if a tool ignores AbortSignal', async () => {
    const fake = client();
    fake.heartbeatExecution.mockRejectedValue(new Error('lease lost'));
    const executor = new ActaeToolExecutor(fake as never, 'run:test');
    await expect(executor.execute(
      'lease-1', 'slow', {}, async () => new Promise<never>(() => undefined),
      { heartbeatIntervalMs: 1 },
    )).rejects.toThrow('lease lost');
    expect(fake.failExecution).toHaveBeenCalledWith('execution-1', 'lease lost', expect.objectContaining({
      claimToken: 'token-1', errorType: 'Error',
    }));
    expect(fake.completeExecution).not.toHaveBeenCalled();
  });

  it('reports contention and cancellation', async () => {
    const invalid = client();
    const invalidExecutor = new ActaeToolExecutor(invalid as never, 'run:test');
    await expect(invalidExecutor.execute('invalid', 'invalid', {}, async () => null, { heartbeatIntervalMs: 0 }))
      .rejects.toThrow(/heartbeatIntervalMs/);
    expect(invalid.claimExecution).not.toHaveBeenCalled();

    const busy = client('in_progress');
    const busyExecutor = new ActaeToolExecutor(busy as never, 'run:test');
    await expect(busyExecutor.execute('busy-1', 'busy', {}, async () => null))
      .rejects.toBeInstanceOf(ToolExecutionInProgressError);

    const fake = client();
    const controller = new AbortController();
    const executor = new ActaeToolExecutor(fake as never, 'run:test');
    const running = executor.execute('cancel-1', 'waiting', {}, async (signal) => {
      await new Promise<void>((_resolve, reject) => signal.addEventListener('abort', () => reject(new Error('cancelled')), { once: true }));
      return null;
    }, { signal: controller.signal });
    controller.abort(new Error('cancelled'));
    await expect(running).rejects.toThrow('cancelled');
    expect(fake.cancelExecution).toHaveBeenCalledWith('execution-1', 'token-1');
  });

  it('provides one run context for record and checkpoint operations', async () => {
    const fake = client();
    const context = new ActaeRunContext({ actae: fake as never, channelId: 'run:test', framework: 'test', runId: 'run-1' });
    await context.record('step.completed', { answer: 42 });
    expect(fake.record).toHaveBeenCalledWith(
      'run:test', 'framework.step.completed', expect.objectContaining({ framework: 'test' }), { actor: 'test' },
    );
    await expect(context.saveCheckpoint(envelope())).resolves.toBe(11);
    await expect(context.loadCheckpoint()).resolves.toEqual(expect.objectContaining({ framework: 'test' }));
    await expect(context.replay({ cursor: 2, limit: 10 })).resolves.toEqual([]);
    const fork = await context.fork('run:fork', { reason: 'variant' });
    expect(fork.channelId).toBe('run:fork');
    expect(fork.parentRunId).toBe('run-1');
    await context.createExperiment('models');
    expect(fake.createExperiment).toHaveBeenCalledWith('models', {
      description: undefined, baselineChannelId: 'run:test',
    });
    await context.addToExperiment('experiment-1');
    await context.createExecutionGroup('group-1');
    await context.scheduleWakeup('2026-08-28T10:00:00Z', { retry: 1 });
  });
});
