# Framework Adapter Contract

Actae does not execute an agent. LangGraph, CrewAI, LangChain, Claude Agent
SDK, OpenAI Agents, Codex, and Copilot still call the model, run tools, choose
the next step, and own their in-memory runtime objects. Actae supplies the
durable boundary around that execution.

This contract makes that boundary consistent across Python, TypeScript, and
Go without pretending that unrelated runtimes expose identical internals.

## The contract

Every integration has four common pieces:

1. `ActaeRunContext` / `RunContext` identifies the framework run and exposes
   events, replay, state, forks, experiments, execution groups, wake-ups, and
   mediated tools on its Actae channel.
2. `CheckpointEnvelope` stores portable data and an optional opaque native
   checkpoint together. The framework remains the only component that
   interprets its native checkpoint.
3. `ActaeToolExecutor` / `ToolExecutor` claims a stable operation key before
   a side effect, replays completed results, heartbeats owned leases, and
   persists completion, failure, or cancellation.
4. `AdapterCapabilities` publishes a machine-readable native support claim.
   Code and documentation can no longer imply exact resume where a framework
   only exposes traces or reconstructed context.

The schema names and feature names are identical in all three SDKs:

```text
actae.framework-checkpoint/v1
actae.framework-event/v1

events, replay, checkpoints, resume, forks, tool_executions,
experiments, execution_groups, wakeups, causal_lineage
```

## What “all Actae features” means

There are two deliberately separate support surfaces.

The common Actae surface is the same for every framework. A user can pass a
run context through their existing agent code and use every Actae primitive
without importing framework-specific helpers. This does not give Actae
control of the framework's event loop or agent runner.

The native lifecycle surface describes what the framework itself lets the
adapter capture and restore. This is where fidelity differs:

| Integration | Native fidelity | Native lifecycle status | Why |
|---|---|---|---|
| LangGraph | `checkpoint_exact` | certified | Native checkpoint saver, pending writes, history, fork and resume |
| Claude Agent SDK | `session_native` | certified | Native `SessionStore`, transcripts, subkeys, summaries and fork |
| GitHub Copilot SDK | `session_native` | certified | Native sessions, event recorder, snapshots and continuation |
| LangChain | `context_seeded` | preview | Messages/tool output can be restored; arbitrary runnable internals cannot |
| CrewAI | `reconstructed` | preview | Completed tasks and outputs reconstruct remaining work |
| OpenAI Agents | `checkpoint_exact` | preview | Tracing plus native serializable `RunState` persistence, resume and fork |
| Codex | `observe_only` | observability | OTLP exports events; Codex rollout storage owns resume |

The SDK exposes both profiles so product copy, tests, and UI can be precise:

```python
from actae_client.adapters import (
    get_adapter_capabilities,
    common_surface_capabilities,
)

native = get_adapter_capabilities("openai-agents")
assert native.resume_fidelity.value == "checkpoint_exact"
assert common_surface_capabilities("openai-agents").full_parity
```

`full_parity` on the common surface means every Actae service primitive is
available. It does not upgrade the framework's native resume fidelity.

## Checkpoint model

Adapters preserve their existing snapshot keys and add one reserved key:

```json
{
  "framework_owned_key": "unchanged",
  "_actae_adapter": {
    "schema": "actae.framework-checkpoint/v1",
    "framework": "langgraph",
    "adapter_version": "2",
    "channel_id": "langgraph:...",
    "event_cursor": 42,
    "portable_state": {},
    "native_checkpoint": {},
    "application_state": {},
    "pending_work": {},
    "manifest": {}
  }
}
```

This is backward-compatible: native loaders continue reading the same keys,
and older snapshots without `_actae_adapter` still load. The common run
context can recognize snapshots written by any upgraded adapter.

## Framework-neutral usage

Python:

```python
from actae_client.adapters import ActaeRunContext

run = ActaeRunContext(
    actae=client,
    channel_id="support:ticket-42",
    framework="langgraph",
    run_id="ticket-42",
)

await run.record("step.started", {"step": "charge"})
result = await run.tools.execute(
    "ticket-42:charge:v1",
    "charge_customer",
    {"customer_id": "cus_123", "amount": 2500},
    lambda: payments.charge("cus_123", 2500),
)

fork = await run.fork("support:ticket-42:model-b", reason="model comparison")
await fork.add_to_experiment("experiment-id", declared_delta={"model": "model-b"})
await run.schedule_wakeup("2026-09-01T09:00:00Z", payload={"reason": "follow-up"})
```

TypeScript uses the same concepts from `@actae/sdk/adapters`; Go exposes
`RunContext`, `NewCheckpointEnvelope`, and `NewToolExecutor` from the root
`actae` package.

## Tool-effect guarantees

The mediated tool path requires a stable key chosen by application code. It
does not guess whether two tool calls are the same operation.

- `claimed` and `reclaimed` execute the user-supplied callable.
- `replayed` returns the stored result without calling the tool.
- `in_progress` raises `ToolExecutionInProgressError`.
- Owned leases heartbeat while the call is running.
- Successful results must be JSON-serializable before completion.
- Failures include type and stack; cancellation marks the ledger entry
  cancelled so a later attempt can reclaim it.

This controls duplicate effects while keeping execution in the user's
framework and codebase.

## Certification gates

An adapter may be called `certified` only when its real framework SDK tests
prove the native protocol. The shared contract suites additionally prove:

- capability dependency validation;
- checkpoint round-trip and incompatible-schema rejection;
- preservation of framework-owned state;
- claim, replay, reclaim, contention, heartbeat, failure and cancellation;
- run-context routing for events, replay, checkpoint, fork, experiments,
  execution groups, and wake-ups;
- Python, TypeScript, and Go schema/feature-name parity.

Observability adapters remain useful, but are never described as exact
resume integrations until their upstream runtime exposes a safe checkpoint
or continuation boundary.

The user-facing facade, safe-effect decorator, native provider recipes and
deterministic-orchestrator carrier model are documented in
[SDK Integration](SDK_INTEGRATION.md).
