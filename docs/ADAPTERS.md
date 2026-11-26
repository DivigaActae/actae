# Framework Adapters — Durable Boundaries, Without Owning Execution

Actae is an SDK plus backend; it does not run the user's agents. Each adapter
maps the framework boundaries that actually exist—native checkpoints,
session stores, callbacks, traces, or telemetry—onto Actae events and state.
Exact resume is available only where the framework exposes enough state.

All adapters share a versioned checkpoint envelope, normalized run context,
mediated tool path, and machine-readable capability profile across Python,
TypeScript, and Go. The architecture and precise native-fidelity matrix are
in [Framework Adapter Contract](FRAMEWORK_ADAPTER_CONTRACT.md).

---

## File Reference

| File | Contents |
|------|----------|
| `sdks/python/src/actae_client/adapters/base.py` | Generic `StateManager` — framework-agnostic save/load/resume |
| `sdks/python/src/actae_client/adapters/contract.py` | Shared run context, checkpoint envelope, normalized events, mediated tools |
| `sdks/python/src/actae_client/adapters/profiles.py` | Machine-readable native and common-surface capability claims |
| `sdks/python/src/actae_client/adapters/langgraph.py` | `ActaeCheckpointSaver` — LangGraph BaseCheckpointSaver protocol |
| `sdks/python/src/actae_client/adapters/crewai.py` | `ActaeCrewStateHook` + `CrewAIResumer` — CrewAI task state |
| `sdks/python/src/actae_client/adapters/langchain.py` | `ActaeContextSaver` + `ChainResumer` — LangChain context |
| `sdks/python/src/actae_client/adapters/claude.py` | `ActaeClaudeSessionStore` + `ActaeClaudeHook` — Claude Agent SDK transcripts & hooks |
| `sdks/python/src/actae_client/adapters/openai_agents.py` | `ActaeTracingProcessor` + `install_actae_tracing` — OpenAI Agents SDK tracing |
| `sdks/python/src/actae_client/adapters/codex.py` | `CodexOTLPReceiver` — local OTLP receiver mirroring Codex events into Actae |
| `sdks/python/src/actae_client/adapters/otel.py` | `ActaeOTelBridge` + `trace_context` — export Actae events as OTel GenAI spans (dependency-free) |
| `sdks/typescript/src/adapters/otel.ts` | TypeScript port of the OTel bridge (subpath `@actae/sdk/adapters/otel`) |
| `sdks/go/otel.go` | Go OTel bridge (`OTelBridge`, `EventToSpan`, `TraceContext`, `Client.ExportChannelToOTel`) using `go.opentelemetry.io/otel/trace` |
| `sdks/python/src/actae_client/runtime.py` | `Actae` facade — scopes, decorators, native providers, OpenAI `RunState`, orchestrator carriers |

---

## StateManager (generic)

Framework-agnostic state manager. Every `save()` creates a new immutable version. Use `list_versions()` to browse history and `get_version()` to inspect a specific snapshot.

```python
from actae_client.adapters.base import StateManager

mgr = StateManager(actae_client, channel="my-agent")

# Resume from last run
state = await mgr.resume()
if state is None:
    state = {"messages": [], "cursor": 0, "completed": []}

# Save context (returns version number)
version = await mgr.save(state)

# Browse history
versions = await mgr.list_versions()
state_v3 = await mgr.get_version(version=3)
await mgr.delete_version(version=1)
```

### StateManager API

| Method | Returns | Description |
|--------|---------|-------------|
| `save(state)` | `int` (version) | Save state snapshot, auto-resolves cursor |
| `load()` | `Optional[Dict]` | Load latest state snapshot |
| `resume(default_state?)` | `Dict` | Load latest or return default |
| `list_versions()` | `List[Dict]` | List version history (metadata only) |
| `get_version(version)` | `Optional[Dict]` | Load specific version |
| `delete_version(version)` | `None` | Delete specific version |

---

## LangGraph Adapter

Implements LangGraph's `BaseCheckpointSaver` protocol (1.x) using Actae as the backend. Every checkpoint is stored as an immutable, cursor-aligned state snapshot, giving deterministic resume, `get_state_history`, thread isolation, and crash recovery — including recovery of interrupted steps via pending writes.

```python
from actae_client.adapters.langgraph import ActaeCheckpointSaver

saver = ActaeCheckpointSaver(actae_client)          # channel defaults to "langgraph"
graph = builder.compile(checkpointer=saver)

# Async (recommended):
result = await graph.ainvoke(input, {"configurable": {"thread_id": "1"}})

# Sync (fresh event loop; raises ActaeLangGraphSyncError inside a running loop):
result = graph.invoke(input, {"configurable": {"thread_id": "1"}})

# On restart (new process, same Actae):
config = {"configurable": {"thread_id": "1"}}
previous = graph.get_state(config)   # loads from Actae via checkpointer
graph.invoke(None, config)           # resumes from last checkpoint
```

### Channel resolution

The effective Actae channel is deterministic per (thread, namespace):

```
channel = f"{prefix}:{sha256(thread_id \x00 checkpoint_ns).hexdigest()[:16]}"
```

- `thread_id` is required (`config["configurable"]["thread_id"]`) and must be a non-empty string; Actae does not infer users or workflows.
- `checkpoint_ns` (subgraph namespaces like `node:<task_id>`) isolates nested graph state on its own channel.
- A `channel_resolver` callable `(config) -> str` may be supplied to `ActaeCheckpointSaver(..., channel_resolver=...)` to take full control of the channel; it must be deterministic and return a non-empty string.

### Storage format (envelope v1)

Each Actae state snapshot is a JSON envelope:

| Field | Description |
|-------|-------------|
| `v` | Envelope version (currently `1`) |
| `thread_id`, `checkpoint_ns` | Identity |
| `checkpoint_id`, `parent_checkpoint_id` | LangGraph checkpoint UUID (uuid6; string sort == chronological) and its parent |
| `checkpoint` | LangGraph checkpoint, serialized losslessly with LangGraph's own serde: `{"t": "<type_tag>", "b": "<base64>"}` (type tag `"msgpack"` for typed objects) |
| `metadata` | `CheckpointMetadata` (source/step/parents) — same typed envelope |
| `pending_writes` | Merged task writes: `[{"task_id", "idx", "channel", "t", "b"}]` |
| `event_cursor`, `event_id` | The Actae event cursor the snapshot is aligned to |

Every snapshot is written by first recording a compact event (`langgraph.checkpoint` / `langgraph.pending_writes`, actor `langgraph-checkpointer`) and persisting with the exact cursor returned — cursors are strictly increasing across a thread, so replay is deterministic.

### Protocol coverage

Full 1.x protocol: `aput`/`put`, `aput_writes`/`put_writes`, `aget_tuple`/`get_tuple`, `alist`/`list`, `adelete_thread`/`delete_thread`, plus `config_specs` and `get_next_version` (string versions `"{step:032}.{random:016}"`). Deprecated `save_checkpoint`/`get_checkpoint` shims remain with a `DeprecationWarning`.

- **Pending writes** are merged idempotently per `(task_id, idx)` — the last batch for a task wins (matches the runtime's in-memory semantics) — and saved as a new snapshot version of the same checkpoint id. This is what restores interrupted steps after a crash.
- **Save/write race**: the runtime submits `aput` on a background executor and can call `put_writes` for a checkpoint that is not yet persisted. Writes are buffered in memory and flushed by `aput` as soon as the checkpoint lands (mirrors `InMemorySaver`'s decoupled writes store).
- **Sync bridge**: sync protocol methods run their async implementation via `asyncio.run()` in a fresh event loop, guarded by a threading lock. Calling them inside an active event loop raises `ActaeLangGraphSyncError`. The client's aiohttp session is created per-loop and closed when the fresh loop exits (see `_close_current_loop_session` in `client.py`).
- **Optional dependency**: the module imports without `langgraph`; constructing the saver then raises `ImportError` with install instructions (`pip install 'actae-client[langgraph]'`). `channel_for_config`, `ActaeLangGraphError`, `ActaeLangGraphSyncError` are importable either way.
- **Errors**: `ActaeLangGraphError` (invalid config, corrupt snapshots, envelope version mismatch) and `ActaeLangGraphSyncError` (sync call inside a running loop) — both subclass `ActaeError`.

### Migration

Data written by the pre-1.1.0 fixed-channel adapter is retained in Actae for audit but is not read or migrated by this implementation.

### Tests

- `sdks/python/tests/test_adapters_extended.py` — protocol-complete suite against an in-memory fake: multi-turn memory (sync + async), thread isolation, history newest-first with unique ids, explicit checkpoint lookup, interrupt/resume with a fresh saver (crash recovery), typed serde (AIMessage tool_calls round-trip), idempotent + deferred `put_writes`, subgraph `checkpoint_ns` isolation, sync-in-loop error, delete_thread, cursor alignment, config validation, deprecated shims, and module-without-langgraph (subprocess).
- `sdks/python/tests/test_langgraph_live.py` — opt-in live suite against a running Actae (gated on `ACTAE_URL`/`ACTAE_API_KEY`; skipped otherwise).
- `sdks/python/tests/test_adapter_fork_live.py` — opt-in live fork suite for the framework adapters (same env gating): `CrewAIResumer.fork`, `ChainResumer.fork`, and `ActaeTracingProcessor.fork_trace`. The `fork` methods only need the `ActaeClient` — crewai/langchain are not required at runtime (`importorskip("agents")` only for the OpenAI Agents fork test).

---

## CrewAI Adapter

Saves CrewAI task completion state after each step. Supports one-line restart with automatic skip of completed tasks.

```python
from actae_client.adapters.crewai import ActaeCrewStateHook, CrewAIResumer
from crewai import Crew

hook = ActaeCrewStateHook(actae_client, channel="my-crew")
crew = Crew(..., step_callback=hook.after_step)
crew.kickoff()

# On restart:
resumer = CrewAIResumer(actae_client, channel="my-crew")
remaining = await resumer.get_remaining_tasks(all_tasks)
crew.kickoff(tasks=remaining)
```

### ActaeCrewStateHook

| Method | Description |
|--------|-------------|
| `after_step(task_output, crew?)` | Record completed task + save state snapshot |
| `load_state()` | Load latest state from Actae |
| `save_state(state)` | Force-save arbitrary state |

State schema:
```json
{
    "completed_tasks": ["task1", "task2"],
    "task_outputs": [{"task": "task1", "output": "..."}],
    "crew_id": "uuid"
}
```

### CrewAIResumer

| Method | Description |
|--------|-------------|
| `get_remaining_tasks(all_tasks)` | Filter out completed tasks |
| `resume(crew, all_tasks)` | Full resume: skip completed, kickoff |

Tasks can be CrewAI Task objects (matched by `.description`) or plain strings.

---

## LangChain Adapter

Saves full agent context (messages, tool outputs, chain metadata) as cursor-aligned state snapshots at configurable intervals.

```python
from actae_client.adapters.langchain import ActaeContextSaver, ChainResumer

saver = ActaeContextSaver(actae_client, channel="my-chain", save_every_n=3)
chain.invoke(input, config={"callbacks": [saver]})

# On restart:
resumer = ChainResumer(actae_client, channel="my-chain")
response = await resumer.resume(chain, default_input)
```

### ActaeContextSaver

Extends LangChain's `BaseCallbackHandler`:

| Callback | What it captures |
|----------|-----------------|
| `on_llm_end()` | Assistant response text + run_id |
| `on_tool_end()` | Tool output + run_id |
| `on_chain_end()` | Chain outputs (triggers immediate save) |

Context schema:
```json
{
    "context": {
        "messages": [{"role": "assistant", "content": "...", "run_id": "..."}],
        "tool_outputs": [{"output": "...", "run_id": "..."}],
        "chain_steps": [{"outputs": "...", "run_id": "..."}]
    }
}
```

### ChainResumer

| Method | Description |
|--------|-------------|
| `resume(chain, default_input)` | Load context, inject last message as input, invoke |

---

## Claude Agent SDK Adapter

Drop-in `claude_agent_sdk.SessionStore` backed by Actae, plus an optional
event-forwarding hooks adapter. Both require `pip install 'actae-client[claude]'`
(`claude-agent-sdk>=0.2`); the module imports without the SDK, but the store
raises `ImportError` on construction.

### ActaeClaudeSessionStore — durable, resumable transcripts

```python
from actae_client.adapters.claude import ActaeClaudeSessionStore
from claude_agent_sdk import ClaudeAgentOptions, query

store = ActaeClaudeSessionStore(actae_client)

async for message in query(
    prompt="Refactor this module",
    options=ClaudeAgentOptions(session_store=store),
):
    ...

# On restart (new process, same Actae): continue the same conversation
async for message in query(
    prompt="Now run the tests",
    options=ClaudeAgentOptions(session_store=store, resume=True),
):
    ...
```

Every `append` writes a cursor-aligned, versioned state snapshot of the
transcript and streams a `claude.session.append` event via `transition()`
(atomic event + snapshot). `load` returns the full transcript, so the SDK
reconstructs conversation context on resume. Implements the complete
protocol: `append`, `load`, `delete`, `list_sessions`, `list_subkeys`,
`list_session_summaries`.

Channel resolution (deterministic per key, use `channel_for_session()` to
compute):

```
claude:<sha256(project_key \x00 session_id)[:16]>           # main transcript
claude:<sha256(project_key \x00 session_id \x00 sub)[:16]>  # subkey (subagent) transcript
claude:idx:<sha256(project_key)[:16]>                       # project index
```

Snapshot format (per key):
```json
{
    "entries": [{"type": "user_message", "uuid": "...", "timestamp": "..."}],
    "mtime": 1754236800000,
    "subkeys": ["subagents/<uuid>"]
}
```

- **Subkeys** (subagent transcripts) live on their own channels and are
  registered in the main transcript's `subkeys` list; `delete` on a main
  transcript cascades to them.
- **Project index** snapshots track per-session `mtime` (storage write time,
  strictly monotonic epoch-ms) plus the SDK-owned summary sidecar produced by
  `fold_session_summary` (stored verbatim, never interpreted).
- `ActaeClaudeError` / `ImportError` — see module docstring for details.

### ActaeClaudeHook — live event forwarding

```python
from actae_client.adapters.claude import ActaeClaudeHook
from claude_agent_sdk import ClaudeAgentOptions, query

hook = ActaeClaudeHook(actae_client)
async for message in query(
    prompt="Hello",
    options=ClaudeAgentOptions(hooks=hook.hooks),
):
    ...
```

The `.hooks` property builds `PreToolUse` / `PostToolUse` /
`PostToolUseFailure` / `UserPromptSubmit` / `Stop` matchers that forward each
occurrence as a `claude.hook.<event>` event on channel
`claude-hooks:<sha256(session_id)[:16]>`. This is event streaming only —
use `ActaeClaudeSessionStore` for persistence. Hook failures are caught and
logged; they never break the agent run.

---

## OpenAI Agents SDK Adapter

For new applications, `actae.openai.run/resume/fork` persists the current
OpenAI Agents SDK's native serializable `RunState`. The tracing processor
below remains the independent span-level observability integration. See
[SDK Integration](SDK_INTEGRATION.md); tracing alone does not restore a run,
while the runner facade does.

`agents.tracing.TracingProcessor` implementation backed by Actae. Requires
`pip install 'actae-client[openai-agents]'` (`openai-agents>=0.19`); the module
imports without the SDK, but constructing/installing raises `ImportError`.

```python
from actae_client.adapters.openai_agents import (
    install_actae_tracing,
    uninstall_actae_tracing,
)

proc = install_actae_tracing(actae_client)   # once, before Runner.run
result = await Runner.run(agent, "Hello")

# Latest run's full span tree:
snapshot = await actae_client.latest_state("openai_agents")
for span in snapshot["state"]["spans"]:
    print(span["type"], span["name"])

# Graceful shutdown (await pending writes):
await proc.flush()
```

What gets recorded, per run:

- `openai.trace.start` event — trace id, workflow name, group id
- `openai.span.end` events — one per span: agent runs, LLM generations,
  function/tool calls, guardrails, handoffs, custom spans (type + name +
  parent + timings + error + type-specific fields)
- `openai.trace.end` via `transition()` — atomic event + cursor-aligned
  snapshot with the complete span tree, versioned per run

Snapshot format:
```json
{
    "trace_id": "...", "workflow_name": "Agent workflow", "group_id": null,
    "metadata": null, "started_at": "...", "ended_at": "...",
    "spans": [
        {"type": "AgentSpanData", "name": "research", "handoffs": ["writer"],
         "span_id": "...", "parent_id": null, "started_at": "...",
         "ended_at": "...", "duration_ms": 1200.0}
    ]
}
```

Design notes:

- **Sync/async safe**: tracing callbacks are synchronous; writes are posted
  to a dedicated background event loop (lazy daemon thread) via
  `run_coroutine_threadsafe`, so the processor works with both sync and
  async `Runner.run` without blocking the workflow. `force_flush()`/
  `shutdown()` wait up to `flush_timeout` (default 5 s); `await flush()`
  gathers pending writes during async shutdown.
- **Duck-typed extraction**: spans are summarized with `getattr` against
  `span_data` fields (name/model/handoffs/tools/triggered/usage/...) so the
  adapter survives SDK version changes. `include_inputs_outputs=False`
  strips span input/output payloads (keep names/timings/errors only).
- **Never raises**: callback exceptions are caught and logged (the SDK also
  wraps processors, but we don't rely on it).
- `install_actae_tracing()` adds the processor alongside existing ones
  (OpenAI backend tracing keeps working). `uninstall_actae_tracing(proc)` is
  best-effort — it reaches into the trace provider's processor list (private
  API) and returns `False` if internals changed.

---

## OpenTelemetry Bridge (observability interop)

`ActaeOTelBridge` (Python `actae_client.adapters.otel`, TypeScript
`@actae/sdk/adapters/otel`, Go `actae.NewOTelBridge`) mirrors a channel's
durable events into an OTel tracer as one span per event, lifting the
OpenTelemetry GenAI semantic conventions (`gen_ai.request.model`, usage tokens,
`gen_ai.tool.name`) and the `actae.*` identity attributes. Python/TS are
dependency-free (duck-typed tracer); Go uses the `go.opentelemetry.io/otel/trace`
API. Use `trace_context(trace_id, span_id)` (Go `actae.TraceContext`) in event
metadata to correlate an Actae execution with an external trace. The bridge is
read-only: it never executes anything. Full mapping, the Codex/OpenAI
importers, and the non-goals live in `docs/OBSERVABILITY_INTEROP.md`.

---

## Adapter Pattern

Every adapter follows the same pattern:

1. **Record**: Agent executes a step → adapter calls `ActaeClient.record()` → event persisted
2. **Save state**: Adapter calls `ActaeClient.save_state()` with cursor + serialized agent state → versioned snapshot
3. **Resume**: Adapter calls `ActaeClient.latest_state()` → deserializes state → agent continues from last checkpoint

The cursor alignment ensures:
- State is always pinned to a specific event
- Replay to cursor then restore state is consistent
- Fork at any step preserves full context
