# AgentSession — Agent Lifecycle Instrumentation

High-level wrapper around an Actae channel that records agent execution steps as events, saves state snapshots at configurable intervals, and supports fork/experiment forking for comparing runs.

---

## Choose your integration depth

Actae integrates into the agent loop; it does not replace the loop. The
integration is not "drop-in" — it requires the application to deliberately
define durable steps, state boundaries, and coordinated operations. Three
levels of integration match the actual SDK surface:

| Integration | Customer changes | Actae provides |
|---|---|---|
| Framework adapter | Configure the framework integration | framework-native checkpoint/state capture |
| AgentSession | Mark logical steps + expose state via `state_fn` | recovery, logical steps, forks, lineage, resume |
| Raw primitives | Explicit `record` / `transition` / `claim_execution` / `save_state` | maximum control over every durable boundary |

Pick the shallowest level that meets the requirements; the raw primitives
remain available underneath every higher-level abstraction. The exact/approximate/
lineage-only fork boundary policies apply at every level — restorable forks
depend on snapshot availability at the requested boundary, and Actae never
silently falls forward.

---

## Overview

`AgentSession` tracks step → cursor mapping so you can fork at a user-facing step number rather than a raw server cursor. Every call to `step()` records an event and optionally saves a state snapshot. The session lifecycle is managed via `async with`:

```
created → started → stepping → completed
                             ↘ crashed
```

---

## File

`sdks/python/src/actae_client/session.py`

---

## Constructor

```python
AgentSession(
    actae: ActaeClient,
    name: str,
    *,
    display_name: Optional[str] = None,
    state_fn: Optional[Callable[[], Optional[Dict[str, Any]]]] = None,
    snapshot_interval: int = 1,
    params: Optional[Dict[str, Any]] = None,
    fence_owner: Optional[str] = None,
    fence_lease_seconds: int = 60,
)
```

| Param | Default | Description |
|-------|---------|-------------|
| `actae` | required | Connected `ActaeClient` instance |
| `name` | required | Session name (used as the Actae channel_id) |
| `display_name` | `None` | Human-readable name for the dashboard (falls back to `name`) |
| `state_fn` | `None` | Callback returning the agent's current state dict (called every `snapshot_interval` steps). May be a plain function **or** an `async def` — coroutines are awaited before saving. |
| `snapshot_interval` | `1` | Save a state snapshot every N steps |
| `params` | `None` | Arbitrary experiment parameters (model name, temperature, etc.) |
| `fence_owner` | `None` | Opt-in per-channel ownership fence. When set, the session claims a lease through the execution ledger (`__session_lock__`) and refuses to start if another live owner holds it. |
| `fence_lease_seconds` | `60` | Fence lease duration. Renewed on every `step`; a stale owner that cannot renew raises `SessionLockedError` before appending. |

### Channel ownership fence (`fence_owner`)

By default a session is a pure recorder — two processes *can* write the same
channel (Actae never owns the agent loop). For long-running workers where that
must never happen, set `fence_owner`:

```python
session = AgentSession(actae, "run-42", fence_owner="worker-1", fence_lease_seconds=15)
async with session:            # claims the channel lease
    await session.step(...)    # renews it; raises SessionLockedError if lost
```

Guarantees (proved by the reliability `session_fence` suite):

- A second live owner **cannot start** — `async with` raises `SessionLockedError`.
- After the lease lapses, a new owner may **reclaim** the channel.
- The original owner's next `step` raises `SessionLockedError` and appends
  **no event**.
- A clean exit releases the lease; a crash lets it expire for the next owner.

The fence uses the idempotent execution ledger's atomic claim + lease +
`claim_token` fencing, with an owner-independent request identity so a
different owner reaches the lease check rather than a request-hash mismatch.
It is Python-SDK-only today (Go/TS parity pending).

---

## Lifecycle

### Start

```python
async with AgentSession(actae, name="exp-v1") as session:
    ...
```

On entry:
1. Records a `session.started` event to the channel
2. Sets `session._channel_id` to the channel name
3. Sets `session._status = "started"`
4. Writes session metadata (status, params, cursors) to channel metadata

### Step

```python
async def step(
    self,
    step_type: str,
    *,
    input: Any = None,
    output: Any = None,
    context: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Event:
```

Records each step as an Actae event with:
- `event_type` = `step_type` (e.g. `"inference"`, `"tool_call"`)
- `payload` = `{"step_number": N, "input": ..., "output": ...}` (+ `context`
  when provided — the delta context for this step, stored in the event
  payload so the dashboard can reconstruct cumulative state)
- `metadata` includes caller's metadata + `step_number`

Retries are safe: `Step` retries transient failures (up to 3 with backoff)
and every retry carries the same per-step `operation_id`, so a request that
persisted server-side but lost its response replays the original event
instead of recording a duplicate. (Requires the server `operation_id`
idempotency support; both the Go and Python SDKs send it automatically.)

The per-step `operation_id` is **deterministic** (all three SDKs):
a UUIDv5 over NUL-joined `(scope="agent-session", step_type, channel_id,
step_number, canonical payload+metadata)`, 256-byte cap. The canonical
serialization is byte-identical to Go's `json.Marshal` of
`{"payload": ..., "metadata": ...}` (payload first, recursively sorted
keys, raw UTF-8, `<`, `>`, `&` and U+2028/29 escaped). Consequences:

- A **crash-recovery re-drive** of the same step (same step_number, same
  content) derives the *same* id, so the server replays the original
  event — no duplicates after a crash.
- Step content that differs derives a distinct id and records a new event.
- Python's `_step_operation_id` mirrors the Go SDK's
  `DeterministicOperationKey` + `canonicalStepContent` exactly; the parity
  vectors `07ca53db-606e-5b76-95bc-697e73bc4c41` (plain) and
  `f1f23da4-abd0-50f9-b62c-1f1d0edd2fbf` (escaping) are locked into BOTH
  test suites (`test_session.py::TestDeterministicStepOperationID` and Go's
  `deterministic_test.go::TestDeterministicOperationKeyKnownVectors`).
- **Live proof** (`test_deterministic_step_id_live.py` in the Python SDK;
  `test/deterministic_live.test.ts` in the TS SDK — both opt-in, same env
  gating as the other live suites): a crash-recovery re-drive of the same
  step replays server-side (exactly one event in replay(), returned Event
  is the original), changed content records anew, and metadata key
  insertion order does not break idempotency (canonical sort absorbs it).
- **Go-exact canonical serialization** (all three SDKs): payload first,
  recursively sorted keys, Go float formatting (`1.0` → `1`, `1e-7`, `1e20`
  fixed notation, `-0.0` → `0` — empirically verified against Go's
  encoding/json), raw UTF-8, `<`/`>`/`&`/U+2028/U+2029 escaped. NaN and
  Infinity fail loudly in every SDK (Go's json.Marshal errors), never a
  silently divergent id. Python accepts any `numbers.Real`/`numbers.Integral`
  value (numpy float/int scalars included) and formats it Go-exactly.
- The TS SDK now derives the same deterministic id as Go/Python (it was
  random-per-call before).

After recording, if `state_fn` is set and step is a multiple of `snapshot_interval`:
- Calls `state_fn()` to get current state (awaited first if it returns an awaitable)
- Saves as a versioned state snapshot via `save_state()`

Returns the persisted `Event` so you can inspect its cursor.

### Complete

On successful exit from `async with`, records a `session.completed` event and updates metadata to `"completed"`.

### Crash

If an exception escapes the `async with` body, records the crash reason in metadata and sets status to `"crashed"`.

---

## Fork

```python
async def fork(
    self,
    at_step: int,
    name: str,
    *,
    display_name: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    state_fn: Optional[Callable[[], Optional[Dict[str, Any]]]] = None,
    reason: Optional[str] = None,
    boundary_mode: str = "exact",
    manifest: Optional[Dict[str, Any]] = None,
    intervention: Optional[Dict[str, Any]] = None,
    tool_policies: Optional[Dict[str, Any]] = None,
) -> AgentSession:
```

`intervention` is an opaque descriptor of the intended change (e.g.
`{"model": "candidate"}`) recorded on the fork metadata; the application reads
`session.intervention` and applies it — Actae never interprets it.
`tool_policies` controls child tool executions (`replay`/`block`/`live`/`auto`,
default `auto`) so a fork cannot silently re-fire an inherited side effect.
`resume(..., fork_at_step=N, intervention=..., tool_policies=...)` forwards
both. See `docs/FORK_RESUME.md` → "Side-effect-aware forking".

Creates a new experiment fork from this session at a specific step:
1. Resolves step → cursor via `_cursor_for_step()`
2. Calls `actae.fork()` to create the fork channel
3. Returns a new *unstarted* `AgentSession` with `forked_from` metadata

The returned session inherits:
- `state_fn` (falls back to parent's)
- `snapshot_interval`
- `params` (merged with fork-specific overrides)

Usage:

```python
fork_session = await parent.fork(at_step=5, name="exp-gpt5", params={"model": "gpt-5"})
async with fork_session:
    for item in dataset[5:]:
        await fork_session.step("inference", input=item, output=llm.call(item, model="gpt-5"))
```

---

## Resume

```python
@classmethod
async def resume(
    cls,
    actae: ActaeClient,
    channel_id: str,
    *,
    fork_at_step: Optional[int] = None,
    name: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    state_fn: Optional[Callable[[], Optional[Dict[str, Any]]]] = None,
    snapshot_interval: int = 1,
) -> AgentSession:
```

Two modes:

### Crash Recovery (no `fork_at_step`)

Resumes a previously crashed session from its last state:

```python
session = AgentSession.resume(actae, "exp-v1")
async with session:
    await session.step("inference", input=next_item, output=result)
```

- Loads metadata (cursors, step_count, params) from channel
- Sets `_is_resumed = True` — on `__aenter__`, skips `_start()` and sets status to `"started"`
- Raises `SessionCompletedError` if the session is already completed

**Hard-kill recovery is log-authoritative.** Session metadata is written on
lifecycle transitions (start, first step, completion, caught crash), so a
**hard** process death (`os._exit`, SIGKILL, power loss) can leave it lagging
— historically as low as step 1. `resume()` therefore reconstructs progress
from the durable event log (every `step()` writes `payload.step_number` in the
same transaction as its event) and only overrides metadata when the log is
**ahead** of it (so retention-purged events can never make a resume go
backwards). This is implemented in all three SDKs
(`_recover_step_progress` / `recoverStepProgress`); it is proven by the
reliability crash suite, which kills a worker with `os._exit(137)` and asserts
the recovered `step_count`.

### Fork-from-existing (`fork_at_step` set)

Forks an existing (even completed) session's state into a new channel:

```python
session = AgentSession.resume(
    actae, "exp-v1",
    fork_at_step=5, name="exp-v2", params={"model": "gpt-5"},
)
```

Calls `_fork_from_channel()` which resolves the step → cursor mapping:
1. **Fast path**: reads `cursors` list from source channel metadata
2. **Fallback**: replays events and resolves by position (validates cursor
   monotonicity). The replay is paged (server caps at 1000 events/call),
   the server's `fork.started` lineage marker is excluded from position
   mapping, and when no saved state boundary exists at the resolved cursor
   the fork falls back to the latest state (`at_cursor=0`) — so
   event-only channels still fork and resume. (Same semantics in the Go
   SDK.)

---

## Step ↔ Cursor Mapping

```python
def _cursor_for_step(self, step_number: int) -> int
```

Internal method that maps a 1-indexed step number to its server cursor by indexing into `self._cursors`.

The `_cursors` list grows with every `step()` call. It's persisted in channel metadata (capped at `_MAX_STORED_CURSORS = 1000`) for crash recovery without replaying events.

---

## Metadata Schema

Written to channel metadata via `update_metadata()`:

```
experiment_metadata: {
    "session_name": str,
    "params": dict,
    "status": "created" | "started" | "stepping" | "completed" | "crashed",
    "started_at": str (rfc3339),
    "step_count": int,
    "cursors": [int],       # step → cursor mapping (capped at 1000)
    "cursors_stored": int,  # actual count stored
    "cursors_warning": str? # present if steps > 1000
    "crash_reason": str?    # present on crash
}
```

---

## Retry Logic

Transient failures (connection errors, 5xx, 429, timeouts) are retried with exponential backoff:

- Max retries: 3 (4 total attempts)
- Base delay: 0.5s
- Max delay: 5.0s
- Backoff: `delay = min(0.5 * 2^(attempt-1), 5.0)`

Applies to: `step()`, `update_metadata()`, `_fork_from_channel()`

---

## Properties

| Property | Type | Description |
|----------|------|-------------|
| `channel_id` | `Optional[str]` | The Actae channel backing this session |
| `name` | `str` | Session name |
| `step_count` | `int` | Number of recorded steps |
| `status` | `str` | Current lifecycle status |
| `cursors` | `List[int]` | Cursor for each recorded step |
| `params` | `Dict[str, Any]` | Experiment parameters |

---

## Session Events

Two reserved event types:

| Event Type | When | Payload |
|------------|------|---------|
| `session.started` | Session enters `async with` | `{"session_name", "params"}` |
| `session.completed` | Session exits `async with` normally | `{"session_name", "total_steps"}` |

---

## Error Types

| Exception | When |
|-----------|------|
| `SessionError` | Status violation (e.g., stepping before start), invalid arguments |
| `SessionCompletedError` | Attempting to resume a completed session without `fork_at_step` |
| `SessionLockedError` | A fenced session cannot start (another live owner) or a stale owner's `step`/start finds the lease reclaimed |

---

## Thread Safety

`AgentSession` is **not thread-safe**. Uses `asyncio.Lock` to serialize concurrent `step()` calls within a single async task. `fork()` and `resume()` should not overlap with `step()`.
