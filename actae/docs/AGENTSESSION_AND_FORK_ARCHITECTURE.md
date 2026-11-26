# AgentSession & Fork Architecture — Actae

> LLM-oriented reference. Everything built across the session: Phase A (fork MVP) → Phase B (experiment management) → Phase C (comparison) → AgentSession (production SDK). Read this to understand the full system before editing any of its files.

---

## Table of Contents

1. [Philosophy & Design Principles](#1-philosophy--design-principles)
2. [Database Schema](#2-database-schema)
3. [Rust Server Infrastructure](#3-rust-server-infrastructure)
4. [Python SDK](#4-python-sdk)
5. [TypeScript SDK](#5-typescript-sdk)
6. [Dashboard — Flow Page & Components](#6-dashboard--flow-page--components)
7. [Authentication Changes](#7-authentication-changes)
8. [Lab Scenarios](#8-lab-scenarios)
9. [Framework Adapters](#9-framework-adapters)
10. [Test Suite](#10-test-suite)
11. [sonic_rs Serialization Problem](#11-sonic_rs-serialization-problem-critical)
12. [Known Limitations](#12-known-limitations)
13. [Quick Reference & Commands](#13-quick-reference--commands)
14. [Key Files — Absolute Paths](#14-key-files--absolute-paths)

---

## 1. Philosophy & Design Principles

**Actae is an event/state/metadata store, not an agent framework.** It provides the infrastructure for forking, resuming, and comparing agent execution pipelines — but does NOT execute agents, run LLM calls, orchestrate workflows, or merge forks.

### Core Constraints

- **Events are immutable.** Never modify `agent_events`. Forking creates a new channel with a metadata link to its parent — original history is never touched. Git model: fork = pointer + metadata, commits = immutable.
- **No cross-channel mutation.** A fork channel owns its events entirely. Parent and child diverge from the fork point.
- **All state is serializable.** Actae only stores JSON blobs. The developer owns state serialization via `state_fn` callbacks.
- **No framework coupling.** `AgentSession` works with any vanilla Python agent code. Adapters (LangGraph, CrewAI, LangChain) are optional.

### Data Flow

```
Developer Python Code
       │
       
  AgentSession (context manager)
       │  step() / fork() / resume()
       
  Python SDK (actae_client) ──HTTP── Actae Server (Rust)
       │                                   │
       │  record / fork / state             │  store.rs ── PostgreSQL
       │                                   │  handler.rs ── auth/rbac
                                          
  Dashboard (Next.js) ──HTTP/WS───  main.rs (axum)
       │
  Flow page: ForkTree, CompareView, ForkButton, MetadataEditor
```

### Versioning

Three `AgentEventType` variants added to enable experiment traceability:
- `RunCreated` → `"run.created"` — recorded when a root session starts
- `ForkCreated` → `"fork.created"` — recorded on parent when fork occurs
- `ForkStarted` → `"fork.started"` — recorded on child when fork session starts

These are defined in `schema.rs:91-96` and serialized via `as_str()` / `from_str()` at lines 141-143 and 183-185.

---

## 2. Database Schema

All migrations live in `src/events/migrations.rs`. Six total versions.

### Migration v1: `agent_events` (append-only event log)
```sql
CREATE TABLE IF NOT EXISTS agent_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cursor BIGSERIAL NOT NULL,
    txid XID8 NOT NULL DEFAULT pg_current_xact_id(),
    channel_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    actor TEXT NOT NULL,
    agent_id TEXT,
    user_id TEXT,
    metadata JSONB,
    channel_cursor BIGINT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tenant_id TEXT
);
```
Indexes: `(channel_id, cursor)`, `(tenant_id, channel_id)`, `(channel_id, cursor ASC, txid)`.

### Migration v2: `channel_cursors` (per-channel gapless cursor counter)
```sql
CREATE TABLE IF NOT EXISTS channel_cursors (
    channel_id TEXT NOT NULL,
    last_cursor BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (channel_id)
);
```
Also adds `channel_cursor` and `txid` columns to `agent_events` (IF NOT EXISTS). Index on `(channel_id, channel_cursor)`.

### Migration v4: `channel_states` (latest state per channel)
```sql
CREATE TABLE IF NOT EXISTS channel_states (
    channel_id TEXT NOT NULL,
    cursor BIGINT NOT NULL,
    state JSONB NOT NULL DEFAULT '{}',
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id)
);
```
Upsert pattern: `INSERT ... ON CONFLICT (channel_id) DO UPDATE`. Only the latest state is kept.

### Migration v5: `channel_metadata` (fork/experiment metadata)
```sql
CREATE TABLE IF NOT EXISTS channel_metadata (
    channel_id TEXT PRIMARY KEY,
    parent_channel_id TEXT,
    origin_run_id TEXT NOT NULL,
    forked_at_cursor BIGINT NOT NULL DEFAULT 0,
    forked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    display_name TEXT,
    reason TEXT,
    experiment_metadata JSONB DEFAULT '{}'
);
```
Indexes: `(parent_channel_id)` for fast fork listing, `(origin_run_id)` for full-tree queries.

### Migration tracking: `_migrations` (auto-managed)
```sql
CREATE TABLE IF NOT EXISTS _migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

> Note: the pre-1.2 label-based snapshot API (`state_snapshots` table,
> `/api/v1/snapshots` routes) was removed. Versioned state lives entirely
> in `channel_states` (see migration v4 + v7) and migration v14 drops any
> leftover `state_snapshots` table from older deployments.

---

## 3. Rust Server Infrastructure

### 3.1 `main.rs` — Route Registration & Handler Functions

**File:** `src/main.rs` (1808 lines)

All fork/experiment routes are registered in the main axum router (lines 470-483):

```rust
// Fork API
.route("/api/v1/channels/fork", post(fork_channel_handler))
// Channel metadata
.route("/api/v1/channels/{channel_id}/metadata", get(channel_metadata_handler))
// Fork listing
.route("/api/v1/channels/{channel_id}/forks", get(list_forks_handler))
// Execution tree
.route("/api/v1/channels/{channel_id}/tree", get(execution_tree_handler))
// Metadata update
.route("/api/v1/channels/{channel_id}/metadata", put(update_metadata_handler))
```

#### Handler Functions (lines 1146-1508)

Each handler follows the same pattern:
1. Load global state via `actae::api::global_state()`
2. Check rate limit via `check_rate_limit()`
3. Deserialize request body from `sonic_rs::from_slice()`
4. Resolve auth via `resolve_auth_context()`
5. Delegate to `state.event_handler.<method>()`
6. Serialize response with `clean_sonic_value()` on any metadata/state fields

**`err_to_status()`** (line 1127): Maps `EventError` variants to `(StatusCode, &'static str)` tuples for consistent error responses across all handlers.

**`clean_sonic_value()`** is applied to:
- `experiment_metadata` in `channel_metadata_handler`, `list_forks_handler`, `update_metadata_handler`
- `payload` in `record_event_handler` (line 818-826), `replay_events_handler` (line 874-884), `query_events_handler`

#### Auth Resolution: `resolve_tenant_from_request()` (lines 678-787)

1. Extracts `x-api-key` header → validates against DB via `validate_api_key()`
2. Falls through to `AuthContext::Anonymous` when `enable_anonymous` is true
3. Blocks anonymous tenant-scoped requests

Anonymous auth is enabled by setting `ENABLE_ANONYMOUS=true`. The `EventHandler` is constructed with `.with_anonymous(config.enable_anonymous)` at line 252-256.

### 3.2 `handler.rs` — EventHandler Auth-Gated Methods

**File:** `src/events/handler.rs` (1011 lines)

The `EventHandler` struct wraps `EventStore` with auth validation. Each public method:
1. Calls `self.validate_auth(auth, channel_id, Permission)` — allowed/forbidden/unauthenticated
2. Optionally calls `self.validate_tenant_access(auth, tenant_id)` — cross-tenant guard
3. Delegates to `self.store.<method>()`

#### Fork/Metadata/Snapshot Methods (lines 530-615)

| Method | Line | Auth | Store Call |
|--------|------|------|------------|
| `fork()` | 532 | Read on source + Write on new | `store.fork_channel()` |
| `get_channel_metadata()` | 547 | Read | `store.get_channel_metadata()` |
| `list_forks()` | 558 | Read | `store.list_forks()` |
| `get_fork_tree()` | 569 | Read on root | `store.get_fork_tree()` |
| `update_metadata()` | 605 | Write | `store.update_metadata()` |

#### `validate_auth()` (lines 287-327)

- `AuthContext::Anonymous` → OK if `self.allow_anonymous`, else `Unauthenticated`
- System/admin channels → always `Forbidden`
- Permission check against auth context's permission list (`can_read()`/`can_write()`)

#### `Anonymous` support in `list_channels()` (lines 250-269)

Special case: anonymous is only allowed if `self.allow_anonymous` is set. Otherwise, anonymous gets `Unauthenticated`.

#### Unit Tests (lines 618-1011)

68 unit tests covering: auth validation (anonymous rejection, system channels, read/write permission checks), event validation (channel_id, event_type, actor, payload size limits), tenant access, `AuthContext` permission checks, `EventError` display and conversion.

#### `with_anonymous()` Builder (lines 49-52)

```rust
pub fn with_anonymous(mut self, allow: bool) -> Self {
    self.allow_anonymous = allow;
    self
}
```
This is called in `main.rs:252-256` with `config.enable_anonymous`.

### 3.3 `store.rs` — EventStore Database Operations

**File:** `src/events/store.rs` (1045 lines)

#### `fork_channel()` (lines 735-850)

Transactional fork operation:
1. **Source existence check** (lines 754-768): SELECT FOR UPDATE on `channel_cursors`, `channel_states`, and `agent_events` — all inside the transaction. Returns `NotFound` if all three return no rows. This prevents TOCTOU races.
2. **origin_run_id computation** (lines 775-789): Checks if parent has its own `channel_metadata`. If yes, uses parent's `origin_run_id` (skips intermediate parent). If no, `origin_run_id = source_channel_id`.
3. **State copy** (lines 791-812): Copies `channel_states` row to new channel with cursor=0.
4. **Metadata insertion** (lines 814-833): INSERT with ON CONFLICT DO NOTHING.
5. **Commit** (lines 835-838).

#### `get_fork_tree()` (lines 901-935)

Recursive tree builder:
1. Get metadata for root (or create synthetic metadata if none exists)
2. Get event_count and latest_cursor
3. List direct children via `list_forks()`
4. Recursively build each child's tree via `Box::pin(self.get_fork_tree(...))`
5. Return `ForkInfo` with metadata, children vec, event_count, latest_cursor

#### `update_metadata()` (lines 1016-1044)

Upsert: `INSERT ... ON CONFLICT (channel_id) DO UPDATE`. Uses `COALESCE` for each field so existing values are preserved when the request doesn't include them. The ON CONFLICT clause is critical — without it, channels created outside the fork API (e.g., via `record()` which doesn't insert metadata) would fail on their first `update_metadata()` call.

### 3.4 `schema.rs` — Request/Response Structs

**File:** `src/events/schema.rs` (677 lines)

#### Key Structs

| Struct | Fields | Purpose |
|--------|--------|---------|
| `ForkRequest` | source_channel_id, new_channel_id, at_cursor, display_name?, reason?, experiment_metadata?, tenant_id? | POST body for fork |
| `ChannelMetadata` | channel_id, parent_channel_id?, origin_run_id, forked_at_cursor, forked_at, display_name?, reason?, experiment_metadata? | DB + API response |
| `ForkInfo` | metadata (ChannelMetadata), children (Vec<ForkInfo>), event_count, latest_cursor | Recursive tree node |
| `UpdateMetadataRequest` | channel_id, display_name?, reason?, experiment_metadata?, tenant_id? | PUT body for metadata update |

#### `serialize_sonic_value_opt` (lines 7-20)

Custom serde serializer for `Option<sonic_rs::Value>` fields. Round-trips through JSON string to produce clean `serde_json::Value` output — this is how `experiment_metadata` avoids the sonic_rs number wrapper problem in HTTP responses.

Applied to `experiment_metadata` fields in `ChannelMetadata` and related structs.

### 3.5 `json_compat.rs` — sonic_rs ↔ PostgreSQL Bridge

**File:** `src/database/json_compat.rs` (159 lines)

#### `JsonValue` Wrapper (lines 12-103)

Wraps `sonic_rs::Value` to implement PostgreSQL's `ToSql` and `FromSql` traits. JSONB format requires a version 1 header byte (0x01) prepended to the UTF-8 JSON string.

#### `sonic_to_serde()` (lines 126-129)

Converts `sonic_rs::Value` → `serde_json::Value` via JSON string round-trip. Used in `serialize_sonic_value_opt`.

#### `clean_sonic_value()` (lines 133-136)

Round-trips a `sonic_rs::Value` through its own JSON string representation. This strips the `$sonic_rs::private::JsonNumber` wrappers that sonic_rs's `Serialize` impl produces.

**Why this is necessary:** `sonic_rs::Value`'s `Serialize` impl wraps numbers as `{"$sonic_rs::private::JsonNumber": "42"}` instead of plain `42`. When axum's `Json<sonic_rs::Value>` serializes a response containing numbers, the sonic wrapper leaks to the client. The round-trip through `sonic_rs::to_string()` + `sonic_rs::from_str()` uses sonic_rs's internal parser which correctly reconstructs number types.

### 3.6 Authorization

Fork and state handlers enforce access through `resolve_auth_context()` +
`EventHandler.validate_auth()` (API key / anonymous gating). The license
feature system was removed.

---

## 4. Python SDK

### 4.1 `client.py` — ActaeClient HTTP Methods

**File:** `sdks/python/src/actae_client/client.py` (678 lines)

Implemented fork/experiment methods:

| Method | Line | Route | Notes |
|--------|------|-------|-------|
| `fork()` | 237 | POST `/api/v1/channels/fork` | Returns `ChannelMetadata` |
| `get_channel_metadata()` | 275 | GET `/api/v1/channels/{id}/metadata` | Returns `None` on 404 |
| `list_forks()` | 286 | GET `/api/v1/channels/{id}/forks` | Returns `List[ChannelMetadata]` |
| `get_fork_tree()` | 291 | GET `/api/v1/channels/{id}/tree` | Returns `ForkInfo` (recursive) |
| `update_metadata()` | 333 | PUT `/api/v1/channels/{id}/metadata` | Returns `ChannelMetadata` |
| `latest_cursor()` | 198 | GET `/api/v1/events/cursor/{id}` | Alias for `get_cursor()` |

Every method that receives `experiment_metadata` in the response applies `_unwrap_sonic()` to strip sonic_rs number wrappers.

### 4.2 `types.py` — Data Classes & sonic_rs Workaround

**File:** `sdks/python/src/actae_client/types.py` (155 lines)

#### `_unwrap_sonic(value: Any) -> Any` (lines 7-28)

Recursively walks a parsed JSON structure and replaces sonic_rs number wrapper dicts with native Python int/float:

```python
# Input from server:
{"$sonic_rs::private::JsonNumber": "42"}

# Output:
42
```

Handles: dict recursion, list recursion, negative numbers, floats, `ValueError`, `OverflowError`, empty strings (returns unmodified value).

Applied in: `Event.from_record()`, `Event.from_replay()`, `Event.from_query()`, `Event.from_broadcast()`, `ChannelMetadata` construction, `latest_state()`, `list_forks()`, `update_metadata()`.

#### `Event` Dataclass (lines 55-124)

Five factory methods:
- `from_record()` — POST /record response
- `from_replay()` — GET /replay response (also fixes missing `metadata` field)
- `from_query()` — POST /query response
- `from_broadcast()` — WS broadcast message

#### `ChannelMetadata` Dataclass (lines 32-40)
#### `ForkInfo` Dataclass (lines 44-51)

### 4.3 `session.py` — AgentSession Context Manager

**File:** `sdks/python/src/actae_client/session.py` (598 lines)

#### Lifecycle State Machine

```
created → started → stepping → completed
                             ↘ crashed
```

| Status | Meaning |
|--------|---------|
| `created` | Object created, no channel |
| `started` | `__aenter__` called, `session.started` event recorded |
| `stepping` | At least one `step()` called |
| `completed` | `__aexit__` (normal), `session.completed` recorded |
| `crashed` | `__aexit__` (exception), metadata updated with crash_reason |

#### Constructor: `__init__()` (line 118)

```python
def __init__(self, actae, name, *, state_fn=None, snapshot_interval=1, params=None):
```

- `name` — used as channel_id (data-plane, not just display)
- `state_fn` — callback `() -> Optional[Dict]`, called every `snapshot_interval` steps
- `snapshot_interval` — clamped to `max(1, snapshot_interval)` (negative/zero → 1)
- `params` — experiment parameters, persisted in metadata

Internal state: `_channel_id`, `_step_count`, `_cursors` (list of ints), `_status`, `_lock` (asyncio.Lock).

#### Enter/Exit: `__aenter__()` (line 182), `__aexit__()` (line 190)

On fresh start: calls `_start()` which records `session.started` event, sets channel_id, calls `_update_metadata_status("started")`.

On resume: sets `_status = "started"`, updates metadata.

On exception exit: records `_crash()` with exception name. On normal exit: records `_complete()` with `session.completed` event.

#### `_start()` (line 198)

```python
ev = await self._actae.record(
    channel_id=self._name,
    event_type=SESSION_STARTED,  # "session.started"
    payload={"session_name": self._name, "params": self._params},
    actor="agent_session",
    metadata={"session_name": ..., "snapshot_interval": ..., "has_state_fn": ...},
)
self._channel_id = ev.channel_id
```

#### `_complete()` (line 212)

Records `session.completed` event with total_steps. Updates metadata status to "completed". Handles failure gracefully (logs exception, doesn't re-raise).

#### `_crash(reason)` (line 230)

Sets `_status = "crashed"`, calls `_update_metadata_status("crashed", extra={"crash_reason": reason})`. Logs a warning.

#### `step()` (line 304)

```python
async def step(self, step_type, *, input=None, output=None, metadata=None) -> Event:
```

1. **Status guard**: raises `SessionError` if status not in `{"started", "stepping"}`.
2. **Lock**: `async with self._lock` — prevents interleaved cursor/step counting.
3. **Record event**: calls `self._actae.record()` with retry via `_retry()`.
4. **Track cursor**: appends `ev.cursor` to `self._cursors`.
5. **Save state**: if `state_fn` is set and `step_count % snapshot_interval == 0`, calls `state_fn()` and `actae.save_state()`.
6. **Update metadata**: if first "stepping" transition, persists to channel_metadata.
7. Returns the Event.

#### `_retry()` (line 78)

Exponential backoff for transient failures:
- Base delay: 0.5s
- Max delay: 5.0s
- Max retries: 3 (+ 1 initial attempt = 4 total)
- Retryable: `ConnectionError`, `APIError` with 5xx or 429, `asyncio.TimeoutError`

#### `fork()` (line 360)

```python
async def fork(self, at_step, name, *, params=None, state_fn=None, reason=None) -> AgentSession:
```

1. **Status guard**: raises `SessionError` if status not in `{"started", "stepping", "completed", "crashed"}`.
2. **Resolve cursor**: `self._cursor_for_step(at_step)` — validates step_number bounds.
3. **Merge params**: parent params + override params.
4. **Call SDK fork**: `self._actae.fork(parent_channel, name, cursor, ...)` with experiment_metadata containing fork info.
5. **Return new session**: creates unstarted `AgentSession` with `_channel_id = name`.

Returns an **unstarted session**. The developer must call `async with fork_session:` to begin recording.

#### `resume()` classmethod (line 416)

```python
@classmethod
async def resume(cls, actae, channel_id, *, fork_at_step=None, name=None, params=None, state_fn=None, snapshot_interval=1) -> AgentSession:
```

**Two modes:**

1. **Crash recovery** (no `fork_at_step`): Loads channel metadata, checks status. If "completed" → `SessionCompletedError`. If "crashed" → auto-resumes, loads step_count and cursors from metadata. Returns a session in "started" state.

2. **Fork-from-existing** (`fork_at_step` set): Calls `_fork_from_channel(source, at_step)` which:
   - Resolves step → cursor via metadata cursors list (fast path)
   - Falls back to replay (slow path) with cursor monotonicity validation
   - Calls `actae.fork()` to create the new channel
   - Returns an unstarted session

#### `_fork_from_channel()` (line 489)

Fork resolution wins:
1. Try metadata `cursors` list (fast — AgentSession-created channels)
2. Fallback: replay up to 10,000 events, validate monotonicity, resolve by position

Monotonicity validation (lines 538-546): If any two consecutive events have non-increasing cursors, raises `SessionError`. This catches channels with interleaved writers where position-based step resolution would silently produce wrong results.

#### `_cursor_for_step()` (line 569)

Validates: `isinstance(step_number, int)`, `step_number >= 1`, `step_number <= len(_cursors)`. Returns `_cursors[step_number - 1]`.

#### `_update_metadata_status()` (line 238)

Read-modify-write pattern:
1. Load existing metadata (preserves fork fields set by `fork()`)
2. Build session metadata dict (session_name, params, status, started_at, step_count, cursors)
3. Cap cursors at 1000 entries (with `cursors_warning` if truncated)
4. Merge: `{**existing, **meta, **extra}` — fork fields survive because they're in `existing`
5. Call `actae.update_metadata()` with retry

#### `_require_status()` (line 173)

```python
_VALID_STEP_STATUSES = frozenset({"started", "stepping"})
_VALID_FORK_STATUSES = frozenset({"started", "stepping", "completed", "crashed"})
```

Raises `SessionError` with descriptive message if status mismatch.

### 4.4 `__init__.py` — Public Exports

**File:** `sdks/python/src/actae_client/__init__.py`

Exports: `ActaeClient`, `Event`, `ChannelMetadata`, `ForkInfo`, `AgentSession`, `SessionError`, `SessionCompletedError`, plus error types.

---

## 5. TypeScript SDK

**File:** `sdks/typescript/src/types.ts` (204 lines, added)

Types: `ForkRequest`, `ChannelMetadata`, `ForkInfo`, `SaveSnapshotRequest`, `SnapshotInfo`, `UpdateMetadataRequest`.

Built declarations: `sdks/typescript/dist/index.d.ts` and `index.d.mts`.

---

## 6. Dashboard — Flow Page & Components

### 6.1 Page Layout (`page.tsx`)

**File:** `dashboard/src/app/dashboard/flow/page.tsx` (136 lines)

The flow page (at `/dashboard/flow`) is the experiment visualization hub. Layout:

```
┌─────────────────────────────────────────────────────────────┐
│  Toolbar (48px)                                             │
│  [Flow] | [Channel ] | [Replay from ___ Go Reset] |        │
│  [Fork] | [→ ⊞ ⇉] | [ Live/Historical] [Inspector]        │
├────────────┬────────────────────────────────────────────────┤
│ Sidebar    │  EventFlowCanvas                               │
│ (260px)    │                                                │
│            │  scroll to zoom · drag to pan · click inspect  │
│ ForkTree │                                                │
│ ────────── │  [inherited events dimmed, dashed edges]       │
│ MetadataEd │                                                │
│ ────────── │                                                │
│ CompareView│                                                │
├────────────┴────────────────────────────────────────────────┤
│  Footer bar (28px): scroll to zoom · drag to pan · click to inspect  │
│  [Hide live feed] [cursor@3 · step ...]   (when Live mode on)       │
└─────────────────────────────────────────────────────────────┘
```

The sidebar (left, 260px fixed) contains three stacked panels in a flex column:
1. **ForkTree** — at top, bordered below
2. **MetadataEditor** — middle, bordered below
3. **CompareView** — bottom, flex-1, scrollable

The main area shows the **EventFlowCanvas** with node-based event visualization. An optional **InspectorPanel** slides in from the right (resizable, 240-600px) when toggled.

Layout modes (horizontal/grid/compact) change how events are arranged on the canvas.

### 6.2 All 12 Flow Components

**Directory:** `dashboard/src/components/flow/` (12 files)

| # | Component | File | Lines | Purpose |
|---|-----------|------|-------|---------|
| 1 | `ChannelSelector` | `ChannelSelector.tsx` | 35 | Dropdown to pick a channel. On change: calls `setActiveChannel()` which triggers `loadEvents()` + `loadForks()`. Auto-loads channel list on mount. |
| 2 | `ReplayControls` | `ReplayControls.tsx` | 59 | Text input (cursor number) + [Go] + [Reset] buttons. Calls `setReplayCursor()` which re-fetches events from that cursor onward. |
| 3 | `ForkButton` | `ForkButton.tsx` | ~80 | Dialog overlay: name input, reason textarea, submit. Calls `forkChannel()` API. Shows error/success. |
| 4 | `LayoutSelector` | `LayoutSelector.tsx` | 33 | Three-mode toggle: `→` (horizontal line), `⊞` (grid, 4 per row), `⇉` (compact). Controls `layoutMode` in store. |
| 5 | `LiveToggle` | `LiveToggle.tsx` | 33 | Green pulse dot when live, gray when historical. Uses `useWebSocket` hook. Shows "connecting..." when WS not connected. |
| 6 | `ForkTree` | `ForkTree.tsx` | ~120 | Recursive expand/collapse tree. Each node shows display_name, reason, forked_at_cursor. Uses `getForkTree` API. |
| 7 | `MetadataEditor` | `MetadataEditor.tsx` | ~70 | Inline display_name + reason editing. Saves via `updateChannelMetadata` on blur/enter. Shows the current channel's metadata. |
| 8 | `CompareView` | `CompareView.tsx` | ~150 | Two-pane fork selector drop-downs + side-by-side event lists. Uses `diffEvents()` and `computeComparisonMetrics()` to show shared/divergent events with latency/tokens/cost comparison. |
| 9 | `EventFlowCanvas` | `EventFlowCanvas.tsx` | ~200 | The main visualization canvas. Renders event nodes as a horizontal flow. Inherited events (from parent before fork) shown dimmed at 40% opacity with dashed gray edges. Built on `@xyflow/react`. |
| 10 | `EventNode` | `EventNode.tsx` | ~60 | Individual event card on the canvas. Shows: type, cursor, timestamp, payload snippet. When `isInherited: true`, renders with a "forked from" badge and dimmed styling. |
| 11 | `StateNode` | `StateNode.tsx` | 42 | State snapshot node on the canvas. Dashed purple border. Shows top 6 state keys as pills, with `+N more`. Clickable to inspect. |
| 12 | `InspectorPanel` | `InspectorPanel.tsx` | 153 | Right-side detail panel. Resizable (drag handle). Sections: Latest State (purple), Channel Stats (4-card grid: events loaded, latest cursor, state cursor, has state), Event Type Breakdown (sorted bar chart), Selected Event Detail (JSON tree). |

### 6.3 `flow-store.ts` — Zustand State

**File:** `dashboard/src/lib/flow-store.ts` (273 lines)

Complete state shape:
```typescript
interface FlowState {
  channels: string[]
  activeChannel: string | null
  events: DashboardEvent[]
  stateSnapshot: { cursor: number; state: Record<string, unknown> } | null
  loading: boolean
  error: string | null
  isLive: boolean
  liveEvents: DashboardEvent[]
  connection: { connected: boolean; authenticated: boolean; error: string | null }
  selectedNode: { type: string; cursor: number; payload?: any; state?: any } | null
  replayFromCursor: number | null
  inspectorOpen: boolean
  inspectorWidth: number
  layoutMode: 'horizontal' | 'grid' | 'compact'
  forks: ChannelMetadata[]
  forkedAtCursor: number | null
}
```

**Key actions:**
- `loadEvents()` — Checks if channel is a fork via `getChannelMetadata()`. If so, replays parent events up to `forked_at_cursor`, marks them `isInherited: true`. Then replays own events. Loads latest state snapshot. In mock mode, uses hardcoded parent/fork maps.
- `loadForks()` — Fetches direct children via `listForks()`.
- `setReplayCursor()` — Sets cursor filter, re-fetches events from that point.
- `addLiveEvent()` — Prepend to `liveEvents` array (max 200), deduplicated by id.

**Fork inheritance:** When `loadEvents()` detects `parent_channel_id` in metadata, it:
1. Sets `forkedAtCursor` from `metadata.forked_at_cursor`
2. Replays ALL events from parent channel
3. Filters to events with `cursor <= forkedAtCursor`
4. Maps them with `{ ...event, isInherited: true, forkedFrom: parentChannel }`
5. Concatenates inherited + own events for display

### 6.4 `diff.ts` — Client-Side Comparison Engine

**File:** `dashboard/src/lib/diff.ts` (51 lines)

```typescript
diffEvents(eventsLeft, eventsRight): DiffResult
computeComparisonMetrics(events): ComparisonMetrics
```

- `diffEvents()`: Finds shared prefix by comparing `event_type` at each position. Returns `{ shared, divergedAt, leftOnly, rightOnly }`.
- `computeComparisonMetrics()`: Sums `latency_ms`, `tokens`/`token_count`, `cost` from event payloads. Returns aggregate.

Used by `CompareView` to render side-by-side comparison. Pure client-side — no server endpoint.

### 6.5 `api.ts` — Dashboard API Client & Mock Data

**File:** `dashboard/src/lib/api.ts` (388 lines)

Standard `fetch()` wrapper with JWT Bearer auth from `localStorage.actae_auth`. On 401, redirects to `/login/`.

**`MOCK_MODE`** (`NEXT_PUBLIC_MOCK_MODE=true`): When enabled, ALL API calls return hardcoded mock data instead of hitting the server. Mock structure:
- 10 mock channels (`agent-research-run`, `exp-a`, `exp-b`, `exp-a-sub`, `langgraph-agent-prod`, `crewai-research-crew`, etc.)
- Per-channel mock events with realistic timestamps, cursor progression, payload shapes, and state snapshots
- Fork tree: `agent-research-run` → `exp-a` (at cursor 3) → `exp-a-sub` (at cursor 2), and `exp-b` (at cursor 3)
- Mock metadata, snapshots, and execution tree responses

**`getWsUrl()`**: Derives `wss://` or `ws://` URL from `window.location`.

### 6.6 `mock-data.ts` — Structured Mock Dataset

**File:** `dashboard/src/lib/mock-data.ts` (342 lines)

Three structured exports:
- `MOCK_CHANNELS` — 10 channel ID strings
- `MOCK_EVENTS` — `Record<string, DashboardEvent[]>` keyed by channel_id, each with 4-5 events + state snapshots
- `MOCK_STATES` — `Record<string, { cursor: number, state: object }>` keyed by channel_id

Events have realistic shapes: `event_type`, `payload.latency_ms`, `payload.tokens`, `payload.cost`, timestamps.

### 6.7 `useWebSocket` Hook

**File:** `dashboard/src/hooks/useWebSocket.ts`

Connects to `getWsUrl()` when `isLive && channel`. Manages connection lifecycle, dispatches incoming events to `addLiveEvent()`. Handles reconnection with exponential backoff.

Connected to `LiveToggle` and the live feed bar in the footer.

---

## 7. Authentication Changes

### Anonymous Auth Flow

1. `main.rs:91`: `ENABLE_ANONYMOUS=true` set in dev mode
2. `main.rs:252-256`: `EventHandler::new(...).with_anonymous(config.enable_anonymous)`
3. `handler.rs:293-296`: `validate_auth()` checks `allow_anonymous` — anonymous is OK
4. `handler.rs:255-258`: `list_channels()` allows anonymous only when `allow_anonymous` is true
5. `main.rs:751-754`: `resolve_tenant_from_request()` falls through to `AuthContext::Anonymous` when api key is invalid and `enable_anonymous` is true

### API Key Auth

Standard `x-api-key` header → `validate_api_key()` → DB lookup in `api_keys` table → returns `ApiKey` with permissions JSON.

### JWT Auth (added)

`Authorization: Bearer <JWT>` header supported by WebSocket auth infrastructure.

### Permission Model

- `AuthContext::Anonymous` — no permissions unless `allow_anonymous`
- `AuthContext::Authenticated` — user_id, tenant_id, permissions vec from JWT
- `AuthContext::ApiKey` — key_id, tenant_id, permissions vec from API key row
- `Permission::Read` → `auth.can_read()` checks for "read" or "admin" permission
- `Permission::Write` → `auth.can_write()` checks for "write" or "admin" permission

---

## 8. Lab Scenarios

**Directory:** `lab/scenarios/fork_experiment/`

### Files

| File | Purpose |
|------|---------|
| `scenario.py` | Lab scenario registration — configures fork experiment scenario |
| `run_demo.py` | Standalone demo: creates 15 events across 4 channels with fork chain |
| `agent_session_demo.py` | Interactive AgentSession demo (4 phases, optional DeepSeek) |
| `research_sim.py` | Framework-based research pipeline simulation |
| `__init__.py` | Package init |

### `agent_session_demo.py`

Interactive demo with 4 phases:
1. Initial run (5 inference steps)
2. Fork at step 3 with "gpt-5" params
3. Resume + fork from "exp-v1" at step 2 with Claude params
4. Comparison (event replay + diff)

Uses DeepSeek v4 when `DEEPSEEK_API_KEY` is set, otherwise simulates with sleep + deterministic latencies.

---

## 9. Framework Adapters

**Directory:** `sdks/python/src/actae_client/adapters/` (5 files)

These are optional framework integrations that use `AgentSession` under the hood. They let existing LangGraph, LangChain, and CrewAI pipelines use Actae as their persistence backend with zero code changes.

### 9.1 `base.py` — StateManager

**File:** `sdks/python/src/actae_client/adapters/base.py`

```python
class StateManager:
    def __init__(self, actae: ActaeClient, channel_id: str)
    async def save(cursor: int, state: dict) -> None
    async def load() -> Optional[Dict[str, Any]]
    async def resume(agent, session: AgentSession) -> None
```

Generic save/load/resume. The `resume()` method:
1. Loads saved state from Actae
2. Replays events from the fork cursor
3. Restores agent memory from state blobs
4. Yields control back to agent loop

### 9.2 `langchain.py` — ChainResumer

**File:** `sdks/python/src/actae_client/adapters/langchain.py`

```python
class ChainResumer:
    def __init__(self, actae: ActaeClient, channel_id: str)
    async def resume_chain(chain, session: AgentSession) -> None
```

For LangChain's `Chain` class. On resume:
1. Loads checkpoint state from Actae
2. Re-invokes the chain with restored context
3. Skips completed steps, continues from interruption

### 9.3 `crewai.py` — CrewAIResumer

**File:** `sdks/python/src/actae_client/adapters/crewai.py`

```python
class CrewAIResumer:
    def __init__(self, actae: ActaeClient, channel_id: str)
    async def resume_crew(crew, session: AgentSession) -> None
```

For CrewAI's `Crew` + `Task` model. On resume:
1. Loads completed tasks from Actae event replay
2. Filters to uncompleted tasks
3. Restores agent context + tool results
4. Re-executes remaining tasks

### 9.4 `langgraph.py` — ActaeCheckpointSaver

**File:** `sdks/python/src/actae_client/adapters/langgraph.py`

```python
class ActaeCheckpointSaver(BaseCheckpointSaver):
    def __init__(self, actae: ActaeClient, channel_id: str)
```

Implements LangGraph's `BaseCheckpointSaver` protocol. Actae acts as the checkpoint store for LangGraph graphs:
- `put(checkpoint)` → `save_state()`
- `get(tuple_id)` → `load_state()` at checkpoint cursor
- `list(config)` → `list_states()` filtered by channel

This means LangGraph agents get crash recovery, fork, and experiment comparison for free — Actae replaces the default in-memory or SQLite checkpoint saver.

---

## 10. Test Suite

### 9.1 Python Tests — `test_session.py` (40 tests)

**File:** `sdks/python/tests/test_session.py` (739 lines)

| Test Group | Count | What |
|------------|-------|------|
| `TestServerAlive` | 1 | Smoke test: server is reachable |
| `TestLifecycle` | 2 | Start + complete, empty session |
| `TestStep` | 4 | Step count, cursor mapping, step_number, input/output |
| `TestStateFn` | 4 | Every step, interval, None skip, exception handling |
| `TestFork` | 5 | Create child, record on child, metadata preserves, params inherit, multiple forks |
| `TestForkEdgeCases` | 6 | Step 0, exceeded step, empty session, non-int, first step, last step |
| `TestResume` | 5 | Unknown channel, completed error, completed+fork, cursors restore, no name |
| `TestEdgeCases` | 6 | Unstarted, name required, metadata persistence, 100 steps, zero interval, tenant |

### 9.2 Python Tests — `test_session_edge.py` (50 tests)

**File:** `sdks/python/tests/test_session_edge.py` (982 lines)

| Test Group | Count | What |
|------------|-------|------|
| `TestStepEdgeValues` | 6 | None type, empty string, unicode, deep nesting, more |
| `TestDataIntegrity` | 5 | Same-step fork, active session fork, crash-fork, chain-fork, fan-out |
| `TestResumeRecovery` | 4 | Crash-continue, double-resume, resume-then-fork, existing channel |
| `TestMetadataPersistence` | 5 | Lifecycle, fork survival, nested params, cursors list, empty cursors |
| `TestConcurrency` | 3 | Shared client, fork+step, 10 concurrent forks |
| `TestStateFn` | 5 | List, string, int, large dict, None session |
| `TestForkTree` | 3 | List forks, experiment_metadata, unstarted fork |
| `TestMixedLifecycle` | 3 | Inner fork, nested fork, double-deep fork |
| `TestSnapshots` | 2 | Single, multiple |
| `TestUnitAndUtils` | 6 | repr, params, cursors, interval, validation, inheritance |
| `TestDeepForkChain` | 1 | 5-level deep fork (chain5 ← chain4 ← ...) |
| `TestLargeSessions` | 2 | 100 steps + monotonicity, 200 steps + fork at 150 |

### 9.3 Rust Integration Tests

**File:** `actae/tests/event_store_fork_test.rs` (8 tests, requires `--test-threads=1`)

| Test | Assertion |
|------|-----------|
| `fork_creates_new_channel_with_metadata` | channel_metadata row exists |
| `fork_copies_parent_state` | channel_states copied |
| `get_fork_tree_returns_correct_hierarchy` | Tree structure |
| `list_forks_returns_direct_children_only` | No grandchildren |
| `origin_run_id_correctly_propagated` | Skips intermediate parents |
| `fork_with_metadata_persists` | display_name + reason + experiment_metadata |
| `get_channel_metadata_returns_none_for_unknown` | None for nonexistent |
| `fork_from_nonexistent_source_fails` | NotFound error |

---

## 11. sonic_rs Serialization Problem (Critical)

### The Problem

`sonic_rs::Value`'s `Serialize` impl wraps numbers as:
```json
{"$sonic_rs::private::JsonNumber": "42"}
```
instead of plain `42`. When axum's `Json<sonic_rs::Value>` serializes a response, the sonic wrapper leaks to the client.

### Why sonic_rs exists

The project mandates `sonic_rs` over `serde_json` for performance (2-5x faster JSON parsing). All JSON in the codebase uses `sonic_rs::Value` including payload, state, and metadata fields. The `JsonValue` wrapper in `json_compat.rs` implements PostgreSQL's `ToSql`/`FromSql` traits for `sonic_rs::Value`.

### Fix Layer 1: Rust — `clean_sonic_value()` in `json_compat.rs`

Round-trips through JSON string:
```rust
pub fn clean_sonic_value(value: &SonicValue) -> SonicValue {
    let json_str = sonic_rs::to_string(value).unwrap_or_else(|_| "null".to_string());
    sonic_rs::from_str(&json_str).unwrap_or(SonicValue::default())
}
```

Applied in all handler functions in `main.rs` that return payload, metadata, or state.

### Fix Layer 2: Rust — `serialize_sonic_value_opt` in `schema.rs`

Custom serde serializer for `Option<sonic_rs::Value>` fields. Round-trips through JSON string to produce clean `serde_json::Value`. Applied to `experiment_metadata` in `ChannelMetadata` and related structs.

### Fix Layer 3: Python — `_unwrap_sonic()` in `types.py`

Recursively replaces `{"$sonic_rs::private::JsonNumber": "42"}` with native `42`. Applied to all Event factory methods, ChannelMetadata construction, state values, snapshot lists, WS acks.

### Risk

Any **new** handler that returns `sonic_rs::Value` payload/metadata/state MUST call `clean_sonic_value()` or use `serialize_sonic_value_opt`. This is not enforced by the type system. The `_unwrap_sonic()` Python side covers all existing paths.

---

## 12. Known Limitations

### 11.1 Cursor List Cap (1000)

`session.py:263`: Only the most recent 1000 cursors are persisted in channel metadata. Channels with >1000 steps use the replay fallback for step→cursor resolution, which has a 10,000-event limit.

### 11.2 Replay Limit (10,000)

`_fork_from_channel()`: Uses `limit=10000` for replay fallback. Channels with >10,000 events get a truncated result. AgentSession-created channels use the metadata-based cursors list (fast path) which avoids this.

### 11.3 Fork Concurrency

`step()` has an `asyncio.Lock` but `fork()` does not. Concurrent forks from the same session are safe because they read the cursors list atomically, but fork+step interleaving is not protected. In practice, developers don't fork concurrently with steps.

### 11.4 No Fork Merging

Forks are independent. No merge capability. This is by design.

### 11.5 No Server-Side Comparison

Comparison is purely client-side (dashboard `diff.ts`). No dedicated comparison API.

### 11.6 pg-embed Data Corruption

Dev mode uses pg-embed. Force-killing Actae (`pkill -9`) can corrupt the pg data directory at `/tmp/actae-dev/`. Fix: `rm -rf /tmp/actae-dev && restart`.

### 11.7 Network Failure Tests

Test coverage for network failures (connection drop mid-step, retry exhaustion, WS disconnect during fork) is incomplete. Requires mocking framework setup.

---

## 13. Quick Reference — How to Build, Run, Test

### Dev Mode

```bash
# Start Actae with embedded PostgreSQL (single-port mode)
cd actae
export ENABLE_ANONYMOUS=true ACTAE_PORT=8002
cargo run -- --dev

# In another terminal, run Python tests
cd <repo-root>
python3 -m pytest sdks/python/tests/test_session.py -v --tb=short
python3 -m pytest sdks/python/tests/test_session_edge.py -v --tb=short

# Run Rust integration tests
cd actae
export DATABASE_URL="postgresql://actae:actae_dev@localhost:5432/actae"
cargo test --test event_store_fork_test -- --test-threads=1
```

### Environment Variables (Dev Mode)

| Variable | Value | Purpose |
|----------|-------|---------|
| `ACTAE_PORT` | 8002 | Single port for API + WebSocket + Health/Metrics |
| `ENABLE_ANONYMOUS` | true | Skip API key validation |
| `ENABLE_TLS` | false | No TLS in dev mode |

### Demo

```bash
cd <repo-root>/lab
python scenarios/fork_experiment/agent_session_demo.py
```

---

## 14. Key Files — Absolute Paths

### Rust Backend (`actae/`)

| Path | What |
|------|------|
| `src/main.rs` | Route registration, all 7 fork handlers, auth resolution, `err_to_status`, `clean_sonic_value` calls, WS port |
| `src/events/handler.rs` | `EventHandler` with auth-gated method wrappers, `validate_auth`, `with_anonymous`, 68 unit tests |
| `src/events/store.rs` | `EventStore` DB operations: `fork_channel()`, `get_fork_tree()`, `save_state()`, `update_metadata()` |
| `src/events/schema.rs` | `ChannelMetadata`, `ForkRequest`, `ForkInfo`, `SaveSnapshotRequest`, `SnapshotInfo`, `UpdateMetadataRequest`, `serialize_sonic_value_opt` |
| `src/events/migrations.rs` | v5 (channel_metadata), v14 (drop legacy state_snapshots) |
| `src/database/json_compat.rs` | `JsonValue`, `sonic_to_serde()`, `clean_sonic_value()` |
| `src/websocket/server.rs` | WS upgrade via axum on main HTTP port (single-port) |
| `src/license/types.rs` | removed (license system dropped) |
| `tests/event_store_fork_test.rs` | 8 Rust integration tests |

### Python SDK (`sdks/python/src/actae_client/`)

| Path | What |
|------|------|
| `client.py` | `fork()`, `get_channel_metadata()`, `list_forks()`, `get_fork_tree()`, `update_metadata()`, `latest_cursor()` |
| `types.py` | `_unwrap_sonic()`, `ChannelMetadata`, `ForkInfo`, `Event` with 5 factory methods |
| `session.py` | `AgentSession` class — complete 598-line context manager |
| `__init__.py` | Exports `AgentSession`, `SessionError`, `SessionCompletedError` |

### Python Tests

| Path | What |
|------|------|
| `sdks/python/tests/test_session.py` | 40 tests |
| `sdks/python/tests/test_session_edge.py` | 50 tests |

### Dashboard (`dashboard/src/`)

| Path | What |
|------|------|
| `lib/flow-store.ts` | Zustand store with fork inheritance support |
| `lib/diff.ts` | Client-side diff + comparison metrics |
| `lib/api.ts` | API client with all fork/metadata methods |
| `components/flow/ForkButton.tsx` | Fork dialog |
| `components/flow/ForkTree.tsx` | Recursive fork tree |
| `components/flow/MetadataEditor.tsx` | Inline metadata editing |
| `components/flow/CompareView.tsx` | Fork comparison |
| `components/flow/EventNode.tsx` | Inherited event rendering |
| `components/flow/EventFlowCanvas.tsx` | Fork inheritance styling |

### Lab

| Path | What |
|------|------|
| `lab/scenarios/fork_experiment/agent_session_demo.py` | Interactive 4-phase demo |
| `lab/scenarios/fork_experiment/scenario.py` | Lab scenario config |
| `lab/scenarios/fork_experiment/run_demo.py` | Standalone demo |
| `lab/scenarios/fork_experiment/research_sim.py` | Research simulation |
