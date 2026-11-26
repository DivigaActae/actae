# Versioned State Snapshots

Immutable, versioned agent state storage. Every `save_state()` creates a new version — nothing is overwritten. Enables crash recovery, rollback, audit, and inspection of any prior snapshot.

---

## Why

Previous design: one state row per `channel_id`, always overwritten. If an agent crashed mid-conversation, the partial state was lost forever. Applications like LangGraph checkpointing, CrewAI task pipelines, and DeepSeek agent loops all need the ability to resume from any prior point in time.

This system gives each save an auto-incrementing `version`, making the full history queryable.

---

## Database Schema

Table: `channel_states`

| Column | Type | Notes |
|--------|------|-------|
| `channel_id` | `VARCHAR(255)` | Part of composite PK |
| `version` | `BIGSERIAL` | Auto-incrementing per channel, part of composite PK |
| `cursor` | `BIGINT` | Event cursor this state is pinned to |
| `state` | `JSONB` | Arbitrary agent state (conversation, thoughts, tools, etc.) |
| `created_at` | `TIMESTAMPTZ` | Auto-set to `NOW()` |

Primary key: `(channel_id, version)` — each row is immutable.

Migration v5 in `actae/src/events/migrations.rs` handles the transition:
1. Drops old `PRIMARY KEY (channel_id)`
2. Drops any orphan sequence if present
3. Adds `version BIGSERIAL NOT NULL`
4. Creates composite `PRIMARY KEY (channel_id, version)`
5. Creates index on `(channel_id, version DESC)` for efficient `ORDER BY`
6. Existing single state per channel is preserved as `version = 1`

Idempotent — safe to run multiple times (guarded by `DO $$ BEGIN ... EXCEPTION WHEN duplicate_table ... END $$`)

---

## API

All endpoints under `/api/v1/state/{channel_id}`.

| Method | Path | Description | Returns |
|--------|------|-------------|---------|
| `POST` | `/state/{channel_id}` | Save state | `{"status":"saved","version":N}` |
| `GET` | `/state/{channel_id}` | Load latest state | `{"status":"ok","data":{...}}` |
| `GET` | `/state/{channel_id}/versions` | List all versions | `{"status":"ok","versions":[...]}` |
| `GET` | `/state/{channel_id}/version/{version}` | Load specific version | `{"status":"ok","data":{...}}` |
| `DELETE` | `/state/{channel_id}/version/{version}` | Delete specific version | `{"status":"deleted"}` |

Query parameters for `GET /versions`:
- `cursor` — pagination cursor (optional)
- `limit` — page size (optional, default 50)

Auth: API key required. Rate-limited.

---

## End-to-End Flow

```
Agent (Python SDK)          Actae Server (HTTP)          Postgres
       |                          |                        |
       |  POST /state/chan-1     |                        |
       |  (state JSON body)      |                        |
       |------------------------->|                        |
       |                          | INSERT INTO channel_states
       |                          | (channel_id, version, cursor, state)
       |                          |----------------------->|
       |                          | RETURNING version      |
       |                          |<-----------------------|
       |  {"version": 3}         |                        |
       |<-------------------------|                        |
       |                          |                        |
       |  -- agent crashes --     |                        |
       |                          |                        |
       |  GET /state/chan-1       |                        |
       |------------------------->|                        |
       |                          | SELECT ... ORDER BY    |
       |                          | version DESC LIMIT 1   |
       |                          |----------------------->|
       |                          |<-----------------------|
       |  {"data": {state_at_v3}}|                        |
       |<-------------------------|                        |
       |                          |                        |
       |  -- or inspect history --|                        |
       |                          |                        |
       |  GET /state/chan-1/      |                        |
       |       versions           |                        |
       |------------------------->| SELECT version, cursor,|
       |                          | created_at ...         |
       |                          |----------------------->|
       |                          |<-----------------------|
       |  {"versions":            |                        |
       |   [{version:1,...},      |                        |
       |    {version:2,...},      |                        |
       |    {version:3,...}]}     |                        |
       |<-------------------------|                        |
```

---

## Key Design Decisions

**Immutability**: `UPDATE` is never used. Each save inserts a new row. This guarantees no data loss and enables point-in-time recovery.

**Composite PK `(channel_id, version)`**: Ensures version uniqueness per channel, not globally. Two channels both have version 1, 2, 3, etc. independently.

**`BIGSERIAL`**: Database-managed auto-increment. No race conditions on version assignment. Each `INSERT` gets the next sequence value atomically.

**`ORDER BY version DESC LIMIT 1`** for latest: Postgres uses the `(channel_id, version DESC)` index for an efficient backward scan — constant time per channel regardless of history depth.

**Version column added, NOT removed from existing rows**: Old states (from before the migration) get `version = 1`. This is implicit — the sequence starts generating version 2+ for new inserts on that channel.

**Save returns version**: The `INSERT ... RETURNING version` pattern gives the caller the exact version atomically. Previously `save_state()` returned `Result<()>` — now returns `Result<i64>`.

**Backward compatibility**: Every existing adapter and call site ignores the returned version. Zero breaking changes at the call site level.

---

## File Reference

### Rust Backend (`actae/src/events/`)

| File | Lines | Role |
|------|-------|------|
| `migrations.rs` | Migration v5 | Schema migration (idempotent ALTER TABLE) |
| `schema.rs` | `StateVersion` struct | `{version, cursor, timestamp}` — Serialize/Deserialize |
| `store.rs` | ~lines 639–770 | Postgres queries: `save_state`, `load_state`, `load_state_version`, `list_states`, `delete_state_version` |
| `handler.rs` | ~lines 475–540 | Axum handlers with auth gating for all 5 state operations |

### `actae/src/main.rs`

- Route registrations at lines ~464–471
- 3 new handler functions for versions endpoints
- `POST /state/{chan}` body changed from empty to returning `{"status":"saved","version":N}`

### Python SDK

| File | Changes |
|------|---------|
| `sdks/python/src/actae_client/client.py` | `save_state()` returns `int` (version). New: `list_states()`, `get_state()`, `delete_state()` |
| `sdks/python/src/actae_client/adapters/base.py` | `StateManager.save()` returns version. New: `list_versions()`, `get_version()`, `delete_version()` |

---

## Optimistic concurrency

`POST /api/v1/state/{channel_id}` accepts optional `expected_version` /
`expected_cursor` guards (see `docs/FORK_RUNTIME.md` §3). A guard mismatch
returns `409 version_conflict` and writes nothing. `expected_version: 0`
means "no snapshot exists yet".

## Tests

### Rust (integration, authoritative — run against real Postgres)

54 tests in `actae/tests/event_store_test.rs`:

| Category | Tests | What |
|----------|-------|------|
| Core save/load | 6 | Basic save returns version, load latest, load specific, list, delete |
| Version edge cases | 4 | Version 0, i64::MAX, negative, duplicate |
| Cursor edges | 3 | cursor=0, cursor=i64::MAX, cursor=-5 |
| Pagination | 4 | limit=1, offset beyond end, cursor-based, no limit |
| JSONB types | 13 | null, bool, int, float, unicode, escape chars, deep nesting, large arrays, empty, mixed, XSS, numeric keys, all types combined |
| Lifecycle | 4 | delete all then save, version inc after delete, immutability, double-delete |
| Concurrency | 2 | 20 parallel saves on same channel, 5 agents × 10 steps each |
| Migration | 2 | migration v5 idempotency, data preserved as version=1 |
| Real-agent simulations | 4 | DeepSeek crash-resume (3 phases), LangGraph checkpoint (4 nodes), CrewAI pipeline (5 tasks), multi-agent (5 agents × 10 steps) |

### Python (HTTP — require Actae server restart with new binary)

39 tests in `tests/test_state_snapshots.py` covering all endpoints, error cases, auth gating, and edge cases.

To run:
```bash
# 1. Start Actae with the new binary
cd actae && cargo run

# 2. In another terminal
python3 -m pytest tests/test_state_snapshots.py -v --embedded
```

---

## Example Usage

```python
from actae_client import ActaeClient

client = ActaeClient("http://localhost:8002", api_key="sk-dev-...")

# Save state — returns version
v1 = client.save_state("chat-1", {"messages": ["hi"]})
v2 = client.save_state("chat-1", {"messages": ["hi", "hello"]})
print(v2)  # 2

# Load latest
state = client.load_state("chat-1")
print(state["messages"])  # ["hi", "hello"]

# Load specific version
state_v1 = client.get_state("chat-1", version=1)

# List all versions with metadata
versions = client.list_states("chat-1")
for v in versions:
    print(f"v{v.version} @ cursor {v.cursor}")

# Delete a version
client.delete_state("chat-1", version=1)
```

---

## Cursor Alignment

Each state snapshot is pinned to an event `cursor`. This establishes the logical ordering between events and states — the state at version N represents the agent's context *after* processing the event at `cursor`. This enables:

1. **Replay to cursor then restore state**: Replay events up to cursor X, then load the state pinned to cursor X.
2. **Visual timeline**: The dashboard can interleave events and states on a unified cursor timeline.
3. **Consistency**: State never references events that haven't been replayed yet.

### Cursor-monotonicity invariant (fork boundary selection)

Snapshot selection for fork boundaries (`resolve_fork_state`,
`load_state_at_or_before`) orders by **`cursor DESC, version DESC`** — the
snapshot CLOSEST to the fork point by cursor wins, regardless of write order.
A late historical snapshot insert (a lower cursor written after a higher one)
never beats the snapshot nearest the boundary. This is the documented
invariant; writers that must support exact-boundary forks should keep snapshot
cursors monotonic per channel (i.e. save at the current event cursor).
