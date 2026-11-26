# Idempotent Tool Executions

Durable, idempotent tool execution ledger backed by the `tool_executions`
table. A client claims a tool run by an idempotency key; the same key replays
the persisted result instead of re-running the tool.

> **Actae never executes tools.** It is a coordination/ledger layer, not an
> executor. The tool itself always runs in the caller's process (agent,
> worker, or SDK user). Actae decides *who may run* and *remembers what
> happened* — nothing more. Read "How it works" below before reading the
> rest of this document.

## How it works (plain English)

Think of Actae as a **scheduler's blackboard**: a durable record of "who is
doing what, and what happened last time". It answers exactly one question for
a fleet of workers (or for a crashed-and-retried worker):

> *"Has this exact request already been done?"*

The unit of work is one `tool_executions` row, identified by
`(channel_id, key_name)` — an idempotency key. The flow:

```
Worker                        Actae                              Worker
  |  POST /api/v1/executions/claim   |
  |  (channel_id, key_name, params)  |
  |---------------------------------->|  1. look up the key
  |                                   |  - never seen?      -> "claimed"  (+ claim_token)
  |                                   |  - seen, done?      -> "replayed" (returns saved result)
  |                                   |  - seen, lease held -> "in_progress"
  |                                   |  - seen, stale lease-> "reclaimed" (+ claim_token)
  |<----------------------------------|
  |                                   |
  |  claimed / reclaimed:             |
  |  THE WORKER RUNS THE TOOL         |  2. tool executes HERE, in the worker
  |  (Actae is not involved at all)    |
  |                                   |
  |  POST /executions/{id}/complete   |  3. report the outcome
  |---------------------------------->|  stores result, emits `tool.completed`
  |                                   |
  |  later, another worker claims     |
  |  the same key with same params -> |  4. "replayed" — returns saved result,
  |                                   |  nobody runs the tool again
```

Concretely, Actae does five things:

1. **Guards against double-runs** — the `(channel_id, key_name)` unique
   constraint plus the race-safe claim (INSERT ... ON CONFLICT DO NOTHING)
   guarantee exactly one worker ever receives `claimed` for a key.
2. **Persists outcomes** — result/error JSON is stored, so `replayed` claims
   return the previous outcome instead of re-executing.
3. **Owns leases** — a running execution holds `claim_token` + `lease_until`;
   the worker must `heartbeat`, and if it dies the lease lapses and another
   worker may `reclaimed` the job.
4. **Detects key collisions** — same key with different params (request hash)
   returns `409 idempotency_key_mismatch`, so a reused key can never silently
   clobber a different request.
5. **Emits lifecycle events** — every attempt writes `tool.started` /
   `tool.completed` / `tool.failed` / `tool.replayed` rows and broadcasts them
   over WebSocket, linked by `depends_on` so dashboards can render the attempt
   chain.

**The key mental model**: `claim` is a lock + memo lookup, `complete`/`fail`
are memo writes, `heartbeat` is lock renewal, `expire_executions` is garbage
collection of dead locks. The tool run itself is entirely outside Actae.

## Concept

- **Idempotency unit**: `(channel_id, key_name)`. A unique constraint makes
  each key claimable exactly once per channel.
- **Request hash**: a SHA-256 fingerprint of `tool_name`, `key_name` and the
  claim `params` (optionally projected through `dedup_fields`). A completed
  execution whose hash differs from the incoming claim is a key collision and
  returns `ExecutionKeyMismatch`.
- **Lifecycle events**: each attempt writes an `agent_events` row
  (`tool.started`, `tool.completed`, `tool.failed`, `tool.replayed`) with a
  `depends_on` edge to the previous attempt's started event, so dashboards can
  render the attempt chain. `metadata.execution_id` identifies the execution.
- **Leases**: a running execution holds `claim_token` + `lease_until`. A
  worker must `heartbeat` before the lease lapses; expired leases are
  reclaimable.

## Status machine

| Status      | Meaning |
|-------------|---------|
| `running`   | An attempt owns the lease (`claim_token` set). |
| `completed` | A result was persisted; claims replay it. |
| `failed`    | A structured error was persisted; claims replay the error. |
| `cancelled` | Explicit cancellation by the owner. |
| `expired`   | Lease lapsed (`expire_executions` sweep). |
| `interrupted` | Reserved; treated like `expired` for reclaim. |

Transitions are guarded by the `claim_token`:
- `complete` / `fail` / `heartbeat` / `cancel` only succeed while the token in
  the row equals the caller's token; otherwise `ExecutionNotOwned`.

## Fork tool policies (side-effect-aware forking)

A channel created by `POST /api/v1/channels/fork` can declare how tool
executions are handled on the **child**, so a fork can continue without
silently re-firing an effect the parent already performed. The policy is an
opaque map set at fork time and stored on `channel_metadata.tool_policies`
(mirrored into the immutable `fork_operations` receipt and echoed by
`GET /channels/{id}/receipt` and `/metadata`):

```json
{"tool_name": "replay" | "block" | "live" | "auto", "*": "<default>"}
```

`tool_name` keys are matched first, then the `"*"` fallback. Only forked
channels consult a policy; a channel with no `parent_channel_id` keeps the
exact behavior above. Policies are resolved against the **ancestor chain,
nearest first** (one bounded recursive query joining the ancestor chain to the
ledger, so only ancestors that recorded the key are considered), so a
fork-of-fork replays a result any ancestor recorded. The policy is **set at
fork time and immutable** — there is no mutation endpoint, and it is mirrored
into the immutable `fork_operations` receipt so provenance cannot drift.

| Policy | Exact match in an ancestor | Miss (no ancestor recorded it) | Mismatch (recorded with different params) |
|---|---|---|---|
| `replay` | materialize `tool.replayed` + replay row on the child | `409 fork_tool_blocked` + `tool.blocked` event | `409 fork_tool_blocked` + `tool.blocked` event |
| `block` | `409 fork_tool_blocked` + `tool.blocked` event | same | same |
| `auto` (default) | materialize `tool.replayed` + replay row | run a normal live claim | run a normal live claim |
| `live` | run a normal live claim | run a normal live claim | run a normal live claim |

Mechanics:

- `EventStore::fork_tool_context(channel_id, tool_name)` resolves the effective
  policy and the ancestor source list; `None` for a non-forked channel.
- `EventStore::replay_fork_execution(request, sources)` reuses the group-fork
  materialization: it writes a `tool.replayed` event (`metadata`
  `{source_execution_id, fork_source_channel_id, classification:"replayed"}`)
  and a completed `tool_executions` row on the **child** with
  `replay_emitted=true`. It never invokes the tool. It tries each ancestor for
  an exact `(channel, key_name)` + request-hash match and returns
  `ForkReplayResult::{Replayed, Miss, Mismatch}`.
- A blocked decision is persisted as an immutable `tool.blocked` event
  (`actor: fork-policy`, payload `{tool_name, key_name, boundary, policy}`) and
  returned as `409 fork_tool_blocked` (code `fork_tool_blocked`). Best-effort:
  failing to record the block never changes the block decision.
- Re-claiming the same `(child, key)` after a materialized fork replay serves
  the child's row and never re-runs. Concurrent auto/replay claims for the same
  `(child, key)` are insert-race safe: the replay INSERT is `ON CONFLICT DO
  NOTHING` and the call retries so the winner's row is observed
  (`replayed`/`miss`/`mismatch`), never a 500 (covered by
  `tests/test_fork_tool_policies.py::TestConcurrency`).
- Unlike a normal in-channel replay (which emits `tool.replayed` only when
  `emit_replay_event` is set), a fork replay **always** records the marker —
  it is provenance for the counterfactual.

The safety claim is bounded and honest: `auto` prevents an identical inherited
effect from firing twice, but exactly-once external effects still require an
idempotent or queryable downstream operation (see the durability caveat below).

## Store methods (`EventStore`)

| Method | Returns | Notes |
|--------|---------|-------|
| `claim_execution(&ClaimExecutionRequest)` | `(ExecutionClaimResult, Option<AgentEvent>)` | Create-or-replay. Outcomes below. |
| `complete_execution(id, token, &CompleteExecutionRequest)` | `(ExecutionInfo, Option<AgentEvent>)` | Persists result, emits `tool.completed`. Duplicate complete is idempotent. |
| `fail_execution(id, token, &FailExecutionRequest)` | `(ExecutionInfo, Option<AgentEvent>)` | Persists error, emits `tool.failed`. Duplicate fail is idempotent. |
| `heartbeat_execution(id, &HeartbeatExecutionRequest)` | `ExecutionInfo` | Extends lease; `ExecutionNotOwned` on stale token. |
| `cancel_execution(id, &CancelExecutionRequest)` | `ExecutionInfo` | Marks `cancelled`; no event emitted. |
| `get_execution(id)` | `Option<ExecutionInfo>` | |
| `list_executions(channel_id, limit)` | `Vec<ExecutionInfo>` | Newest first, capped at 100. |
| `expire_executions()` | `u64` | Sweep `running` rows past lease. |
| `delete_execution(id)` | `bool` | Hard delete. |

`claim_execution` outcomes (`ExecutionClaimResult.status`):

| Status | When | What the caller should do |
|--------|------|---------------------------|
| `claimed` | Brand-new execution, or reclaim after a prior `failed` status. | Run the tool; then `complete`/`fail` with the returned `claim_token`. |
| `reclaimed` | A stale/abandoned run (expired lease, interrupted, cancelled) was taken over. | Prior outcome is ambiguous; resolve before re-running non-idempotent tools. |
| `replayed` | A completed execution with a matching hash exists. | Use `result`; do NOT run the tool. |
| `in_progress` | Another owner holds a valid lease. | Wait for `lease_until` or treat as contention. |

Additionally, a claim against a **tombstoned** completed execution returns HTTP
`409 result_expired` (`ExecutionResultExpired`), not `replayed` and not a
silent re-execution. This happens when retention has expired the result:

## Retention and the replay window

Tool-execution bodies (params/result/error) are retained per
`TOOL_EXECUTION_RETENTION_DAYS` (0 = keep forever, the default). When a
completed/failed execution's `completed_at` crosses the window, the
`tool_execution_tombstone` scheduler task **nulls the params/result/error
bodies and sets `expired_at`** on the row. The tiny tombstone keeps only the
key identity, tool, request hash, status, and timestamps.

Consequently:

> **Tool deduplication / result replay is guaranteed only within the plan's
> execution-data retention window.**

After the window, a retried idempotency key fails loudly with
`409 result_expired` (`ExecutionResultExpired`) — Actae knows the operation
existed but can no longer serve its result. The caller must resolve the prior
outcome downstream before retrying with a fresh key. This is deliberate: an
expired key silently executing again (the alternative) would violate the
durable side-effect coordination promise.

`claim_token` is an `Option<Uuid>` at the store layer; the HTTP/SDK request
types carry it as `Option<String>` and parse it.

## Dedup fields

`dedup_fields: Option<Vec<String>>` restricts request identity to the listed
top-level `params` keys. Volatile fields (timestamps, nonces, attempt ids)
can be excluded so legitimate retries hash identically. Without
`dedup_fields`, the full `params` document is hashed (key order matters).

## Schema

`tool_executions` (migration v13):

| Column | Type | Notes |
|--------|------|-------|
| `execution_id` | UUID PK | |
| `channel_id` | TEXT | part of idempotency unit |
| `key_name` | TEXT | part of idempotency unit |
| `tool_name` | TEXT | |
| `request_hash` | TEXT | SHA-256 of request identity |
| `status` | TEXT | default `running` |
| `attempts` | INTEGER | increments per claim/reclaim |
| `claim_token` | UUID | owner proof while running |
| `lease_until` | TIMESTAMPTZ | lease expiry |
| `actor` | TEXT | stamped on lifecycle events |
| `started_event_id` / `completed_event_id` | UUID | linked `agent_events` rows |
| `started_cursor` / `completed_cursor` | BIGINT | channel cursors of the events |
| `replay_emitted` | BOOL | one-time `tool.replayed` marker |
| `params` | JSONB | default `{}` |
| `result` / `error` | JSONB | persisted terminal payloads |
| `created_at` / `updated_at` / `completed_at` | TIMESTAMPTZ | |

Unique constraint `(channel_id, key_name)`.

## Metrics

- `actae_executions_total{outcome}` — claimed / reclaimed / replayed /
  completed / failed
- `actae_execution_replays_total` — replayed executions
- `actae_execution_duration_seconds` — histogram of completed execution wall
  clock

## Wiring

### HTTP API

Routes registered in `actae/src/main.rs`, handlers in `actae/src/handlers/executions.rs`:

| Method & Path | Body / Params | Success | Errors |
|---------------|---------------|---------|--------|
| `POST /api/v1/executions/claim` | `ClaimExecutionRequest` JSON | `201` `{status, execution, result, claim_token}` | `400` bad body; `409 idempotency_key_mismatch` |
| `POST /api/v1/executions/{id}/complete` | `CompleteExecutionRequest` JSON | `200` `{status: "completed", execution}` | `400` invalid id/token; `404 execution_not_found`; `409 execution_not_owned` |
| `POST /api/v1/executions/{id}/fail` | `FailExecutionRequest` JSON (`message` required) | `200` `{status: "failed", execution}` | same as complete |
| `POST /api/v1/executions/{id}/heartbeat` | `HeartbeatExecutionRequest` JSON | `200` `{status: "ok", execution}` | same as complete |
| `POST /api/v1/executions/{id}/cancel` | `CancelExecutionRequest` JSON | `200` `{status: "cancelled", execution}` | same as complete |
| `GET /api/v1/executions/{id}` | — | `200` `{status: "ok", execution}` | `400` invalid id; `404 execution_not_found` |
| `GET /api/v1/executions?channel_id=&limit=` | query (`limit` 1–100, default 50) | `200` `{status: "ok", executions, count}` | `400` missing channel |
| `DELETE /api/v1/executions/{id}` | — | `200` `{status: "deleted"}` | `400` invalid id; `404` |

All endpoints require write (or read for GET/list) permission on the execution's
channel via the standard `X-API-Key` auth. Errors map through
`handlers::err_to_status`:

| `status` string | HTTP | Meaning |
|-----------------|------|---------|
| `idempotency_key_mismatch` | 409 | key already used with different request hash |
| `execution_not_found` | 404 | execution id does not exist |
| `execution_not_owned` | 409 | stale/invalid claim token for a running execution |
| `invalid_request` | 400 | malformed body, execution id, or claim token |

### Payload caps

Tool bodies are subject to the same 1 MiB per-payload cap as event payloads and
state snapshots (a tool result is persisted as JSONB and replayed into a
`tool.completed` event, so it cannot bypass the limit through a different
route). Enforced in the handlers before any DB write:

- `params` on `POST /api/v1/executions/claim`
- `result` on `POST /api/v1/executions/{id}/complete`
- `message` + `stack` combined on `POST /api/v1/executions/{id}/fail`

A body under the 2 MB HTTP body cap but over 1 MiB returns
`400 invalid_request` ("exceeds maximum size of 1048576 bytes"). Keep large
tool artifacts in object storage and store a pointer/reference in the result.

### WebSocket broadcasting

`EventHandler::claim_execution` / `complete_execution` / `fail_execution`
broadcast the persisted lifecycle events (`tool.started`, `tool.completed`,
`tool.failed`, `tool.replayed`) to the channel's subscribers via
`spawn_broadcast` — the standard `{"type": "broadcast", "topic": ...,
"payload": {"event": {...}}}` wrapper. Broadcast event payloads include
`event_type`, `actor`, `metadata`, and `depends_on` (the prior attempt's
started event id), so subscribers can reconstruct the attempt chain.

Concurrency notes:

- **Claim racing**: a brand-new key is inserted with
  `INSERT ... ON CONFLICT (channel_id, key_name) DO NOTHING`; a claim that
  loses the insert race rolls back and retries (bounded), then observes the
  winner's row under its row lock (`in_progress` / `replayed` / `reclaimed`).
  Exactly one `tool.started` is ever written for the first attempt.
- **Request-hash determinism**: `canonical_json` sorts object keys
  lexicographically and `dedup_fields` is treated as an unordered set, so
  identical requests always fingerprint identically regardless of JSON key
  ordering or runtime hash-map ordering.

### Python SDK

Methods on `ActaeClient` (see `sdks/python/src/actae_client/client.py`):

- `claim_execution(channel_id, key_name, tool_name, params=None, *, dedup_fields=None, lease_seconds=None, emit_replay_event=False, actor=None) -> ExecutionClaim`
- `complete_execution(execution_id, claim_token=None, result=None) -> ExecutionInfo`
- `fail_execution(execution_id, message, *, claim_token=None, error_type=None, stack=None) -> ExecutionInfo`
- `heartbeat_execution(execution_id, claim_token=None, lease_seconds=None) -> ExecutionInfo`
- `cancel_execution(execution_id, claim_token=None) -> ExecutionInfo`
- `get_execution(execution_id) -> ExecutionInfo`
- `list_executions(channel_id, limit=50) -> List[ExecutionInfo]`
- `delete_execution(execution_id) -> bool`

Dataclasses `ExecutionInfo` / `ExecutionClaim` in `actae_client.types`;
errors `IdempotencyKeyMismatchError` (409), `ExecutionNotOwnedError` (409),
`ExecutionNotFoundError` (404) in `actae_client.errors`. Lifecycle broadcasts
arrive at the standard `on_message(topic, event)` callback with
`event.event_type` set to the real event type (e.g. `"tool.started"`) and
`event.metadata` carrying `{"execution_id": ...}`.

The SDK auto-appends `/ws` when deriving `ws_endpoint` from `endpoint`
(actae serves WebSocket at `/ws`).
