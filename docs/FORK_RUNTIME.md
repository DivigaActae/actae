# Actae Fork Runtime

Durable execution substrate for agent workflows: exact-boundary forks,
atomic event+state transitions, optimistic concurrency, durable consumer
groups, and persisted wake-ups.

This document describes the runtime semantics and the full API surface.
Implementation details live in `actae/src/events/` (store + handler) and
`actae/src/handlers/`.

---

## 1. Fork semantics

`POST /api/v1/channels/fork`

```json
{
  "source_channel_id": "parent-channel",
  "new_channel_id": "child-channel",
  "at_cursor": 42,
  "operation_id": "uuid-or-any-string",
  "manifest": { "model": "gpt-5", "seed": 42 },
  "expected_version": 3,
  "expected_cursor": 5
}
```

| `at_cursor` | Behavior |
|---|---|
| `0` (default) | Fork from the **latest saved state**. No snapshot → lineage-only child (`restorable=false`, allowed). |
| `> 0` | Fork from the snapshot whose `cursor <= at_cursor`, choosing the **closest cursor** (`ORDER BY cursor DESC, version DESC`). None → `409 snapshot_boundary_required`, nothing written. |

**Cursor space:** `at_cursor` and snapshot `cursor`s live in **per-channel
cursor space** — the `agent_events.cursor` used by replay/query and the
dashboard timeline. Each channel numbers its events 1, 2, 3, … independently
(gapless); `global_cursor` (formerly `cursor`, a server-wide BIGSERIAL) is an
internal ordering detail exposed only for cross-channel query ordering. State
snapshots are pinned to the recorded event's per-channel cursor
(`save_state`), and the SDK / dashboard fork at per-channel event cursors.

**Exact-boundary provenance (audit item 2).** A fork records BOTH the
requested and the RESOLVED boundary. `forked_at_cursor` now means the
**resolved** snapshot cursor the child actually inherited — never the
requested one. The immutable provenance (migration v17) is:

| Column | Meaning |
|---|---|
| `requested_at_cursor` | The cursor the caller asked for (`0` = latest). |
| `resolved_state_cursor` | The snapshot cursor actually inherited. |
| `resolved_event_id` | The parent boundary event the child's `fork.started` `depends_on`. |
| `source_state_version` | The source snapshot version copied (0 = none). |
| `source_state_sha256` | SHA-256 fingerprint of the copied state JSON. |
| `restorable` | True when a state snapshot was copied (a restorable checkpoint). |
| `manifest` | The immutable fork manifest (model, prompts, tools, env fingerprint, deps, seed). |
| `reproducibility` | `state_exact` \| `context_exact` \| `execution_replayable` \| `best_effort` (see §10). |
| `outcome` / `result_score` / `deleted_at` | Experiment lifecycle (Phases 9/11). |

The `fork.started` payload carries the same provenance
(`requested_at_cursor`, `resolved_cursor`, `resolved_event_id`,
`source_state_version`, `source_state_sha256`, `restorable`,
`reproducibility`). Snapshot selection is by **closest cursor**, not write
order — a late historical snapshot insert at a lower cursor never beats the
snapshot nearest the fork point.

**The immutable fork receipt.** `POST /fork` returns (and
`GET /api/v1/channels/{id}/receipt` reproduces) the receipt:

```json
{
  "fork_id": "child-channel",
  "source_channel_id": "parent-channel",
  "child_channel_id": "child-channel",
  "requested_cursor": 42,
  "resolved_cursor": 40,
  "resolved_event_id": "...",
  "source_state_version": 17,
  "source_state_sha256": "...",
  "restorable": true,
  "replayed": false,
  "manifest": {...},
  "reproducibility": "state_exact"
}
```

This is the centerpiece of the dashboard and audit narrative.

Forking is a single transaction that:

1. (Idempotency — see below.)
2. Optional optimistic guard: `expected_version` / `expected_cursor` reject
   the fork unless the source's latest state matches (`409 version_conflict`)
   — "fork latest" can mean "latest as of the version I observed".
3. Resolves the exact state boundary (`resolve_fork_state`).
4. Copies the resolved snapshot into the child; records the resolved cursor,
   source version and SHA-256.
5. Writes the child's **first event**: `fork.started`
   (`cursor = 1`, actor `system`) with `depends_on` pointing at the
   parent's boundary event. The child's own `channel_cursors` row is created
   here (last_cursor = 1), so the fork is immediately discoverable.
6. Inserts `channel_metadata` with the full provenance + manifest +
   reproducibility grade.

The source-existence check inside the transaction
(`SELECT 1 FROM channel_cursors / channel_states / agent_events ... FOR UPDATE`)
uses `LIMIT 1` on the `channel_states` probe — `tokio-postgres`'s `query_opt`
raises a `row_count` error when a query returns more than one row, so without
the limit forking an event-less channel that had ≥2 saved state snapshots
(common for state-only adapters like `ActaeContextSaver`, which writes a new
version on every save) returned HTTP 500.

### Idempotency (audit item 5)

Standard idempotency semantics, ledger-first (the `fork_operations` table is
the authoritative `operation_id → child_channel_id` map):

- **Same `operation_id` + identical canonical request** (source, `at_cursor`,
  display name, reason, `experiment_metadata`, `manifest` — the hash now
  includes `new_channel_id`) → the **original** child is looked up via the
  ledger's stored `child_channel_id` and returned with `"replayed": true`.
- **Same `operation_id` + different request** (different child id, boundary,
  source, …) → `409 idempotency_conflict` — never an inconsistent-state error.
- **Existing `new_channel_id` + identical definition** → replay.
- **Existing `new_channel_id` + different source or boundary** →
  `409 channel_conflict`.
- **Deleted channel replay** → `410 gone` (Phase 11 soft-delete).
- Concurrency-safe: `fork_operations` rows are inserted with
  `ON CONFLICT DO NOTHING` inside the fork transaction; 10 concurrent forks
  with the same `operation_id` yield exactly one creation and nine replays.

Errors: `at_cursor < 0` → 400; unknown source channel → 404;
`at_cursor > 0` with no boundary → `409 snapshot_boundary_required`;
idempotency drift → `409 idempotency_conflict` / `409 channel_conflict`.

When `tool_policies` is supplied it participates in the request hash (appended
only when present, so a policy-free fork hashes byte-identically to the pre-v29
algorithm and an in-flight retry still replays).

### Side-effect-aware forks (v29)

A fork may declare `tool_policies` — an opaque
`{"tool_name": "replay"|"block"|"live"|"auto", "*": <default>}` map — so the
child can continue **without silently re-firing an effect the parent already
performed**. The default is `auto`: an exact inherited tool result is replayed;
new or changed work runs live. `replay`/`block` refuse to execute on a miss or
mismatch and persist a `tool.blocked` boundary (`409 fork_tool_blocked`);
`live` restores today's behavior. Full policy table, mechanics, and the
ancestor-chain resolution live in `docs/TOOL_EXECUTIONS.md` →
"Fork tool policies". The policy is stored on `channel_metadata.tool_policies`,
mirrored into the immutable `fork_operations` receipt, and echoed by the fork
response, `GET /receipt`, `GET /metadata`, and `GET /forks`. It is set at
fork time (there is no mutation endpoint, preserving receipt integrity).

Tested live by `tests/test_fork_tool_policies.py` (12 tests).

### Server-owned step index (Phase 8)

`POST /api/v1/events/record` accepts an optional `step_number`; the server
records a durable `channel_steps (channel_id, step_number, cursor, event_id)`
row in the same transaction as the event. `GET
/api/v1/channels/{channel_id}/steps/{step_number}` resolves a step to its
owning channel (walking `parent_channel_id` via the recorded
`forked_at_step`) + cursor. SDKs use it first and fall back to their legacy
replay-based resolution when the step isn't indexed.

Tested end-to-end by `tests/test_step_index.py` (13 live tests): exact
step→(cursor, event_id) resolution, unrecorded/zero steps → 404, idempotent
index writes under `ON CONFLICT DO NOTHING`, fork lineage resolution
(a fork inherits steps 1..N from the ancestor via `experiment_metadata
.forked_at_step`, steps past the boundary resolve on the fork itself,
fork-of-fork resolves to the deepest ancestor), and the execution tree
(`GET /api/v1/channels/{channel_id}/tree`) shape for roots, leaves and
nested children with `event_count`/`latest_cursor`/`truncated`/`max_depth`.

## 2. Atomic transitions

`POST /api/v1/events/transition`

```json
{
  "channel_id": "ch",
  "type": "agent.started",
  "payload": { "seq": 1 },
  "state": { "turns": 1 },
  "metadata": { "actor": "bot" },
  "expected_version": 3,
  "expected_cursor": 5
}
```

Commits the event **and** its state snapshot in one transaction — either both
exist or neither does. The snapshot's `cursor` equals the event's per-channel
`cursor`. Response: `201 { "event": {...}, "state_version": N }`.

An optional `operation_id` makes the transition idempotent: re-sending the same
`operation_id` with the same canonical request replays the original event +
state version instead of committing a second time; the same id with different
content is `409 idempotency_conflict`. This is what lets a worker that dies
after the commit but before reading the response recover safely (the reliability
`atomicity` suite proves it with a real `os._exit(137)`; the Python SDK exposes
it as `transition(..., operation_id=...)`).

The event is broadcast live to WebSocket subscribers exactly like
`POST /api/v1/events/record`.

## 3. Optimistic concurrency

`POST /api/v1/state/{channel_id}` accepts optional guards:

- `expected_version` — reject unless the channel's latest state version
  matches; `0` means "no snapshot exists yet".
- `expected_cursor` — reject unless the latest state cursor matches
  (`0` also accepts "no snapshot yet").

Both guards are checked **inside the transaction**: a channel-scoped
transaction advisory lock (`pg_advisory_xact_lock(hashtext(channel_id))`)
serializes guarded writes, then `FOR UPDATE` locks the latest snapshot row.
The advisory lock is load-bearing — `FOR UPDATE` on a `SELECT` that returns
no row (the "first snapshot, `expected_version = 0`" case) locks nothing, so
without it concurrent first-writes would all pass the guard. Exactly one
winner, losers get `409 version_conflict` and nothing is written. The same
guards are accepted by `POST /api/v1/events/transition`.

## 4. Durable consumer groups

A named group is bound to a channel. Consumers join with a lease, claim
event batches, ack progress; offsets and leases are **persisted**, so a
worker restart resumes from its last acked offset.

| Route | Body | Behavior |
|---|---|---|
| `POST /api/v1/groups` | `{ group_id, channel_id, metadata? }` | Create (upsert) |
| `GET /api/v1/groups?channel_id=` | — | List |
| `DELETE /api/v1/groups/{group_id}` | — | Delete + cascade consumers/offsets |
| `POST /api/v1/groups/{group_id}/join` | `{ consumer_id, lease_seconds? }` | Register/renew lease + durable offset row |
| `POST /api/v1/groups/{group_id}/work` | `{ consumer_id, limit? }` | Claim a batch |
| `POST /api/v1/groups/{group_id}/ack` | `{ consumer_id, cursor }` | Commit offset (needs active lease) |
| `POST /api/v1/groups/{group_id}/heartbeat` | `{ consumer_id, lease_seconds? }` | Extend lease |
| `GET /api/v1/groups/{group_id}/offsets` | — | Per-consumer `last_cursor`/`claimed_cursor` |

Semantics (at-least-once, lease-based):

- **Claim** reserves a batch of events (`FOR UPDATE SKIP LOCKED`) starting
  after `max(committed, valid claimed watermark, own claimed)` where the
  watermark only counts consumers with **active** leases. Concurrent claims
  are serialized per group (the group row is locked `FOR UPDATE` first), so
  no event is delivered twice while leases are active.
- **Ack** durably advances `last_cursor`. A crashed consumer's unacked work
  is redelivered after its lease lapses.
- **Lease expiry**: the `consumer_lease_reaper` scheduler task
  (every 30 s) marks expired consumers; their claims become reclaimable.
  An expired consumer must `join` again before claiming/acking.
- Leases default to 60 s (1 s – 86400 s).
- Errors: unknown group → `404 group_not_found`; missing/expired lease →
  `409 consumer_not_found`.

Schema (migration v11): `consumer_groups`, `group_consumers`,
`group_offsets (last_cursor, claimed_cursor)`.

## 5. Persisted wake-ups

`POST /api/v1/scheduler/wakeups`

```json
{ "channel_id": "ch", "run_at": "2026-08-01T10:00:00Z", "payload": {...} }
```

At or after `run_at` the `wakeup_dispatcher` scheduler task (every 5 s)
fires a `scheduler.wakeup` event on the channel (actor `system`, metadata
`{ "wakeup_id": ... }`) and marks the wake-up `done`. Subscribers receive it
**live** over WebSocket; everyone else can replay it. Wake-ups survive
server restarts.

| Route | Behavior |
|---|---|
| `POST /api/v1/scheduler/wakeups` | Schedule → `201 { id, status: "pending", ... }` |
| `GET /api/v1/scheduler/wakeups?channel_id=&status=` | List |
| `GET /api/v1/scheduler/wakeups/{id}` | Get |
| `DELETE /api/v1/scheduler/wakeups/{id}` | Cancel (pending only; otherwise `409 wakeup_not_pending`) |

Statuses: `pending → done | failed | cancelled`. Dispatch is single-writer
safe (`FOR UPDATE SKIP LOCKED` in the dispatch transaction) — concurrent
dispatchers never double-fire. Failed batches are retried with exponential
backoff (`attempts`, `next_attempt_at`). Schema: `scheduled_wakeups`
(migration v12).

## 6. Python SDK surface

| Method | Server |
|---|---|
| `fork(source, child, at_cursor=0, *, operation_id=None, manifest=None, expected_version, expected_cursor, ...)` | fork → immutable `ForkReceipt` (raises `SnapshotBoundaryError` on 409, `IdempotencyConflictError`, `ChannelConflictError`) |
| `transition(channel_id, type, payload, state, *, operation_id, expected_version, expected_cursor, ...)` | transition |
| `save_state(..., *, expected_version, expected_cursor)` | conditional save (raises `VersionConflictError`) |
| `create_group / list_groups / delete_group / join_group / claim_work / ack_work / heartbeat / group_offsets` | consumer groups |
| `schedule_wakeup / list_wakeups / get_wakeup / cancel_wakeup` | wake-ups |

`AgentSession.fork` / `resume(fork_at_step=)` use a **strict-by-default**
boundary policy: `boundary_mode="exact"` raises `NoRestorableCheckpointError`
when no snapshot exists at the requested step — it NEVER silently falls
forward to the latest state (temporal contamination). `"approximate"` falls
back to the latest state and reports the drift via
`session.resolved_boundary_cursor` / `requested_boundary_cursor`;
`"lineage_only"` creates the fork with no state copy
(`boundary_restorable=False`).

## 9. Structural diff + decision trail

Two read-only surfaces turn forks into a falsifiable-debugging and
audit tool.

### `GET /api/v1/channels/diff?left={channel}&right={channel}`

Structural diff of two channels' **latest saved states** — not their event
lists. Walks both JSONB states recursively and emits one entry per differing
path. This answers "what did fork B know that fork A didn't at the point of
divergence".

Response:

```
{
  "left_channel_id", "right_channel_id",
  "left":  {"channel_id", "cursor", "state"},
  "right": {"channel_id", "cursor", "state"},
  "common": {"channel_id": <lca>, "cursor", "state"} | null,
  "left_diverged_at_cursor": int | null,
  "right_diverged_at_cursor": int | null,
  "entries": [
    {"path": ["cfg", "retries"], "kind": "added"|"removed"|"changed",
     "left": ..., "right": ...},
    ...
  ],
  "truncated": bool,
  "entry_count_total": int,
  "max_entries": int
}
```

- `kind` convention: **added** = right-only, **removed** = left-only,
  **changed** = both sides have the path but the values differ.
- `common` resolves via the **lowest common ancestor** of the two channels in
  the fork lineage (not merely the shared `origin_run_id`): for a
  parent/child diff the parent is the ancestor, for siblings it is their
  shared fork parent. The ancestor's latest saved state is returned.
- **Divergence is per-side** (audit item 4): lineage is modeled as EDGES.
  `left_diverged_at_cursor` / `right_diverged_at_cursor` are each fork's
  fork boundary (the resolved edge immediately after the LCA); a side that
  IS the LCA has `null` (it never diverged). Siblings forked at different
  cursors are asymmetric and BOTH values are reported. This replaces the old
  single `diverged_at_cursor`, which was wrong (it read the shared-prefix
  nodes' `forked_at_cursor`, which is `0` for the root).
- Arrays are diffed positionally up to the shorter length; the longer
  array's tail is emitted per-index as `added`/`removed`.
- **Truncation is honest** (audit item 10): `entries` is capped at
  `max_entries` (500), but `entry_count_total` counts every differing path
  (capped internally at 100_000 to bound work) and `truncated` is true when
  the response is partial. Never trust a diff as complete without checking
  `truncated`.
- Channels without saved state diff against `{}` (all keys appear as
  `added`).

Auth: read access required on **both** channels.

### `GET /api/v1/channels/{channel_id}/trail`

Lineage-as-audit: every decision in a run traces back to a fork point.
Returns:

```
{
  "channel_id", "origin_run_id",
  "ancestry": [
    {"channel_id", "parent_channel_id", "forked_at_cursor",
     "display_name", "reason"},   // channel itself first, then each ancestor
    ...
  ],
  "boundary": {
    "source_channel_id", "requested_cursor", "resolved_cursor",
    "resolved_event_id", "source_state_version", "source_state_sha256",
    "restorable", "state"
  } | null,
  "executions": [ /* tool_executions ledger rows for this channel */ ],
  "truncated": bool,
  "max_depth": int
}
```

- `ancestry` walks `parent_channel_id` up to the root (bounded at depth 10,
  same as the execution tree) and always includes the channel itself, even
  for roots created without an explicit metadata row.
- `boundary` is the **immutable fork-time boundary** (audit item 3): the
  child's FIRST `channel_states` row (the copied snapshot) plus the stored
  provenance. It is NEVER derived from the parent's mutable latest state, so
  an `at_cursor=0` fork stays correct even after the parent advances. `null`
  for roots and lineage-only forks.
- `executions` reuses `GET /api/v1/executions?channel_id=` (the idempotent
  `tool_executions` ledger) so the trail ties each tool call to the fork.
- `truncated` is true when `ancestry` was cut off at `max_depth` (a deep
  lineage is reported, never silently trimmed).

### Execution tree truncation

`GET /api/v1/channels/{id}/tree` returns `"truncated": bool` and
`"max_depth": 10` at the top level. A tree deeper than the depth cap is
reported as truncated instead of silently dropping forks.

## 10. Reproducibility grading

The server computes a grade at fork time and stores it in
`channel_metadata.reproducibility` (also on the receipt and `fork.started`):

| Grade | Condition |
|---|---|
| `state_exact` | `restorable` AND the boundary is exact (`requested == resolved`, or `requested == 0` = latest, self-consistent). |
| `context_exact` | `state_exact` AND the manifest carries model + environment + dependencies + seed. |
| `execution_replayable` | `state_exact` AND the source has a tool-execution ledger. |
| `best_effort` | everything else (approximate boundary, lineage-only, missing manifest). |

## 11. Experiment platform, promotion, GC, comparison

- **Groups** (Phase 9): `POST/GET /api/v1/experiments`, `GET /experiments/{id}`,
  `POST /experiments/{id}/members`, `GET /experiments/{id}/rank`. A group is a
  baseline plus N variant forks with declared parameter deltas; ranking
  sorts by `result_score` desc (nulls last).
- **Outcomes** (Phase 9): `PATCH /api/v1/channels/{id}/outcome` records
  `promoted | rejected | inconclusive | crashed` + optional score.
- **Promotion** (Phase 9): `POST /api/v1/channels/{id}/promote` is an
  append-only merge — the child's latest state is copied onto the parent at
  its next cursor, a `run.promoted` event is recorded on the parent
  (`depends_on` = child's last event), and the child is marked promoted.
- **GC / retention** (Phase 11): `DELETE /api/v1/channels/{id}?recursive=`
  soft-deletes (keeps `channel_metadata.deleted_at`) and purges the physical
  rows; non-recursive deletes of a parent with children are rejected (409);
  idempotent replays of deleted forks return `410 gone`.
- **Comparison** (Phase 10): `GET /api/v1/channels/compare?left=&right=`
  combines the state diff with the fork-manifest diff, tool-call diff, output
  diff and latency/token/cost metrics delta. The dashboard exposes this as
  the full-width **Compare** tab in the Flow bottom panel; it deliberately
  uses this authoritative document instead of inferring a shared prefix from
  event type/position. The Inspector retains a compact state-only diff.

## 12. Migrations added

| Version | Name | Contents |
|---|---|---|
| v10 | `create_fork_operations` | idempotency ledger |
| v11 | `create_consumer_groups` | groups + consumers + offsets |
| v12 | `create_scheduled_wakeups` | persisted wake-ups with retry |
| v17 | `fork_boundary_provenance` | requested/resolved boundary, source version + sha256, restorable, manifest, reproducibility, outcome, result_score, deleted_at |
| v18 | `channel_steps` | server-owned step index |
| v19 | `experiment_groups` | experiment platform (groups + members) |
| v29 | `fork_tool_policies` | `channel_metadata.tool_policies` + `fork_operations.tool_policies` (side-effect-aware forks) |

## 13. Scheduler tasks

Registered in `main.rs`:

- `consumer_lease_reaper` (30 s) — expires lapsed leases.
- `wakeup_dispatcher` (5 s) — fires due wake-ups and broadcasts them.

## 14. SDK surface

| SDK | Method |
|---|---|
| Python | `client.diff_states(left, right)` / `diff_states_sync`, `client.decision_trail(channel)` / `decision_trail_sync`, `client.fork(...) → ForkReceipt`, `client.get_fork_receipt(channel)`, `client.resolve_step(...)`, experiment/outcome/promote/delete/compare helpers |
| Go | `client.DiffStates(ctx, left, right)`, `client.DecisionTrail(ctx, channel)`, `client.Fork(...) → ForkReceipt`, `client.GetForkReceipt(...)`, `client.ResolveStep(...)`, experiment/outcome/promote/delete/compare helpers |
| Dashboard | Full-width Compare tab (state/config/tools/output/metrics), compact State Diff in Inspector, Decision Trail (lineage + boundary + tool executions) |
