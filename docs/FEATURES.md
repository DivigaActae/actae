# Actae — Feature Reference

Distributed execution groups and counterfactual Group Fork are documented in [Execution Groups](EXECUTION_GROUPS.md): durable cross-machine member messaging, lease fencing, durable waits, multi-parent causal graphs, frozen/live/reactive policies, and replay/live/block tool receipts.

> **Purpose.** This file is the feature encyclopedia of Actae: what features
> exist, what each one does, how it works, and the exact API surface —
> from the high-level mental model down to routes, JSON fields, defaults and
> failure semantics. It is written to be read by both humans and LLMs
> ("tell me what Actae can do and how") and is grounded in the current code.
>
> **Verification anchors.** Line numbers point into the current checkout
> (`actae/src/...`); treat them as anchors, not contracts. The companion
> `docs/SERVER_INTERNALS.md` is the server-internals twin (routing table,
> WS wire protocol, scheduler registry, embedded-PG mechanics, config
> parsing) with `file:line` anchors everywhere.

**Product stage:** Production (server 0.1.x). **Source layout:** `actae/` (Rust
server), `sdks/{python,go,typescript}/`, `dashboard/` (Next.js),
`saas/` (control plane + portal), `tests/` (integration), `examples/`
(runnable demos).

---

## 1. What Actae is (high level)

Actae is a **durable execution substrate and event backbone for autonomous
systems** — AI agents first, generic event-driven systems second. It is one
server that provides:

- a durable, ordered, per-channel **event log** (record / replay / query);
- **versioned, cursor-aligned state** saved atomically with events
  (transitions);
- **exact-boundary forking** of an execution — a child channel inherits real
  state, lineage and an immutable provenance receipt;
- **live delivery** over WebSocket with cursor catch-up (past + present);
- **idempotent tool-execution coordination** (claim/lease/result ledger);
- **durable consumer groups** (leases, offsets, ordered work claims);
- **persisted scheduled wake-ups**;
- **lineage, structural diff, decision trail, experiment groups and
  promotion** for comparing and promoting forks;
- crash-recovery semantics (an execution can resume from durable state);
- SDKs in Python, Go and TypeScript plus adapters for LangGraph, LangChain,
  Claude Agent SDK, OpenAI Agents, CrewAI, Codex and GitHub Copilot;
- a web dashboard and a bundled managed PostgreSQL (the default) so
  one binary is enough to run it anywhere.

### What Actae does NOT do

Actae is infrastructure under the agent, not the agent's brain:

- it does not run an LLM, choose prompts/models, or plan the next step;
- it does not execute tools — the caller executes the tool under a
  claim/lease, and Actae coordinates the retry (records results, fences stale
  owners, replays completed outcomes). Idempotency of the external effect
  still depends on the downstream operation.
- it does not auto-rerun an application's suffix after a fork (the caller
  continues; SDK `AgentSession` makes that ergonomic);
- it does not merge forks back into parents (promotion marks outcome +
  closes a channel; merging is app-owned).

### The mental model

- **Channel** = the durable identity of one execution stream (agent run,
  LangGraph thread, Claude session, workflow instance, business process).
  Each channel has its own monotonic event **cursor**.
- **Event** = immutable typed record: `id`, `channel_id`, `event_type`,
  opaque JSON `payload`, `actor` (+ optional `agent_id`/`user_id`),
  per-channel `cursor`, `timestamp`, optional `depends_on` (causal parent
  event id), optional `operation_id` (idempotency key).
- **State** = opaque JSON document per channel, saved **against a cursor**
  and versioned independently.
- **Transition** = one database transaction that commits an event **and** a
  state snapshot — the two can never disagree.
- **Fork** = new child channel whose first event (`fork.started`) is
  causally linked to the parent's boundary event, with the parent's saved
  state copied over (or not, depending on boundary mode).

---

## 2. Architecture at a glance

```
┌────────────────────────────────────────────────────────────┐
│  actae (Rust, axum + tokio-postgres + sonic_rs)            │
│                                                            │
│  HTTP API (/api/v1/*)   WS (/ws)   /healthz /readyz        │
│  /metrics /metrics.json          dashboard (embedded)      │
│                                                            │
│  auth/security (API keys HMAC, JWT, rate limits, mTLS)     │
│  scheduler (lease reaper, wakeup dispatch, retention,      │
│             key reauthorize sweeps, checkpoints)           │
│  control-plane sync (keys + subscription lock, 60s)        │
│  managed PostgreSQL child by default (or external PG)      │
└────────────────────────────────────────────────────────────┘
        │
        ▼
   PostgreSQL (event log, states, metadata, ledgers, auth)
```

- One port serves HTTP API + WebSocket + health/metrics + dashboard.
- Every API key and WS auth path is subject to the **subscription lock**:
  when the control-plane subscription lapses, all API keys get `402`
  (dashboard JWT sessions exempt); after 72 h of no phone-home the server
  serves `503` for protected access.
- Events are written to PostgreSQL **before** broadcast (durable first,
  live second).

---

## 3. Feature: durable event log

| Route | Purpose |
|---|---|
| `POST /api/v1/events/record` | persist one typed event |
| `POST /api/v1/events/transition` | persist event + state snapshot atomically |
| `GET /api/v1/events/replay/{channel_id}` | ordered history from a cursor |
| `POST /api/v1/events/query` | filtered cross-channel search |
| `GET /api/v1/events/cursor/{channel_id}` | latest per-channel cursor |
| `GET /api/v1/events/verify/{channel_id}` | tamper-evident hash-chain check |

### 3.1 record

`POST /api/v1/events/record` — body: `{channel_id, event_type, payload,
actor, operation_id?, metadata?, depends_on?}`.

- **Cursor assignment** is done inside one database transaction under a row
  lock on the channel's `channel_cursors` row — concurrent publishers get a
  single ordered sequence with no gaps or duplicates.
- **Idempotency**: reusing the same `operation_id` on the same channel
  returns the originally persisted event (same cursor, same id) instead of
  recording a duplicate. A *mismatched* reuse (different payload/type for
  the same operation id) fails with `409 idempotency_conflict`.
- Validation: identifiers, event type, actor, payload size (body cap 2 MB),
  auth, rate limits — all checked before persistence.
- The event is then available for replay **and** broadcast to WS
  subscribers.

### 3.2 transition

`POST /api/v1/events/transition` — persists an event and a state snapshot
in **one transaction**; the snapshot's cursor equals the event's cursor.
Guards:

- `expected_version` — the state save fails with `409 version_conflict` if
  the channel's current state version differs (stale-writer protection);
- `expected_cursor` — same for the event cursor (protects against writing
  from an unexpected boundary).

Idempotent replay (same rules as `record`, plus the snapshot):

- Reusing an `operation_id` whose transition already committed returns the
  **original** event + the original state version — nothing is inserted
  again (no duplicate snapshot at the cursor, no second event), and the
  replay is evaluated **before** the version guards, so a transport retry
  carrying stale `expected_*` still resolves idempotently.
- The content comparison is canonical (key-order-insensitive): a retry that
  re-serializes the same payload with a different key order replays.
- A *mismatched* reuse (different type/payload/metadata/state for the same
  operation id) fails with `409 idempotency_conflict` instead of silently
  serving the old result for a new operation.

Use it for state machines, agent steps, workflows — anywhere the replay
evidence and the recoverable state must never disagree.

### 3.3 replay

`GET /api/v1/events/replay/{channel_id}?cursor=N&limit=M&event_type=T`

- Returns events with `cursor >= N`, oldest first.
- Limits: the handler accepts up to 5000; the store clamps to 1000 per
  request. SDK helpers paginate (loop with `cursor = last_seen_cursor`)
  for more.
- Optional `event_type` filter.
- Typical uses: rebuild context after restart, backfill a new consumer,
  inspect a failure window, recover a WS gap, reconstruct a run for audit.

### 3.4 query

`POST /api/v1/events/query` — analytical read across channels with filters:
`channel_ids`, `event_type`, `payload` key/value, `actor`, `agent_id`,
`user_id`, `operation_id` (dedup), `depends_on`, `since`/`until` time
bounds, ordering, `limit` (default 100, max 1000).

**Keyset pagination (additive, actae.api.v1):** pass an opaque `page_token`
(returned as `next_page_token` when `has_more` is true) to continue across
channels on the stable internal `global_cursor` — no omission/duplication under
concurrent inserts. Tokens are HMAC-signed, versioned, bound to the normalized
filters and caller scope, expire in 15 minutes, and expose no SQL value.
Reusing a token with changed filters → `400 invalid_page_token`; expired →
`410 page_token_expired`; continuation purged by retention →
`410 page_token_unsatisfiable`. Without a `page_token` the legacy `offset`
pagination runs unchanged. Engine: `actae/src/events/page_token.rs` +
`EventStore::query_page`.

### 3.5 cursor

`GET /api/v1/events/cursor/{channel_id}` — the latest per-channel cursor;
used to resume from the end, seed a WS catch-up, or decide "is there
anything new?".

---

## 4. Feature: versioned state

| Route | Purpose |
|---|---|
| `POST /api/v1/state/{channel_id}` | save a new state version (cursor-aligned) |
| `GET /api/v1/state/{channel_id}` | load the latest state |
| `GET /api/v1/state/{channel_id}/versions` | list versions |
| `GET /api/v1/state/{channel_id}/version/{version}` | load one version |
| `DELETE /api/v1/state/{channel_id}/version/{version}` | delete one version |

- State is opaque JSON stored in PostgreSQL JSONB, saved **against an event
  cursor** — "this is the state as of cursor N".
- Saves may pass `expected_version`/`expected_cursor` optimistic guards
  (see 3.2); conflicts return a typed `VersionConflictError`, never a
  silent overwrite.
- The server stores a SHA-256 fingerprint of the canonical JSON so forks
  and fidelity checks can prove *what exactly* was inherited.
- The application decides what state means: agent context, LangGraph
  checkpoint envelope, conversation, workflow variables, a domain
  aggregate.

---

## 5. Feature: forking (the flagship)

| Route | Purpose |
|---|---|
| `POST /api/v1/channels/fork` | create a child fork (idempotent) |
| `GET /api/v1/channels/{id}/receipt` | immutable fork provenance receipt |
| `GET /api/v1/channels/{id}/steps/{n}` | server-owned step → cursor/event resolution |

### 5.1 What a fork is

A fork creates a **new channel** from a source channel at a requested event
cursor. In one transaction the server:

1. resolves the latest saved state snapshot at or before the requested
   cursor (exact-boundary mode);
2. copies it into the child channel (new state version 1);
3. writes `fork.started` as the child's first event (cursor 1, actor
   `system`, `depends_on` = the parent boundary event id);
4. persists lineage metadata (`parent_channel_id`, `origin_run_id`,
   requested/resolved cursors, source state version + SHA-256) in
   `fork_operations` (immutable receipt) and `channel_metadata`.

The result: a fork with **inherited state but an independent future event
stream**. A fork is a state-and-lineage operation — not a process clone and
not a copy of trace rows.

### 5.2 Boundary resolution

- `at_cursor > 0`: the server resolves the latest snapshot with
  `cursor <= at_cursor`. If **no snapshot exists at or before the requested
  cursor**, the request fails with `409 snapshot_boundary_required`
  (surface as `NoRestorableCheckpointError` in SDKs) — the SDK default is
  strict, **no silent fall-forward to a later checkpoint**.
- `at_cursor = 0`: fork from the **latest** saved state (a "fork here and
  refine from now" primitive; `StateManager.fork` uses this).
- Lineage chain: `origin_run_id` walks the parent chain to the root — a
  fork-of-a-fork keeps the original root's origin id.
- Because the step index (5.4) maps `(channel, step N)` → cursor/event
  across the lineage, `fork at step N` on a fork-of-a-fork resolves to the
  **ancestor that actually recorded step N**.

### 5.3 ForkReceipt (immutable provenance)

`GET /api/v1/channels/{id}/receipt` returns the `ForkReceipt` recorded at
fork time (never mutated afterwards):

| Field | Meaning |
|---|---|
| `fork_id` / `source_channel_id` / `child_channel_id` | identity |
| `requested_cursor` | what the caller asked for (0 = latest) |
| `resolved_cursor` | the snapshot cursor actually inherited |
| `resolved_event_id` | parent boundary event the `fork.started` depends on |
| `source_state_version` | source snapshot version copied (0 = none) |
| `source_state_sha256` | SHA-256 of the inherited state |
| `restorable` | true = a state snapshot was copied (checkpoint) |
| `replayed` | true = served from the idempotency replay path |
| `manifest` | caller-supplied reproducibility manifest (model, env, deps, seed) |
| `reproducibility` | grade computed at fork time (see below) |
| `tool_policies` | side-effect policy map for the child (`replay`/`block`/`live`/`auto`; `null` = `auto`) |

**Reproducibility grades** (stored on `channel_metadata.reproducibility`):

| Grade | Meaning |
|---|---|
| `state_exact` | restorable checkpoint at the exact requested boundary |
| `context_exact` | `state_exact` + manifest covering model + environment + dependencies + seed |
| `execution_replayable` | `state_exact` + the source has a tool-execution ledger |
| `best_effort` | approximate boundary, lineage-only, or missing manifest |

The receipt is the answer to "what did this fork actually inherit, and is
it reproducible?" — `SDK_REFERENCE`-level SDKs expose
`boundary_restorable`, `requested/resolved_boundary_cursor`,
`source_state_version`, `source_state_sha256`, `reproducibility`.

### 5.4 Server-owned step index

`POST /fork` and SDK `AgentSession.step()` maintain a durable
`(channel_id, step_number) → cursor + event_id` mapping (`channel_steps`).
`GET /api/v1/channels/{id}/steps/{n}` resolves step N without inferring it
from event positions (which break when lifecycle markers or nested forks
exist). This is what makes **"fork at step N"** exact — even across
lineage. `GET /api/v1/channels/{id}/steps` returns the highest indexed step
(`0` = none) in O(1), which SDK crash recovery uses to reconstruct true
progress after a hard process death instead of trusting lagging metadata.

### 5.5 Fork idempotency

Fork creation accepts an `operation_id`; a retry returns the **same** child
channel and a `replayed: true` receipt. Distinct conflict errors:
`409 idempotency_conflict` (operation id reused with different parameters)
and `409 channel_conflict` (child channel id already exists).

### 5.6 SDK boundary modes

All three SDKs (`AgentSession.fork` / `AgentSession.resume(fork_at_step=)`)
expose `boundary_mode`:

- `exact` (default) — strict; raises `NoRestorableCheckpointError` when no
  snapshot exists at/before the requested step;
- `approximate` — falls back to the latest saved state and reports the
  drift in the receipt;
- `lineage_only` — creates provenance and a child stream **without copying
  restorable state** (`restorable: false`).

Saving state every step (`snapshot_interval=1` + a `state_fn`) makes every
step independently forkable. **The value proposition**: run steps 1..N
once, fork their real state, iterate on step N+1 without paying to
reproduce the prefix.

### 5.7 Side-effect-aware forks (v29)

A fork may carry a `tool_policies` map
(`{"tool_name": "replay"|"block"|"live"|"auto", "*": default}`, default
`auto`) so a fork can continue **without silently re-firing an effect the
parent already performed**. `auto` replays an exact inherited result and runs
new/changed work live; `replay`/`block` refuse (persisting a `tool.blocked`
boundary, `409 fork_tool_blocked`); `live` is the old behavior. Policies are
resolved against the ancestor chain nearest-first (fork-of-fork replays a
result any ancestor recorded). Actae still never executes the tool. See
`docs/TOOL_EXECUTIONS.md` → "Fork tool policies".

---

## 6. Feature: lineage, comparison and experiments

| Route | Purpose |
|---|---|
| `GET/PUT /api/v1/channels/{id}/metadata` | read / update lineage + annotations |
| `GET /api/v1/channels/{id}/forks` | direct children |
| `GET /api/v1/channels/{id}/tree` | recursive execution tree (counts, latest cursors) |
| `GET /api/v1/channels/diff` | structural state diff of two channels |
| `GET /api/v1/channels/compare` | combined "what changed?" document |
| `GET /api/v1/channels/{id}/trail` | decision trail (lineage + boundary + tools) |
| `PATCH /api/v1/channels/{id}/outcome` | annotate outcome/score |
| `POST /api/v1/channels/{id}/promote` | promote a fork (closes source) |
| `DELETE /api/v1/channels/{id}` | delete a channel (`recursive` option) |
| `POST /api/v1/channels/{id}/close` | stamp `closed_at` |

### 6.1 Metadata

`channel_metadata` holds: `parent_channel_id`, `origin_run_id`,
`forked_at_cursor`, `forked_at`, `display_name`, `reason`,
`experiment_metadata` (opaque JSON — AgentSession persists step cursors
here), `outcome`/`result_score` fields, `manifest` + `reproducibility`.
`PUT` is an upsert, so it works even for channels without prior metadata.
Outcome vocabulary (SDK types): `promoted | rejected | inconclusive |
crashed`.

### 6.2 Diff

`GET /api/v1/channels/diff?left=A&right=B` — recursive comparison of the
**latest saved state** of two channels: added / removed / changed paths
(objects by key, arrays positionally). Response is honest about ancestry:

- `left_diverged_at_cursor` / `right_diverged_at_cursor` — the fork edge
  where each fork departed the shared chain (per-side divergence);
- `truncated` / `entry_count_total` — defensive caps (entry count, depth)
  mean a comparison is **never silently partial**.

Use case: prove a proposed intervention changed only the intended state,
or "what did the winning fork know that the other didn't?"

### 6.3 Compare ("what changed?")

`GET /api/v1/channels/compare` — the combined document for a fork pair:
state diff + configuration deltas + tool outputs + outcome metrics, tabbed
in the dashboard's `CompareView`.

### 6.4 Decision trail

`GET /api/v1/channels/{id}/trail` — lineage chain from the fork to its
root, the immutable inherited boundary (`ForkBoundary`: source channel,
requested/resolved cursors, resolved event id, source state version,
SHA-256 — derived from the **child's first snapshot**, never the parent's
mutable latest), and the tool-execution ledger entries of the channel. This
is the server-side basis for "how did this fork reach its outcome?".

### 6.5 Experiments

| Route | Purpose |
|---|---|
| `POST /api/v1/experiments` | create an experiment group |
| `GET /api/v1/experiments` | list groups |
| `GET /api/v1/experiments/{id}` | group detail |
| `POST /api/v1/experiments/{id}/members` | add a channel as baseline/variant |
| `GET /api/v1/experiments/{id}/rank` | rank members by outcome score |

An experiment is a baseline + variant channels with declared parameter
deltas; members carry outcomes and scores; ranking orders members by
outcome. **Promotion** (`POST /channels/{id}/promote`) marks the winner,
stamps the channel with a `promoted` marker and closes the source channel —
the durable "this fork won" decision. (`docs/EXPERIMENTS.md` is the
full reference.)

### 6.6 Lifecycle

`DELETE /channels/{id}` (optionally recursive) removes a channel and its
children; `POST /channels/{id}/close` stamps `closed_at` on the registry
row (used by promotion and by operators freezing a fork).

---

## 7. Feature: idempotent tool-execution ledger

| Route | Purpose |
|---|---|
| `POST /api/v1/executions/claim` | create / replay / claim a tool operation |
| `GET /api/v1/executions` | list executions for a channel |
| `GET /api/v1/executions/{id}` | one ledger record |
| `POST /api/v1/executions/{id}/complete` | persist a successful result |
| `POST /api/v1/executions/{id}/fail` | persist a structured failure |
| `POST /api/v1/executions/{id}/heartbeat` | extend the lease |
| `POST /api/v1/executions/{id}/cancel` | cancel a running execution |
| `DELETE /api/v1/executions/{id}` | delete a record |

**Actae never executes the tool.** The caller claims a logical operation
keyed by `(channel, key)`; the server hashes the relevant request
parameters (`request_hash`) and answers with one of four outcomes:

| Outcome | Meaning |
|---|---|
| `claimed` | you own a new execution (returns `claim_token`, `execution_id`) — run the tool |
| `replayed` | an identical completed execution already exists — use its stored result (plus a one-time `tool.replayed` marker event) |
| `reclaimed` | a prior lease expired — the work is re-queued, `attempts` incremented |
| `in_progress` | another worker owns the lease — back off |

- **Fencing**: `complete`/`fail`/`heartbeat`/`cancel` require the
  `claim_token`; a stale worker's token is rejected (`ExecutionNotOwned`).
- **Lease**: extended via heartbeat; expired leases become reclaimable
  (at-least-once semantics for the side effect).
- **Idempotency**: completion is idempotent; reusing the key with
  *different* parameters fails with `IdempotencyKeyMismatch`.
- The execution's `started_event_id` becomes the `depends_on` of its
  lifecycle events — the ledger is causally linked to the event stream.
- **Fork policies**: a forked channel can declare `tool_policies`
  (`replay`/`block`/`live`/`auto`, default `auto`) so a fork replays an
  inherited result instead of re-firing it, or blocks it outright; see §5.7.
- Use for costly or irreversible effects: payments, email, external writes,
  LLM/tool requests that must not repeat after an ambiguous timeout.

---

## 8. Feature: durable consumer groups

| Route | Purpose |
|---|---|
| `POST /api/v1/groups` | create/upsert a group (name, channel, optional batch size/lease) |
| `GET /api/v1/groups` | list groups (optionally by channel) |
| `DELETE /api/v1/groups/{id}` | delete group + offsets |
| `POST /api/v1/groups/{id}/join` | join or renew a consumer lease (returns `consumer_id`) |
| `POST /api/v1/groups/{id}/work` | claim an ordered work batch (`FOR UPDATE SKIP LOCKED`) |
| `POST /api/v1/groups/{id}/ack` | commit progress (`claimed_cursor` advances) |
| `POST /api/v1/groups/{id}/heartbeat` | extend the consumer lease |
| `GET /api/v1/groups/{id}/offsets` | inspect per-consumer offsets/claims |

- **Durability**: offsets and claims live in PostgreSQL; acknowledged
  progress survives restarts; unacknowledged work is reclaimable after the
  owner's lease expires (reaper task, `consumer_lease_reaper`).
- **At-least-once**: claim → process → ack. Re-joining resets
  `claimed_cursor` to the acked watermark; a crashed consumer's in-flight
  range is redelivered (never skipped silently).
- **Ordering**: work claims are ordered by cursor; `SKIP LOCKED` keeps
  concurrent workers from double-claiming.
- **Design rule** (see `docs/COORDINATION.md`): keep the work channel free
  of non-work events — a group claims *everything* on its channel. The
  reference pattern is two channels: work + audit.
- `docs/COORDINATION.md` covers guarantees, crash-recovery semantics and
  the demo that proves each one.

---

## 9. Feature: scheduled wake-ups

| Route | Purpose |
|---|---|
| `POST /api/v1/scheduler/wakeups` | schedule `{channel_id, at, payload, topic?}` |
| `GET /api/v1/scheduler/wakeups` | list |
| `GET /api/v1/scheduler/wakeups/{id}` | get one |
| `DELETE /api/v1/scheduler/wakeups/{id}` | cancel a pending wake-up |

- A background dispatcher (`dispatch_due_wakeups`, 5 s loop) claims due
  rows with `FOR UPDATE SKIP LOCKED` (single-writer safety), inserts a
  **`scheduler.wakeup` event** (actor `system`, metadata `{wakeup_id}`) in
  the same transaction, and broadcasts it live over WS.
- States: `pending` → `done`, or `failed` with retry after
  `next_attempt_at` (backoff).
- Wake-ups survive server restarts (persisted schedule) — the agent process
  does not need to stay alive for delayed continuation, reminders, timeout
  handling, workflow timers.
- **Durable human approval** is packaged on top of wake-ups + replay:
  `request_approval` / `decide_approval` / `wait_for_approval` (and
  `AgentSession.require_approval`) record `approval.requested` /
  `approval.decided` / `approval.expired` events; the agent process need not
  stay alive, and Actae never resumes anything itself. See
  `docs/HUMAN_APPROVAL.md`.

---

## 10. Feature: WebSocket real-time protocol

`GET /ws` (upgrade) — one protocol for live events + catch-up. Full wire
reference in `docs/SERVER_INTERNALS.md`; the summary:

**Client → server messages** (`{type: ...}` envelopes):

| Type | Purpose |
|---|---|
| `auth` | API key or dashboard JWT (`X-API-Key` or `Bearer`) |
| `subscribe` | `{topic, cursor?}` — cursor = catch-up from there |
| `subscribe_many` | `{topics: [{topic, cursor?}], request_id?}` — multi-channel subscribe (additive, ≤100 topics); each entry gets the same treatment as `subscribe`, then a `connection/event=subscribed_many` summary echoes the per-topic results + `request_id` |
| `unsubscribe` | `{topic}` |
| `broadcast` | publish a message (persisted event, idempotent via `operation_id`) |
| ping / heartbeat | keepalive (server replies; also server-initiated pings) |

**Server → client messages**:

| Type | Purpose |
|---|---|
| `connection` | welcome: `connection_id`, auth status, subscriptions |
| `subscription` | confirmation with the starting cursor |
| `broadcast` / `event` | a persisted event delivered to subscribers |
| `direct_message` | point-to-point message |
| `database_change` | notification envelope |
| `ack` / `nack` | publish confirmation (includes the persisted event) or rejection with reason |
| `cursor_sync` | `{topic, cursor}` — catch-up marker after replay |
| `error` | protocol errors |
| heartbeat | ping/pong |

Semantics that matter:

- **Durable first, live second**: a publish is persisted to PostgreSQL
  before it is broadcast; acks carry the persisted event.
- **No echo**: the server does *not* echo a broadcast back to the
  publishing connection — `echo_self`/`EchoSelf`/`echoSelf` is a
  client-side convenience (single-client demos) and does not change server
  delivery.
- **Catch-up**: `subscribe(topic, cursor=N)` replays history then sends
  `cursor_sync` and continues live. SDKs track per-topic watermarks and
  resubscribe from them after reconnect (no gaps, no duplicates).
- **Delivery**: a delivery tracker + dead-letter queue handle undeliverable
  messages; batching loops (200 µs window, ≤16 frames) coalesce bursts;
  message size cap 1 MB; connection cap 1000 default.
- **Auth checkpoints**: authentication happens on `auth` (and before
  register/broadcast processing — subscribe requires auth first), with a
  WS auth cache and rate limiting; connection-pool reuse clears the
  principal.

---

## 11. Feature: authentication & security

### 11.1 Credentials

- `X-API-Key: sk-...` — API keys stored as **HMAC-SHA256 hashes** with a
  server secret; raw keys are shown only once at creation/delivery. API keys
  authenticate SDK/API traffic; they are **not** the dashboard login method.
- `Authorization: Bearer <jwt>` — dashboard session JWT. Three login paths
  mint one: portal **SSO** (`POST /api/v1/auth/sso` with a control-plane
  handoff token; `auth_method: "sso"`), password login
  (`POST /api/v1/auth/login`), or the retained programmatic
  `POST /api/v1/auth/api-key-login` (`auth_method: "api_key"`; `me` + WS auth
  skip the users-table lookup for those tokens).
- **Dashboard identity is the Actae portal account.** On an enrolled instance
  `POST /api/v1/auth/login` delegates to the control plane
  (`POST /v1/installations/{id}/dashboard-token`, see `docs/CONTROL_PLANE.md`);
  the portal's "Open dashboard" button mints a one-time SSO URL
  (`#sso=…`), so the user lands already authenticated. A standalone instance
  uses its local email/password table (first-boot admin below). `/api/v1/auth/config`
  returns `{enable_signup, sso, setup_required}`.
- `Authorization: Bearer <api-key>` — API key in Bearer form.
- `ENABLE_ANONYMOUS=true` — anonymous mode (dev/test); metrics still never
  pass anonymously.
- Optional local email/password (signup gated by `ENABLE_SIGNUP`, default
  false — 403 on *signup* otherwise; login is always reachable and returns
  401 for unknown accounts), and optional **mTLS** (`ENABLE_MTLS` opt-in
  hardening tier requiring TLS + client CA).

### 11.2 First-boot provisioning

An instance with an empty `api_keys` table auto-generates an initial key
(`source='local'`, printed once in the banner; `ACTAE_INITIAL_API_KEY`
seeds it deterministically for CI/compose). The boot that mints/seeds a key
also publishes it to `<ACTAE_CONFIG_DIR>/client.json` (mode 0600, atomic) so
the Python/Go/TypeScript SDKs authenticate with zero configuration; the SDKs
resolve explicit > env (`ACTAE_API_KEY`/`ACTAE_URL`) > file, and `actae login`
re-establishes it (see `docs/CLIENT_CREDENTIALS.md`). A **standalone** instance with
an empty `public.user` table additionally provisions a first-boot dashboard
admin (email `ACTAE_INITIAL_ADMIN_EMAIL`, default `admin@actae.local`,
`role='admin'`; password `ACTAE_INITIAL_ADMIN_PASSWORD` or — when unset — the
admin is created passwordless with a one-time **setup token** printed once and
redeemed at the dashboard `/login` setup form via `POST /api/v1/auth/setup`;
`GET /api/v1/auth/config` reports `setup_required`).
Enrolled instances never create local users — they authenticate through the
portal. No first-boot exit gate — an instance is always usable. **Local
dashboard auto-login**: the dashboard calls `POST /api/v1/auth/local-session` on
load and, for a same-machine browser on a standalone instance, receives an admin
session with no login/setup form (peer + `Host` + `Origin` must all be loopback;
`local_auto_login` in `/auth/config`; `ACTAE_LOCAL_AUTO_LOGIN=0` opts out) — the
"no account required" path. Enrolled/remote access still requires a real sign-in.

Dashboard sessions are **server-revocable**: each JWT's `jti` is recorded in
`public.dashboard_sessions`; `POST /api/v1/auth/logout` revokes one,
`POST /api/v1/auth/logout-all` revokes all for the user, and a password reset
revokes them too (checked with a ~30 s cache). Users have a `role`
(`admin`/`member`); admins manage accounts via `/api/v1/users` (list/create,
`PATCH`/`DELETE /{id}`, `POST /{id}/password`) and the dashboard **Team** page —
last-admin and self-delete guards prevent lockout. See
`docs/SELF_HOSTED_AUTH.md`.

### 11.3 Rate limits

- HTTP auth: token bucket, default **5/s, burst 20** per IP
  (`ACTAE_IP_AUTH_RATE` / `ACTAE_IP_AUTH_CAPACITY` — raise for CI runs),
  with TTL-evicting key cleanup and lockout after repeated failures.
- WS: connection-level message/auth rate limits; peer IP is derived from
  the socket (trusted proxies only) so limits are never spoofable.

### 11.4 Subscription enforcement (402/503)

Every API key — control-plane-synced or locally created — is subject to the
in-memory subscription lock evaluated in `resolve_auth_context` (HTTP) and
the WS authenticator. Lapsed subscription → `402` for all API keys;
dashboard JWTs exempt (an admin can always reach the dashboard). Control-
plane outage → **72 h offline grace** (`ACTAE_SUBSCRIPTION_GRACE_SECS`),
then protected access → `503` (fail-closed rather than silently enabled).

### 11.5 Hardening

2 MB body cap, security headers on every response, peer-IP middleware,
access log (one line per request; probes excluded unless
`ACTAE_ACCESS_LOG_ALL=1`), TLS/HTTPS with cert paths, input validation
(channel-id grammar, event-field validation), system/admin channel rules,
bounded caches, `GET /metrics` + `/metrics.json` gated by API key or
dashboard session (anonymous never passes).

---

## 12. Feature: retention & tamper-evidence

### 12.1 Hash chain

Every event row carries `hash` and `prev_hash`; the hash is computed over
`(channel_id, cursor, event_type, canonical payload, prev_hash)`, so the
chain is per-channel and link-anchored. `GET /api/v1/events/verify/{channel_id}`
recomputes the whole chain and reports `valid`, `verified_events`,
`unverified_events` (events written before the ledger existed),
`broken_at_cursor`, `first_bad_hash`.

### 12.2 Provenance-aware retention

`EVENT_RETENTION_DAYS` (default 30) — the purge task runs on
`RETENTION_CHECK_INTERVAL_SECS` (3600) and is **provenance-aware**: events
referenced by `depends_on`, by `fork_operations.resolved_event_id`, or as a
fork boundary cursor are **never purged** (removing them would break fork
provenance). Purged rows are first copied to `event_archive` in the same
transaction; `EVENT_ARCHIVE_RETENTION_DAYS` (default 0 = keep forever)
governs a separate permanent-erase pass. There are also explicit
purge/purge_all operations used by tests and operators.

---

## 13. Feature: managed storage, CLI and lifecycle

### 13.1 `actae`

One self-contained binary: API + dashboard + a **managed PostgreSQL
child** (the default when no external database is configured). Same code path
for dev, CI and production — the only difference is
where the PG binaries come from:

1. `ACTAE_PG_BIN_DIR` → 2. already-extracted `pg-bin/` → 3. embedded bytes
   (release builds) → 4. **host-installed PostgreSQL** adopted by dev
   builds (own data dir, port, scram auth; a running host server is never
   touched; existing-cluster PG major must match, else the pinned download
   is used) → 5. verified pinned download (dev builds only).
   `ACTAE_PG_NO_HOST=1` forces the download. **Release binaries are fully
   offline**: the PG package is baked in, SHA-256-verified against
   `actae/pg-checksums.json` before atomic extraction; platform mismatch
   fails loudly with guidance (Alpine uses dedicated `-alpine` artifacts).

Lifecycle features: data persists across restarts; `ACTAE_EMBEDDED_PG_WIPE=1`
recreates the cluster and fails loudly if the wipe cannot complete; HTTP
port auto-bumps when busy (`ACTAE_AUTO_BUMP`, default on — each instance
gets its own HTTP/PG port pair and data dir); a cross-platform
data-dir lock (`.actae.lock`) makes concurrent starts fail fast; graceful
shutdown (`pg_ctl stop` bounded, then kill+reap) on Ctrl+C/SIGTERM/SIGHUP;
after `kill -9` the next start **proves ownership** (postmaster.pid must
record our data dir + port and `pg_ctl status` must confirm a live server
with matching PID) before reclaiming an orphan; Unix sockets disabled
(loopback TCP only — no `/tmp/.s.PGSQL.*` collisions); the superuser
password is generated once into `pg.pwfile` (0600) and printed only on the
boot that minted it; the startup banner (URL, pg port, data dir, psql
command) prints at the very end of startup; each instance writes
`<data-dir>/url` for `actae doctor`.

### 13.2 CLI

| Subcommand | Purpose |
|---|---|
| `actae auth login/status/logout/sync` | control-plane device flow, enrollment state |
| `actae enroll <key>` | paste-to-enroll (one-shot portal key) |
| `actae keys create/list/revoke` | local API-key management (raw key prints once) |
| `actae login [--url] [--api-key]` / `actae logout [--all]` | save/remove SDK client credentials (`<ACTAE_CONFIG_DIR>/client.json`); no flags = local auto-mint + verify + rotate the prior key, `--url` = remote (verifies the key) |
| `actae upgrade` | self-update from the control plane (legacy/grandfathered Self-Hosted entitlement; the sold plans are Cloud tiers) |
| `actae doctor` | list running managed-storage instances |
| `actae` (no args) / `actae --database-url <dsn>` | run the server: bundled managed PostgreSQL, or an external database |

### Editions

Actae ships as a **free self-hosted community edition** and **Cloud** (the
retired $99 Self-Hosted Team plan is grandfathered for existing subscribers
only). The community edition is the same binary running **standalone** (no
control-plane configuration → no subscription enforcement) and is downloaded
anonymously from `GET /v1/community/download/{platform}`; `actae upgrade` and
the portal/fleet require a paid entitlement. An **optional, card-free account**
(no gate on the download) links a self-hosted instance (`actae enroll <key>`)
using `plan_code='free'`, which is perpetually `active` and never enforced;
**link = consent**, so an unlinked instance reports nothing. Successful
anonymous downloads are counted (no raw IP/UA/account) for the top of the
funnel. Full matrix: `docs/EDITIONS.md`; measurement: `docs/MEASUREMENT.md`.

---

## 14. Feature: control plane (SaaS sync)

Self-hosted instances phone home to the SaaS control plane
(`docs/CONTROL_PLANE.md` is the contract; `docs/SAAS_BACKEND.md` the
backend):

- **Sync loop** every 60 s (`ACTAE_SYNC_INTERVAL_SECS`): fetches
  `{subscription, rev, keys}` — raw keys delivered until first **ack**,
  then hash-only; local reconcile upserts active keys and revokes removed
  ones; a backwards `rev` forces a full reconcile.
- **Enrollment**: `actae auth login` device flow, `actae enroll sk-…`
  paste-to-enroll, `ACTAE_ENROLL_KEY` one-shot at boot, or env credentials
  (install id + secret). A standalone first boot without any credentials
  is always usable (see 11.2).
- **License tokens**: every control-plane response carries an **Ed25519
  license token** (`base64url(payload).base64url(sig)`, ±300 s skew, ≤30 d
  TTL) verified with the **build-time** public key
  (`ACTAE_LICENSE_PUBKEY`; dev/test builds share a test key) — fake control
  planes cannot mint tokens.
- **Enforcement**: in-memory lock updated only by the sync loop; evaluated
  per request (HTTP + WS); 402/503 semantics in 11.4.

---

## 15. Feature: SDKs (Python / Go / TypeScript)

Feature-parity trio. Python (`actae_client`, aiohttp, async-first with sync
facades), Go (module `github.com/divigaactae/actae/sdks/go`,
context-based), TypeScript (`@actae/sdk`, Node 20+ ESM-only, single runtime
dependency `ws`). All three expose the **complete server surface**: events
(record/replay/query/cursor/transition), state (save/load/versions/delete),
channels (fork/receipt/metadata/forks/tree/diff/compare/trail/steps/
outcome/promote/delete/close), experiments, executions, groups, wake-ups,
health/metrics, local auth, capabilities (`can/can_read/can_write/
can_publish/can_fork`), and user preferences.

### 15.1 Conventions (per-SDK)

| Concern | Python | Go | TypeScript |
|---|---|---|---|
| Optional scalars | kwargs | pointers (`actae.Ptr(v)`) | `undefined` |
| `autoReconnect` | `auto_reconnect=True` default | `*bool` (nil = true) | `autoReconnect?: boolean` (undefined = true) — setting `timeout` can't flip it |
| int64 fidelity | `_unwrap_sonic` | `unwrapSonic`/`AsInt64`/`AsBigInt` | `parseJson`/`asInt64`/`asBigInt` — never silently rounded; `stringify` emits bigint as raw JSON number tokens |
| Errors | typed exceptions (`AuthError`, `RateLimitError(retry_after_seconds)`, `SnapshotBoundaryError`, `NoRestorableCheckpointError`, `IdempotencyConflictError`, `ChannelConflictError`, `VersionConflictError`, `ConsumerError`, `IdempotencyKeyMismatchError`, `ExecutionNotOwnedError`, `ExecutionNotFoundError`, `SessionError`, ...) | typed error structs usable with `errors.As` | mirrored error classes |

The int64/sonic plumbing exists because the server (sonic-rs) wraps integers
that exceed `Number.MAX_SAFE_INTEGER` as `{"$sonic_rs::private::JsonNumber":
"42"}`; SDKs unwrap transparently so payloads round-trip.

### 15.2 WebSocket clients

subscribe/unsubscribe with confirmation, `stream()` (async iterator /
channel of live events ending on disconnect), publish with ack (the
persisted event), per-topic reconnect watermarks + catch-up dedup,
heartbeats, backoff reconnect, `echo_self` client-side echo. Callback
semantics: `on_message` accumulates, other callbacks are single-slot.

### 15.3 AgentSession (framework-neutral lifecycle)

`created → started → stepping → completed | crashed`; `async with` /
`Start()`/`Complete()`/`Crash()`; `step(step_type, input, output, context,
metadata, operation_id)` — the operation id makes retries idempotent
(same event, no duplicate); a caller-owned `state_fn` saves state at
`snapshot_interval` (default 1). `resume(channel)` reconstructs progress
after a crash (completed sessions refuse in-place continuation —
`SessionCompletedError` — unless you explicitly fork). `fork(at_step=N,
boundary_mode=...)` returns an **unstarted** session primed at step N+1
with inherited state; `resume(..., fork_at_step=N)` is fork + continue.
Every fork exposes the receipt properties (boundary restorable, resolved
cursor, state version, SHA-256, reproducibility).

When `operation_id` is not supplied, `Step` derives it **deterministically**
(all three SDKs): UUIDv5 over NUL-joined `(scope="agent-session",
step_type, channel_id, step_number, canonical payload+metadata)` with a
256-byte cap; the canonical serialization byte-matches Go's
`json.Marshal` of `{"payload": ..., "metadata": ...}` (payload first,
recursively sorted keys, Go-exact float formatting — `1.0` → `1`,
`1e-7`, `1e20` fixed notation — raw UTF-8,
`<`/`>`/`&`/U+2028/U+2029 escaped; NaN/Infinity fail loudly in every
SDK, like Go's json.Marshal). Same step + same content → same id →
server replays the original event after a crash-recovery re-drive;
changed content → distinct id → new event.

**Channel ownership fence (opt-in):** `AgentSession(..., fence_owner=...)`
claims a per-channel lease through the idempotent execution ledger
(`__session_lock__`). A second live owner cannot start, a lapsed lease can be
reclaimed, and a stale owner's next `step` raises `SessionLockedError` before
appending anything (proved by the reliability `session_fence` suite). Off by
default — Actae never owns the agent loop, and a bare `POST /events/record`
remains unfenced. Python SDK only today.

### 15.4 StateManager

The smallest framework-neutral adapter: `save`/`load`/`resume(default)`/
`fork(new_channel)`/`list_versions`/`get_version`/`delete_version` —
"fork at the current point, refine from here" without the full AgentSession
lifecycle.

### 15.5 Transactional state store (`state.Store[T]`, Go)

A typed read-modify-write store on top of a single channel: `Commit` runs
the classic optimistic-concurrency loop — load latest snapshot →
`Mutate(current *T) → Command` → `Transition` (event + resulting snapshot
in one server-side transaction, guarded by `expected_version`/
`expected_cursor`). `VersionConflictError` reloads and re-applies `Mutate`
(max 5 attempts); transport failures retry the exact same transition with a
**deterministic operation id** (`actae.DeterministicOperationKey(scope,
action, identity...)`, a stable UUIDv5) so retries and restarts resolve
server-side idempotently. The event stream and the state object can never
disagree, because both are written by one transaction. `Mutate` must be
pure w.r.t. its input and report the same `OperationKey` per logical
operation (idempotent re-application). `Load`/`LoadVersion` decode
snapshots back into `T` (unknown versions → 404).

---

## 16. Feature: framework adapters

| Adapter | SDK(s) | What it does |
|---|---|---|
| **LangGraph** | Python, TS | `ActaeCheckpointSaver` implements the full `BaseCheckpointSaver` protocol over Actae state (async-first, deferred writes, subgraph namespaces, typed serde, `delete_thread`); **`fork_thread()`** performs a native LangGraph fork — complete checkpoint context (message history included) into a new Actae channel, returns a config ready to resume the fork (optional `manifest`/`experiment_metadata` declare what the fork changes, powering the dashboard config diff) |
| **Claude Agent SDK** | Python, TS | `ActaeClaudeSessionStore` (full `SessionStore` protocol: append/load/fork/delete/list/subkeys/summaries, mtime monotonicity, restart durability) + `ActaeClaudeHook` (hook events → channel events); fork copies the persisted transcript |
| **OpenAI Agents SDK** | Python, TS | `ActaeTracingProcessor` (tracing-processor lifecycle: spans → channel events, versioned run snapshots, PII stripping, background sync delivery); `fork_trace`/`forkTrace` copies one run's snapshot to a new channel for structural diffing (the Agents SDK is stateless — no native resume) |
| **LangChain** | Python, TS | `ActaeContextSaver` (records LLM/tool/chain results, saves context periodically), `ChainResumer` (remaining-work detection + resume + `fork` with inherited `{"context": ...}`) |
| **CrewAI** | Python (no TS port) | `ActaeCrewStateHook` (crew/task progress after steps), `CrewAIResumer` (load state, `get_remaining_tasks`, resume, `fork` inheriting `completed_tasks`) |
| **Codex OTLP** | Python, TS | `CodexOTLPReceiver` — local HTTP receiver for Codex OTLP log exports, mirrors execution events into Actae channels + maintains a state snapshot per channel, no Codex process changes |
| **GitHub Copilot** | Go | `actae/copilot` — `Recorder` (every Copilot `SessionEvent` → `copilot.<type>` channel event, dedup by event id, `Backfill` from session history), `RecordingHooks` (hooks → `copilot.hook.<name>`), `Manager` (track by session id, `Fork` at an event, `ContinueSession` = fork + new Copilot session recording into the fork channel, snapshots). Full reference: `docs/GOLANG_SDK.md` |

Adapters live behind lazy imports / subpath exports — the core SDKs have
zero framework dependencies.

---

## 17. Feature: dashboard

Next.js app served by Actae (embedded in release builds, dev checkout or
`ACTAE_DASHBOARD_DIR`). Auth: **Actae portal account** — enrolled instances
log in with the account used at the portal (the portal's "Open dashboard"
button auto-logs in via a one-time `#sso=…` handoff), standalone instances
use local email/password. API keys authenticate API/SDK traffic, not the
dashboard. Pages:

| Page | Features |
|---|---|
| **Overview** | DB-backed stats, per-channel event counts (`channels.stats`), fork/channel table |
| **Flow** | interactive execution graph + timeline: channel selector, inherited events for forked channels (dimmed, "forked from" badge), live WS updates, replay-position controls (sparse-cursor aware), **Export JSON** / **Copy JSON** (filter-respecting channel record — active Type/Fields filters applied exactly like the canvas — as a valid-JSON doc for LLM investigation), layouts (horizontal/grid/compact), event-type + payload-field filters, event/state nodes, event log/timeline/metadata tabs, inspector (text/JSON payload), **ForkTree**, **MetadataEditor**, **StateDiffPanel** (structural diff: per-side divergence, shared ancestor, honest truncation), **DecisionTrailPanel** (lineage + immutable fork boundary + tool ledger), **ExecutionTimeline**, **CompareView** ("what changed?" — state/config/tools/output/metrics tabs), 15 s channel auto-refresh, preferences persistence (`prefs.flow`) |
| **Events** | oldest-first table, deep links from Flow, channel/type/field filters, live subscription (selected channel or all), dedup by `id`/`channel:cursor`, Live pill |
| **Experiments** | experiment groups + member diff |

Forks are **code-created** — the dashboard visualizes and compares; it is
not the execution authority that creates application forks.

---

## 18. Data model (logical tables)

Event schema migrations v1–v24; client schema migrations v1–v10.

| Table | Contents |
|---|---|
| `agent_events` | immutable event log: `uuid`, `global_cursor` (BIGSERIAL), `channel_id`, `event_type`, `payload` JSONB, actor/agent/user, `depends_on`, `operation_id`, per-channel `cursor`, `timestamp`, `hash`/`prev_hash` (chain) |
| `channel_cursors` | per-channel next-cursor watermark (tx-locked) |
| `channels` | fork registry: parent, fork boundary cursors, outcome, promoted, `closed_at` |
| `channel_states` | versioned cursor-aligned JSONB snapshots |
| `channel_metadata` | lineage, display_name/reason, experiment_metadata, manifest, reproducibility |
| `channel_steps` | server-owned `(channel, step) → cursor/event` index |
| `fork_operations` | immutable fork receipts + idempotency (request hash) |
| `event_hash_chain` | tamper-evident chain bookkeeping |
| `tool_executions` | claim tokens, `request_hash`, leases, results |
| `consumer_groups` / `consumer_group_members` / `consumer_group_offsets` | durable groups, leases, offsets |
| `scheduled_wakeups` | persisted timers + dispatch state |
| `experiments` / `experiment_members` | groups, members, rank/outcome |
| `event_archive` | retention archive (purged rows land here) |
| `usage_delivery_outbox` | outbox for usage metrics, incl. the non-quota `fork_operations` / `recovery_operations` activation meters |
| `server_checkpoints` | scheduler crash recovery (schema version, last cursor, task last-runs, clean-shutdown flag) |
| `user`, `session`, `api_keys`, `subscription_state`, `preferences` (client schema) | auth + control-plane lock state + per-identity opaque prefs (≤64 KB, `{"prefs": {...}}` envelope) |

---

## 19. API catalog (complete route table)

All routes below are on the main port; body cap 2 MB; `GET /ws` upgrades.
(The identical inventory with handler anchors lives in
`docs/SERVER_INTERNALS.md`.)

| Domain | Route | Purpose |
|---|---|---|
| Health | `GET /healthz`, `GET /readyz` | liveness / readiness probes (open) |
| Metrics | `GET /metrics`, `GET /metrics.json` | Prometheus text / structured JSON (API-key or session gated) |
| Events | `POST /api/v1/events/record` | persist one event |
| Events | `POST /api/v1/events/transition` | atomic event + state |
| Events | `GET /api/v1/events/replay/{channel_id}` | replay from cursor (limit/event_type) |
| Events | `POST /api/v1/events/query` | filtered cross-channel search |
| Events | `GET /api/v1/events/cursor/{channel_id}` | latest cursor |
| Events | `GET /api/v1/events/verify/{channel_id}` | hash-chain verification |
| Channels | `GET /api/v1/channels` | list channels + stats |
| Channels | `POST /api/v1/channels/fork` | fork at a cursor (idempotent) |
| Channels | `GET /api/v1/channels/diff` | structural state diff |
| Channels | `GET /api/v1/channels/compare` | combined "what changed?" doc |
| Channels | `GET /api/v1/channels/{id}/receipt` | immutable fork receipt |
| Channels | `GET/PUT /api/v1/channels/{id}/metadata` | read / upsert metadata |
| Channels | `GET /api/v1/channels/{id}/forks` | direct children |
| Channels | `GET /api/v1/channels/{id}/tree` | recursive execution tree |
| Channels | `GET /api/v1/channels/{id}/trail` | decision trail |
| Channels | `GET /api/v1/channels/{id}/steps` | highest indexed step (O(1) recovery probe) |
| Channels | `GET /api/v1/channels/{id}/steps/{n}` | resolve step n → cursor/event |
| Channels | `PATCH /api/v1/channels/{id}/outcome` | set outcome/score |
| Channels | `POST /api/v1/channels/{id}/promote` | promote fork (closes source) |
| Channels | `DELETE /api/v1/channels/{id}` | delete channel (recursive) |
| Channels | `POST /api/v1/channels/{id}/close` | stamp closed_at |
<!-- channels-list-stats -->
`GET /api/v1/channels` returns `{channels: string[], stats: {<id>: {event_count,
latest_cursor, last_event_at, display_name, parent_channel_id}}}`. The `stats`
object is additive (SDKs read only `channels`); `display_name` and
`parent_channel_id` come from a `channel_metadata` LEFT JOIN so clients get
names + fork lineage without a per-channel metadata request. `list_channels_handler`
→ `EventStore::list_channels_with_stats` (`src/events/store.rs`).
| State | `POST /api/v1/state/{channel_id}` | save version (guards) |
| State | `GET /api/v1/state/{channel_id}` | load latest |
| State | `GET /api/v1/state/{channel_id}/versions` | list versions |
| State | `GET /api/v1/state/{channel_id}/version/{version}` | load one version |
| State | `DELETE /api/v1/state/{channel_id}/version/{version}` | delete one version |
| Groups | `POST /api/v1/groups`, `GET /api/v1/groups` | create / list |
| Groups | `DELETE /api/v1/groups/{id}` | delete |
| Groups | `POST /api/v1/groups/{id}/join` | join / renew lease |
| Groups | `POST /api/v1/groups/{id}/work` | claim work batch |
| Groups | `POST /api/v1/groups/{id}/ack` | ack progress |
| Groups | `POST /api/v1/groups/{id}/heartbeat` | extend lease |
| Groups | `GET /api/v1/groups/{id}/offsets` | consumer offsets |
| Experiments | `POST /api/v1/experiments`, `GET /api/v1/experiments` | create / list |
| Experiments | `GET /api/v1/experiments/{id}` | detail |
| Experiments | `POST /api/v1/experiments/{id}/members` | add member |
| Experiments | `GET /api/v1/experiments/{id}/rank` | rank by outcome |
| Wake-ups | `POST /api/v1/scheduler/wakeups`, `GET /api/v1/scheduler/wakeups` | schedule / list |
| Wake-ups | `GET/DELETE /api/v1/scheduler/wakeups/{id}` | get / cancel |
| Executions | `POST /api/v1/executions/claim` | claim/replay/reclaim |
| Executions | `GET /api/v1/executions`, `GET /api/v1/executions/{id}` | list / get |
| Executions | `POST /api/v1/executions/{id}/complete` `/fail` `/heartbeat` `/cancel` | lifecycle |
| Executions | `DELETE /api/v1/executions/{id}` | delete record |
| Auth | `POST /api/v1/auth/signup` `/login` `/sso` `/setup` `/api-key-login` `/logout` `/logout-all` | portal SSO / local auth / first-run setup / programmatic key→JWT / revoke session(s) |
| Auth | `GET /api/v1/auth/config`, `GET /api/v1/auth/capabilities`, `GET /api/v1/auth/me`, `POST /api/v1/auth/local-session` | flags (`enable_signup`, `sso`, `setup_required`, `local_auto_login`) / permissions / identity (incl. `role`) / loopback auto-login |
| Users | `GET/POST /api/v1/users`, `PATCH/DELETE /api/v1/users/{id}`, `POST /api/v1/users/{id}/password` | admin user management (roles, reset) |
| Prefs | `GET/PUT /api/v1/user/preferences` | per-identity opaque prefs |
| Realtime | `GET /ws` | WebSocket protocol (see §10) |

Error mapping is centralized (`err_to_status`): `401` auth, `403` forbidden
(ENABLE_SIGNUP/anon rules), `409` for `channel_conflict` /
`idempotency_conflict` / `snapshot_boundary_required` / `version_conflict` /
`fork_tool_blocked` / `counterfactual_blocked`,
`404` not found, `429` rate limit, `402` subscription lock, `503` grace
exhausted.

---

## 20. Configuration (key env vars)

Full parsing reference: `actae/src/config/mod.rs` + `docs/SERVER_INTERNALS.md`.

| Group | Vars (default) |
|---|---|
| Network | `ACTAE_HOST` (loopback w/o TLS, 0.0.0.0 with), `ACTAE_PORT` (8002), `ENABLE_TLS`/`TLS_CERT_PATH`/`TLS_KEY_PATH`, `ENABLE_MTLS`/`CA_CERT_PATH` (false — opt-in), `ENABLE_COMPRESSION` (true, level 6) |
| DB | `DATABASE_URL`/`POSTGRES_*` (external always wins), `ACTAE_PG_BIN_DIR`, `ACTAE_PG_NO_HOST`, `ACTAE_EMBEDDED_PG_PORT` (5546), `ACTAE_EMBEDDED_PG_PASSWORD`/`WIPE`, `ACTAE_AUTO_BUMP` (1), `ACTAE_DATA_DIR` (~/.actae/data) |
| Auth | `ACTAE_INITIAL_API_KEY`, `ACTAE_INITIAL_ADMIN_EMAIL`/`PASSWORD` (standalone first-boot dashboard admin), `API_KEY_HASH_SECRET` (defaults to `JWT_SECRET`), `API_KEY_HEADER` (`X-API-Key`), `JWT_SECRET`/`EXPIRATION` (86400)/issuer/audience, `ENABLE_SIGNUP` (false), `ENABLE_ANONYMOUS` (false), `ACTAE_IP_AUTH_RATE` (5) / `ACTAE_IP_AUTH_CAPACITY` (20), `ACTAE_WRITE_CLIENT_CREDENTIALS` (1 — publish SDK `client.json`; `0` disables), `ACTAE_LOCAL_AUTO_LOGIN` (1 — loopback dashboard auto-login; `0` requires a sign-in) |
| WS | `WS_MAX_CONNECTIONS` (1000), `WS_CONNECTION_TIMEOUT` (30), `WS_PING_INTERVAL` (30), `WS_MAX_MESSAGE_SIZE` (1 MB), `MESSAGE_BUFFER_SIZE` (10000) |
| Retention | `EVENT_RETENTION_DAYS` (30), `EVENT_ARCHIVE_RETENTION_DAYS` (0), `STATE_RETENTION_DAYS` (0), `TOOL_EXECUTION_RETENTION_DAYS` (0), `RETENTION_CHECK_INTERVAL_SECS` (3600) |
| Quotas | `EVENT_MAX_EVENTS_PER_MONTH` (0), `EVENT_MAX_INGRESS_BYTES_PER_MONTH` (0), `EVENT_MAX_DURABLE_WRITTEN_BYTES_PER_MONTH` (0), `EVENT_MAX_RETAINED_BYTES` (0) |
| Control plane | `ACTAE_CONTROL_PLANE_URL` (https://saas.diviga.dev), `ACTAE_INSTALL_ID`, `ACTAE_ENROLL_SECRET`/`KEY`, `ACTAE_SYNC_INTERVAL_SECS` (60), `ACTAE_SUBSCRIPTION_GRACE_SECS` (259200), `ACTAE_CA_BUNDLE`, `ACTAE_CONFIG_DIR` (~/.actae), `ACTAE_LICENSE_PUBKEY` (build-time) |
| Observability | `LOG_LEVEL` / `LOG_FORMAT` (build-dependent: release = `compact` + quiet startup gate, debug = verbose `json`), `RUST_LOG`, `ACTAE_ACCESS_LOG_ALL` (false), `ACTAE_NO_BROWSER` |

---

## 21. Semantics that matter (cross-cutting)

- **Idempotency everywhere**: `operation_id` on record/transition/publish/
  fork/execution claim; retries return the original result, mismatches are
  typed `409`s.
- **Ordering**: per-channel cursors are the ordering boundary, assigned
  under a row lock — concurrent publishers never interleave within a
  channel. A separate global cursor exists for cross-channel ordering.
- **Durability first**: events persist before broadcast; ack = persisted.
- **Strict-by-default forks**: no silent fall-forward — `exact` mode
  refuses to inherit a later checkpoint (`NoRestorableCheckpointError`).
- **Honest diffs**: truncation is reported, never silent.
- **int64 fidelity**: big integers survive the server round-trip as exact
  values in all SDKs (see 15.1).
- **No server echo** on WS publish (client-side `echoSelf` only).
- **Payloads are opaque JSON** — no LLM-specific schemas imposed; the same
  substrate handles agent events, business events, IoT signals.

---

## 22. Testing & evidence

Integration suite `tests/` (run via `./tests/run_tests.sh [--embedded|--all
|--smoke|--ci]`; opt-in: `ACTAE_LIFECYCLE=1` embedded lifecycle suite,
`ACTAE_CONFORMANCE=1` control-plane conformance suite): core event store,
replay/query advanced, transitions + optimistic concurrency, state
snapshots, fork boundaries, consumer groups, wake-ups, tool executions,
fan-out/broadcast, WS lifecycle + advanced protocol, transport parity,
auth/JWT/security tiers, input sanitization, purge/retention, resilience,
concurrency fence, CLI keys, preferences, SDK full coverage.

Runnable proof in `examples/`:

- `fork_resume_demo.py` — flagship fork+resume with a **measured
  token-savings report** + fidelity check vs a no-fork control (`--dry-run`
  needs no API key);
- `fork_fidelity_suite.py` / `fork_fidelity_langgraph.py` — LLM-judged
  "fork output ≡ no-fork re-run" evidence (DeepSeek judge, 5 scenarios,
  statistical repeats, JSON evidence reports) — `docs/FORK_FIDELITY.md`;
- `coordination/demo.py` — 6-phase PASS/FAIL demo of fan-out, consumer
  groups, crash recovery, fleet takeover, wake-ups, live==durable-log
  (`--go-worker` proves cross-SDK parity);
- `langgraph-comparison/`, `agent-support-system/` — framework comparison
  and support-agent simulations.

---

## 23. Where to go next

| Topic | Doc |
|---|---|
| Server internals (routes/WS wire/config/anchors) | `docs/SERVER_INTERNALS.md` |
| Product narrative & positioning | `docs/PRODUCT_CONTEXT.md` |
| Competitive playbook (Temporal objection handling) | `docs/TEMPORAL_COMPETITIVE.md` |
| Fork/transition/guards runtime semantics | `docs/FORK_RUNTIME.md` |
| Fork-resume workflow + token savings | `docs/FORK_RESUME.md` |
| LLM-judged fork fidelity evidence | `docs/FORK_FIDELITY.md` |
| Experiments/outcome/promotion | `docs/EXPERIMENTS.md` |
| Consumer groups & coordination | `docs/COORDINATION.md` |
| Tool-execution ledger | `docs/TOOL_EXECUTIONS.md` |
| Hosted storage accounting & quotas | `docs/STORAGE_ACCOUNTING.md` |
| State system | `docs/STATE_SYSTEM.md` |
| AgentSession lifecycle | `docs/AGENT_SESSION.md` |
| Adapters | `docs/ADAPTERS.md` |
| SDKs | `sdks/python/README.md`, `sdks/python/SDK_REFERENCE.md`, `docs/GOLANG_SDK.md`, `docs/TYPESCRIPT_SDK.md` |
| Dashboard | `docs/DASHBOARD_FORKS.md`, `docs/DASHBOARD_PAGES.md`, `docs/DASHBOARD_WORKFLOW.md` |
| Control plane | `docs/CONTROL_PLANE.md`, `docs/SAAS_BACKEND.md` |
| Deployment | `docs/PRODUCTION.md`, `AGENTS.md` |
