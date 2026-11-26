# Actae Coordination Layer

Durable coordination primitives for agent fleets: consumer groups (durable,
lease-based work distribution), persisted wake-ups (durable timers), and the
multi-subscriber real-time backbone (cursor replay + live WebSocket
subscriptions). Everything lives in one binary + one Postgres — no Kafka, no
NATS, no cron.

This document covers the guarantees, the API surface, how they are proven (the
pytest suites and the runnable demo), and the known semantics/limitations that
any integrator must understand.

---

## 1. The real-time backbone (multi-subscriber fan-out)

Any number of WebSocket subscribers can watch a channel live while late
joiners catch up from a cursor:

- `subscribe(channel, cursor=N)` replays events after `N`, then transitions
  to the live stream (see the SDK `subscribe()` — `wait=True` blocks until the
  subscription ack).
- Broadcasts are delivered per-subscriber, in cursor order, at-least-once
  (DeliveryTracker + Postgres DLQ for undeliverable events).
- **Guarantee:** while a subscriber stays connected it sees the identical,
  gapless, ordered stream as the durable log. Subscribers never receive a
  broadcast for events that predate their subscription (unless they ask with
  `cursor`).

**Proven by:** `tests/test_reconnect_live.py` (no dup/gap across a reconnect,
cursor-replay exclusivity) and phases 1 + 6 of `examples/coordination/demo.py`
(every watcher reproduces the full channel log exactly).

---

## 2. Durable consumer groups

A group is bound to one channel. Consumers join with a lease, claim batches
of events, ack progress. Offsets and leases are persisted in Postgres
(`consumer_groups`, `group_consumers`, `group_offsets`).

### API

| Route | SDK (Python / Go) | Purpose |
|---|---|---|
| `POST /api/v1/groups` | `create_group` | create (upsert) a group |
| `POST /api/v1/groups/{g}/join` | `join_group` | register/renew a consumer lease + offset row |
| `POST /api/v1/groups/{g}/work` | `claim_work` | claim a batch (at-least-once, lease-based) |
| `POST /api/v1/groups/{g}/ack` | `ack_work` | commit progress (advances `last_cursor`) |
| `POST /api/v1/groups/{g}/heartbeat` | `heartbeat` | extend the lease |
| `GET /api/v1/groups/{g}/offsets` | `group_offsets` | per-consumer offsets |
| `DELETE /api/v1/groups/{g}` | `delete_group` | delete + cascade |

### Semantics

- **Claims are disjoint while leases are active.** The claim start is
  `max(own committed, max claimed over consumers with ACTIVE leases, own
  claimed)`; batches are reserved with `FOR UPDATE SKIP LOCKED` inside a group
  row lock, so two live consumers never receive the same event.
- **Offsets advance only on `ack`.** A crash mid-batch leaves the unacked
  range reclaimable after the lease lapses.
- **Lease reaper.** The `consumer_lease_reaper` task (every 30s) marks lapsed
  leases `expired`; their claims become reclaimable. An expired consumer must
  `join` again.
- **At-least-once.** A crashed consumer's unacked work is redelivered. The
  application must be idempotent for side effects — Actae's own
  tool-executions ledger provides exactly that (see §4).

### Proven by

`tests/test_consumer_groups.py` — lease expiry redelivery, heartbeat,
parallel-claim stress (5 consumers × 50 events, no double delivery),
idempotent fork under concurrency. Plus phases 2–4 of the demo.

---

## 3. Crash-recovery semantics (read this before integrating)

Offsets are **per consumer id**, not group-global. Concretely:

- **A consumer re-joining with the same id resumes from its last *acked*
  offset** (`last_cursor`). Since commit `0b1a91d1`/this change, `join`
  also drops stale claims (`claimed_cursor` is reset to `last_cursor`) — a
  crashed-and-restarted worker reclaims exactly its unacked work instead of
  skipping it. (Regression test: `test_rejoin_resumes_from_last_acked_offset`.)
- **A fresh consumer resumes from the group's active claim watermark.** If at
  least one other consumer holds a lease, the fresh consumer starts after the
  max claimed by active consumers. If the entire fleet has left (all leases
  lapsed), the watermark is 0 and the fresh consumer **re-reads the channel
  from the start**.

> **Why the reset matters.** Without it, `join` preserved the stale
> `claimed_cursor`, so a restarted worker's claim started at its old claim
> point and *silently skipped its own unacked work* (data loss). The demo's
> phase 3 caught this in testing; it is fixed and covered by a regression
> test.

> **Known limitation.** There is no durable *group-level* committed watermark,
> so a brand-new consumer after a total fleet collapse re-reads from cursor 0.
> This is safe — the ledger replays completed receipts instead of re-running
> side effects (phase 4 of the demo proves exactly-once *processing* under
> exactly this condition: every completed result is replayed, never re-executed)
> — but it is broader than a Kafka-style group offset. If you need
> fresh consumers to resume at the last acked position after a full fleet
> restart, a group-level watermark is the natural follow-up.

**Work channel hygiene.** A group claims *every* event on its channel, and
`ack` advances a watermark, so **non-work events must not share the work
channel**: a worker that acks past another consumer's in-flight (crashed)
range can skip that range permanently. Put work on a clean channel and route
telemetry/audit (e.g. the ledger's `tool.*` receipts) to a separate channel.
The demo's two-channel layout is the reference pattern.

---

## 4. Idempotent tool executions (the coordinated-retry half)

`tool_executions` is a durable ledger keyed by `(channel_id, key_name)`.
`claim_execution` is create-or-replay: a completed execution with a matching
request hash returns the persisted result with status `replayed` and emits a
one-time `tool.replayed` marker — the tool is **not** re-run. Claims carry a
lease and a `claim_token`; only the owner may `complete`/`fail`/`heartbeat`.
`tool.started` / `tool.completed` / `tool.failed` / `tool.replayed` events are
emitted on the channel passed to `claim_execution`.

**The division of labour:** consumer groups are the at-least-once delivery
layer; the ledger is the at-most-once *result-replay* layer. Together they give
exactly-once processing when the group redelivers (crash, lease lapse,
or a full fleet reset that re-reads everything) — **provided the external
side effect is idempotent or queryable**. The ledger closes the coordination
window, not the gap between an external call and the commit that records it:
if the external operation succeeds but the worker dies before Actae records
completion, the lease expires and a later worker may re-run the operation.
Actae coordinates the retry, fences stale owners, and replays recorded
results; it cannot by itself guarantee that a non-idempotent downstream
operation never executes twice.

Full doc: `docs/TOOL_EXECUTIONS.md`.

---

## 5. Persisted wake-ups (durable timers)

`POST /api/v1/scheduler/wakeups` schedules an event; the `wakeup_dispatcher`
task (every 5s) fires a `scheduler.wakeup` event on the channel at/after
`run_at`, marks the wake-up `done`, and broadcasts it live (subscribers) and
replayably (everyone). Wake-ups survive server restarts. Cancelling a pending
wake-up returns `cancelled` and it never fires.

**Proven by:** `tests/test_wakeups.py` (dispatch, gapless cursors, live WS
delivery, cancel-before-fire, restart survival via Postgres) and demo phase 5.

---

## 6. Running the demo

```bash
cd actae && ACTAE_IP_AUTH_RATE=5000 ACTAE_IP_AUTH_CAPACITY=10000 cargo run
cd examples/coordination && python demo.py            # or: python demo.py --go-worker
```

See `examples/coordination/README.md` for the phase-by-phase table. The demo
exits non-zero if any guarantee fails, so it is a cheap CI smoke test for the
coordination layer (needs a live Actae).

### Quick manual verification

```bash
# A worker dies mid-batch; its ledger-completed invoice is replayed, not re-run
python demo.py --crash-events 3 --keep        # grep SIDE_EFFECT ran=false in the logs
```
