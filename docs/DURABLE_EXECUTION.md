# Durable Execution Layer

Actae is not merely an event store — it is a durable execution layer for AI
agents (selling point 2). Every piece of the pipeline below is already
shipped and individually tested; this document packages them into one
coherent contract.

## The contract

```text
receive work
  → claim operation            (idempotent tool executions, durable consumer groups)
  → transition event + state   (atomic record_transition, per-channel gapless cursor)
  → schedule retry / wakeup    (persisted wake-ups, consumer leases)
  → recover after crash        (leases, offsets, wake-ups survive restarts)
  → replay evidence            (cursor-based replay, decision trails, fork receipts)
```

## Building blocks

| Layer | Mechanism | Tests |
|-------|-----------|-------|
| Atomic writes | `record`/`record_transition` persist the event and its state snapshot in one Postgres transaction; per-channel gapless cursors via `channel_cursors` | `event_store_test.rs`, `test_fork_resume_*` |
| Idempotency | `operation_id` on record/publish/transition/fork replays the original result | `test_publish_idempotent_*`, parity suite |
| Tool executions | `tool_executions` ledger — claim/complete/fail/heartbeat/cancel, claims are owned and idempotent | `test_tool_executions.py` |
| Durable work distribution | consumer groups — lease-based claims, persisted per-consumer offsets, crash recovery | `test_groups*`, `examples/coordination/demo.py` |
| Scheduling | persisted wake-ups fired by the `wakeup_dispatcher` | `test_wakeups*` |
| Recovery | consumer leases expire and become reclaimable; offsets resume from last ack; WS reconnect replays from the last seen cursor | `test_reconnect_live.py` |
| Evidence | replay from any cursor, decision trails, immutable fork receipts (`GET /channels/{id}/receipt`) | fork/experiment suites |

## Lifecycle anchor

A first-class channel registry (created/closed/archive timestamps, owner,
parent, retention policy, integrity status, last cursor) is the intended
lifecycle anchor that ties these tables together, so deleting one artifact
can never leave a partial run behind. Until that lands, channel existence is
inferred from the union of the event, cursor, state, metadata, and fork
tables (see `AUDIT_REMEDIATION_PLAN.md` Phase 5.2).

## Operational notes

- Retention is provenance-aware: events referenced by `depends_on`, fork
  boundaries, or fork receipts are never purged; purged rows are archived to
  `event_archive` in the same transaction.
- The `channels` registry, a tamper-evident event hash chain, and
  capability-aware SDKs are the next layer on top of this contract
  (see `docs/AUTHORIZATION.md` and `AUDIT_REMEDIATION_PLAN.md` Phase 6).
