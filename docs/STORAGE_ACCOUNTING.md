# Storage Accounting and Quota Model

**Status:** implemented (PR3A). Documents how Actae meters durable storage for
hosted plans and how retention lifecycles interact with those meters.

## Why this exists

The commercial model meters two things: **monthly durable data written** and
**current retained data**. These are distinct because retention and state
versioning make monthly writes diverge from disk usage:

- Every `save_state` creates a new immutable version.
- Forks copy inherited state into the child channel (Actae performs the copy
  internally, not the customer).
- Completed tool-execution results are stored for replay.

Without an explicit retained-data ceiling, state versions and tool results can
accumulate indefinitely despite advertised event retention — an unbounded
storage path. This model closes that.

## The two meters

### 0. `fork_operations` and `recovery_operations` (activation meters — never quotas)

Two separate, non-quota usage meters: one row per successful fork (incl.
`resume(fork_at_step=)`) and one row per same-channel crash recovery — either a
session-metadata `crashed`/`stepping` → `started` transition, **or** a recorded
`run.recovered` marker event. Both are written to the same
`usage_delivery_outbox` in the respective operation's transaction, and
back the free-tier **differentiated-value** funnel stage
(`activation_metrics` `reached_differentiated_value` /
`fork_operations` / `recovery_operations`) plus the local `actae_forks_total`
and `actae_recoveries_total` counters (`forks.total` / `recoveries.total` in
`GET /metrics.json`). Delivered only from a linked instance, and neither has a
plan limit (`PLAN_LIMITS` maps no cap for them, so no 80/90/100% warnings).
See `docs/MEASUREMENT.md`.

### 1. `durable_written_bytes` (monthly flow meter)

Incremented whenever **customer-originated** durable payload bytes enter Actae:

- event payload + metadata;
- state snapshots (`transition` and `save_state`);
- tool-execution params (on claim);
- tool-execution result / structured error (on complete/fail).

Enforced against `EVENT_MAX_DURABLE_WRITTEN_BYTES_PER_MONTH` (0 = unlimited).
**Writes stop when this cap OR the retained cap is reached — whichever comes
first** (the same atomic write aborts in both cases).

### 2. `retained_bytes` (current gauge)

The logical customer payload bytes stored **right now**:

- retained event payload + metadata;
- retained state JSON (all versions, including fork-generated copies);
- retained tool params/result/error bodies.

Enforced against `EVENT_MAX_RETAINED_BYTES` (0 = unlimited). **This is
deliberately NOT PostgreSQL physical disk usage** — indexes, TOAST, bloat,
WAL, and dead tuples are excluded. The physical DB/WAL/backup footprint is
monitored separately for COGS capacity planning.

**Fork-generated state copies count toward retained data but NOT toward the
monthly durable-written meter** — the customer did not upload those bytes;
Actae performed the copy. `channel_states.write_source` distinguishes
`'customer'` from `'fork'` writes.

## Enforcement

Quotas are enforced atomically by PostgreSQL triggers in the same transaction
as the write:

- `trg_enforce_monthly_event_quota` — events (also feeds `durable_written`).
- `trg_enforce_state_storage_quota` — `channel_states` inserts; fork copies
  count retained only, customer writes count both.
- `trg_enforce_tool_storage_quota_{insert,update}` — `tool_executions`
  params/result/error; a reclaim that clears bodies releases retained bytes
  (the monthly meter never decreases).
- `trg_release_retained_{events,states,tools}` — DELETE triggers that release
  retained bytes when retention or channel deletion removes rows, so the
  gauge never drifts.

The shared `account_durable_write(p_usage_date, p_durable_bytes,
p_retained_delta)` function raises `ACTAE_QUOTA_EXCEEDED` (→ HTTP
`429 quota_exceeded` / `EventStoreError::QuotaExceeded`) when a cap is
exceeded, aborting the whole write transaction (event + state + outbox +
counter all roll back together).

**Serialization (migration v31).** Every accounting path updates the shared
`usage_quota_monthly` row and/or the `usage_storage_state` singleton, so
concurrent writers contend on those rows. Without a consistent lock order two
writes could deadlock (`deadlock detected`, SQLSTATE `40P01`) and surface as
HTTP `500`. Each accounting trigger now takes
`pg_advisory_xact_lock(hashtextextended('actae_usage_accounting', 0))` **first**,
so the whole accounting section runs under one transaction-scoped lock with a
single lock order and concurrent writers queue instead of deadlocking. The
singleton update already serialized these writes, so throughput is unchanged.

Control-plane usage reporting flows through the `usage_delivery_outbox`:
events and state writes emit `durable_written_bytes` rows; tool params/results
emit `tool_params_bytes` / `tool_result_bytes` / `tool_error_bytes`; the
`retained_bytes_snapshot` scheduler emits the current retained gauge hourly as
a point-in-time value (the control plane overwrites, not sums, it).

## Production PostgreSQL upsert (SaaS `record_usage`)

The SaaS backend records usage on **PostgreSQL in production** (`usage_daily`
daily aggregate upsert in `saas/backend/src/actae_saas/hosted_state.py`).
PostgreSQL treats bare target-column references in `ON CONFLICT DO UPDATE`
expressions as **ambiguous** when the `VALUES` list uses the same column name
(`quantity = quantity + excluded.quantity` fails with
`AmbiguousColumn: column reference "quantity" is ambiguous`). SQLite tolerates
this, so unit tests against SQLite never caught it — every production usage
batch 500'd, the runtime's outbox delivery retried forever, and the customer
portal stayed at 0%. **All `usage_daily` upsert expressions MUST qualify the
target table**: `quantity = usage_daily.quantity + excluded.quantity`,
`source_generation = CASE WHEN usage_daily.source_generation > ... END`.

Guards: `saas/backend/tests/test_postgres.py::test_usage_ingest_upsert_on_postgres`
(Postgres-only, gated on `SAAS_TEST_POSTGRES_URL`) exercises the ingest
endpoint + aggregates + gauge overwrite + idempotency; the opt-in live E2E
(`ACTAE_LIVE=1 tests/test_live_e2e.py`) runs a real Actae binary against a
PostgreSQL-backed SaaS backend and asserts the event reaches `usage_daily`
and the runtime outbox drains; the hosted Docker E2E
(`tests/test_hosted_docker_e2e.py`) asserts the customer portal surfaces
`events_ingested`/`ingress_bytes`/`durable_written_bytes` for a spawned
tenant.

## Retention lifecycles

| Resource | Policy | Behavior |
|---|---|---|
| Events | `EVENT_RETENTION_DAYS` | Age-based purge, provenance anchors preserved |
| State snapshots | `STATE_RETENTION_DAYS` | Age-based purge of old versions; latest snapshot, fork-boundary-referenced snapshots, and provenance anchors are always preserved |
| Tool executions | `TOOL_EXECUTION_RETENTION_DAYS` | **Tombstone** (not delete): null params/result/error, set `expired_at`; the key identity survives so a retried key fails loudly with `result_expired` instead of silently re-executing |
| Consumer offsets | never age-purged | Live coordination state; removed only by group/channel deletion or lifecycle cleanup of a vanished owning resource |

A zero value for any retention window preserves that resource indefinitely
(the default, preserving historical self-hosted behavior). Hosted plans set
explicit windows.

## Tool-execution tombstone (`result_expired`)

When a completed/failed execution crosses the tool-execution retention window,
the bodies are purged but a tiny tombstone remains (key, tool, hash, status,
timestamps). A later claim on the same idempotency key returns HTTP
`409 result_expired` (`ExecutionResultExpired`) rather than `replayed` and
rather than silently executing again. The caller must resolve the prior
outcome downstream before retrying with a fresh key. The idempotency window is
therefore explicit, not accidental.

## Env vars

| Var | Default | Meaning |
|---|---|---|
| `EVENT_MAX_DURABLE_WRITTEN_BYTES_PER_MONTH` | 0 | Monthly durable-written cap (0 = unlimited) |
| `EVENT_MAX_RETAINED_BYTES` | 0 | Current retained-logical-payload cap (0 = unlimited) |
| `STATE_RETENTION_DAYS` | 0 | State-version purge horizon (0 = keep all) |
| `TOOL_EXECUTION_RETENTION_DAYS` | 0 | Tool-body tombstone horizon (0 = keep bodies) |
| `EVENT_MAX_INGRESS_BYTES_PER_MONTH` | 0 | **Legacy alias** — still read and enforced as a separate monthly payload+metadata cap (0 = unlimited). Pre-dates durable-written; kept for back-compat and still a live hard cap alongside the durable-written one. |

`EVENT_MAX_INGRESS_BYTES_PER_MONTH` is the former name for the monthly write
meter; both it and `EVENT_MAX_DURABLE_WRITTEN_BYTES_PER_MONTH` are enforced by
the DB triggers (they are independent caps, and a write is rejected when
either is exceeded). Prefer the durable-written name for new deployments and
customer-facing docs.

## Tests

- `durable_written_and_retained_quotas_enforce_atomically`
- `retained_data_cap_blocks_state_writes_when_full`
- `transition_quota_rejects_atomically_both_event_and_state`
- `tool_claim_and_complete_count_against_durable_written_quota`
- `fork_state_copy_counts_toward_retained_but_not_durable_written`
- `tombstone_expired_executions_nulls_bodies_and_rejects_retry`