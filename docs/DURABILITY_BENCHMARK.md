# Durability Benchmark — crash consistency of external side effects

A reproducible benchmark that measures the question durable-execution claims
live or die on:

> A worker performs a side effect, then the process dies in the window between
> the effect landing on an external system and the worker recording that it
> finished. On restart, how many times does the effect fire again?

Harness: `examples/durability_benchmark.py`
Committed reports: `examples/durability-benchmark-results.json` (full matrix),
`examples/durability-benchmark-sweep-results.json` (crash at every step)
Unit tests: `sdks/example-tests/python/test_durability_benchmark.py` (no server)

## Why it uses real crashes

The worker is a separate OS process that dies with `os._exit(137)` at an
injected point, so in-memory progress is genuinely lost. The external system is
durable and independent of the worker's memory. A simulated exception would not
prove anything — the whole problem is what survives process death.

## External services

Two interchangeable services model the external system (both run by default;
`--services` selects a subset):

- `sqlite` — a local SQLite database.
- `http` — a real local HTTP service (`HttpEffectService`, stdlib
  `http.server`) with `POST /effects` (idempotent on `downstream_key` when
  `honors_key`), `GET /effects/{key}` (status query), `GET /effects` (all rows)
  and `POST /reset`. It has two one-shot **ambiguous-timeout** modes for a
  chosen key: `drop_after_commit` (the server commits, then drops the
  connection — the effect landed but the caller cannot know) and
  `drop_before_commit` (the connection is dropped *without* applying — the
  reconciling "not applied" fork). It can also inject **`slow_seconds`**
  (commit, then reply after the client's timeout — a real in-flight timeout)
  and **`status_lag_seconds`** (an eventually-consistent status query that
  hides a just-applied effect for a window).

## Design

- **6 steps**, each with one external effect, identified by a logical key
  `step-<n>`.
- **Crash points**:
  - `after_effect` — the effect landed; completion was not recorded (the
    ambiguous window).
  - `after_complete` — completion was recorded; the process died before the
    next step.
  - `transport_drop` (HTTP only) — the service committed, then dropped the
    response; the worker gets a transport error and does not know whether the
    effect landed.
  - `transport_fail` (HTTP only) — the service dropped the response *without*
    committing; the effect did not land, but the caller still cannot know. This
    is the "not applied" fork a reconciling client must handle (and the
    fork where a naive "assume it failed" retry is correct while a naive
    "assume it succeeded" retry loses the effect).
- **Crash step**: `--crash-at N` (repeatable, default 3) or `--crash-at-all`
  (every internal step `0..STEPS-1`).
- **Recovery strategies**:
  - `naive` — no durable progress; a restart re-runs the whole pipeline.
  - `checkpoint` — writes a progress marker *after* each step; a crash before
    the marker re-runs that step.
  - `durable` — claims each tool through Actae's idempotent execution ledger
    with a stable per-step key, then completes it. A restart replays completed
    steps and re-claims the crashed one.
  - `durable_query` — like `durable`, but on an ambiguous outcome (a
    `reclaimed` claim or a lost response) it **queries** the downstream status
    endpoint and reconciles instead of blindly re-applying.
- **Downstream behaviours**:
  - `non_idempotent` — the external system applies the effect on every call
    (most third-party APIs).
  - `honors_key` — the external system deduplicates on the caller-supplied key.
    Crucially, **only the ledger strategies have a stable operation key to
    supply**; `naive`/`checkpoint` have no durable identity, so each retry
    presents a fresh key and the downstream cannot dedupe.

Recovery runs are retried until the pipeline completes (bounded by
`MAX_ATTEMPTS`). For the ledger strategies a crashed owner leaves a live lease
(`lease_seconds = 2`); the runner sleeps past the lease so the next claim
returns `reclaimed` rather than `in_progress`.

The benchmark does **not** compare Actae to other durable-execution products —
that would require running each product. It compares recovery strategies on the
same effect protocol.

## Committed results

Run against a managed-storage Actae on 2026-09-21. Duplicate effects = total
applications minus distinct logical steps; lost effects = steps that never
applied. Both services, all crash points/downstreams, crash at step 3.

| Service | Strategy | Crash point | Downstream | Duplicates | Lost | Applications |
|---------|----------|-------------|------------|-----------:|-----:|-------------:|
| sqlite | naive | after_effect | non_idempotent | 4 | 0 | 10 |
| sqlite | naive | after_effect | honors_key | 4 | 0 | 10 |
| sqlite | naive | after_complete | non_idempotent | 4 | 0 | 10 |
| sqlite | naive | after_complete | honors_key | 4 | 0 | 10 |
| sqlite | checkpoint | after_effect | non_idempotent | 1 | 0 | 7 |
| sqlite | checkpoint | after_effect | honors_key | 1 | 0 | 7 |
| sqlite | checkpoint | after_complete | non_idempotent | 0 | 0 | 6 |
| sqlite | checkpoint | after_complete | honors_key | 0 | 0 | 6 |
| sqlite | durable | after_effect | non_idempotent | 1 | 0 | 7 |
| sqlite | durable | after_effect | honors_key | 0 | 0 | 6 |
| sqlite | durable | after_complete | non_idempotent | 0 | 0 | 6 |
| sqlite | durable | after_complete | honors_key | 0 | 0 | 6 |
| sqlite | durable_query | after_effect | non_idempotent | 0 | 0 | 6 |
| sqlite | durable_query | after_effect | honors_key | 0 | 0 | 6 |
| sqlite | durable_query | after_complete | non_idempotent | 0 | 0 | 6 |
| sqlite | durable_query | after_complete | honors_key | 0 | 0 | 6 |
| http | naive | after_effect | non_idempotent | 4 | 0 | 10 |
| http | naive | after_effect | honors_key | 4 | 0 | 10 |
| http | naive | after_complete | non_idempotent | 4 | 0 | 10 |
| http | naive | after_complete | honors_key | 4 | 0 | 10 |
| http | naive | transport_drop | non_idempotent | 4 | 0 | 10 |
| http | naive | transport_drop | honors_key | 4 | 0 | 10 |
| http | naive | transport_fail | non_idempotent | 3 | 0 | 9 |
| http | naive | transport_fail | honors_key | 3 | 0 | 9 |
| http | checkpoint | after_effect | non_idempotent | 1 | 0 | 7 |
| http | checkpoint | after_effect | honors_key | 1 | 0 | 7 |
| http | checkpoint | after_complete | non_idempotent | 0 | 0 | 6 |
| http | checkpoint | after_complete | honors_key | 0 | 0 | 6 |
| http | checkpoint | transport_drop | non_idempotent | 1 | 0 | 7 |
| http | checkpoint | transport_drop | honors_key | 1 | 0 | 7 |
| http | checkpoint | transport_fail | non_idempotent | 0 | 0 | 6 |
| http | checkpoint | transport_fail | honors_key | 0 | 0 | 6 |
| http | durable | after_effect | non_idempotent | 1 | 0 | 7 |
| http | durable | after_effect | honors_key | 0 | 0 | 6 |
| http | durable | after_complete | non_idempotent | 0 | 0 | 6 |
| http | durable | after_complete | honors_key | 0 | 0 | 6 |
| http | durable | transport_drop | non_idempotent | 1 | 0 | 7 |
| http | durable | transport_drop | honors_key | 0 | 0 | 6 |
| http | durable | transport_fail | non_idempotent | 0 | 0 | 6 |
| http | durable | transport_fail | honors_key | 0 | 0 | 6 |
| http | durable_query | after_effect | non_idempotent | 0 | 0 | 6 |
| http | durable_query | after_effect | honors_key | 0 | 0 | 6 |
| http | durable_query | after_complete | non_idempotent | 0 | 0 | 6 |
| http | durable_query | after_complete | honors_key | 0 | 0 | 6 |
| http | durable_query | transport_drop | non_idempotent | 0 | 0 | 6 |
| http | durable_query | transport_drop | honors_key | 0 | 0 | 6 |
| http | durable_query | transport_fail | non_idempotent | 0 | 0 | 6 |
| http | durable_query | transport_fail | honors_key | 0 | 0 | 6 |

### Crash at every step (sweep)

`examples/durability-benchmark-sweep-results.json` injects the ambiguous window
(`after_effect`, and HTTP `transport_drop` / `transport_fail`) at **every**
internal step 0..5, against a `non_idempotent` downstream, for `durable` and
`durable_query`:

| Crash step | durable (after_effect) | durable_query (after_effect) | durable (transport_drop) | durable_query (transport_drop) | durable (transport_fail) | durable_query (transport_fail) |
|-----------:|-----------------------:|-----------------------------:|-------------------------:|-------------------------------:|-------------------------:|-------------------------------:|
| 0 | 1 dup | 0 dup | 1 dup | 0 dup | 0 dup | 0 dup |
| 1 | 1 dup | 0 dup | 1 dup | 0 dup | 0 dup | 0 dup |
| 2 | 1 dup | 0 dup | 1 dup | 0 dup | 0 dup | 0 dup |
| 3 | 1 dup | 0 dup | 1 dup | 0 dup | 0 dup | 0 dup |
| 4 | 1 dup | 0 dup | 1 dup | 0 dup | 0 dup | 0 dup |
| 5 | 1 dup | 0 dup | 1 dup | 0 dup | 0 dup | 0 dup |

(`sqlite` after_effect rows are identical; the `transport_*` rows are HTTP-only;
`honors_key` is omitted because the stable key already dedupes it. No lost
effects anywhere. `transport_fail` is 0 duplicates even for blind `durable`
because the effect never landed — re-applying is correct; it is the mirror of
`transport_drop`, where blind `durable` duplicates because the effect did land.)

### How to read it

- **A durable ledger alone does not close the ambiguous window** against a
  non-idempotent downstream: `durable | after_effect | non_idempotent` shows 1
  duplicate, and so does `durable | transport_drop | non_idempotent`, because
  Actae cannot know an effect landed that was never recorded. Nothing can,
  without a downstream check.
- **The ledger's real contribution is the stable operation key.** When the
  downstream honours it, `durable | after_effect | honors_key` drops to 0,
  while `checkpoint | after_effect | honors_key` stays at 1 — because the
  checkpoint strategy has no durable identity to put in the key.
- **`durable_query` closes the window even against a non-idempotent
  downstream**, at every crash step and through both ambiguous transport
  forks (`transport_drop` and `transport_fail`), because it asks the
  downstream what happened instead of guessing.
- **The ambiguity is two-sided and both sides are tested.** `transport_drop`
  (effect landed, response lost) is where blind retries duplicate;
  `transport_fail` (effect never landed, response lost) is where a naive
  "assume it succeeded" policy loses work. No strategy lost work in either
  fork, and `durable_query` resolved both without duplicates.
- **No strategy lost work.** Every cell's lost count is 0; the failure mode
  here is duplication, not loss. Recovery position is a separate concern
  covered by the fork/fidelity suites.
- `naive` is worst in every column: a restart re-runs every already-applied
  step. This is the cost of "just retry the job."

## Reproduce

```bash
# Start Actae (embedded), then:
ACTAE_URL=http://127.0.0.1:8010 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
  python3 examples/durability_benchmark.py --url http://127.0.0.1:8010 \
  --api-key sk-dev-0000000000000000000000 --services sqlite http \
  --out examples/durability-benchmark-results.json

# Crash at every step (durable vs durable_query, both ambiguous branches):
python3 examples/durability_benchmark.py --url http://127.0.0.1:8010 \
  --api-key sk-dev-0000000000000000000000 --services sqlite http \
  --strategies durable durable_query \
  --crash-points after_effect transport_drop transport_fail \
  --downstreams non_idempotent --crash-at-all \
  --out examples/durability-benchmark-sweep-results.json

# Machinery only (no server round-trips for the ledger; still real kills + a
# real SQLite/HTTP service):
python3 examples/durability_benchmark.py --strategies naive checkpoint --json
```

Unit tests for the measurement arithmetic and the HTTP service (no server):

```bash
python3 -m pytest sdks/example-tests/python/test_durability_benchmark.py -v
```

## Limitations (honest)

- One crash per run, 6 steps, one process model. It isolates the ambiguous
  window; it is not a throughput or latency benchmark.
- The SQLite service models a durable, non-transactional third party. The HTTP
  service adds real network semantics (a committed-then-dropped response and a
  dropped-without-commit response), but not partial writes, arbitrary
  in-flight retries, or multi-region latency.
- For the ledger strategies, the result depends on lease timing: a recovery
  that arrives before the lease lapses returns `in_progress` and must wait;
  that wait is modeled by the runner's retry loop, not measured as latency.
- `honors_key` presumes the downstream supports idempotency keys, and
  `durable_query` presumes a status/query endpoint. Where the downstream
  supports neither, `durable | after_effect | non_idempotent` is the honest
  ceiling and reconciliation is the caller's job.
- **`durable_query` presumes a strongly-consistent status query.** The
  `status_lag_seconds` service mode models an eventually-consistent downstream:
  a just-applied effect is not visible to the status query for a window, so an
  immediate reconcile sees "not applied" and re-applies. The unit test
  `test_http_service_eventual_consistency_hides_a_just_applied_effect` pins
  this — it is the precise condition under which even `durable_query`
  duplicates, and why the query endpoint's consistency matters.
- The `slow_seconds` mode commits then replies after the client's timeout; the
  unit test `test_http_service_slow_response_commits_then_times_out` pins that
  the caller sees a transport error for an effect that did land.
