# Reliability Evidence

Actae makes falsifiable claims about runtime behavior under failure — atomic
transitions, crash recovery, side-effect replay, fork boundaries, and consumer
coordination. This document describes the harness that **tests those claims and
publishes the raw result**, so a buyer can verify them instead of taking them on
faith.

- Harness: `examples/reliability/` (`runner.py`, `scenarios.py`, `worker.py`, `report.py`)
- Committed reports: `examples/reliability-reports/` (`latest.json`, `latest.md`, per-run copies)
- Unit tests (no server): `sdks/example-tests/python/test_reliability_harness.py`

## What it runs

| Suite | What it proves | Evidence |
|---|---|---|
| **crash** | A worker is killed with `os._exit(137)` *after* an external side effect is coordinated through the ledger. The recorded steps survive; a resume replays the recorded result instead of re-applying the effect. | Real subprocess death (`worker.py`), a file-backed external system (one line per application), server event history, ledger replay status |
| **fork** | A fork does not silently re-fire an inherited effect (`auto` replays), refuses a new irreversible call (`block`, persisted `tool.blocked` boundary), and stores an `intervention` descriptor the application applies. | Live fork + `claim_execution` status, event replay, fork metadata |
| **coordination** | Two live consumers never double-claim an active range; acked work is not redelivered; a lapsed lease's unacked work is redelivered. | Live consumer-group API (`claim_work`/`ack_work`/`join_group`), offset counts |
| **soak** (`soak.py`) | Correctness holds under concurrency and recovery stays cheap as a channel grows: an N-way concurrent `auto` fork-replay storm resolves every claim to `replayed` with zero errors; M sequential steps record at a measured rate; `latest_step_number` + resume latency stays flat. | `actae.soak.v1` JSON with status counts, throughput and p50/p95/p99 |
| **stale_worker** | A stale owner's `complete` is fenced by `claim_token` (`execution_not_owned`); an active heartbeat holds the lease; and — the honest boundary — bare channel writes are **not** fenced (any authenticated actor may write), so side effects must be owned through the ledger. | Two live claims on one key, lease lapse + reclaim, heartbeat vs reclaim, actor-attributed channel writes |
| **atomicity** | A `transition` (event + state snapshot in one transaction) commits atomically across a real kill, and re-sending the same `operation_id` replays the original instead of writing a second event + snapshot; a reused id with different content is `409 idempotency_conflict`. | Real kill mid-transition (`atomicity_worker.py`), event count, snapshot cursor == event cursor, idempotent replay, conflict |
| **long_tool** | A tool that outlives its lease **without heartbeating** is reclaimed by a second worker, the stale owner's completion is fenced, and the non-idempotent downstream fires twice (the documented cost); **with heartbeating** the lease is not stolen and the effect fires once. | Concurrent claim/reclaim/heartbeat against the ledger with a live lease, effect count |
| **session_fence** | `AgentSession(fence_owner=...)` claims a per-channel write lease: a second live owner cannot start, a lapsed lease can be reclaimed, and the stale owner's next `step` raises `SessionLockedError` with **no** event appended. | Live sessions on one channel, lease lapse + takeover, rejected step, event count unchanged |

The suites run against a real `actae` instance over HTTP. Nothing is
mocked or simulated except the external "system" in the crash suite (a local
file), which is deliberate: it is an independent, durable observer of how many
times the effect fired.

## Running it

```bash
cd actae && cargo run          # Actae on :8002
cd .. && ACTAE_URL=http://localhost:8002 \
  ACTAE_API_KEY=sk-dev-0000000000000000000000 \
  python3 examples/reliability/runner.py      # writes examples/reliability-reports/
```

`--suites crash,fork,coordination,stale_worker,atomicity,long_tool,session_fence`
runs a subset (the default is all seven); `--out DIR` changes the output
directory; `--run-id` names the report. The process exits non-zero if any check
fails, so it doubles as a smoke test.

Soak/load (same server):

```bash
ACTAE_URL=http://127.0.0.1:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
  python3 examples/reliability/soak.py --concurrency 8 --iterations 200 --steps 500
```

CI runs the evidence-harness unit tests, the full runner (all seven suites), the
durability benchmark (SQLite + HTTP), the fork showcase, the repo-agent demo
and a bounded soak in the `reliability` job (`.github/workflows/ci.yml`),
uploading the reports.

## Report schema (`actae.reliability.v1`)

```json
{
  "schema": "actae.reliability.v1",
  "run_id": "reliability-dacb084a",
  "generated_at": "2026-09-21T07:33:51+00:00",
  "actae_url": "http://localhost:8002",
  "server_version": "0.1.0",
  "suites": [
    {"name": "crash", "checks": [{"name": "...", "ok": true, "detail": ""}],
     "passed": 6, "total": 6, "ok": true, "error": null}
  ],
  "passed": 16, "total": 16, "verdict": "PASS"
}
```

A suite with zero checks is **not** treated as a pass, and a suite that raised
records the error and is reported as failed — a broken harness can never render
green.

## Latest committed run

`examples/reliability-reports/latest.json` — 7 suites, 41/41 checks, verdict
`PASS` (Actae 0.1.0, embedded). Headline observations:

- A worker killed with exit 137 after coordinating a `charge` left the external
  system at exactly one application; re-claiming the same logical effect after
  resume returned `replayed` and did not fire again. The hard kill skipped the
  SDK's metadata write, but resume reconstructed the true step count (2) from
  the event log instead of resuming at step 1.
- A fork with default policies replayed an inherited `email.send` result
  (never started the tool on the child); `block` refused a new call and
  persisted a `tool.blocked` boundary; the intervention descriptor round-tripped.
- Two live consumers in one group produced disjoint claims (A=5, B=0), acked
  work was not redelivered, and a lapsed lease redelivered the full unacked batch.
- A stale owner's `complete` was rejected with `execution_not_owned` after the
  lease was reclaimed; a heartbeated lease was not stolen. A bare channel write
  from a second actor was accepted — the documented un-fenced boundary.
- A killed `transition` worker left exactly one event and a snapshot whose
  cursor matched; re-sending the same `operation_id` replayed the original
  (event count unchanged), and a different payload under that id was
  `409 idempotency_conflict`.
- A tool that outlived its lease without heartbeating was reclaimed and fired
  the non-idempotent downstream twice (the documented lease cost); with
  heartbeating it was not reclaimed and fired once.
- A fenced `AgentSession` refused a second live owner, allowed a new owner to
  reclaim a lapsed lease, and rejected the stale owner's next step
  (`SessionLockedError`) with **no** event appended.

## Honest limitations

- This is **not** a throughput, latency, or scale benchmark. It measures
  correctness of failure semantics on a single embedded instance.
- The crash suite's external system is a local file, not a real third-party
  API. The guarantee demonstrated is: *Actae replays a recorded result and
  fences stale owners.* Whether a side effect can be re-executed safely still
  depends on the downstream being idempotent or queryable (see
  `docs/TOOL_EXECUTIONS.md`). Actae closes the coordination window, not the gap
  between an external call and the commit that records it.
- **Channel-level ownership fencing is opt-in.** `AgentSession(fence_owner=...)`
  claims a per-channel lease through the execution ledger (the `session_fence`
  suite proves it), but a bare `POST /events/record` from any authenticated
  actor is still accepted by design — Actae never owns the agent loop. Use the
  fence (or per-effect `claim_execution`) when two workers must never write the
  same channel.
- A tool that outlives its lease without heartbeating **will** be reclaimed and
  can fire a non-idempotent downstream twice (`long_tool` pins this). Heartbeat
  long tools, or keep them shorter than the lease.
- Consumer groups are evaluated on a clean work channel; mixing telemetry into
  a work channel changes claim semantics (see `docs/COORDINATION.md`).
- Reports are point-in-time. Re-run the harness on the hardware and version you
  care about rather than quoting the committed numbers as a general SLA.

## Server-kill chaos (`tests/test_server_crash_consistency.py`)

A separate opt-in suite SIGKILLs the **Actae process** (not the worker) mid-run
and proves committed state survives a hard restart: acknowledged events persist,
per-channel cursors stay gapless, the hash chain still verifies, an atomic
transition's event + snapshot remain consistent, and the restarted server
accepts new writes. It boots its own managed-storage instance:

```bash
ACTAE_SERVER_CRASH=1 python3 -m pytest tests/test_server_crash_consistency.py -v
```

This is the server-side counterpart to the worker-crash suites; the guarantees
rest on PostgreSQL transaction durability plus the embedded-mode orphan reclaim.

## Related evidence

- `docs/DURABILITY_BENCHMARK.md` — a deeper, statistics-oriented comparison of
  naive / checkpoint / durable / durable-query recovery across crash windows,
  crash steps and idempotent vs non-idempotent vs queryable downstreams, over
  both a SQLite and a real HTTP external service (real process kills).
- `docs/FORK_FIDELITY.md` — LLM-judged "fork output ≡ no-fork re-run" evidence.
- `docs/REPO_AGENT_DEMO.md` — the same guarantees on a real repo-maintenance
  agent (real tools, real crash, real fork).
- `docs/FORK_SHOWCASE.md` — runnable counterfactual fork demo + claims index.
- `docs/EXECUTION_INTEGRITY.md` — tamper-evident per-channel hash chain.
