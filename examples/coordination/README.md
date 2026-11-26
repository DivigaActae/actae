# Actae Coordination Demo

A production-grade, end-to-end demonstration of Actae's coordination layer:
durable consumer groups, lease-based crash recovery, the idempotent
tool-executions ledger, persisted wake-ups, and multi-subscriber real-time
fan-out — all asserted automatically, with a PASS/FAIL summary and a non-zero
exit code on any failure.

```
producer (demo.py) ──record──▶  work channel  ──broadcast──▶ K watchers (WS)
                                   │                           (fan-out proof)
wakeup ──schedule──▶               │
                                   ▼
consumer group "grp-…" ──claim/ack──▶ N workers (Python + optional Go)
   ▲                                 │ process via idempotent ledger
   └── durable per-consumer offsets ─┘     │
       lease reaper ── redelivers orphans ─▶ tool.* receipts → audit channel
```

Two channels, two jobs:

* **work channel** — the group's queue. Carries *only* `invoice.created`
  events (plus one wake-up in phase 5). Keeping non-work events off it is what
  makes claim/ack/redelivery semantics exact.
* **audit channel** — the ledger's replayable trail: `worker.started` and one
  `tool.*` receipt per side effect. This is the coordinated-retry proof: a
  completed result is replayed, never re-executed, across every redelivery in
  the demo's controlled crash scenario.

## What it proves (phases)

| Phase | Guarantee | Assertion |
|---|---|---|
| 0 · Setup | — | fresh channels + consumer group + watchers connected |
| 1 · Real-time backbone | Multi-subscriber fan-out | every watcher receives the identical, ordered, gapless stream live |
| 2 · Consumer groups | Parallel processing, no double delivery | all invoices completed exactly once; durable offsets advance; no worker double-processed an invoice |
| 3 · Crash recovery | Restart resumes from last acked offset; ledger replays | a worker dies mid-batch; the same consumer id re-joins and reclaims only its unacked work; the ledger-completed invoice is replayed, not re-run |
| 4 · Fleet takeover | Lease reaper redelivers to a fresh worker; system-wide exactly-once processing | a second worker dies; fresh workers reclaim the orphaned batch after the reaper frees it; every invoice in the whole demo has exactly one completed side effect (completed results replay, never re-execute) |
| 5 · Persisted wake-ups | Schedule, fire, live delivery, cancel | due wake-up fires to live subscribers and is replayable; a future one stays pending, then cancels, and never fires |
| 6 · Final proof | Live stream == durable log | every watcher reproduced the full gapless channel log exactly |

The headline number: across every delivery, two crashes, and a total fleet
reset, **no completed side effect is ever re-executed** — the ledger replays
recorded results instead, and the demo's deterministic crash points prove it.
(Exactly-once external effects in general additionally require an idempotent
or queryable downstream operation; this demo's side effect is deterministic
and its crash points occur after the ledger commit.)

## Quick start (dev mode)

```bash
# 1. Start Actae (dev mode, high auth limits so the demo isn't rate-limited)
cd actae
ACTAE_IP_AUTH_RATE=5000 ACTAE_IP_AUTH_CAPACITY=10000 cargo run -- --dev

# 2. In another terminal:
pip install -e ./sdks/python
cd examples/coordination
python demo.py                          # default: 3 workers, 20 events, 3 watchers

# Cross-SDK: worker 0 is a Go-SDK worker, proving two SDKs share one group
python demo.py --go-worker              # builds examples/coordination worker.go
```

Docker (TLS) works too — point the env vars at the stack:

```bash
ACTAE_URL=https://localhost:8002 ACTAE_WS_URL=wss://localhost:8002 \
ACTAE_API_KEY=test-api-key-for-integration-tests python demo.py
```

## Options

```
--workers N            worker count per fleet        (default 3)
--watchers K           WebSocket watcher count       (default 3)
--events M             invoices in phase 1/2         (default 20)
--crash-events N2      invoices in the crash phase   (default 5)
--post-crash-events P  invoices in the takeover phase(default 3)
--go-worker            spawn worker 0 from the Go SDK (cross-SDK proof)
--wait-timeout S       poll timeout for phases       (default 45)
--keep                 keep temp artifacts (worker/watcher logs)
```

## Expected pacing

* Phases 0–3: ~20s (no reaper wait — the crashed consumer resumes its own work).
* Phase 4: ~30–40s (the lease reaper runs on a real 30s cadence; the banner says so).
* Phase 5–6: ~15s. Total ~1.5–2 min.

## What the pieces are

* `demo.py` — orchestrator: spawns subprocesses, drives all phases, asserts
  every guarantee from the durable log, prints the summary, exit code 0/1.
* `worker.py` — a durable worker: `join → claim → process via ledger → ack`,
  with a heartbeat thread and a deterministic `--crash` mode (ledger-complete
  invoice #2, die before acking it).
* `worker.go` — the same worker via the Go SDK (drain mode), proving
  heterogeneous SDKs coordinate on one group.
* `watcher.py` — a WebSocket subscriber that records every broadcast; the
  fan-out and live-stream==log proofs are built on its records.

## How to read the logs

Every subprocess emits `KEY<TAB>JSON` lines to its `--out` file (kept with
`--keep`). Key lines: `READY` / `JOINED` / `CLAIMED` / `SIDE_EFFECT`
(`status=claimed|replayed`, `ran=true|false`) / `ACKED` / `DRAINED` / `CRASH`.
A `SIDE_EFFECT` with `ran=false,status=replayed` is the "no completed side
effect is re-executed" proof — the ledger returned the recorded result
instead of re-running it.

## Design notes

* **Clean work channel.** The consumer group claims *every* event on its
  channel, so the work channel must carry only work. Side-effect receipts go
  to the audit channel. (In an early version the receipts shared the work
  channel and a survivor's "ack past noise" skipped a crashed worker's
  in-flight range permanently.)
* **Per-consumer offsets.** Offsets are per consumer id, not group-global.
  A re-joining consumer resumes from its last *acked* offset; a fresh
  consumer resumes from the active claim watermark (after a total fleet
  collapse that is the group start — the ledger replays completed receipts,
  so re-reading is always safe). See `docs/COORDINATION.md`.
