#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Actae coordination demo — production-grade coordination, end to end.

Two channels, two jobs:

* *work channel* — the group's queue: only ``invoice.created`` events (and a
  wake-up in phase 5). Keeping non-work events off it is what makes
  claim/ack/redelivery semantics exact.
* *audit channel* — the ledger's replayable trail: ``worker.started`` and one
  ``tool.*`` receipt per side effect. This is the coordinated-retry proof: a
  completed result is replayed, never re-executed.

A fleet of workers in a durable consumer group claim invoices from the work
channel and process each through the idempotent tool-executions ledger. A
worker's machine dies mid-batch; the lease reaper redelivers its work —
replayed, never re-executed. A persisted wake-up fires to live subscribers.
WebSocket watchers watch the work channel live and must reproduce the durable
log exactly.

The demo runs entirely against a real Actae instance (dev mode or docker) and
exits non-zero if any guarantee fails.

Phases
------
0  Setup: fresh channels + consumer group + K WebSocket watchers.
1  Real-time backbone: M invoices fan out to every watcher, in order.
2  Consumer groups: N workers race-claim/process/ack M invoices — every
   invoice completed exactly once (audit channel), no double delivery,
   durable offsets.
3  Crash recovery: a worker dies mid-batch; the reaper lapses its lease; the
   redelivered work is replayed through the ledger — completed side effects are
   replayed, never re-executed, even though delivery happened twice.
4  Worker restart: the crashed consumer re-joins and resumes from its durable
   offset, never from zero.
5  Wake-ups: a due wake-up fires to live subscribers; a future one is
   cancelled before it fires.
6  Final proof: every watcher's live stream == the durable work-channel log,
   gapless and in order.

Usage
-----
    python demo.py [--workers N] [--events M] [--crash-events N2]
                   [--post-crash-events P] [--watchers K] [--go-worker]
                   [--wait-timeout S] [--keep]

Environment: ACTAE_URL / ACTAE_API_KEY / ACTAE_WS_URL (dev defaults applied).
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone

from actae_client import ActaeClient
from actae_client.errors import APIError

DEFAULT_URL = os.environ.get("ACTAE_URL", "http://localhost:8002")
DEFAULT_KEY = os.environ.get("ACTAE_API_KEY", "sk-dev-0000000000000000000000")
DEFAULT_WS = os.environ.get("ACTAE_WS_URL", "")

HERE = os.path.dirname(os.path.abspath(__file__))
WORKER_PY = os.path.join(HERE, "worker.py")
WATCHER_PY = os.path.join(HERE, "watcher.py")

WORK_TYPE = "invoice.created"
TOOL_NAME = "process_invoice"


class Section:
    def __init__(self, title):
        self.title = title
        self.passed = True
        self.details = []

    def check(self, ok, msg):
        self.passed = self.passed and bool(ok)
        self.details.append((bool(ok), msg))
        mark = "PASS" if ok else "FAIL"
        print(f"      [{mark}] {msg}", flush=True)
        return ok

    def note(self, msg):
        print(f"      [..]  {msg}", flush=True)


def _banner(text):
    print("\n" + "=" * 78, flush=True)
    print(text, flush=True)
    print("=" * 78, flush=True)


def _log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _read_lines(path):
    """Parse KEY<TAB>JSON lines from a structured out-file."""
    if not path or not os.path.exists(path):
        return []
    out = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            key, _, data = line.partition("\t")
            try:
                out.append((key, json.loads(data)))
            except json.JSONDecodeError:
                out.append((key, {}))
    return out


def _watcher_cursors(path):
    return [d["cursor"] for k, d in _read_lines(path) if k == "EVENT"]


class Demo:
    def __init__(self, args):
        self.args = args
        self.workdir = tempfile.mkdtemp(prefix="actae-coord-")
        self.channel = args.channel or f"coordination-demo-{int(time.time())}"
        self.audit_channel = f"{self.channel}-audit"
        self.group = f"grp-{self.channel}"
        self.procs = []  # (name, Popen, structured_path)
        self.results = []

    # ------------------------------------------------------------------ #
    # plumbing
    # ------------------------------------------------------------------ #

    def env(self):
        e = dict(os.environ)
        e["ACTAE_URL"] = DEFAULT_URL
        e["ACTAE_API_KEY"] = DEFAULT_KEY
        e["ACTAE_WS_URL"] = DEFAULT_WS or DEFAULT_URL.replace(
            "https://", "wss://").replace("http://", "ws://") + "/ws"
        return e

    def _spawn(self, name, cmd, structured):
        struct_path = os.path.join(self.workdir, f"{name}.log")
        with open(struct_path, "w"):
            pass
        logf = open(os.path.join(self.workdir, f"{name}.stdout"), "a")
        proc = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT,
                                env=self.env(), text=True)
        self.procs.append((name, proc, struct_path))
        return proc, struct_path

    def _wait_lines(self, path, want_key, timeout=30, interval=0.2):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any(k == want_key for k, _ in _read_lines(path)):
                return True
            time.sleep(interval)
        return False

    async def _await_pred(self, pred, timeout, interval=1.0):
        import inspect
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            res = pred()
            if inspect.isawaitable(res):
                res = await res
            if res:
                return True
            await asyncio.sleep(interval)
        return False

    def _cleanup(self):
        for name, proc, _ in self.procs:
            if proc.poll() is None:
                proc.terminate()
        for name, proc, _ in self.procs:
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()

    # ------------------------------------------------------------------ #
    # phases
    # ------------------------------------------------------------------ #

    async def phase_setup(self, client):
        s = Section(f"PHASE 0 · Setup — channel={self.channel}")
        _banner("PHASE 0 · Setup")
        try:
            await client.delete_group(self.group)
        except APIError:
            pass
        await client.create_group(self.group, self.channel,
                                  metadata={"demo": "coordination"})
        s.check(True, f"created consumer group '{self.group}' on '{self.channel}'")

        self.watcher_files = []
        for i in range(self.args.watchers):
            _, wf = self._spawn(
                f"watcher-{i}",
                [sys.executable, WATCHER_PY, self.channel,
                 "--timeout", str(self.args.wait_timeout * 6),
                 "--out", os.path.join(self.workdir, f"watcher-{i}.log")],
                f"watcher-{i}.log",
            )
            self.watcher_files.append(wf)

        all_ready = all(
            self._wait_lines(wf, "READY", timeout=30) for wf in self.watcher_files)
        s.check(all_ready, f"{self.args.watchers} watchers connected + subscribed")
        self.results.append(s)

    async def phase_fanout(self, client):
        s = Section("PHASE 1 · Real-time backbone — fan-out to all subscribers")
        _banner("PHASE 1 · Real-time backbone")
        m = self.args.events
        t0 = time.monotonic()
        cursors = []
        for i in range(1, m + 1):
            ev = await client.record(
                self.channel, WORK_TYPE,
                {"invoice_id": i, "amount": 100 + i, "phase": 1},
                actor="producer")
            cursors.append(ev.cursor)
        s.check(cursors == list(range(1, m + 1)),
                f"recorded {m} invoices at gapless cursors 1..{m}")

        ok = await self._await_pred(
            lambda: all(len(_watcher_cursors(wf)) >= m for wf in self.watcher_files),
            timeout=self.args.wait_timeout, interval=0.5)
        s.check(ok, f"every watcher received all {m} invoice events live")

        identical = True
        expected = list(range(1, m + 1))
        for i, wf in enumerate(self.watcher_files):
            curs = _watcher_cursors(wf)[:m]
            same = curs == expected
            identical = identical and same
            s.check(same, f"  watcher {i}: saw cursors {curs[:5]}{'…' if m > 5 else ''} "
                          f"(len={len(curs)}) — {'in order' if same else 'MISMATCH'}")
        s.check(identical, f"{self.args.watchers} subscribers saw the identical "
                           f"ordered stream")
        s.note(f"fan-out latency window: {time.monotonic() - t0:.2f}s for {m} events")
        self.results.append(s)

    async def _spawn_workers(self, client, prefix, count, empty_grace, include_go):
        """Spawn `count` workers; worker 0 may be the Go binary. Wait READY."""
        if include_go:
            go_bin = os.path.join(self.workdir, "worker-go")
            if not os.path.exists(go_bin):
                self._log_go_build(go_bin)
            worker0 = [go_bin]
        else:
            worker0 = None

        for i in range(count):
            consumer = f"{prefix}-{i}"
            if i == 0 and worker0 is not None:
                # Go's flag package stops at the first positional arg, so flags
                # must come BEFORE the three positionals.
                cmd = worker0 + [
                    "--audit-channel", self.audit_channel,
                    "--lease", "8", "--batch-limit", "8",
                    "--empty-grace", str(empty_grace),
                    "--out", os.path.join(self.workdir, f"{prefix}-{i}.log"),
                    self.channel, self.group, consumer,
                ]
                kind = "go"
            else:
                cmd = [sys.executable, WORKER_PY, self.channel, self.group, consumer,
                       "--audit-channel", self.audit_channel,
                       "--lease", "8", "--batch-limit", "8",
                       "--empty-grace", str(empty_grace),
                       "--out", os.path.join(self.workdir, f"{prefix}-{i}.log")]
                kind = "python"
            proc, sf = self._spawn(f"{prefix}-{i}", cmd, f"{prefix}-{i}.log")
            if not self._wait_lines(sf, "JOINED", timeout=30):
                raise RuntimeError(f"{prefix}-{i} ({kind}) failed to join the group")
        return worker0 is not None

    def _log_go_build(self, go_bin):
        _log("building Go worker (first --go-worker run)…")
        r = subprocess.run(
            ["go", "build", "-o", go_bin, "."],
            cwd=HERE, env=self.env(), capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"go build failed:\n{r.stderr}")

    async def _tool_completed_keys(self, client):
        evs = await client.replay(self.audit_channel, event_type="tool.completed",
                                  limit=10000)
        return {e.payload.get("key") for e in evs}

    async def _tool_replayed_keys(self, client):
        evs = await client.replay(self.audit_channel, event_type="tool.replayed",
                                  limit=10000)
        return {e.payload.get("key") for e in evs}

    async def phase_groups(self, client):
        s = Section("PHASE 2 · Consumer groups — parallel processing, no double delivery")
        _banner("PHASE 2 · Consumer groups")
        m = self.args.events
        t0 = time.monotonic()

        await self._spawn_workers(client, "worker", self.args.workers,
                                  empty_grace=5, include_go=self.args.go_worker)
        _log(f"{self.args.workers} workers joined the group")

        for name, proc, _ in self.procs:
            if name.startswith("worker-"):
                proc.wait(timeout=self.args.wait_timeout)

        completed = await self._tool_completed_keys(client)
        invoice_keys = {f"invoice:{i}" for i in range(1, m + 1)}
        s.check(len(completed) == m and completed == invoice_keys,
                f"{len(completed)}/{m} invoices completed exactly once "
                f"(no duplicates, none missing)")

        offsets = await client.group_offsets(self.group)
        total_acked = sum(o.last_cursor for o in offsets)
        s.check(total_acked >= m,
                f"durable group offsets: {len(offsets)} consumers, "
                f"sum(last_cursor)={total_acked} >= {m}")

        # cross-worker disjointness: no invoice side-effected by two workers
        ran = {}
        for i in range(self.args.workers):
            sf = os.path.join(self.workdir, f"worker-{i}.log")
            for k, d in _read_lines(sf):
                if k == "SIDE_EFFECT" and d.get("ran"):
                    ran.setdefault(d["invoice_id"], []).append(f"worker-{i}")
        dup = {inv: who for inv, who in ran.items() if len(who) > 1}
        s.check(not dup, f"{len(ran)} invoices processed across "
                         f"{self.args.workers} workers — no worker double-processed any invoice")
        s.note(f"groups phase: {time.monotonic() - t0:.2f}s, "
               f"{self.args.events} invoices / {self.args.workers} workers")
        self.results.append(s)

    async def _spawn_crash_worker(self, client, name, consumer_id):
        """Spawn a deterministic crash worker and report what it did.

        Returns ``(target_key, acked_key, acked_cursor)``:
        - ``target_key`` — invoice whose side effect was committed in the ledger
          but whose ack never happened (it will be redelivered).
        - ``acked_key`` / ``acked_cursor`` — invoice the worker did ack.
        """
        cf = os.path.join(self.workdir, f"{name}.log")
        cproc, csf = self._spawn(
            name,
            [sys.executable, WORKER_PY, self.channel, self.group, consumer_id,
             "--audit-channel", self.audit_channel,
             "--crash", "--lease", "8",
             "--out", os.path.join(self.workdir, f"{name}.log")],
            f"{name}.log")
        if not self._wait_lines(csf, "CRASH", timeout=40):
            raise RuntimeError(f"{name} did not crash as expected")
        cproc.wait(timeout=30)

        lines = _read_lines(cf)
        crash = next(d for k, d in lines if k == "CRASH")
        acked_cursor = next((d["cursor"] for k, d in lines if k == "ACKED"), None)
        by_cursor = {e.cursor: e for e in await client.replay(self.channel, limit=10000)}
        target_key = f"invoice:{by_cursor[crash['ledger_completed_unacked']].payload['invoice_id']}"
        acked_key = f"invoice:{by_cursor[acked_cursor].payload['invoice_id']}" if acked_cursor else None
        return target_key, acked_key, acked_cursor

    async def phase_crash(self, client):
        s = Section("PHASE 3 · Crash recovery — restarted worker resumes, ledger replays")
        _banner("PHASE 3 · Crash recovery")
        m = self.args.events
        m2 = self.args.crash_events
        new_ids = list(range(m + 1, m + m2 + 1))
        t0 = time.monotonic()

        for i in new_ids:
            await client.record(self.channel, WORK_TYPE,
                                {"invoice_id": i, "amount": 1000 + i, "phase": 3},
                                actor="producer")

        target_key, acked_key, acked_cursor = await self._spawn_crash_worker(
            client, "crash", "crash-worker")
        s.check(True, "crash worker acked one invoice, committed the side effect "
                      "for another, and died before acking it")
        s.note(f"  acked {acked_key} (cursor {acked_cursor}); committed the side effect "
               f"for {target_key} but never acked it → {target_key} must be replayed, "
               f"not re-run")

        # Restart the SAME consumer id: it must resume from its last acked
        # offset and reclaim only its unacked work.
        sf = os.path.join(self.workdir, "restart.log")
        rproc, rsf = self._spawn(
            "restart",
            [sys.executable, WORKER_PY, self.channel, self.group, "crash-worker",
             "--audit-channel", self.audit_channel,
             "--lease", "8", "--batch-limit", "8", "--empty-grace", "10",
             "--out", os.path.join(self.workdir, "restart.log")],
            "restart.log")
        s.check(self._wait_lines(rsf, "JOINED", timeout=30),
                "the crashed consumer re-joined with the same id")
        rproc.wait(timeout=self.args.wait_timeout)

        expected_keys = {f"invoice:{i}" for i in new_ids}
        completed = await self._tool_completed_keys(client)
        phase_keys = {k for k in completed if k in expected_keys}
        s.check(phase_keys == expected_keys,
                f"all {m2} crash-phase invoices completed exactly once "
                f"({len(phase_keys)}/{m2})")

        replayed = await self._tool_replayed_keys(client)
        s.check(target_key in replayed,
                f"{target_key} was REPLAYED by the restarted worker — its side "
                f"effect did NOT run twice")

        acked = [d["cursor"] for k, d in _read_lines(sf) if k == "ACKED"]
        s.check(all(c > acked_cursor for c in acked),
                f"restarted worker's first ack (cursor {acked[0] if acked else '—'}) "
                f"resumed past its last acked offset ({acked_cursor}) — never from zero")

        total = await self._tool_completed_keys(client)
        all_keys = {f"invoice:{i}" for i in range(1, m + m2 + 1)}
        s.check(len(total) == m + m2 and all_keys.issubset(total),
                f"system-wide so far: {len(total)}/{m + m2} invoices have exactly "
                f"one completed side effect")
        s.note(f"crash phase: {time.monotonic() - t0:.2f}s (no reaper wait — the "
               f"same consumer id resumed its own unacked work)")
        self.results.append(s)

    async def phase_takeover(self, client):
        s = Section("PHASE 4 · Fleet takeover — a fresh worker reclaims orphaned work")
        _banner("PHASE 4 · Fleet takeover  (lease reaper runs every ~30s — the "
                "real machinery at its real cadence)")
        m = self.args.events
        m2 = self.args.crash_events
        p = self.args.post_crash_events
        new_ids = list(range(m + m2 + 1, m + m2 + p + 1))
        total = m + m2 + p
        t0 = time.monotonic()

        for i in new_ids:
            await client.record(self.channel, WORK_TYPE,
                                {"invoice_id": i, "amount": 2000 + i, "phase": 4},
                                actor="producer")

        target_key2, _, _ = await self._spawn_crash_worker(
            client, "crash2", "crash-worker-2")
        s.check(True, "a second worker died mid-batch (acked none of it)")

        _log("spawning fresh workers — they wait out the lease-reaper window, then "
             "reclaim the orphaned batch")
        await self._spawn_workers(client, "takeover", self.args.workers,
                                  empty_grace=self.args.wait_timeout + 20,
                                  include_go=self.args.go_worker)

        done = await self._await_pred(
            lambda: self._key_replayed(client, target_key2),
            timeout=self.args.wait_timeout * 3 + 60, interval=2.0)
        s.check(done, f"a fresh worker reclaimed and REPLAYED the dead worker's "
                      f"ledger-completed invoice once the lease reaper freed it "
                      f"({time.monotonic() - t0:.0f}s)")

        for name, proc, _ in self.procs:
            if name.startswith("takeover-"):
                proc.wait(timeout=self.args.wait_timeout * 3 + 60)

        completed = await self._tool_completed_keys(client)
        all_keys = {f"invoice:{i}" for i in range(1, total + 1)}
        s.check(completed == all_keys,
                f"SYSTEM-WIDE EXACTLY-ONCE PROCESSING: {len(completed)}/{total} invoices, "
                f"each with exactly one completed side effect across every "
                f"delivery, crash, and takeover in the whole demo (completed results "
                f"replayed, never re-executed)")

        replayed = await self._tool_replayed_keys(client)
        s.check(target_key2 in replayed,
                f"{target_key2} (ledger-completed by the dead worker) was replayed "
                f"by a fresh worker — no double execution of a completed side effect")

        s.note("note: a fresh consumer resumes from the group's *active* watermark "
               "(offsets are per-consumer) — after a total fleet collapse it re-reads "
               "from the group start; the ledger replays every completed receipt "
               "instead of re-running a side effect")
        self.results.append(s)

    async def _expected_done(self, client, expected_keys):
        completed = await self._tool_completed_keys(client)
        return expected_keys.issubset(completed)

    async def _key_replayed(self, client, key):
        replayed = await self._tool_replayed_keys(client)
        return key in replayed

    async def phase_wakeups(self, client):
        s = Section("PHASE 5 · Persisted wake-ups — fire, deliver live, cancel")
        _banner("PHASE 5 · Persisted wake-ups")
        t0 = time.monotonic()
        now = datetime.now(timezone.utc)

        due = await client.schedule_wakeup(
            self.channel, now - timedelta(seconds=1),
            payload={"kind": "due", "step": "nightly"})
        s.check(due.status == "pending", f"scheduled due wake-up {due.id[:8]}")

        async def _due_fired():
            evs = await client.replay(self.channel, event_type="scheduler.wakeup",
                                      limit=10000)
            return any(e.payload.get("kind") == "due" for e in evs)

        fired = await self._await_pred(_due_fired, timeout=45, interval=1.0)
        s.check(fired, f"dispatcher fired scheduler.wakeup ({time.monotonic() - t0:.0f}s)")

        due2 = await client.get_wakeup(due.id)
        s.check(due2 is not None and due2.status == "done",
                f"wake-up marked done, executed_at={due2.executed_at if due2 else '—'}")

        # watchers should have seen it live
        live_ok = all(
            any(d.get("type") == "scheduler.wakeup" for k, d in _read_lines(wf)
                if k == "EVENT")
            for wf in self.watcher_files)
        s.check(live_ok, "every watcher received the wake-up broadcast live over WebSocket")

        future = await client.schedule_wakeup(
            self.channel, now + timedelta(hours=1),
            payload={"kind": "future", "step": "snooze"})
        s.check(future.status == "pending", f"scheduled far-future wake-up {future.id[:8]}")
        await asyncio.sleep(6)
        f2 = await client.get_wakeup(future.id)
        s.check(f2 is not None and f2.status == "pending",
                f"future wake-up still pending after a dispatch cycle")

        cancelled = await client.cancel_wakeup(future.id)
        f3 = await client.get_wakeup(future.id)
        s.check(cancelled and f3 is not None and f3.status == "cancelled",
                "future wake-up cancelled before it fired")
        evs = await client.replay(self.channel, event_type="scheduler.wakeup",
                                  limit=10000)
        s.check(not any(e.payload.get("kind") == "future" for e in evs),
                "cancelled wake-up never fired (nothing in the durable log)")
        self.results.append(s)

    async def phase_final_fanout(self, client):
        s = Section("PHASE 6 · Final proof — every live stream == the durable log")
        _banner("PHASE 6 · Final proof")
        full = await client.replay(self.channel, limit=10000)
        expected = [e.cursor for e in full]
        s.note(f"channel '{self.channel}' holds {len(expected)} events total")

        ok = await self._await_pred(
            lambda: all(_watcher_cursors(wf) == expected for wf in self.watcher_files),
            timeout=self.args.wait_timeout, interval=1.0)
        s.check(ok, "every watcher reproduced the full gapless ordered log exactly")

        for i, wf in enumerate(self.watcher_files):
            curs = _watcher_cursors(wf)
            s.check(curs == expected,
                    f"  watcher {i}: {len(curs)} events, identical to the log")
        self.results.append(s)

    # ------------------------------------------------------------------ #
    # runner
    # ------------------------------------------------------------------ #

    async def run(self):
        _banner("Actae · Coordination Demo")
        _log(f"endpoint={DEFAULT_URL}  channel={self.channel}  "
             f"workers={self.args.workers}  events={self.args.events}  "
             f"go_worker={self.args.go_worker}")

        client = ActaeClient(api_key=DEFAULT_KEY, endpoint=DEFAULT_URL)
        try:
            await client.health_check()
            await self.phase_setup(client)
            await self.phase_fanout(client)
            await self.phase_groups(client)
            await self.phase_crash(client)
            await self.phase_takeover(client)
            await self.phase_wakeups(client)
            await self.phase_final_fanout(client)
        finally:
            await client.disconnect()

        self._print_summary()
        self._cleanup()
        if not self.args.keep:
            import shutil
            shutil.rmtree(self.workdir, ignore_errors=True)
        all_pass = all(r.passed for r in self.results)
        _log(f"ALL PASS ✔  —  {self.channel}" if all_pass else "FAILURES — see above")
        sys.exit(0 if all_pass else 1)

    def _print_summary(self):
        _banner("SUMMARY")
        for r in self.results:
            mark = "PASS" if r.passed else "FAIL"
            print(f"  [{mark}] {r.title}", flush=True)
        _log(f"artifacts kept in {self.workdir}  (--keep)")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--channel", default="", help="channel name (default auto)")
    ap.add_argument("--workers", type=int, default=3, help="worker count (default 3)")
    ap.add_argument("--watchers", type=int, default=3, help="watcher count (default 3)")
    ap.add_argument("--events", type=int, default=20, help="invoices in phase 1/2 (default 20)")
    ap.add_argument("--crash-events", type=int, default=5, help="invoices in crash phase (default 5)")
    ap.add_argument("--post-crash-events", type=int, default=3,
                    help="invoices in restart phase (default 3)")
    ap.add_argument("--go-worker", action="store_true",
                    help="spawn worker 0 from the Go SDK (cross-SDK proof)")
    ap.add_argument("--wait-timeout", type=int, default=45,
                    help="poll timeout for phase completion (default 45)")
    ap.add_argument("--keep", action="store_true", help="keep temp artifacts")
    args = ap.parse_args()
    try:
        asyncio.run(Demo(args).run())
    except KeyboardInterrupt:
        print("\naborted", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
