# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""MVCC Concurrency Fence: validates txid column and gapless replay under concurrent writes.

Covers: CC-01, CC-02 from the test strategy.

The txid XID8 column with DEFAULT pg_current_xact_id() ensures a late-joining
subscriber never sees out-of-order cursor gaps under concurrent multi-agent writes.
"""

import json
import time
import threading
import random


def _concurrent_write(actae, channel_id, worker_id, results):
    """Worker that records events in a tight loop."""
    try:
        payload = {"worker": worker_id, "data": random.randint(0, 9999)}
        result = actae.record(
            channel_id,
            "chaos.action",
            payload,
            f"worker-{worker_id}",
        )
        results.append(result["event"]["cursor"])
    except Exception as exc:
        results.append(exc)


class TestConcurrentWrites:
    """CC-01: Parallel writes produce gapless cursor sequence."""

    WORKER_COUNT = 15

    def test_gapless_cursor_under_concurrent_writes(self, actae, ch_name):
        results = []
        threads = [
            threading.Thread(target=_concurrent_write, args=(actae, ch_name, i, results))
            for i in range(self.WORKER_COUNT)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        errors = [r for r in results if isinstance(r, Exception)]
        assert len(errors) == 0, f"{len(errors)} workers failed: {errors[:3]}"

        cursors = sorted(r for r in results if isinstance(r, int))
        assert len(cursors) == self.WORKER_COUNT, (
            f"Expected {self.WORKER_COUNT} cursors, got {len(cursors)}"
        )

        for i in range(1, len(cursors)):
            assert cursors[i] > cursors[i - 1], (
                f"Gap at index {i}: cursor[{i - 1}]={cursors[i - 1]}, "
                f"cursor[{i}]={cursors[i]}"
            )

        events = actae.replay(ch_name, cursor=0, limit=100)
        replayed_cursors = [e["cursor"] for e in events]
        assert len(replayed_cursors) == self.WORKER_COUNT
        for i in range(1, len(replayed_cursors)):
            assert replayed_cursors[i] > replayed_cursors[i - 1], (
                f"Replay gap at {i}: {replayed_cursors}"
            )

    def test_gapless_with_batch_concurrent(self, actae, ch_name):
        """Multiple batches of concurrent writes to stress MVCC."""
        all_cursors = []
        for batch in range(3):
            results = []
            threads = [
                threading.Thread(
                    target=_concurrent_write,
                    args=(actae, ch_name, f"{batch}-{i}", results),
                )
                for i in range(10)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            cursors = sorted(r for r in results if isinstance(r, int))
            all_cursors.extend(cursors)

        all_cursors.sort()
        for i in range(1, len(all_cursors)):
            assert all_cursors[i] > all_cursors[i - 1], (
                f"Gap at global index {i}: {all_cursors}"
            )


class TestTxidColumn:
    """CC-02: txid assigned on insert."""

    def test_txid_is_not_null(self, actae, pg_cursor, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        pg_cursor.execute(
            "SELECT txid FROM agent_events WHERE channel_id = %s",
            (ch_name,),
        )
        rows = pg_cursor.fetchall()
        assert len(rows) == 1
        txid = rows[0][0]
        assert txid is not None, "txid is NULL — MVCC fence not working"

    def test_txid_is_increasing(self, actae, pg_cursor, ch_name):
        actae.record(ch_name, "test.event", {"seq": 1}, "bot")
        actae.record(ch_name, "test.event", {"seq": 2}, "bot")
        pg_cursor.execute(
            "SELECT txid FROM agent_events WHERE channel_id = %s ORDER BY cursor ASC",
            (ch_name,),
        )
        txids = [row[0] for row in pg_cursor.fetchall()]
        assert len(txids) == 2
        assert txids[1] >= txids[0], (
            f"txid decreased: {txids[0]} -> {txids[1]}"
        )
