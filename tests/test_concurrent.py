# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Concurrent and simultaneous operation tests.

Tests multiple operations happening at the same time from different
clients/connections to verify thread safety and correctness.
"""

import concurrent.futures
import threading
import time
import pytest



class TestConcurrentRecords:
    """Multiple threads recording events simultaneously."""

    WORKER_COUNT = 20

    def test_concurrent_records_all_succeed(self, actae, ch_name):
        results = []
        errors = []
        lock = threading.Lock()

        def record_event(idx):
            try:
                resp = actae.raw_post("/api/v1/events/record", {
                    "channel_id": ch_name, "event_type": "conc.test",
                    "payload": {"worker": idx}, "metadata": {"actor": f"worker-{idx}"},
                })
                with lock:
                    results.append((idx, resp.status_code))
            except Exception as e:
                with lock:
                    errors.append((idx, str(e)))

        threads = [threading.Thread(target=record_event, args=(i,)) for i in range(self.WORKER_COUNT)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"{len(errors)} workers failed: {errors[:3]}"
        success = sum(1 for _, status in results if status == 201)
        assert success == self.WORKER_COUNT, f"Only {success}/{self.WORKER_COUNT} succeeded"


class TestConcurrentQueries:
    """Multiple concurrent queries to the event store."""

    def test_concurrent_replay(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        actae.record(ch_name, "test.event", {}, "bot")
        actae.record(ch_name, "test.event", {}, "bot")

        def do_replay():
            return actae.replay(ch_name, cursor=0, limit=10)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(do_replay) for _ in range(10)]
            for f in concurrent.futures.as_completed(futures):
                events = f.result()
                assert len(events) >= 3


class TestConcurrentMixed:
    """Mixed record, replay, query, cursor operations."""

    def test_read_during_write(self, actae, ch_name):
        results = []
        lock = threading.Lock()

        def writer():
            for i in range(10):
                actae.record(ch_name, "test.event", {"seq": i}, "writer")
                time.sleep(0.01)

        def reader():
            for _ in range(5):
                events = actae.replay(ch_name, cursor=0, limit=100)
                with lock:
                    results.append(len(events))
                time.sleep(0.02)

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)
        w.start()
        r.start()
        w.join()
        r.join()

        assert len(results) >= 1

    def test_cursor_during_record(self, actae, ch_name):
        cursors = []
        lock = threading.Lock()

        def writer():
            for i in range(10):
                actae.record(ch_name, "test.event", {"seq": i}, "writer")
                time.sleep(0.005)

        def cursor_reader():
            for _ in range(5):
                c = actae.cursor(ch_name)
                if c is not None:
                    with lock:
                        cursors.append(c)
                time.sleep(0.01)

        w = threading.Thread(target=writer)
        r = threading.Thread(target=cursor_reader)
        w.start()
        r.start()
        w.join()
        r.join()

        assert len(cursors) >= 1
        assert all(c > 0 for c in cursors)
