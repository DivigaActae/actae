# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Real-world agent workload simulations.

Covers: RW-01 through RW-04 from the test strategy.
"""

import pytest



class TestCustomerSupportConversation:
    SEQUENCE = [
        "agent.started", "tool.search_orders",
        "tool.search_orders.completed", "tool.check_carrier",
        "tool.check_carrier.completed", "message.sent",
        "agent.completed",
    ]

    def test_full_conversation_sequence(self, actae, ch_name):
        for et in self.SEQUENCE:
            actae.record(ch_name, et, {}, "support-v1")
        events = actae.replay(ch_name, cursor=0, limit=20)
        assert len(events) == len(self.SEQUENCE)
        for i, expected in enumerate(self.SEQUENCE):
            assert events[i]["type"] == expected

    def test_late_joining_observer(self, actae, ch_name):
        for et in self.SEQUENCE:
            actae.record(ch_name, et, {}, "support-v1")
        events = actae.replay(ch_name, cursor=0, limit=20)
        assert len(events) == 7
        assert set(e["actor"] for e in events) == {"support-v1"}

    def test_cursor_after_conversation(self, actae, ch_name):
        for et in self.SEQUENCE:
            actae.record(ch_name, et, {}, "support-v1")
        assert actae.cursor(ch_name) is not None

    def test_query_support_events(self, actae, ch_name):
        for et in self.SEQUENCE:
            actae.record(ch_name, et, {}, "support-v1")
        completed = actae.query(event_type="agent.completed", channel_ids=[ch_name], limit=10)
        assert len(completed) == 1


class TestHumanInTheLoop:
    def test_approval_before_refund(self, actae, ch_name):
        actae.record(ch_name, "agent.started", {}, "support-v1")
        actae.record(ch_name, "human.approval.requested", {"amount": 249.99}, "support-v1")
        actae.record(ch_name, "human.approval.granted", {"approved_by": "supervisor"}, "human-supervisor")
        actae.record(ch_name, "tool.process_refund", {"amount": 249.99}, "support-v1")
        actae.record(ch_name, "tool.process_refund.completed", {"refund_id": "RF-001"}, "support-v1")
        actae.record(ch_name, "agent.completed", {"resolution": "refunded"}, "support-v1")
        events = actae.replay(ch_name, cursor=0, limit=10)
        assert len(events) == 6
        human_idx = next(i for i, e in enumerate(events) if e["type"] == "human.approval.granted")
        refund_idx = next(i for i, e in enumerate(events) if e["type"] == "tool.process_refund")
        assert human_idx < refund_idx

    def test_multiple_actor_timeline(self, actae, ch_name):
        actae.record(ch_name, "agent.started", {}, "agent")
        actae.record(ch_name, "human.approval.requested", {}, "agent")
        actae.record(ch_name, "human.approval.granted", {}, "human")
        actae.record(ch_name, "agent.completed", {}, "agent")
        events = actae.replay(ch_name, cursor=0, limit=10)
        assert [e["actor"] for e in events] == ["agent", "agent", "human", "agent"]


class TestMultiAgentCoordination:
    def test_monitor_before_worker(self, actae, ch_name):
        actae.record(ch_name, "agent.started", {"agent": "monitor"}, "monitor")
        actae.record(ch_name, "checkpoint.created", {"action": "review", "assigned": "worker"}, "monitor")
        actae.record(ch_name, "agent.completed", {"resolution": "handoff"}, "monitor")
        actae.record(ch_name, "agent.started", {"agent": "worker"}, "worker")
        actae.record(ch_name, "tool.review_logs", {"period": "last_1h"}, "worker")
        actae.record(ch_name, "tool.review_logs.completed", {"errors": 3}, "worker")
        actae.record(ch_name, "agent.completed", {"resolution": "escalated"}, "worker")
        events = actae.replay(ch_name, cursor=0, limit=20)
        assert len(events) == 7
        monitor_idx = [i for i, e in enumerate(events) if e["actor"] == "monitor"]
        worker_idx = [i for i, e in enumerate(events) if e["actor"] == "worker"]
        assert max(monitor_idx) < min(worker_idx)

    def test_coordination_query(self, actae, ch_name):
        actae.record(ch_name, "checkpoint.created", {"assigned": "worker"}, "monitor")
        cps = actae.query(event_type="checkpoint.created", channel_ids=[ch_name], limit=10)
        assert len(cps) >= 1


class TestCheckpointResume:
    def test_checkpoint_and_resume(self, actae, ch_name):
        actae.record(ch_name, "agent.started", {"agent": "runner"}, "runner")
        actae.record(ch_name, "tool.process", {"batch": 1}, "runner")
        actae.record(ch_name, "tool.process.completed", {"ok": True}, "runner")
        actae.record(ch_name, "checkpoint.created", {"progress": "33%"}, "runner")
        actae.record(ch_name, "tool.process", {"batch": 2}, "runner")
        actae.record(ch_name, "tool.process.completed", {"ok": True}, "runner")
        actae.record(ch_name, "checkpoint.created", {"progress": "66%"}, "runner")
        actae.record(ch_name, "agent.completed", {"resolution": "done"}, "runner")
        events = actae.replay(ch_name, cursor=0, limit=20)
        cps = [e for e in events if e["type"] == "checkpoint.created"]
        assert len(cps) == 2
        events_after = actae.replay(ch_name, cursor=cps[-1]["cursor"], limit=10)
        assert len(events_after) >= 1
        assert events_after[0]["cursor"] > cps[-1]["cursor"]


class TestSDLCWorkflow:
    def test_sdlc_pipeline(self, actae, ch_name):
        steps = [
            ("agent.started", "pm-agent", {"ticket": "JIRA-1048"}),
            ("tool.started", "architect-agent", {"tool": "db_schema_gen"}),
            ("tool.completed", "architect-agent", {"tool": "db_schema_gen"}),
            ("agent.started", "coder-agent", {"task": "implement"}),
            ("memory.written", "coder-agent", {"key": "auth_context"}),
            ("tool.completed", "coder-agent", {"tool": "write_file"}),
            ("human.approval.requested", "reviewer-agent", {"pr_url": "..."}),
        ]
        for et, actor, payload in steps:
            actae.record(ch_name, et, payload, actor)
        events = actae.replay(ch_name, cursor=0, limit=20)
        assert len(events) == len(steps)
        assert set(e["actor"] for e in events) == {"pm-agent", "architect-agent", "coder-agent", "reviewer-agent"}
        for i, (et, actor, _) in enumerate(steps):
            assert events[i]["type"] == et
            assert events[i]["actor"] == actor
