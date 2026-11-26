# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the durability benchmark's measurement logic (no server).

These pin the arithmetic behind ``examples/durability_benchmark.py``: the
external-service ledger's dedup behaviour and the duplicate/lost-effect
summaries that the benchmark reports. The harness itself (real process crashes
against a live Actae) is run manually and its committed report lives in
``examples/durability-benchmark-results.json``.

    python3 -m pytest sdks/example-tests/python/test_durability_benchmark.py -v
"""

import sys
import time
from pathlib import Path

_EXAMPLES = Path(__file__).resolve().parents[3] / "examples"
sys.path.insert(0, str(_EXAMPLES))

from durability_benchmark import (  # noqa: E402
    Config,
    HttpEffectService,
    Result,
    TransportError,
    http_apply,
    http_status,
    init_service,
    load_marker,
    render_markdown,
    save_marker,
    service_apply,
    summarize_application_rows,
    summarize_service,
)


def _service(tmp_path):
    db = str(tmp_path / "service.db")
    init_service(db)
    return db


def test_non_idempotent_downstream_applies_every_invocation(tmp_path):
    db = _service(tmp_path)
    for _ in range(3):
        service_apply(db, "step-1", "step-1", honors_key=False)

    summary = summarize_service(db, expected_keys=6)
    assert summary["total_applications"] == 3
    assert summary["duplicate_applications"] == 2
    assert summary["max_applications_per_key"] == 3
    assert summary["distinct_keys"] == 1
    assert summary["lost_keys"] == 5


def test_honors_key_dedupes_a_stable_downstream_key(tmp_path):
    db = _service(tmp_path)
    # The ledger strategy: the same deterministic key on every attempt.
    service_apply(db, "step-1", "step-1", honors_key=True)
    service_apply(db, "step-1", "step-1", honors_key=True)

    summary = summarize_service(db, expected_keys=6)
    assert summary["total_applications"] == 1
    assert summary["duplicate_applications"] == 0


def test_honors_key_cannot_dedupe_without_a_stable_key(tmp_path):
    db = _service(tmp_path)
    # naive/checkpoint: no durable identity, so each attempt presents a fresh key.
    service_apply(db, "step-1", "step-1:aaa", honors_key=True)
    service_apply(db, "step-1", "step-1:bbb", honors_key=True)

    summary = summarize_service(db, expected_keys=6)
    assert summary["total_applications"] == 2
    assert summary["duplicate_applications"] == 1


def test_summary_reports_no_loss_when_every_step_fires(tmp_path):
    db = _service(tmp_path)
    for step in range(6):
        service_apply(db, f"step-{step}", f"step-{step}", honors_key=False)

    summary = summarize_service(db, expected_keys=6)
    assert summary["distinct_keys"] == 6
    assert summary["lost_keys"] == 0
    assert summary["duplicate_applications"] == 0


def test_marker_round_trip_and_missing_default(tmp_path):
    marker = str(tmp_path / "progress.json")
    assert load_marker(marker) == 0
    save_marker(marker, 4)
    assert load_marker(marker) == 4
    save_marker(marker, 5)
    assert load_marker(marker) == 5


def test_render_markdown_has_one_row_per_result(tmp_path):
    results = [
        Result(
            strategy="durable",
            crash_point="after_effect",
            downstream="honors_key",
            child_exit_codes=[137, 0],
            recovery_attempts=2,
            metrics={
                "total_applications": 6,
                "distinct_keys": 6,
                "expected_keys": 6,
                "duplicate_applications": 0,
                "max_applications_per_key": 1,
                "lost_keys": 0,
            },
        )
    ]
    table = render_markdown(results)
    assert "| durable | after_effect | honors_key | 0 | 0 | 6 |" in table


def test_config_name_is_stable():
    config = Config("durable", "after_effect", "honors_key", "ch", "http://x", "k")
    assert config.name == "durable|after_effect|honors_key"


def test_summarize_application_rows_matches_arithmetic():
    rows = [
        {"key": "step-1", "downstream_key": "a"},
        {"key": "step-1", "downstream_key": "b"},
        {"key": "step-2", "downstream_key": "c"},
    ]
    summary = summarize_application_rows(rows, expected_keys=3)
    assert summary["total_applications"] == 3
    assert summary["distinct_keys"] == 2
    assert summary["duplicate_applications"] == 1
    assert summary["lost_keys"] == 1


def test_http_service_dedupes_a_stable_key():
    service = HttpEffectService()
    url = service.start()
    try:
        first = http_apply(url, "step-1", "step-1", honors_key=True)
        second = http_apply(url, "step-1", "step-1", honors_key=True)
        assert first["applied"] is True
        assert second["applied"] is False
        status = http_status(url, "step-1")
        assert status["applications"] == 1
    finally:
        service.stop()


def test_http_service_fresh_keys_are_not_deduped():
    service = HttpEffectService()
    url = service.start()
    try:
        http_apply(url, "step-1", "step-1:aaa", honors_key=True)
        http_apply(url, "step-1", "step-1:bbb", honors_key=True)
        assert http_status(url, "step-1")["applications"] == 2
    finally:
        service.stop()


def test_http_service_drop_after_commit_reports_unknown_then_queryable():
    service = HttpEffectService(drop_for_key="step-3")
    url = service.start()
    try:
        # First request commits, then the connection is dropped: transport error.
        try:
            http_apply(url, "step-3", "step-3", honors_key=False)
            raised = False
        except TransportError:
            raised = True
        assert raised
        # The effect did land and is now queryable — the caller can reconcile.
        status = http_status(url, "step-3")
        assert status["applied"] is True
        # A later identical request succeeds normally (the drop was one-shot).
        again = http_apply(url, "step-3", "step-3", honors_key=True)
        assert again["applied"] is False  # honors_key saw it already
    finally:
        service.stop()


def test_http_status_reports_not_applied():
    service = HttpEffectService()
    url = service.start()
    try:
        assert http_status(url, "never") == {"key": "never", "applied": False, "applications": 0}
    finally:
        service.stop()


def test_http_service_drop_before_commit_leaves_effect_unapplied_then_retry_applies():
    service = HttpEffectService(drop_for_key="step-3", drop_before_commit=True)
    url = service.start()
    try:
        try:
            http_apply(url, "step-3", "step-3", honors_key=False)
            raised = False
        except TransportError:
            raised = True
        assert raised
        # The effect did NOT land: the status query says "not applied".
        assert http_status(url, "step-3")["applied"] is False
        # The one-shot drop has passed, so the retry applies exactly once.
        result = http_apply(url, "step-3", "step-3", honors_key=False)
        assert result["applied"] is True
        assert http_status(url, "step-3")["applications"] == 1
    finally:
        service.stop()


def test_http_service_eventual_consistency_hides_a_just_applied_effect(tmp_path):
    # A weakly-consistent downstream: the effect landed, but the status query
    # does not report it for `status_lag_seconds`. This is the exact condition
    # under which durable_query would falsely re-apply.
    service = HttpEffectService(status_lag_seconds=0.4)
    url = service.start()
    try:
        http_apply(url, "step-1", "step-1", honors_key=False)
        # Immediately: not visible (a reconciling client would think it failed).
        assert http_status(url, "step-1")["applied"] is False
        time.sleep(0.5)
        # After the lag window: visible.
        assert http_status(url, "step-1")["applied"] is True
        assert http_status(url, "step-1")["applications"] == 1
    finally:
        service.stop()


def test_http_service_slow_response_commits_then_times_out():
    # The effect commits, but the reply is slower than the client's timeout:
    # the caller sees a transport error for an effect that DID land.
    service = HttpEffectService(slow_for_key="step-2", slow_seconds=0.4)
    url = service.start()
    try:
        try:
            http_apply(url, "step-2", "step-2", honors_key=False, timeout=0.1)
            raised = False
        except TransportError:
            raised = True
        assert raised
        assert http_status(url, "step-2")["applied"] is True
    finally:
        service.stop()
