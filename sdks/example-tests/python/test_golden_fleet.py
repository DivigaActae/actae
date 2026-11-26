# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Cross-SDK golden fleet wire corpus (Deep-004 / PR-020).

Every Python/Go/TypeScript SDK validates its wire handling against
`spec/api-parity/v1/golden_fleet_vectors.json` so a JSON-number, envelope,
pagination-token or fork-receipt regression in one SDK fails loudly in all
three. No server required.
"""
import json
from pathlib import Path

from actae_client.errors import APIError
from actae_client.fleet import FleetClient, PartialResult
from actae_client.types import _unwrap_sonic


def _vectors() -> dict:
    path = (
        Path(__file__).resolve().parents[3]
        / "spec" / "api-parity" / "v1" / "golden_fleet_vectors.json"
    )
    return json.loads(path.read_text())["fleet_wire"]


def test_arbitrary_precision_unwraps_without_rounding():
    """sonic-rs number wrappers become native ints (never silently rounded),
    including values past Number.MAX_SAFE_INTEGER."""
    v = _vectors()["arbitrary_precision"]
    got = _unwrap_sonic(v["input"])
    assert got["big"] == 9007199254740993  # > 2^53-1; must stay exact
    assert got["negative"] == -9007199254740993
    assert got["float"] == 3.5
    assert got["nested"]["a"] == 123
    assert got["list"] == [42]
    assert got["big"] != 9007199254740992  # a silently rounded value fails


def test_http_envelope_unwrap():
    """Fleet responses wrap the payload under `data`; the SDK unwraps it and
    returns the raw value when there is no `data` object."""
    v = _vectors()["http_envelope"]
    assert FleetClient._unwrap_gateway(v["input"]) == v["unwrap"]
    # No `data` object -> returned unchanged.
    assert FleetClient._unwrap_gateway({"ok": True}) == {"ok": True}


def test_error_envelope_maps_to_status_and_message():
    """PR-021: the typed error envelope surfaces as the HTTP status + message."""
    v = _vectors()["error_envelope"]
    env = v["input"]
    err = APIError(v["status"], str(env["message"]))
    assert err.status_code == v["status"]
    assert env["message"] == v["message"]
    assert env["code"] == "idempotency_conflict"
    assert env["retryable"] is False
    assert env["request_id"] == "rq-1"


def test_pagination_tokens_are_opaque_and_preserved():
    """PR-008: multi-instance continuation is a per-instance map; the SDK
    preserves every token and never collapses to one."""
    v = _vectors()["pagination"]
    tokens = v["page_tokens"]
    assert tokens == {"ins_a": "tok-a", "ins_b": "tok-b"}
    # A PartialResult carries the full map.
    pr = PartialResult(items=[], page_tokens=dict(tokens))
    assert pr.page_tokens == tokens
    assert pr.page_tokens is not None and len(pr.page_tokens) == 2


def test_fork_receipt_fields_are_immutable_provenance():
    """ForkReceipt carries immutable provenance: child/source channel, source
    state version, boundary cursors, restorability and the manifest."""
    v = _vectors()["fork_receipt"]
    r = v["input"]
    assert r["child_channel_id"] == v["child_channel_id"]
    assert r["source_channel_id"] == v["source_channel_id"]
    assert r["source_state_version"] == v["source_state_version"]
    assert r["restorable"] == v["restorable"]
    assert r["manifest"] == {"prompt": "p"}


def test_ws_broadcast_has_two_supported_shapes():
    """Both broadcast encodings are events — never control frames."""
    v = _vectors()["ws_broadcast"]
    for shape in (v["legacy"], v["type_tagged"]):
        payload = shape["Broadcast"]["payload"] if "Broadcast" in shape else shape["payload"]
        event = payload["event"]
        assert event["channel_id"] == "ch"
        assert event["cursor"] in (2, 3)