#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Generate the small, dependency-free operation registries used by SDKs.

The JSON manifest remains the source of truth.  Generated registries are
intentionally metadata-only: request/response bodies continue to use the
canonical schemas and the clients perform normal runtime validation.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
OPS = json.loads((Path(__file__).parent / "manifest.json").read_text())["operations"]


def fleet_supported(op: dict) -> bool:
    """PR-016: an operation is fleet-supported when it carries a real
    data-plane scope. Auth/session/dashboard operations (`scope == "public"`)
    are NOT fleet commands — federation command authority is intentionally not
    dashboard/session authority. The same predicate drives the gateway's
    capability advertisement and the SDKs' local UnsupportedOperation guard."""
    scope = op.get("scope", "")
    return bool(scope and scope != "public")


py = '''"""Generated from spec/api-parity/v1/manifest.json; do not edit manually."""\nfrom dataclasses import dataclass\nfrom typing import Dict, Optional\n\n@dataclass(frozen=True)\nclass ManifestOperation:\n    id: str\n    method: str\n    path: str\n    action: str\n    confirmation: bool\n    idempotency: str\n    transport: str\n    scope: str\n    fleet_supported: bool\n    request_schema: Optional[str]\n    response_schema: Optional[str]\n\nMANIFEST_OPERATIONS = (\n'''
for op in OPS:
    py += f'    ManifestOperation({op["id"]!r}, {op["method"]!r}, {op["path"]!r}, {op["action"]!r}, {bool(op.get("confirmation"))!r}, {op.get("idempotency", "none")!r}, {op.get("transport", "http")!r}, {op.get("scope", "")!r}, {fleet_supported(op)!r}, {op.get("request_schema")!r}, {op.get("response_schema")!r}),\n'
py += ')\nMANIFEST_BY_ID: Dict[str, ManifestOperation] = {op.id: op for op in MANIFEST_OPERATIONS}\n\ndef manifest_operation(operation_id: str) -> ManifestOperation:\n    try:\n        return MANIFEST_BY_ID[operation_id]\n    except KeyError as exc:\n        raise ValueError(f"unknown API manifest operation: {operation_id}") from exc\n'
(ROOT / "sdks/python/src/actae_client/manifest.py").write_text(py)

ts = '''/** Generated from spec/api-parity/v1/manifest.json; do not edit manually. */\nexport type ManifestOperation = Readonly<{ id: string; method: string; path: string; action: string; confirmation: boolean; idempotency: string; transport: string; scope: string; fleetSupported: boolean; requestSchema: string | null; responseSchema: string | null }>;\nexport const MANIFEST_OPERATIONS = [\n'''
for op in OPS:
    ts += f'  {{ id: {json.dumps(op["id"])}, method: {json.dumps(op["method"])}, path: {json.dumps(op["path"])}, action: {json.dumps(op["action"])}, confirmation: {str(bool(op.get("confirmation"))).lower()}, idempotency: {json.dumps(op.get("idempotency", "none"))}, transport: {json.dumps(op.get("transport", "http"))}, scope: {json.dumps(op.get("scope", ""))}, fleetSupported: {str(fleet_supported(op)).lower()}, requestSchema: {json.dumps(op.get("request_schema"))}, responseSchema: {json.dumps(op.get("response_schema"))} }},\n'
ts += '''] as const satisfies readonly ManifestOperation[];
export const MANIFEST_BY_ID: Readonly<Record<string, ManifestOperation>> = Object.fromEntries(MANIFEST_OPERATIONS.map((op) => [op.id, op]));
export function manifestOperation(operationId: string): ManifestOperation {
  const op = MANIFEST_BY_ID[operationId];
  if (!op) throw new Error(`unknown API manifest operation: ${operationId}`);
  return op;
}
'''
(ROOT / "sdks/typescript/src/manifest.ts").write_text(ts)

go = '''// Code generated from spec/api-parity/v1/manifest.json; DO NOT EDIT.\npackage actae\n\n// ManifestOperation describes one canonical operation.\ntype ManifestOperation struct {\n\tID string\n\tMethod string\n\tPath string\n\tAction string\n\tConfirmation bool\n\tIdempotency string\n\tTransport string\n\tScope string\n\tFleetSupported bool\n\tRequestSchema string\n\tResponseSchema string\n}\n\nvar ManifestOperations = map[string]ManifestOperation{\n'''
for op in OPS:
    go += f'\t{json.dumps(op["id"])}: {{ID: {json.dumps(op["id"])}, Method: {json.dumps(op["method"])}, Path: {json.dumps(op["path"])}, Action: {json.dumps(op["action"])}, Confirmation: {str(bool(op.get("confirmation"))).lower()}, Idempotency: {json.dumps(op.get("idempotency", "none"))}, Transport: {json.dumps(op.get("transport", "http"))}, Scope: {json.dumps(op.get("scope", ""))}, FleetSupported: {str(fleet_supported(op)).lower()}, RequestSchema: {json.dumps(op.get("request_schema") or "")}, ResponseSchema: {json.dumps(op.get("response_schema") or "")}}},\n'
go += '''}\n\nfunc ManifestOperationByID(operationID string) (ManifestOperation, bool) {\n\top, ok := ManifestOperations[operationID]\n\treturn op, ok\n}\n'''
(ROOT / "sdks/go/manifest.go").write_text(go)
try:
    # Keep generated Go deterministic and CI-friendly when the toolchain is
    # available; registries remain usable on environments without gofmt.
    subprocess.run(["gofmt", "-w", str(ROOT / "sdks/go/manifest.go")], check=True)
except (FileNotFoundError, subprocess.SubprocessError):
    pass
