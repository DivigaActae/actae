#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Generate the complete OpenAPI 3.1 spec from the manifest.

The manifest is the source of truth for all 75 operations.  This script
emits a complete OpenAPI document with every path, method, operation ID,
scope, and schema reference — replacing the hand-authored 7-path fixture
that was too weak to catch cross-layer mismatches (PR-018).
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

SPEC_DIR = Path(__file__).parent
MANIFEST = json.loads((SPEC_DIR / "manifest.json").read_text())
OPS = MANIFEST["operations"]

# ---- Collect all unique schemas from manifest references ---- #
ALL_SCHEMAS: set[str] = set()
for op in OPS:
    for key in ("request_schema", "response_schema"):
        s = op.get(key)
        if s:
            ALL_SCHEMAS.add(s)

# Add baseline schemas that the compatibility checker expects (V1_BASELINE_FIELDS).
# These are domain entity schemas that may not appear as request/response references
# but must never be removed from the API contract.
BASELINE_SCHEMAS = {
    "actae.api.v1.Event",
    "actae.api.v1.PublishEventRequest",
    "actae.api.v1.QueryRequest",
    "actae.api.v1.QueryResponse",
    "actae.api.v1.ForkRequest",
    "actae.api.v1.ForkResponse",
    "actae.api.v1.StateResponse",
}
ALL_SCHEMAS.update(BASELINE_SCHEMAS)

# ---- Build the OpenAPI paths ---- #
paths: dict = {}
for op in OPS:
    path = op["path"]
    method = op["method"].lower()
    op_id = op["id"]
    scope = op.get("scope", "")
    action = op.get("action", "")
    confirmation = bool(op.get("confirmation"))
    idempotency = op.get("idempotency", "none")
    transport = op.get("transport", "http")
    summary = op.get("summary", op_id)

    # Convert path placeholders: {channel_id} -> {channel_id} (already correct)
    # But ensure consistent formatting
    parameters = []
    for match in re.finditer(r"\{(\w+)\}", path):
        param_name = match.group(1)
        parameters.append({
            "name": param_name,
            "in": "path",
            "required": True,
            "schema": {"type": "string"},
        })

    req_schema = op.get("request_schema")
    resp_schema = op.get("response_schema")

    operation: dict = {
        "operationId": op_id,
        "summary": summary,
        "x-actae-scope": scope,
        "x-actae-action": action,
        "x-actae-confirmation": confirmation,
        "x-actae-idempotency": idempotency,
        "x-actae-transport": transport,
        "x-actae-fleet-supported": scope != "public",
        "tags": [scope.split(":")[0] if ":" in scope else scope],
    }

    if parameters:
        operation["parameters"] = parameters

    if req_schema:
        short = req_schema.rsplit(".", 1)[-1]
        operation["requestBody"] = {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {"$ref": f"#/components/schemas/{short}"}
                }
            },
        }

    if resp_schema:
        short = resp_schema.rsplit(".", 1)[-1]
        operation["responses"] = {
            "200": {
                "description": "Success",
                "content": {
                    "application/json": {
                        "schema": {"$ref": f"#/components/schemas/{short}"}
                    }
                },
            }
        }
    else:
        operation["responses"] = {
            "200": {"description": "Success"}
        }

    if path not in paths:
        paths[path] = {}
    paths[path][method] = operation

# ---- Collect schemas actually used in paths ---- #
used_schemas: set[str] = set()
for path_methods in paths.values():
    for op in path_methods.values():
        for req in op.get("requestBody", {}).get("content", {}).values():
            ref = req.get("schema", {}).get("$ref", "")
            if ref.startswith("#/components/schemas/"):
                used_schemas.add(ref.split("/")[-1])
        for resp in op.get("responses", {}).values():
            for content in resp.get("content", {}).values():
                ref = content.get("schema", {}).get("$ref", "")
                if ref.startswith("#/components/schemas/"):
                    used_schemas.add(ref.split("/")[-1])

# ---- Build component schemas (stubs for validation) ---- #
components_schemas = {}
for schema_ref in sorted(ALL_SCHEMAS):
    short = schema_ref.rsplit(".", 1)[-1]
    components_schemas[short] = {
        "type": "object",
        "description": f"Stub for {schema_ref} — full schema lives in the Rust types.",
        "x-actae-full-name": schema_ref,
    }

# Event is part of the stable wire contract; keep its canonical fields explicit
# in the generated fixture so compatibility tests can detect accidental removal.
components_schemas["Event"]["properties"] = {
    field: {"type": "object"} for field in
    ("id", "channel_id", "type", "payload", "actor", "cursor", "channel_cursor", "timestamp", "depends_on")
}

# ---- Assemble the OpenAPI doc ---- #
openapi = {
    "openapi": "3.1.0",
    "info": {
        "title": "Actae Instance API (actae.api.v1)",
        "version": "actae.api.v1",
        "description": (
            "Generated from spec/api-parity/v1/manifest.json — the source of truth "
            "for all 75 canonical operations.  Do not edit by hand."
        ),
    },
    "paths": dict(sorted(paths.items())),
    "components": {
        "schemas": components_schemas,
    },
    "x-actae-generated-from": "manifest.json",
    "x-actae-operation-count": len(OPS),
    "x-actae-schema-count": len(ALL_SCHEMAS),
    "x-actae-path-count": len(paths),
}

(SPEC_DIR / "actae.api.v1.openapi.json").write_text(
    json.dumps(openapi, indent=2, ensure_ascii=False) + "\n"
)

print(f"Generated OpenAPI: {len(paths)} paths, {len(ALL_SCHEMAS)} schemas, {len(OPS)} operations")
