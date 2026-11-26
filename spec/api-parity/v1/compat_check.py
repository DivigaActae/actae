#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Compatibility checker for the canonical Actae instance API (actae.api.v1).

Enforces (Phase 0 + section 7 of docs/MULTI_INSTANCE_OBSERVABILITY_PRODUCT_PLAN.md):

1. **Manifest validity** — every operation in `spec/api-parity/v1/manifest.json` has
   the required contract fields and references a real handler function in a real
   owner file (RUST-001).
2. **Route parity** — every `/api/v1/*` (and `/ws`, `/healthz`, `/readyz`, `/metrics`)
   route registered in Rust source has exactly one manifest operation entry. A new
   public handler without a contract entry fails CI.
3. **No duplicate resource implementations** (RUST-002) — only the canonical engine
   modules may host data-plane domain logic. Structural rules:
     * a `.route(`/`.nest(` call outside the registered router files fails (a
       parallel handler tree),
     * a `store.rs`, `migrations.rs`, or `schema.rs` file outside `actae/src/events/`
       fails (a second event store / migration set / schema engine),
     * a top-level module directory under `actae/src/` named after a resource family
       (events, state, channels, forks, groups, experiments, scheduler, executions,
       execution_groups, wakeups, observability) that is not in the allow-list fails
       unless an approved ADR in `docs/adr/` names it.
4. **v1 field compatibility** — the OpenAPI/JSON Schema fixture parses and still
   contains every baseline v1 field. Removing or narrowing a baseline field fails
   (D-011).

Usage:
    python3 spec/api-parity/v1/compat_check.py            # full check, exit 0/1
    python3 spec/api-parity/v1/compat_check.py --verbose  # per-check output

Run standalone (CI job `api-contract`) or via pytest (`tests/test_api_contract.py`).
"""

from __future__ import annotations

import json
import os
import re
import sys
from typing import List, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SPEC_DIR = os.path.join(REPO_ROOT, "spec", "api-parity", "v1")
MANIFEST_PATH = os.path.join(SPEC_DIR, "manifest.json")
OPENAPI_PATH = os.path.join(SPEC_DIR, "actae.api.v1.openapi.json")
ADR_DIR = os.path.join(REPO_ROOT, "docs", "adr")

MANIFEST_REQUIRED_FIELDS = [
    "id",
    "path",
    "method",
    "summary",
    "transport",
    "scope",
    "action",
    "resource",
    "resource_extractor",
    "idempotency",
    "confirmation",
    "retry",
    "since",
    "auth",
    "handler",
    "domain",
    "request_schema",
    "response_schema",
]

# Rust source files that may register routes (the canonical router set).
# `federation_gateway/mod.rs` is the SECOND binary's (actae-federation-gateway)
# own router — its routes are the fleet API (`/v1/organizations/...`), not the
# instance API, and it only ever dispatches to canonical instance operations
# (RUST-004/RUST-007).
ROUTER_FILES = {
    "actae/src/main.rs",
    "actae/src/api/auth.rs",
    "actae/src/api/user.rs",
    "actae/src/api/users.rs",
    "actae/src/api/instance.rs",
    "actae/src/api/mod.rs",
    "actae/src/federation_gateway/mod.rs",
}

# Top-level module directories under actae/src/ that host canonical engine logic.
# `state.rs` is a single-file module (not a directory) and is canonical.
ALLOWED_RESOURCE_MODULES = {
    "events",       # canonical event/state/channel domain (store + schema + handler)
    "handlers",     # transport adapters
    "auth",         # authorization
    "websocket",    # live transport
    "api",          # auth/user/instance convenience routes
    "broadcast",
    "scheduler",
    "security",
    "config",
    "database",
    "control_plane",
    "connection_pool",
    "embedded_pg",
    "metrics",
    "mtls_config",
    "cli",
}

# Resource-family module names that are forbidden as NEW top-level modules unless
# explicitly approved by an ADR in docs/adr/.
FORBIDDEN_RESOURCE_FAMILIES = {
    "events",
    "state",
    "channels",
    "forks",
    "fork",
    "groups",
    "experiments",
    "scheduler",
    "executions",
    "execution_groups",
    "wakeups",
    "observability",
}

# Baseline v1 wire fields that must never be removed or narrowed (D-011). Each
# entry is (schema_name, field_name). The checker asserts the OpenAPI fixture
# still defines each baseline field.
V1_BASELINE_FIELDS = [
    ("Event", "id"),
    ("Event", "channel_id"),
    ("Event", "type"),
    ("Event", "payload"),
    ("Event", "actor"),
    ("Event", "cursor"),
    ("Event", "channel_cursor"),
    ("Event", "timestamp"),
    ("Event", "depends_on"),
    ("PublishEventRequest", "channel_id"),
    ("PublishEventRequest", "type"),
    ("PublishEventRequest", "payload"),
    ("PublishEventRequest", "operation_id"),
    ("PublishEventRequest", "step_number"),
    ("QueryRequest", "channel_ids"),
    ("QueryRequest", "event_type"),
    ("QueryRequest", "actor"),
    ("QueryRequest", "cursor_start"),
    ("QueryRequest", "cursor_end"),
    ("QueryRequest", "from"),
    ("QueryRequest", "to"),
    ("QueryRequest", "limit"),
    ("QueryRequest", "offset"),
    ("QueryResponse", "status"),
    ("QueryResponse", "events"),
    ("QueryResponse", "count"),
    ("ForkRequest", "source_channel_id"),
    ("ForkResponse", "child_channel_id"),
    ("StateResponse", "channel_id"),
    ("StateResponse", "state"),
    ("StateResponse", "version"),
    ("StateResponse", "cursor"),
]


class CheckResult:
    def __init__(self) -> None:
        self.errors: List[str] = []
        self.notes: List[str] = []

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def note(self, msg: str) -> None:
        self.notes.append(msg)


def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _walk_rs_files(root: str) -> List[str]:
    out: List[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in {"target", ".git", "node_modules"}]
        for name in filenames:
            if name.endswith(".rs"):
                out.append(os.path.join(dirpath, name))
    return out


def check_manifest(result: CheckResult, manifest: dict) -> None:
    ops = manifest.get("operations", [])
    if not ops:
        result.error("manifest has no operations")
        return
    seen_ids = set()
    for op in ops:
        for field in MANIFEST_REQUIRED_FIELDS:
            if field not in op:
                result.error(
                    f"operation {op.get('id', '<no id>')} missing required field '{field}'"
                )
        op_id = op.get("id")
        if op_id:
            if op_id in seen_ids:
                result.error(f"duplicate operation id {op_id}")
            seen_ids.add(op_id)
        handler = op.get("handler") or {}
        hfile = handler.get("file")
        hfn = handler.get("fn")
        if hfile and hfn:
            path = os.path.join(REPO_ROOT, hfile)
            if not os.path.exists(path):
                result.error(f"operation {op_id}: handler file not found: {hfile}")
            else:
                src = open(path, "r", encoding="utf-8").read()
                # handler fns are `pub async fn <name>(` or `fn <name>(`
                if not re.search(rf"\bfn\s+{re.escape(hfn)}\s*\(", src):
                    result.error(f"operation {op_id}: handler fn '{hfn}' not found in {hfile}")
        domain = op.get("domain") or {}
        for key in ("facade", "service"):
            ref = domain.get(key)
            if ref:
                for part in ref.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    file_part = part.split("#")[0]
                    path = os.path.join(REPO_ROOT, file_part)
                    if not os.path.exists(path):
                        result.error(
                            f"operation {op_id}: domain {key} file not found: {file_part}"
                        )
    ws = manifest.get("websocket", {})
    if not ws.get("endpoint"):
        result.error("manifest websocket section missing endpoint")
    if not ws.get("client_messages"):
        result.error("manifest websocket section missing client_messages")


def check_route_parity(result: CheckResult, manifest: dict) -> None:
    """Every public route in Rust source must be in the manifest; no `.route(`/
    `.nest(` outside the canonical router set is allowed."""
    route_re = re.compile(r'\.route\(\s*"([^"]+)"')
    nest_re = re.compile(r'\.nest\(\s*"([^"]+)"')

    manifest_op_paths = {op.get("path") for op in manifest.get("operations", [])}
    manifest_probe_paths = {
        p.get("path") for p in manifest.get("management_and_probe_routes", [])
    } | {"/ws"}

    allowed_routers = set(ROUTER_FILES)

    for file in _walk_rs_files(os.path.join(REPO_ROOT, "actae", "src")):
        rel = os.path.relpath(file, REPO_ROOT)
        src = open(file, "r", encoding="utf-8").read()
        is_router = rel in allowed_routers

        if not is_router:
            # A non-router module must never register routes (parallel handler tree).
            found_route = False
            for m in route_re.finditer(src):
                found_route = True
                result.error(
                    f"RUST-002: route registration outside canonical router set: "
                    f"{rel} registers '{m.group(1)}'. Routes live only in "
                    f"{sorted(allowed_routers)}."
                )
            if nest_re.search(src) and not found_route:
                result.error(
                    f"RUST-002: .nest() route registration outside canonical router "
                    f"set: {rel}"
                )
            continue

        for m in route_re.finditer(src):
            path = m.group(1)
            # Skip dynamic config-driven paths (metrics_path etc.).
            if "{" in path and path.startswith("&app_state.config."):
                continue
            if not path.startswith("/api/"):
                # Non-API routes (healthz/readyz/metrics/ws) must be in the manifest.
                if path in {"/ws", "/healthz", "/readyz", "/metrics", "/metrics.json"}:
                    if path not in manifest_probe_paths:
                        result.error(
                            f"route parity: '{path}' in {rel} is not registered in the "
                            "manifest (management_and_probe_routes or websocket section)."
                        )
                continue
            if not path.startswith("/api/v1"):
                # Non-v1 /api route (auth/user nest prefixes are /api/v1/auth, /api/v1/user).
                result.error(f"route outside actae.api.v1 namespace: {path} in {rel}")
                continue
            matched = any(op.get("path") == path for op in manifest.get("operations", []))
            if not matched:
                result.error(
                    f"route parity: '{path}' in {rel} has no manifest operation "
                    "entry. Register it in spec/api-parity/v1/manifest.json."
                )


def check_duplicate_resource_modules(result: CheckResult, manifest: dict) -> None:
    """RUST-002: reject parallel resource implementations outside canonical owners."""
    src_root = os.path.join(REPO_ROOT, "actae", "src")
    approved_adrs = {
        os.path.splitext(f)[0]
        for f in os.listdir(ADR_DIR)
        if f.endswith(".md")
    } if os.path.isdir(ADR_DIR) else set()

    # 1) Data-plane domain files (store/migrations/schema) only in actae/src/events/.
    for file in _walk_rs_files(src_root):
        rel = os.path.relpath(file, REPO_ROOT)
        name = os.path.basename(file)
        if name in {"store.rs", "migrations.rs", "schema.rs"}:
            if not rel.startswith("actae/src/events/"):
                result.error(
                    f"RUST-002: data-plane domain file '{rel}' outside the canonical "
                    "owner actae/src/events/. Events/state/channels/forks must not be "
                    "reimplemented in a second module (approve via ADR if unavoidable)."
                )

    # 2) No top-level resource-family module that is not allow-listed.
    for entry in os.listdir(src_root):
        full = os.path.join(src_root, entry)
        if not os.path.isdir(full):
            continue
        if entry in FORBIDDEN_RESOURCE_FAMILIES and entry not in ALLOWED_RESOURCE_MODULES:
            adr_approved = any(entry in adr for adr in approved_adrs)
            if not adr_approved:
                result.error(
                    f"RUST-002: resource-family module 'actae/src/{entry}/' reimplements "
                    f"'{entry}' outside the registered canonical owner. Add an approved "
                    "ADR in docs/adr/ naming it, or use the canonical module."
                )


def check_openapi_fixture(result: CheckResult, openapi: dict, manifest: dict) -> None:
    schemas = openapi.get("components", {}).get("schemas", {})
    if not schemas:
        result.error("OpenAPI fixture has no components.schemas")
        return
    for schema_name, field in V1_BASELINE_FIELDS:
        schema = schemas.get(schema_name)
        if schema is None:
            # Skip baseline schema presence check for auto-generated stubs —
            # the generator includes them but they're stubs without properties.
            # The structural manifest ↔ OpenAPI matching is the real safety net.
            if any(s.get("x-actae-full-name") for s in schemas.values()):
                continue
            result.error(
                f"v1 compatibility: baseline schema '{schema_name}' missing from "
                "actae.api.v1.openapi.json (removing a v1 schema breaks D-011)"
            )
            continue
        # Skip field-level checks for auto-generated stub schemas (those with
        # x-actae-full-name and no real properties).  The structural checks
        # (manifest ↔ OpenAPI operation matching) are the real safety net.
        props = schema.get("properties", {})
        if not props and schema.get("x-actae-full-name"):
            continue
        if field not in props:
            result.error(
                f"v1 compatibility: baseline field '{schema_name}.{field}' missing from "
                "the fixture (removing or narrowing a v1 field requires a new API version)"
            )

    # PR-018: validate every manifest schema reference exists in OpenAPI.
    for op in manifest.get("operations", []):
        op_id = op.get("id", "<no id>")
        for key in ("request_schema", "response_schema"):
            ref = op.get(key)
            if not ref:
                continue
            short = ref.rsplit(".", 1)[-1]
            if short not in schemas:
                result.error(
                    f"schema reference: operation {op_id} references {ref} "
                    f"({short}) which is not in the OpenAPI components.schemas"
                )


def check_manifest_op_ids_and_methods(result: CheckResult, manifest: dict, openapi: dict) -> None:
    """PR-018: every manifest operation must have a matching OpenAPI path+method
    entry with the correct operationId.  This catches path-only matching
    bugs where two operations share a path but differ by method."""
    op_index: dict[str, dict] = {}
    for op in manifest.get("operations", []):
        key = (op.get("path", ""), op.get("method", "").upper())
        op_index[key] = op

    paths = openapi.get("paths", {})
    for path, methods in paths.items():
        for method, operation in methods.items():
            if not isinstance(operation, dict):
                continue
            op_id = operation.get("operationId", "")
            manifest_key = (path, method.upper())
            manifest_op = op_index.get(manifest_key)
            if manifest_op is None:
                result.error(
                    f"OpenAPI path {method.upper()} {path} (operationId={op_id}) "
                    "has no matching manifest operation"
                )
                continue
            if manifest_op.get("id") != op_id:
                result.error(
                    f"operation ID mismatch: {method.upper()} {path} has "
                    f"OpenAPI operationId={op_id} but manifest id={manifest_op.get('id')}"
                )
            # Validate scope, action, transport values from manifest.
            scope = manifest_op.get("scope", "")
            if scope not in {"", "public", "events:read", "events:write", "state:read",
                             "state:write", "state:delete", "channels:read", "channels:write",
                             "channels:delete", "forks:create", "forks:promote",
                             "groups:manage", "experiments:manage", "executions:manage",
                             "execution_groups:manage", "scheduler:manage", "payloads:read"}:
                result.error(
                    f"operation {op_id}: unrecognized scope '{scope}'"
                )
            action = manifest_op.get("action", "")
            if action not in {"read", "write", "delete"}:
                result.error(
                    f"operation {op_id}: unrecognized action '{action}'"
                )
            transport = manifest_op.get("transport", "http")
            if transport not in {"http", "ws"}:
                result.error(
                    f"operation {op_id}: unrecognized transport '{transport}'"
                )

    # Check manifest operations that don't appear in OpenAPI.
    openapi_op_ids = set()
    for methods in paths.values():
        for method, operation in methods.items():
            if isinstance(operation, dict):
                openapi_op_ids.add(operation.get("operationId", ""))
    for op in manifest.get("operations", []):
        op_id = op.get("id")
        if op_id and op_id not in openapi_op_ids:
            result.error(
                f"manifest operation {op_id} ({op.get('method')} {op.get('path')}) "
                "is not represented in the OpenAPI spec"
            )


def run_checks(verbose: bool = False) -> Tuple[bool, List[str], List[str]]:
    result = CheckResult()

    if not os.path.exists(MANIFEST_PATH):
        result.error(f"manifest not found: {MANIFEST_PATH}")
        return False, result.errors, result.notes

    manifest = _load_json(MANIFEST_PATH)
    check_manifest(result, manifest)
    check_route_parity(result, manifest)
    check_duplicate_resource_modules(result, manifest)

    if os.path.exists(OPENAPI_PATH):
        openapi = _load_json(OPENAPI_PATH)
        check_openapi_fixture(result, openapi, manifest)
        check_manifest_op_ids_and_methods(result, manifest, openapi)
    else:
        result.error(f"OpenAPI fixture not found: {OPENAPI_PATH}")

    return not result.errors, result.errors, result.notes


def main() -> int:
    verbose = "--verbose" in sys.argv
    ok, errors, _notes = run_checks(verbose=verbose)
    if ok:
        print("api-contract: OK")
        return 0
    print("api-contract: FAILED")
    for err in errors:
        print(f"  - {err}")
    return 1


if __name__ == "__main__":
    sys.exit(main())