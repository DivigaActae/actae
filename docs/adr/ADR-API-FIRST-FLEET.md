# ADR-API-FIRST-FLEET

**Status:** Accepted (implementation blueprint; see `docs/MULTI_INSTANCE_OBSERVABILITY_PRODUCT_PLAN.md` for tracking)
**Date:** 2026-08-28
**Fork:** `actae-api`

## Context

Actae has one durable engine and one canonical instance API (`/api/v1/*` + `/ws`).
We need a multi-instance fleet layer (gateway, NATS live fan-out, SaaS organization
access, self-hosted relay) without ever creating a second event/state/channel API in
Rust. The risk is "parallel implementation" drift: a fleet-facing module that
reimplements fork, state mutation, event recording, or authorization instead of
calling the canonical engine.

## Decision

1. **D-001 — One instance API.** `POST /api/v1/events/record`, `/events/transition`,
   `/events/replay`, `/events/query`, `/state/*`, `/channels/*` (fork, promote,
   metadata, steps, forks, tree, trail, receipt, close), `/groups/*`,
   `/execution-groups/*`, `/experiments/*`, `/scheduler/wakeups`, `/executions/*`,
   and `/ws` are the product API. No `/api/v1/observability/*` and no second
   event/state/channel protocol will be created.
2. **D-002 — API completeness.** Every supported data-plane capability is reachable
   through the canonical API. The manifest (`spec/api-parity/v1/manifest.json`)
   inventories all of them.
3. **D-003 — SDKs are clients, not another platform.** SDK methods are typed wrappers
   over documented API/WS requests. A capability cannot exist only in one SDK.
4. **D-004 — Dashboard parity.** Dashboard data-plane calls use documented public
   endpoints and schemas.
5. **D-005 — Fleet parity without a generic proxy.** The fleet gateway routes a typed,
   versioned manifest of canonical instance operations across authorized instances.
   It never forwards arbitrary customer-controlled paths, headers, or methods.
6. **D-006 — Preserve the data-plane boundary.** Event/state payloads stay in Actae
   instances. SaaS durable storage holds identity, routing, policy, quotas, and audit
   metadata — not payloads. NATS Core carries transient live frames only; JetStream
   is disabled for payload subjects.
7. **D-007 — Permissions are explicit.** Every request needs a named action scope plus
   instance/channel resource authorization. Read-only credentials cannot mutate.
8. **D-008 — Hosted and self-hosted both work.** Hosted instances use server-owned
   routes. Self-hosted uses an explicit outbound relay; the gateway never dials a URL
   supplied by a customer request.
9. **D-009 — Fork is storage, not execution.** `POST /api/v1/channels/fork` returns an
   exact child channel ID and inherited boundary; it never starts an LLM process.
   `fork-and-schedule` is a separate durable workflow.
10. **D-010 — At-least-once live delivery.** Deduplicate by `(instance_id, event_id)`.
    Per-channel cursor is ordered; no cross-instance causal total order is promised.
11. **D-011 — API versions are contracts.** Additive `actae.api.v1` schemas only.
    Removal/type narrowing requires a new version and a published deprecation period.

## Direct → fleet mapping

| Capability | Direct (canonical) | Fleet route |
|---|---|---|
| Fork | `POST /api/v1/channels/fork` | `POST /v1/organizations/{org}/instances/{instance}/forks` |
| Save state | `POST /api/v1/state/{channel}` | `PUT /v1/organizations/{org}/instances/{instance}/state/{channel}` |
| Record event | `POST /api/v1/events/record` | `POST /v1/organizations/{org}/instances/{instance}/events` |
| Query events | `POST /api/v1/events/query` | `POST /v1/organizations/{org}/events/query` (merged, partial) |
| Live stream | `/ws` (canonical) | `GET /v1/organizations/{org}/events/stream?ticket=...` |

The gateway issues typed canonical API calls to authorized instances. It contains no
second implementation of fork, state mutation, event recording, or authorization.

## Rust consolidation rules (RUST-001 … RUST-007)

- **RUST-001 — Route-to-owner inventory.** `spec/api-parity/v1/manifest.json` maps
  every public operation (HTTP + WS) to its canonical handler and domain service.
  One implementation owner per operation. Enforcement: `tests/test_api_contract.py`.
- **RUST-002 — No duplicate resource modules.** CI fails if a new parallel resource
  family reimplements events/state/channels/forks outside the registered canonical
  handler/domain owner without an approved ADR in `docs/adr/`.
- **RUST-003 — Keep the existing layers.** Route handlers (`handlers/*`) remain
  transport adapters; `events`/state/domain services (`events/handler.rs`,
  `events/store.rs`) remain business logic; `auth` remains authorization;
  `websocket` remains live transport. Add new capability logic to the appropriate
  existing layer.
- **RUST-004 — Fleet dispatch calls canonical contracts only.** The gateway calls
  direct canonical API operations (or a deliberately shared, manifest-tested service
  interface if a network hop is later justified). No second implementation.
- **RUST-005 — Deprecation register.** Any temporary route alias, old dashboard
  adapter, duplicate serializer, or protocol shim is recorded in
  `docs/API_DEPRECATIONS.md` with owner, replacement, telemetry, minimum support
  period, and removal version.
- **RUST-006 — Delete only with evidence.** Remove a temporary adapter only after all
  callers use the canonical contract, compatibility telemetry is zero for a full
  published window, docs/SDKs are updated, and direct/fleet parity plus regression
  suites pass.
- **RUST-007 — Keep the gateway narrow.** `actae-federation-gateway` contains
  authentication, routing, aggregation, bounded live fan-out, and typed dispatch only.
  It must not link to a second event store, migration set, channel-state
  implementation, or data-plane authorization engine.

## Privacy and capacity

- SaaS durable storage never contains event/state payloads or raw secrets.
- Source policy (enabled, mode, allowed channels/types, denied types, JSON Pointer
  redactions, metadata-only mode, max output size, policy version) redacts **before**
  source-instance egress. Local operator policy may only narrow centralized policy.
- Page tokens are opaque, authenticated, versioned, bound to normalized filters and
  caller scope, and expire in 15 minutes. They expose no SQL value and cannot be
  reused with changed filters.
- Multi-instance display order is `(timestamp, instance_id, event_id)`, not causal
  order. Multi-instance reads return `partial: true` with typed per-instance errors
  and authenticated page state.

## Failure modes and capacity envelope

- Bounded fan-out (max instances per multi-read, deadline/cancellation).
- Per-instance circuit breaker; partial errors surfaced in-band.
- Connector leases with fencing generation; a restart has no cross-org frame and no
  permanent post-backfill gap (dedupe by `(instance_id, event_id)`).
- Bounded WS frame size, NATS frame size, queue depth, and response body size.

## Consequences

- **Positive:** one engine, one contract, three SDKs + dashboard + fleet all speak the
  same API; CI enforces single-ownership; risk of drift is structural, not cultural.
- **Negative:** the gateway cannot offload work to a second store (it must call the
  canonical instance API); multi-instance queries are explicitly partial.
- **Cost:** the compatibility checker and manifest must be maintained alongside every
  route change — the cost is small and enforced by CI.