# API deprecation register (RUST-005)

Every temporary compatibility adapter, route alias, old dashboard adapter,
duplicate serializer, or protocol shim is recorded here with its owner,
replacement, telemetry, minimum support period, and removal version. Per
RUST-006, nothing is removed until all callers use the canonical contract,
compatibility telemetry is zero for a full published window, docs/SDKs are
updated, and direct/fleet parity plus regression suites pass.

There are **no active temporary compatibility adapters** at this time. The
canonical engine (`actae/src/events/*`, `actae/src/handlers/*`,
`actae/src/auth/*`, `actae/src/websocket/*`) is the single implementation of
every data-plane operation. This register exists to make future shims visible
and time-boxed.

## Register

| # | Shim / alias | Owner | Replacement | Telemetry | Min support | Removal version | Status |
|---|---|---|---|---|---|---|---|
| — | (none) | — | — | — | — | — | — |

## Existing backward-compatibility fields (kept, not deprecated)

The following wire fields are **retained** for older SDK clients and are NOT
deprecated (they have no removal target):
- `channel_cursor` / `channel_cursor_start` / `channel_cursor_end` — deprecated
  aliases for `cursor` / `cursor_start` / `cursor_end` on replay/query requests.
- `event_type` `type` alias on `PublishEventRequest`.
- `/api/portal/*` SaaS routes — compatibility aliases for the future
  `/api/v1/management` namespace during a published deprecation window (Phase 3).

## Deprecation policy (D-011)

1. Additive `actae.api.v1` schemas only; never rename/remove existing fields.
2. Any removal or type narrowing requires a new API version plus a published
   deprecation window recorded in this register **before** the change ships.
3. Removal is allowed only with RUST-006 evidence.

## Open item

- **Per-channel read enforcement on `POST /api/v1/events/query`.** The current
  direct query path does not authorize each requested channel against the
  principal's read patterns (pre-existing behavior; full-scope keys unaffected).
  Because this would be a behavior change for scoped keys, it must ship as an
  additive, backward-compatible tightening (e.g. honoring only channels the
  principal can read) with a compatibility window — it is not removed/shipped
  without the deprecation process above.