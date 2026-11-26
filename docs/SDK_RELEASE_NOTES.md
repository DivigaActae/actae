# SDK fleet release notes

## Next release

- Added manifest-generated operation registries and generic typed dispatch in
  Python (`call_operation`), Go (`CallManifest`), and TypeScript
  (`callOperation`).
- Added multi-instance inventory/query, opaque pagination, partial-result
  errors, fork/state/event helpers, and transport-neutral fleet sessions.
- Added one-use ticket fleet streams with bounded `(instance_id,event_id)`
  dedupe and reconnect cursor handoff. Browser TypeScript remains ticket-only.
- Added split manifest envelopes (`path_params`, `query`, `body`) so GET
  filters survive fleet dispatch without being mixed into mutation bodies;
  legacy combined `params` envelopes remain accepted by the gateway.
- Added a separate token-exchange endpoint option for hosted topologies and
  cursor-aware ticket providers for reconnect/resume.
- Added shared cross-SDK golden vectors, manifest parity tests, release
  examples, compatibility checks, and opt-in hosted/relay black-box tests.

This is additive. Existing direct clients and WebSocket APIs are unchanged.
Fleet mutations require an idempotency key (generated only for a single
one-shot call when omitted) and destructive operations require an explicit
confirmation target.
