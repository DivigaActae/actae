# Transport Parity Contract

HTTP and WebSocket are two views of the same event store. This is the
**tested contract** (selling point 4) enforced by
`tests/test_transport_parity.py` against a live Actae:

| Concern | HTTP | WebSocket |
|---------|------|-----------|
| Authentication | `X-API-Key` / Bearer JWT | `auth` message (same credential, same hash) |
| Authorization | `EventHandler::validate_auth` on the channel | same function, on the exact subscribe/broadcast topic |
| Event representation | `Event {id, channel_id, type, payload, actor, cursor, timestamp}` | Ack / broadcast carry the identical shape |
| Idempotency | `operation_id` on record | `operation_id` on publish (same replay semantics) |
| Error codes | `err_to_status` codes | `Error` frames (`AUTH_REQUIRED`, ...) |
| Subscription lock | enforced at auth | enforced at auth |

## What parity means operationally

- A channel created over WS is addressable over HTTP and vice versa (shared
  channel-id grammar, `[A-Za-z0-9._:-]`, 1..=256 bytes — no slashes).
- A publish over WS is immediately re-playable over HTTP with the same
  cursor/payload.
- Retrying a publish with the same `(topic, operation_id)` returns the
  original event over both transports.
- An unauthenticated WS connection cannot subscribe or broadcast: the
  server answers with `AUTH_REQUIRED` before processing anything.
- A read-restricted key that requests a forbidden channel gets a
  `subscription_error` and is **not** registered as a live subscriber —
  events broadcast on that channel never reach it (parity suite
  `test_scope_denied_subscribe_never_receives_live_events`, gated on
  `ACTAE_RESTRICTED_KEY`).
- Auth rate limiting keys on the trusted peer IP, not a client-supplied
  `X-Forwarded-For`: the parity suite
  `test_auth_rate_limit_ignores_spoofed_forwarded_for` proves rotating
  spoofed XFF values cannot bypass the per-IP bucket (a 429 is returned
  once the real peer's bucket is exhausted).

## Ack correlation

Every WS publish carries a fresh `request_id`; the server echoes it in the
Ack. SDKs dispatch acks by `request_id` so concurrent publishes (and a
delayed ack after a timeout) can never be consumed by the wrong caller.
Acks without a `request_id` (legacy servers) fall back to a FIFO queue.

## Running the suite

```bash
ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-... \
  python3 -m pytest tests/test_transport_parity.py -v
```

Each scenario runs the same assertions over HTTP and WS, so a divergence in
representation, authorization, idempotency, or error behavior fails the
test.
