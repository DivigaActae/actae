# SDK compatibility and release gate

The Python, Go, and TypeScript clients consume the same
`spec/api-parity/v1/manifest.json`. Generated operation registries are checked
against all 75 manifest entries in every SDK test suite. A named helper is
preferred where one exists; all named fleet helpers use the gateway's manifest
dispatch route (there is no hidden direct-instance proxy). `call_operation`
(Python), `CallManifest` (Go), and `callOperation` (TypeScript) provide a
typed, manifest-bound escape hatch for every operation and reject
missing/unknown path parameters before I/O.

| Surface | Python | Go | TypeScript | Browser |
|---|---:|---:|---:|---:|
| Direct canonical HTTP/WS | 3.10+ | 1.25+ | Node 20+ | stream-only |
| Fleet inventory/query/fork/state | yes | yes | yes | no |
| Ticketed fleet stream | `stream(ticket=...)` | `StreamWithReconnect` | `streamWithReconnect` | `fleetStream` |
| Token exchange in browser | never | n/a | never | never |
| Shared deterministic vectors | required | required | required | n/a |

Fleet stream tickets are one-use and Origin-bound. Reconnect must obtain a new
ticket through a provider (`ticket_provider(cursor)` in Python,
`TicketProvider(ctx, cursor)` in Go, or `ticketProvider(cursor)` in
TypeScript); the last observed cursor is sent as `resume_cursor`. Delivery is at-least-once and SDKs deduplicate by
`(instance_id,event_id)` using bounded memory. The gateway's canonical cursor
subscription is the source of truth; clients never fabricate a cursor or
silently claim exactly-once delivery.

Hosted deployments may expose token exchange and the gateway on different
origins. Set `ACTAE_FLEET_TOKEN_URL` (or `tokenEndpoint`/`TokenEndpoint`) for
the organization-token exchange; ordinary fleet calls continue to use the
fleet gateway endpoint. Organization credentials are never persisted or
accepted by the browser stream package.

Release commands:

```bash
python3 -m pytest sdks/python/tests -q
(cd sdks/go && gofmt -l . && go vet ./... && go test -race ./...)
(cd sdks/typescript && npm test && npm run typecheck && npm run build && npm pack --dry-run)
python3 spec/api-parity/v1/compat_check.py
```

Hosted and relay black-box tests are opt-in and require deployed credentials:
`ACTAE_FLEET_E2E=1 pytest tests/test_fleet_gateway_e2e.py -v`.
