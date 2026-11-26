# Threat model

Scope: the single-node runtime, its dashboard, and the SDKs. The managed control
plane is out of scope for this document.

## Assets

1. Agent execution history and state (may contain prompts, tool I/O, customer data).
2. API keys and their permissions.
3. The event log's integrity (a forged event is a forged history).
4. Side-effect ledger entries (a forged claim can cause duplicate or lost work).
5. Local database files and backups.

## Trust boundaries

| Boundary | From | To | Control |
|---|---|---|---|
| B1 | Browser | Dashboard HTTP/WS | Same-origin policy, API key or JWT, CSRF on state-changing routes |
| B2 | SDK/client | HTTP API | API key, per-resource permissions, rate limit |
| B3 | SDK/client | WebSocket | Auth handshake before any subscribe/publish; per-channel authorization |
| B4 | Runtime | Embedded PostgreSQL | Local socket, file permissions |
| B5 | Operator | CLI/config | File permissions on `~/.actae` |
| B6 | Runtime | Control plane | Opt-in enrollment, TLS, Ed25519 license verification |

## Threats and mitigations

| # | Threat | Mitigation | Status |
|---|---|---|---|
| T1 | Unauthenticated WS subscribe reads events | Auth required before subscribe; negative test | Implemented |
| T2 | Cross-origin browser connects to local API | Origin allowlist, exact (scheme, host, port) match, same-origin default | Implemented |
| T3 | API key leaks in logs | Keys hashed at rest; redaction in logs; key printed once on first run | Implemented |
| T4 | Spoofed `X-Forwarded-For` bypasses rate limits | Trusted-proxy CIDR list, peer address by default | Implemented |
| T5 | Replay of a broadcast message | Request-id correlation on acks; per-channel cursor | Implemented |
| T6 | Fork inherits a side effect and re-fires it | Tool policies: replay recorded result, block, or run live | Implemented |
| T7 | Two workers claim the same task | Lease + fencing token; stale writes rejected | Implemented |
| T8 | Malformed permissions grant access | Parser fails closed (empty = deny) | Implemented |
| T9 | Local dashboard exposed to the network | Binds loopback by default; explicit `--host` required | Verify in release checklist |
| T10 | Malicious event payload (XSS in dashboard) | Output encoding; CSP; no `dangerouslySetInnerHTML` on event data | Verify per release |
| T11 | Supply chain (dependency compromise) | Lockfiles, Dependabot, cargo-deny, SBOM, provenance attestation | Implemented |
| T12 | Secrets in the repository | gitleaks in CI, history never published | Implemented |

## Out of scope

- Denial of service through resource exhaustion on a host you control.
- Attacks requiring root on the machine running Actae.
- The managed service, its multi-tenancy and billing.
- Third-party dependencies (report upstream; we track advisories).

## Open items

- T9 and T10 need a per-release manual check; they are not yet automated.
- The control-plane client is still compiled into this tree; see
  `docs/control-plane-split.md`.
