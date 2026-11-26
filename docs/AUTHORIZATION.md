# Authorization Model

Actae's security model after the audit remediation (Phases 0–5 of
`AUDIT_REMEDIATION_PLAN.md`). This is the canonical reference for how
credentials are resolved and how every action is authorized, on **both**
transports (HTTP and WebSocket).

## One authorization service for every transport

There is a single authorization implementation: `auth/authorization.rs`.

```text
authenticate(credential, request_context)  -> Principal
authorize(Principal, Action, Resource)     -> Allow / Deny
```

- **HTTP** (`resolve_auth_context` in `main.rs`) and **WebSocket**
  (`WebSocketAuthenticator`) both build a `Principal` through
  `auth::authorization` and convert it to the `AuthContext` consumed by
  `EventHandler` (see `auth/types.rs`).
- **API keys resolve exactly once**: `resolve_api_key` hashes with
  `api_key_hash_secret` (HMAC-SHA256, the standard used by `actae keys
  create` and control-plane synced keys) and falls back to raw SHA-256 for
  legacy keys. There is no other hash/fallback logic anywhere. WS no longer
  uses a different (`jwt_secret`) hash — a key works identically over HTTP
  and WebSocket.

## Key scopes (`ApiKeyPermissions`)

Stored in `api_keys.permissions` as:

```json
{ "read": ["public.*"], "write": [], "delete": [], "admin": [] }
```

Each list holds **resource patterns**:

| Pattern | Matches |
|---------|---------|
| `*` | every channel |
| `project-a.*` | any channel starting with `project-a.` |
| `project-a.session-1` | exactly that channel |
| (empty list) | nothing for that action |

`admin` patterns grant every action on the matched resource. The legacy
capability-array shape (`["read", "write", "read:public.*"]`) is still
accepted for backward compatibility.

**Fail closed**: an empty, malformed, or unrecognized `permissions` payload
denies everything. A read-only key can never write; a channel-restricted key
can never touch other channels. Before the audit, permission JSON that did
not match the old parser silently granted full read/write.

## Authorization decisions

`AuthContext::has_permission(action, resource)` and the helpers
`can_read(channel)` / `can_write(channel)` apply `ApiKeyPermissions::check`
to the exact channel id. `EventHandler::validate_auth` runs this check for
every operation (record, replay, state, fork, metadata, groups, wakeups,
executions, and WebSocket subscribe/broadcast) — the channel id is the
resource, so scoped keys are enforced end to end. `system.*` and `admin.*`
channels are always denied. `list_channels` only returns channels the
caller may read.

The dashboard session JWT grants full access (`read`/`write`/`admin` all
`["*"]`).

## WebSocket specifics

- **No message is processed before a completed `auth` handshake.** A
  `subscribe`/`broadcast` sent before `auth` gets an `AUTH_REQUIRED` error
  and is dropped. (Anonymous mode still requires the handshake; the
  anonymous principal is then authorized like any other, with no grants.)
- The connection stores the **full `AuthContext`** (key id, key source,
  expiry, scopes). Subscribe and broadcast authorize against that principal
  on the exact channel — there is no synthesized full-access permission
  list.
- **Subscribe is authorized BEFORE live registration.** A synchronous
  `can_read(channel)` check runs before the connection is inserted into the
  topic subscriber set, so an out-of-scope key can never be a live
  subscriber (no registration ⇒ no live-event disclosure, even while the
  authoritative DB replay check runs). The `EventHandler` re-checks
  authoritatively before replay.
- **Request/ack correlation**: every WS publish carries a fresh
  `request_id`; the server echoes it in the Ack. SDKs dispatch acks by
  `request_id` (see `docs/TRANSPORT_PARITY.md`).

## Origin policy (WebSocket)

Browser connections are restricted to **same-origin by default**:
`WEBSOCKET_ALLOWED_ORIGINS` unset ⇒ the `Origin` header must match the
request `Host` (scheme derived from TLS). Configure exact origins
(`https://app.example.com`) for cross-origin; `*` is an explicit opt-in
wildcard. Matching is exact on (scheme, lowercase host, effective port) —
no substring matching, so allowing `https://example.com` never allows
`https://example.com.attacker.test`. CLI/server SDK clients that omit the
`Origin` header are always allowed.

**Managed-storage exception**: a bare `actae` on a loopback host (no TLS)
auto-configures a small allowlist instead of falling back to same-origin —
the Next.js dev server (`http://localhost:3000` / `http://127.0.0.1:3000`)
**and the Actae server's own origin** (`http://localhost:<port>` /
`http://127.0.0.1:<port>`, where `<port>` is the effective post-auto-bump
port). The own-origin entries matter because the managed instance *serves*
the dashboard: without them the browser's WS upgrade is rejected with
"Origin not allowed" and the LiveToggle shows Offline even while a stack is
running. A non-loopback `ACTAE_HOST` (or TLS) returns `None` → same-origin
policy; an operator-set `WEBSOCKET_ALLOWED_ORIGINS` always wins. The
auto-list matches only the `localhost`/`127.0.0.1` spellings — a custom
loopback-resolving hostname (container port-forwarders like
`i-<id>-eu1.localhost`) needs an explicit `WEBSOCKET_ALLOWED_ORIGINS` entry.

## Client IP (never trust the caller)

`X-Forwarded-For` is **never** trusted by default. The effective client IP
comes from the socket peer; `ACTAE_TRUSTED_PROXIES` (a CIDR list) opts a
deployment into honoring forwarding headers, and only from immediate peers
in that list (rightmost-untrusted chain semantics).

Every auth, audit, lockout and rate-limit path consumes the **single trusted
accessor** `authorization::client_ip(headers)`, which reads ONLY the
middleware-derived `x-actae-client-ip` header. There is no fallback to a
client-supplied `X-Forwarded-For` anywhere: if the middleware did not run,
callers have no IP rather than a spoofed one. (The two places `main.rs`
reads the raw header are the trusted-proxy middleware itself and the WS
connection identity derivation — both gate on the socket peer being a
configured trusted proxy.)

## Revocation and scope refresh

- New authentications re-verify key status on every cached WS auth (one
  indexed SELECT), so a revoked key fails immediately.
- Open sockets are re-authorized by a 30s `credential_reauthorizer` sweep
  and immediately after control-plane syncs. Each pass re-resolves the
  **full key row** and either:
  - de-authenticates a revoked/expired/deleted key (principal cleared,
    subscriptions dropped, `AUTH_REVOKED` error frame), or
  - **atomically replaces** the socket's `AuthContext` with the key's
    current permissions, drops subscriptions the narrower read scope no
    longer covers, and clears permission caches — so a scope reduction
    (write → read-only, or a narrower channel set) takes effect on open
    sockets without a reconnect.
- The control-plane notifier fires on key **updates** (upserts) as well as
  revocations, so scope changes propagate immediately.
- **DB-failure policy (explicit)**: if the pool cannot hand out a
  connection, or the single batched key query fails, the sweep pass is
  aborted and retried on the next tick; we never keep sockets on
  unverifiable permissions for long, and never kill every socket on a
  transient DB error (its own DoS). All keys are fetched in ONE `ANY` query
  per pass, so the sweep is O(keys + sockets) round trips rather than
  O(sockets). Sweep passes are mutex-serialized so the 30s task and the
  control-plane notifier never overlap. New authentications always hit the
  DB and fail closed.
- CLI `actae keys revoke` runs in a separate process; it updates the DB and
  the sweep picks it up within 30s.

## Stable errors

Authentication backend failures never leak database/configuration detail.
HTTP returns `{"status": "authentication_backend_error", "request_id": ...}`
and logs the detail server-side; WebSocket sends a generic
`Authentication failed`.

## Capability introspection

`GET /api/v1/auth/capabilities` returns the caller's parsed
`ApiKeyPermissions` plus `expires_at` and `key_source`, so SDKs can expose
`can_read/can_write/can_publish/can_fork` before attempting an operation.
