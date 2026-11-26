# Actae Server Internals

Execution-group coordination and Group Fork routes are implemented by `handlers::execution_groups`; see [Execution Groups](EXECUTION_GROUPS.md) for persistence invariants and SDK contracts.

Dense technical reference for the Rust server in `actae/` (crate `actae`,
axum 0.8 + tokio-postgres + sonic_rs). Covers HTTP routing, the WebSocket
protocol, auth/security, the background scheduler, and managed-storage mode.
Line numbers refer to the current checkout; treat them as anchors, not
contracts. This file is written for LLM context injection and for humans who
need the exact identifiers, not prose.

---

## 1. Source map (`actae/src/`)

| Area | Files | Contents |
|------|-------|----------|
| Server bootstrap | `main.rs` (~1690 L) | Env config load, DB pools, security manager, scheduler registry, router build (all routes live here), WS upgrade, `resolve_auth_context`, graceful shutdown, managed-storage startup, CLI dispatch |
| API modules | `api/mod.rs`, `api/auth.rs`, `api/user.rs`, `api/instance.rs` | `/api/v1/auth/*` + `/api/v1/user/*` + `/api/v1/instance` routes (the only handlers not registered directly in `main.rs`) |
| Handlers | `handlers/{events,state,channels,groups,experiments,wakeups,executions}.rs` | HTTP handler bodies behind the routes in `main.rs` |
| WebSocket | `websocket/{message,server,handler,connection_manager,delivery,mod,connection,batching,backpressure,batch_broadcaster,security}.rs` | Message model + validation, connection lifecycle, broadcast/delivery, batching |
| Events | `events/{handler,schema,store,page_token}.rs` | Event recording, replay, cursor, hash chain, retention purge, usage outbox, keyset page tokens |
| Auth | `auth/{mod,api_key,local_keys,jwt,authorization,websocket_auth,types,rate_limiter}.rs` | API-key hashing/provisioning, JWT, HTTP+WS auth, permission parsing |
| Security | `security/{mod,rate_limit,validator,cache,monitor,...}.rs` | `SecurityManager`, `MultiRateLimiter`, connection validation, threat monitor |
| Control plane | `control_plane/{mod,client,sync,enrollment,license}.rs` | SaaS sync, key deltas, subscription lock, Ed25519 license verification |
| Scheduler | `scheduler/{mod,checkpoint}.rs` | `TaskScheduler` + `ServerCheckpoint` persistence |
| Embedded PG | `embedded_pg.rs` (~2460 L) | Managed PostgreSQL: binary resolution, port bumping, wipe, orphan reclaim, data-dir lock |
| Config | `config/mod.rs` (~1276 L), `config/logging.rs` | Env-var config, `redact_db_url()` |
| Misc | `https_server.rs`, `dashboard/dashboard.rs`, `connection_pool/connection_pool.rs` | TLS serving, dashboard static fallback, PG pool with circuit breakers |
| Fleet gateway (2nd binary, `federation-gateway` feature) | `federation_gateway/{config,auth,saas,dispatch,merge,stream,bus}.rs`, `bin/federation_gateway.rs` | `actae-federation-gateway`: typed fleet routes → canonical instance API only (RUST-004/RUST-007), `fleet_access` token auth, multi-instance query merge, hosted `/ws` connector + one-use ticket gate, in-memory/NATS live bus, strict config |

Global state pattern: `api/mod.rs` keeps `GLOBAL_SERVER_STATE: OnceLock<AppServerState>`
(set via `set_global_state`, read via `global_state()`/`try_global_state()`) because
axum 0.8 only serves `Router<()>` — state is fetched globally instead of via
extractors.

---

## 2. HTTP routing

All routes are registered in `main.rs:675-810` on the main router (same port
serves API + dashboard + WS + health). The only nested routers are
`api/mod.rs:51-56`: `.nest("/api/v1/auth", auth::routes())`,
`.nest("/api/v1/user", user::routes())` and
`.nest("/api/v1/instance", instance::routes())`.

The authoritative route-to-owner inventory is `spec/api-parity/v1/manifest.json`
(75 operations + WS messages). CI (`spec/api-parity/v1/compat_check.py` +
`tests/test_api_contract.py`) fails when a new public route has no manifest
entry, when a route is registered outside the router set, or when a v1 field is
removed. See `docs/adr/ADR-API-FIRST-FLEET.md`.

### 2.1 Full route table

| Method | Path | Handler module | Notes |
|--------|------|----------------|-------|
| GET | `{health_check_path}` (default `/healthz`) | `health_check_handler` | DB + connection + system components |
| GET | `{readiness_check_path}` (default `/readyz`) | `readiness_check_handler` | 503 when connection utilization > 95% or DB not ready |
| GET | `{metrics_path}` (default `/metrics`) | `metrics_handler` | API-key gated |
| GET | `/metrics.json` | `metrics_json_handler` | API-key gated |
| POST | `/api/v1/events/record` | `handlers::events::record_event_handler` | Record a new agent event |
| POST | `/api/v1/events/transition` | `handlers::events::transition_handler` | Atomic event+state transition |
| GET | `/api/v1/events/replay/{channel_id}` | `handlers::events::replay_events_handler` | Replay events from a channel |
| POST | `/api/v1/events/query` | `handlers::events::query_events_handler` | Query events across channels |
| GET | `/api/v1/events/cursor/{channel_id}` | `handlers::events::latest_cursor_handler` | Latest cursor for resume |
| GET | `/api/v1/events/verify/{channel_id}` | `handlers::events::verify_chain_handler` | Tamper-evident hash-chain verification |
| GET | `/api/v1/channels` | `handlers::events::list_channels_handler` | List active channels |
| POST | `/api/v1/state/{channel_id}` | `handlers::state::save_state_handler` | Cursor-aligned snapshot |
| GET | `/api/v1/state/{channel_id}` | `handlers::state::load_state_handler` | Latest snapshot |
| GET | `/api/v1/state/{channel_id}/versions` | `handlers::state::list_states_handler` | Version history |
| GET | `/api/v1/state/{channel_id}/version/{version}` | `handlers::state::load_state_version_handler` | Specific version |
| DELETE | `/api/v1/state/{channel_id}/version/{version}` | `handlers::state::delete_state_version_handler` | Delete a version |
| POST | `/api/v1/channels/fork` | `handlers::channels::fork_channel_handler` | Fork at a cursor |
| GET | `/api/v1/channels/diff` | `handlers::channels::diff_channels_handler` | Structural state diff |
| GET | `/api/v1/channels/compare` | `handlers::channels::compare_channels_handler` | "What changed?" comparison |
| PATCH | `/api/v1/channels/{channel_id}/outcome` | `handlers::channels::set_outcome_handler` | Experiment outcome |
| POST | `/api/v1/channels/{channel_id}/promote` | `handlers::channels::promote_channel_handler` | Promote fork |
| DELETE | `/api/v1/channels/{channel_id}` | `handlers::channels::delete_channel_handler` | Delete channel |
| GET | `/api/v1/channels/{channel_id}/metadata` | `handlers::channels::channel_metadata_handler` | Read metadata |
| PUT | `/api/v1/channels/{channel_id}/metadata` | `handlers::channels::update_metadata_handler` | Update metadata |
| GET | `/api/v1/channels/{channel_id}/steps` | `handlers::channels::latest_step_handler` | Highest indexed step (0 = none); O(1) recovery probe |
| GET | `/api/v1/channels/{channel_id}/steps/{step_number}` | `handlers::channels::resolve_step_handler` | Server-owned step index |
| GET | `/api/v1/channels/{channel_id}/forks` | `handlers::channels::list_forks_handler` | Direct children |
| GET | `/api/v1/channels/{channel_id}/tree` | `handlers::channels::execution_tree_handler` | Recursive execution tree |
| GET | `/api/v1/channels/{channel_id}/trail` | `handlers::channels::decision_trail_handler` | Lineage + boundary + tool ledger |
| GET | `/api/v1/channels/{channel_id}/receipt` | `handlers::channels::fork_receipt_handler` | Immutable fork receipt |
| POST | `/api/v1/channels/{channel_id}/close` | `handlers::channels::close_channel_handler` | Stamp `closed_at` |
| POST/GET | `/api/v1/groups` | `handlers::groups::{create,list}_group_handler` | Consumer groups |
| DELETE | `/api/v1/groups/{group_id}` | `handlers::groups::delete_group_handler` | Delete group |
| POST | `/api/v1/groups/{group_id}/join` | `handlers::groups::join_group_handler` | Join group |
| POST | `/api/v1/groups/{group_id}/work` | `handlers::groups::claim_work_handler` | Lease-based work claim |
| POST | `/api/v1/groups/{group_id}/ack` | `handlers::groups::ack_handler` | Ack + advance offset |
| POST | `/api/v1/groups/{group_id}/heartbeat` | `handlers::groups::heartbeat_handler` | Renew lease |
| GET | `/api/v1/groups/{group_id}/offsets` | `handlers::groups::offsets_handler` | Per-consumer offsets |
| POST/GET | `/api/v1/experiments` | `handlers::experiments::{create,list}_experiment_handler` | Experiment platform |
| GET | `/api/v1/experiments/{group_id}` | `handlers::experiments::get_experiment_handler` | Get experiment |
| POST | `/api/v1/experiments/{group_id}/members` | `handlers::experiments::add_experiment_member_handler` | Add member |
| GET | `/api/v1/experiments/{group_id}/rank` | `handlers::experiments::rank_experiment_handler` | Ranking |
| POST/GET | `/api/v1/scheduler/wakeups` | `handlers::wakeups::{create,list}_wakeup_handler` | Persisted wake-ups |
| GET/DELETE | `/api/v1/scheduler/wakeups/{id}` | `handlers::wakeups::{get,cancel}_wakeup_handler` | Wake-up by id |
| POST | `/api/v1/executions/claim` | `handlers::executions::claim_execution_handler` | Idempotent tool-execution claim |
| GET | `/api/v1/executions` | `handlers::executions::list_executions_handler` | List executions |
| GET/DELETE | `/api/v1/executions/{id}` | `handlers::executions::{get,delete}_execution_handler` | By id |
| POST | `/api/v1/executions/{id}/complete` | `handlers::executions::complete_execution_handler` | Complete |
| POST | `/api/v1/executions/{id}/fail` | `handlers::executions::fail_execution_handler` | Fail |
| POST | `/api/v1/executions/{id}/heartbeat` | `handlers::executions::heartbeat_execution_handler` | Heartbeat |
| POST | `/api/v1/executions/{id}/cancel` | `handlers::executions::cancel_execution_handler` | Cancel |
| POST | `/api/v1/auth/signup` | `api::auth::signup_handler` | 403 unless `ENABLE_SIGNUP=true` |
| POST | `/api/v1/auth/login` | `api::auth::login_handler` | Enrolled → control-plane credential check; standalone → local `public.user` |
| POST | `/api/v1/auth/sso` | `api::auth::sso_handler` | Exchange a control-plane SSO handoff token for a session JWT |
| POST | `/api/v1/auth/setup` | `api::auth::setup_handler` | First-run: redeem the one-time setup token to set the admin password (standalone, `setup_required` only) |
| POST | `/api/v1/auth/api-key-login` | `api::auth::api_key_login_handler` | Key → session JWT (programmatic; not the dashboard default) |
| POST | `/api/v1/auth/local-session` | `api::auth::local_session_handler` | Loopback dashboard auto-login: mints an admin session for a same-machine browser (standalone only; refused for non-loopback peer/`Host`/`Origin` or `ACTAE_LOCAL_AUTO_LOGIN=0`) |
| GET | `/api/v1/auth/config` | `api::auth::auth_config_handler` | `{"enable_signup": bool, "sso": bool, "setup_required": bool, "local_auto_login": bool}` |
| GET | `/api/v1/auth/me` | `api::auth::me_handler` | Session user (incl. `role`) |
| POST | `/api/v1/auth/logout` | `api::auth::logout_handler` | Revoke the caller's session (`jti`) |
| POST | `/api/v1/auth/logout-all` | `api::auth::logout_all_handler` | Revoke every session for the user ("sign out everywhere") |
| GET | `/api/v1/auth/capabilities` | `api::auth::capabilities_handler` | Key scopes (`can_read` etc.); additive: `api_version`, `engine_version`, `scopes`, `limits`, `operations` |
| GET/POST | `/api/v1/users` | `api::users::{list,create}_user_handler` | Admin: list / create dashboard users |
| PATCH/DELETE | `/api/v1/users/{id}` | `api::users::{update,delete}_user_handler` | Admin: update role/name / delete (last-admin + self guards) |
| POST | `/api/v1/users/{id}/password` | `api::users::reset_password_handler` | Admin: reset a user's password (revokes their sessions) |
| GET/PUT | `/api/v1/user/preferences` | `api::user::{get,put}_preferences_handler` | Opaque per-identity prefs |
| GET | `/api/v1/instance` | `api::instance::instance_handler` | Instance identity (install_id, version, mode, features) for federation/diagnostics |
| GET | `/ws` | `ws_upgrade_handler` (`main.rs:1303`) | WebSocket upgrade |

Everything else falls through to `actae::dashboard::fallback_router()`
(dashboard static files — lowest priority).

### 2.2 Middleware stack (applied outer→inner at `main.rs:776-810`)

1. `SetResponseHeaderLayer` security headers: `x-content-type-options: nosniff`,
   `x-frame-options: DENY`, `referrer-policy: strict-origin-when-cross-origin`,
   CSP `default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self'
   'unsafe-inline'; connect-src 'self' wss:; img-src 'self' data:; font-src 'self'`.
2. CORS: from `HTTP_ALLOWED_ORIGINS` (list) or wide-open `Any` when unset.
3. `DefaultBodyLimit::max(2 * 1024 * 1024)` (2 MB body cap). Per-payload caps
   are enforced per-route in the handlers: 1 MiB (`MAX_EVENT_SIZE`,
   `handler.rs:23`) for event payloads, state snapshots (publish-with-state +
   transition), and tool-execution bodies — `params` (claim,
   `validate_tool_payload_size`, `handler.rs:589`), `result` (complete), and
   `error` message+stack (fail, `validate_tool_error_size`). A body under the
   2 MB HTTP cap but over the 1 MiB payload cap returns `400 InvalidRequest`.
4. `resolve_peer_ip_middleware` — real client IP from socket peer; trusts
   `X-Forwarded-For` only from configured proxies; records `x-actae-client-ip`.
   The WS upgrade path (`main.rs:1312-1339`) does the same via
   `resolve_client_addr(peer, xff, &trusted_proxies)`.
5. `access_log` — one line per request (`HTTP request method=... path=...
   status=... duration_ms=...`) at **debug** (the dashboard polls every few
   seconds, so info would flood the console); excludes probes/dashboard even
   at debug unless `ACTAE_ACCESS_LOG_ALL=1`. The `tokio_postgres` NOTICE
   stream is silenced at warn by default (`config/logging.rs`).

### 2.3 Server startup & shutdown (`main.rs:812-899`)

- TLS on → `HttpsServer` (validate TLS files, fail fast, stop embedded PG);
  HTTP → `axum_server`. In managed-storage mode the socket was already bound
  (`http_listener`) so the HTTP port is reserved before PG work starts.
- Bind failure is reported through a oneshot `server_failure_rx` (never
  `process::exit`), so embedded PG is always stopped gracefully.
- Shutdown: `tokio::select!` on SIGINT / SIGTERM / SIGHUP / server failure,
  then the graceful shutdown path (stops WS server, scheduler, embedded PG via
  `pg_ctl stop` bounded by timeout, then kill+reap).

### 2.4 Health/readiness/metrics

- `/healthz` (default): `{"service":"actae","version":...,"status":"ok|unhealthy|error",
  "instance_id":..., "mode":"embedded|production", "embedded_pg":bool,
  "components":{"database":{main_pool_healthy,client_pool_healthy,...},
  "connections":{active_connections,utilization_percent,max_connections,...},
  "system":{uptime_seconds,process_id,threads}}}`. 503 when DB unhealthy.
- `/readyz` (default): 503 when `utilization_percent > 95.0` or DB not ready.
- `/metrics` + `/metrics.json`: API-key gated (`X-API-Key` or dashboard Bearer
  JWT; anonymous never passes) — a locked instance stays observable.
  `/metrics.json` includes `forks: {total}` and `recoveries: {total}` (the
  local `actae_forks_total` / `actae_recoveries_total` counters: fork and
  same-channel crash-recovery operations on this instance, always available
  with no phone-home). The same events are metered transactionally to
  `usage_delivery_outbox` as the `fork_operations` / `recovery_operations`
  usage metrics and delivered to the control plane only when the instance is
  linked. See `docs/MEASUREMENT.md`.

### 2.5 Keyset pagination on `POST /api/v1/events/query` (page tokens)

Phase 1 additive contract (section 2.2 of the product plan). A query without a
`page_token` runs the **legacy offset-based path unchanged** (D-011). When a
`page_token` is supplied the query continues from an opaque, authenticated
`global_cursor` bound (`events/page_token.rs`):

- **Opaque** — base64url payload + HMAC-SHA256 (`api_key_hash_secret`,
  domain-separated); exposes no SQL value.
- **Versioned** — `pt_v1.<claims>.<sig>`; a format change mints a new version.
- **Filter-bound** — the server re-derives `filter_fingerprint` from the
  CURRENT request (sorted channel ids, type, actor, cursor bounds, from/to) and
  rejects reuse with changed filters (`400 invalid_page_token`).
- **Scope-bound** — `principal_scope_fingerprint` (identity + read grants);
  replay by a different principal is rejected.
- **Short-lived** — 15 minutes; expired reuse returns `410 page_token_expired`.
- **Retention-aware** — the handler compares the token bound against
  `store.min_global_cursor()`; if retention purged the continuation,
  `410 page_token_unsatisfiable`.
- **No omission/duplication** — `store.query_page` orders by the internal
  `global_cursor` (stable cross-channel order), uses strict `global_cursor <
  bound` keyset continuation, and fetches `limit + 1` rows to compute
  `has_more` without a second query.

Response adds `next_page_token` (null on the last page) and `has_more`; the
existing `events`/`count` fields are unchanged.

---

## 3. WebSocket protocol

Implementation: `websocket/message.rs` (message model + validation + zero-copy
serialization), `websocket/server.rs` (connection loop + batching),
`websocket/handler.rs` (routing + auth gate + subscribe/broadcast),
`websocket/connection_manager.rs` (subscriptions, replay, cleanup). The format
is explicitly "compatible with the Go Actae message format". JSON is built and
parsed with `sonic_rs`; every message has a `"type"` discriminator.

### 3.1 Wire format (client → server)

| `type` | Fields | Notes |
|--------|--------|-------|
| `subscribe` | `topic: string` (required), `cursor?: i64`, `timestamp?: i64` | Cursor = replay from cursor |
| `unsubscribe` | `topic: string`, `timestamp?: i64` | |
| `broadcast` | `topic`, `payload` (required), `timestamp?: i64`, `operation_id?: uuid-string`, `request_id?: string` | Publish; `operation_id` makes retries idempotent; `request_id` echoed verbatim in the Ack |
| `message` | `topic`, `payload`, `timestamp?: i64` | Direct message (same shape as broadcast, separate type) |
| `database_change` | `topic`, `event` (`INSERT`/`UPDATE`/`DELETE`), `payload`, `timestamp?: i64` | CDC-style |
| `auth` | `token?: string`, `api_key?: string`, `payload` (null/missing tolerated → `{}`) | Handshake |
| `ping` | (shorthand `{"type":"ping"}`) | Accepted as `connection/event=ping`; handler replies `pong` |

### 3.2 Wire format (server → client)

| `type` | Fields | Notes |
|--------|--------|-------|
| `connection` | `event: string`, `payload` | `connected` (welcome, `payload={connection_id, timestamp}`), `authenticated` (`{"status":"success"}`), `ping`, `pong` |
| `subscription` | `topic`, `event` (`subscribed`/`unsubscribed`/`error`), `payload` | Confirmation |
| `cursor_sync` | `topic`, `cursor: i64` | Sent after subscribe-with-replay |
| `broadcast` | `topic`, `payload`, `timestamp?`, `operation_id?`, `request_id?` | Delivery to subscribers |
| `error` | `message: string`, `code?: string`, `details?: value` | Error codes below |
| `ack` | `id: uuid`, `cursor?`, `channel_cursor?`, `timestamp?`, `payload?`, `channel_id?`, `event_type?`, `actor?`, `request_id?` | Full event envelope after a persisted broadcast |
| `nack` | `id: uuid`, `reason?: string` | Negative ack (client → server too) |

### 3.3 Error codes (in `error.code`)

`AUTH_REQUIRED`, `INVALID_JSON`, `INVALID_FORMAT`, `INVALID_TOPIC`,
`PAYLOAD_TOO_LARGE`, `MISSING_REQUIRED`, `INVALID_CHARACTERS`,
`SECURITY_VIOLATION`, `RATE_LIMIT_EXCEEDED`, `MESSAGE_BLOCKED`,
`RATE_LIMITED`, `SECURITY_ERROR`, `INTERNAL_ERROR`.

### 3.4 Validation rules (`Message::validate`, `message.rs:425-694`)

- Topic: 1..=256 bytes; charset `[A-Za-z0-9._:-]` only (no `/`, no whitespace);
  rejects `..` and `//` (path traversal), SQL keywords (`select`, `insert`,
  `update`, `delete`, `drop`, `union`, `script`, `javascript`).
- Subscribe: max 3 `*` wildcards; cannot subscribe to `system.`/`admin.` topics.
- Broadcast: cannot target `system.`/`admin.`; payload object must not contain
  `cmd`/`command`/`exec` keys; payload object keys must not start `__` or be
  `constructor`/`prototype`; string values scanned for XSS (`<script`,
  `javascript:`, `onclick`, `onerror`, `onload`) and SQLi (`'; drop`,
  `union select`, `1=1`, `or '1'='1'`) patterns.
- Payload limits: nesting depth ≤ 20, string ≤ 100000 bytes, array ≤ 10000
  elements, object key ≤ 1000 chars, serialized payload ≤ `max_payload_size`
  (WS_MAX_MESSAGE_SIZE, 1 MB default).
- `database_change`: event must be `INSERT`/`UPDATE`/`DELETE`; topic must not
  contain `information_schema`, `pg_`, `mysql`.
- Parse guard (`from_json`): JSON nesting depth is checked before `sonic_rs`
  parse (`json_compat::json_nesting_depth` > `MAX_JSON_DEPTH` → error) — a
  hostile deep payload cannot stack-overflow the server.
- `MessageValidation` enum: `Valid | InvalidFormat | MissingRequired |
  InvalidTopic | PayloadTooLarge | InvalidCharacters | SecurityViolation |
  RateLimitExceeded`.

### 3.5 Connection lifecycle (`server.rs:316-439`)

1. On upgrade: welcome `connection/connected` with `connection_id` + epoch-ms
   timestamp is queued via the connection manager.
2. `tokio::select!` loop: incoming WS frames (text → `handle_incoming_message`,
   close/ping/pong/binary, errors) vs. outgoing flume channel vs. shutdown
   broadcast.
3. **Auth gate** (`handler.rs:83-109`): every message except `auth` and
   `connection/event=ping` requires a completed handshake (`auth_context` set
   on the connection); otherwise `error/code=AUTH_REQUIRED`. (Previously an
   anonymous connection could replay any channel's history.)
4. Outbound batching: when an outgoing message arrives, drain the flume channel
   with a 200 µs aggregation window, sending at most 16 frames per write
   (`Vec::with_capacity(16)`).
5. Cleanup: `remove_connection` on exit; inactive-connection reaper and
   per-IP/per-user limits run in the background.

### 3.6 Subscribe & broadcast flow (`handler.rs`)

- **Subscribe** (`handle_subscribe_message`): validates topic, registers with
  the connection manager, sends `subscription/subscribed`, and — when a cursor
  is provided — replays persisted events (replay limit 1000,
  `connection_manager.rs:460`) before sending `cursor_sync` with the latest
  cursor. Replay messages arrive as `broadcast`-type deliveries.
- **SubscribeMany** (`handle_subscribe_many_message`, additive `actae.api.v1`):
  a single frame `{"type":"subscribe_many","topics":[{"topic","cursor"}...],
  "request_id"}`. Each topic runs through the exact same `subscribe_one` path as
  a single subscribe (authorization → registration → cursor replay →
  confirmation), then a `connection/event=subscribed_many` summary frame with
  per-topic `results` and the echoed `request_id` is sent. Bounded to
  `Message::MAX_SUBSCRIBE_MANY_TOPICS` = 100 topics per frame. No second WS
  endpoint (D-001).
- **Broadcast** (`handle_broadcast_message`, `handler.rs:686-815`): re-checks
  the auth context, enforces `can_write(topic)`, then builds a
  `PublishEventRequest` (`event_type: "broadcast"`, `actor` = connection
  `user_id` or `"anonymous"`, `operation_id` passthrough) and calls
  `event_handler.record(request, &auth_context, Some(connection_id))`. The
  third argument is the sender connection id, so **broadcasts are not echoed
  back to the publishing connection**. On success the publisher gets an `ack`
  with the full persisted event envelope; on failure a stable `error` message
  ("Record failed" — real error is logged server-side).
- **Ping** → `connection/event=pong` reply.
- Unauthenticated/non-handshake messages → `AUTH_REQUIRED`; unknown types →
  generic `error` "Unsupported message type".

### 3.7 Delivery & backpressure

- `connection_manager` delivers via per-connection flume channels
  (`OutgoingMessage::Serialized(Arc<Bytes>)` for pre-serialized fan-out, or
  `Message`); full queues drop messages with a warning (slow consumer), and a
  slow connection is dropped after consecutive send failures
  (`delivery.rs:480`).
- Fan-out: broadcast to a topic iterates subscribers with the sender excluded;
  a ring-buffer path kicks in at high fan-out (≥100 subscribers,
  `RING_FANOUT_THRESHOLD`; ring capacity 65536, `RING_DEFAULT_CAPACITY`).
- Heartbeat: server-side ping cadence from `WS_PING_INTERVAL` (30 s default).

---

## 4. Authentication & security

### 4.1 HTTP auth resolution (`resolve_auth_context`, `main.rs:1102-1299`)

Order: **X-API-Key → Bearer JWT → anonymous** (all routes that take
`enforce_subscription`).

- **X-API-Key path**: pre-auth rate-limit check with the key prefix
  (`key_val[..8]`) as the lockout identity; 429 `{"status":"blocked"|"rate_limited",
  "retry_after_seconds":N}` when limited. Then `validate_api_key` (HMAC-SHA256
  of the key against the stored hash — legacy SHA-256 fallback verified; key
  rows carry `source` = `control_plane` or `local`). Success → `AuthContext::ApiKey
  {key_id, key_source, expires_at, permissions}`. Invalid key → 401
  `{"status":"unauthorized","error":"Invalid API key"}` (or anonymous
  fall-through when `ENABLE_ANONYMOUS=true`). Backend error → 500
  `{"status":"authentication_backend_error","request_id":...}` — details never
  leak (audit item 15).
- **Bearer JWT path**: `validate_token` (HS256, expiry, iss/aud) — dashboard
  sessions get `ApiKeyPermissions::full()`. 401 `{"status":"unauthorized",
  "error":"Invalid or expired session token"}` on failure.
- **No credentials**: subscription lock checked first (so locked instances
  explain why), then `ENABLE_ANONYMOUS`; else 401 "Authentication required".
  When anonymous is enabled it grants **both read and write** on HTTP *and*
  WebSocket (subscribe/publish), matching `EventHandler::validate_auth`'s
  anonymous short-circuit — an anonymous handshake is not accepted and then
  denied at the subscribe gate.
- Every resolution feeds `security_manager.record_auth_success/failure` (IP +
  key-prefix or user lockout accounting).
- `subscription_block` produces **402 `{"status":"subscription_required"}`**
  when the control plane reports a lapsed subscription and **503
  `{"status":"subscription_unverifiable"}`** when the plane is unreachable past
  the offline grace (`ACTAE_SUBSCRIPTION_GRACE_SECS`, default 259200 s / 72 h).
  JWT (dashboard) and metrics are exempt; every API key is subject to the lock.

### 4.2 Session JWTs (`api/auth.rs:147-195`)

- HS256, secret = `JWT_SECRET`; default TTL 24 h (`JWT_EXPIRATION` 86400 s);
  `iss` = `JWT_ISSUER` (default `actae`), `aud` = `JWT_AUDIENCE` (default
  `actae-clients`); claims include `sub`, `iat`, `nbf`, `jti` (random uuid),
  optional `auth_method` (`"sso"` for portal SSO, `"api_key"` for key-logins)
  and `metadata` (`{"key_name": ...}` for key-logins).
- `/api/v1/auth/api-key-login` refuses to mint a session when the subscription
  lock is active (402/503), so a lapsed key cannot bypass the lock with a JWT.
- `me` skips the users-table lookup for `auth_method == "api_key"` sessions
  (SaaS identity — no local user row; name comes from `metadata.key_name`).
  SSO sessions upsert a real `public.user` row keyed by the portal customer id,
  so `me` resolves them normally.
- **Portal SSO** (`mint_portal_session`): enrolled instances delegate login to
  the control plane. `login_handler` calls
  `ControlPlaneManager::authenticate_dashboard` →
  `POST /v1/installations/{id}/dashboard-token`; `sso_handler` verifies a
  control-plane-issued handoff token directly (the portal's "Open dashboard"
  path). Both verify `license::verify_dashboard_token` (Ed25519 signature,
  `scope == "dashboard-sso"`, TTL ≤ 15 min, `install_id` binding), claim the
  `jti` in `public.dashboard_sso_tokens` (single-use, migration v10), upsert
  the identity, and mint a session. Standalone instances have no `install_id`
  and reject SSO; they use local password login (`public.user`). A first-boot
  local admin is provisioned by `provision_initial_user` (`main.rs`) when the
  user table is empty and the instance is not enrolled.

### 4.3 API keys (`auth/local_keys.rs`, `auth/mod.rs`)

- Stored hashed (HMAC-SHA256 under `API_KEY_HASH_SECRET`, defaulting to
  `JWT_SECRET`); the raw key is shown once at creation. `actae keys create|
  list|revoke` CLI manages rows (revocation keyed on `api_keys.id`, surfaced
  as `key_id` in auth contexts).
- First boot with an empty `api_keys` table auto-provisions an initial key
  (`provision_initial_keys`, `main.rs:1527`): `ACTAE_INITIAL_API_KEY` if set,
  else a generated `sk-…` printed once in the banner (`source='local'`). The
  boot that mints/seeds a key also publishes it to
  `<ACTAE_CONFIG_DIR>/client.json` (`client_credentials.rs`, atomic 0600) so the
  SDKs authenticate with no copy-paste; `actae login` / `actae logout` manage
  the file (see `docs/CLIENT_CREDENTIALS.md`).
- Key permissions parsed via `parse_api_key_permissions` (`auth/authorization.rs`);
  `capabilities` endpoint exposes `auth_method`, `key_id`, `key_source`,
  `permissions`, `expires_at`.

### 4.4 WebSocket auth (`auth/websocket_auth.rs`)

- `auth` message → `WebSocketAuthenticator::authenticate` — tries JWT token
  first, then API key; result cached in a map keyed
  `api_key:<sha256-of-key-16-bytes>` with 300 s TTL and a 10000-entry cap so
  the DB isn't hit per message.
- Reauthorization: a control-plane revoke wakes `connection_manager.reauthorize_all`
  (immediate, via `revoked_notifier()`) and the `credential_reauthorizer`
  scheduler task bounds the window to ~30 s for out-of-process revokes.
- WS `AuthResult` mirrors HTTP: `Authenticated`/`ApiKey` carry `user_id` or
  `key_id` + permissions; failures are recorded against IP (and key prefix).

### 4.5 Rate limits (`security/mod.rs:94-110`, `security/rate_limit.rs`)

`MultiRateLimiter` with token buckets + TTL eviction (`cleanup_interval_seconds`
300):

| Limit | Rate | Capacity | Source |
|-------|------|----------|--------|
| IP connections | 10 | 50 | `websocket_max_connections_per_ip` |
| IP messages | 100 | 1000 | `websocket_max_message_rate` |
| IP auth | 5 | 20 | `ACTAE_IP_AUTH_RATE` / `ACTAE_IP_AUTH_CAPACITY` |
| User messages | 200 | 2000 | `websocket_max_message_rate` |
| User subscriptions | 10 | 100 | hardcoded |
| Global connections | `max_connections * 2` | `max_connections * 5` | `WS_MAX_CONNECTIONS` |
| Global messages | 10000 | 100000 | `websocket_max_message_rate` |

Auth failures accumulate to lockout (5 failures / 300 s per identity).
`SecurityManager` also runs connection validation (IP blocklist/allowlist),
a `SecurityCache` (auth cache 2000 / permission cache 10000 / TTL 300 s), a
threat monitor, and optional batch processor / advanced cache / crypto manager /
threat detection (all default-on, `ENABLE_*` toggles).

### 4.6 Control plane & license tokens (`control_plane/`)

- Client (`client.rs`): `REQUEST_TIMEOUT` 30 s, `CONNECT_TIMEOUT` 10 s;
  `DEFAULT_CONTROL_PLANE_URL = "https://saas.diviga.dev"`; endpoints:
  `/v1/installations/{id}/keys`, `/v1/installations/{id}/keys/ack`,
  `/v1/installations/{id}/usage` (submit + dedup), `/v1/device_codes`,
  `/v1/device_codes/poll`, `/v1/installations/enroll`,
  `/v1/installations/{id}/dashboard-token` (portal-credential → SSO handoff),
  `DELETE /v1/installations/{id}/enrollment` (logout), health.
- Sync loop: immediate first sync, then `ACTAE_SYNC_INTERVAL_SECS` (60 s)
  with exponential backoff; `rev`-based deltas; acks sent only after the plane
  durably dedupes; revocation drops keys from the next sync; informational
  fields (`cancel_at_period_end`, `current_period_end`) do NOT bump `rev`.
- **License tokens** (`license.rs`): every enforcement-relevant response
  carries `license` = `base64url(payload_json) "." base64url(ed25519_sig)` with
  canonical-JSON payload `{v:1, kid:"k1", install_id, subscription, rev,
  keys_sha256, enrollment_secret_sha256, iat, exp}`. `verify_license` enforces
  version, kid, install_id, clock skew ±300 s (`CLOCK_SKEW_SECS`), and TTL
  ≤ 30 days (`MAX_TOKEN_TTL_SECS`) — the anti-replay cap. A *valid* token
  declaring `suspended`/`cancelled` → hard 402; an invalid/expired/unknown-kid
  token → offline-grace/503 (never hard-locks the instance). Public key is
  embedded at build time via `ACTAE_LICENSE_PUBKEY` (base64url Ed25519;
  `build.rs` fails on invalid values); dev/test builds default to the shared
  test key (seed `f0734c77…`). `verify_license_signature` (signature only,
  no time checks) powers `actae auth status` diagnostics.
- Enrollment: device flow (CLI polls `/v1/device_codes/poll` every 5 s) or
  env/file credentials (`ACTAE_ENROLL_SECRET` / `ACTAE_ENROLL_KEY`); enrollment
  file lives under `ACTAE_CONFIG_DIR` (default `~/.actae`) with mode 0600;
  `ACTAE_ENROLL_KEY` is one-shot paste-to-enroll and idempotent (skipped when
  env credentials or the enrollment file exist).

### 4.7 User preferences (`api/user.rs`)

- `GET/PUT /api/v1/user/preferences` — opaque JSON object per identity, shared
  between dashboard (JWT) and SDK (X-API-Key) via the same `user_id` namespace.
- PUT body `{"prefs": {...}}` (whole-object replace, upsert), 413 over the
  64 KB cap (`MAX_PREFS_BYTES`), 400 for non-object envelope/prefs, 401 without
  credentials, 402/503 under the subscription lock. Table
  `public.preferences (user_id TEXT PK, prefs JSONB DEFAULT '{}', updated_at)`
  (migration v8). No per-IP rate limit by design (DoS surface equals a valid
  credential's).

---

## 5. Background scheduler

`TaskScheduler` (`scheduler/mod.rs`): tasks registered with `register(name,
period, timeout)` (fallible) or `register_infallible(name, period)`; the first
tick happens after one period (no immediate run); each run is bounded by its
timeout; runs are serialized per task. Error semantics: `TaskError::Transient`
→ warn + retry (escalates to disable after 10 consecutive), `TaskError::Permanent`
→ the task is disabled. Metrics: `actae_scheduler_task_runs_total`,
`actae_scheduler_task_errors_total`, `actae_scheduler_task_duration_seconds`
(histogram), `actae_scheduler_task_last_success_timestamp`,
`actae_scheduler_running_tasks` (gauge). Shutdown via broadcast channel.

Registry (`main.rs:435-633`):

| Task | Period | Timeout | Notes |
|------|--------|---------|-------|
| `event_retention_purge` | `RETENTION_CHECK_INTERVAL_SECS` (3600) | 60 s | Purge events older than `EVENT_RETENTION_DAYS` (30) |
| `event_archive_erasure` | same | 60 s | Only registered when `EVENT_ARCHIVE_RETENTION_DAYS > 0` (0 = keep forever) |
| `state_retention_purge` | same | 60 s | Only when `STATE_RETENTION_DAYS > 0`; purges old state versions, preserving latest + fork-boundary/provenance anchors |
| `tool_execution_tombstone` | same | 60 s | Only when `TOOL_EXECUTION_RETENTION_DAYS > 0`; nulls params/result/error and sets `expired_at` on old completed/failed executions |
| `usage_outbox_delivery` | 30 s | 60 s | Only when enrolled; batch ≤ 1000; ack only after control plane durably dedupes |
| `retained_bytes_snapshot` | 3600 s | 60 s | Only when enrolled; delivers the current retained-logical-payload gauge (point-in-time overwrite, not a daily sum) |
| `credential_reauthorizer` | 30 s | — | Infallible; re-checks every WS connection's key vs DB, de-auths revoked/expired |
| `server_checkpoint` | 30 s | — | Infallible; persists `ServerCheckpoint` |
| `consumer_lease_reaper` | 30 s | 60 s | Expire lapsed group leases |
| `wakeup_dispatcher` | 5 s | 60 s | Fire due `scheduler.wakeup` events, ≤ 50 per tick, broadcast live |

Plus (not in the scheduler): the delivery retry worker (`retry_tracker.run_retry_worker`,
`main.rs:400-408`), the control-plane sync loop, and the revoke notifier task.

---

## 6. Managed storage (`embedded_pg.rs`)

Actae's default backend is a bundled PostgreSQL child, started when no external
database is configured. `--database-url` / `DATABASE_URL` / `POSTGRES_*` / a
`--config` `[database]` section switch to an external database (external always
wins). `ACTAE_STORAGE=managed|external` forces the choice and
`ACTAE_DEPLOYMENT_MODE=hosted` forbids managed. See
`docs/STORAGE_CONFIGURATION.md`.

### 6.1 PostgreSQL binary resolution (`resolve_binaries`, `embedded_pg.rs:524`)

1. `ACTAE_PG_BIN_DIR` — explicit override (validated layout, `bin/` tree or
   plain bin dir).
2. Already-extracted install under the data dir (`pg-bin/`).
3. Embedded bytes baked into release binaries (`pg-embedded` feature;
   SHA-256 pins in `actae/pg-checksums.json` verified at build *and* before
   extraction; the archive is extracted atomically — staged sibling dir +
   fsync + rename — with size caps and full-layout validation).
4. **Host-installed PostgreSQL** (dev/default builds only): detected via
   `pg_config`/PATH/well-known install dirs; adopted when the existing
   cluster's PG major matches the host PG (else the pinned download is used so
   data is never stranded). Never touches a running host server — Actae runs
   its own isolated cluster (own data dir, port, scram auth).
5. Verified zonky download (dev builds only). `ACTAE_PG_NO_HOST=1` forces this
   path. Release binaries never download — a platform mismatch fails loudly.

### 6.2 Ports, bumping, lock (`embedded_pg.rs:903-1180`)

- `port_busy(port)` probe; `reserve_http_listener(http_port)` binds the HTTP
  socket first (no port race while PG initializes).
- `ACTAE_AUTO_BUMP=1` (default): a busy HTTP port bumps to the next free port
  with its own PG port + data dir — 8002 → PG 5546 → `~/.actae/data`;
  8003 → 5547 → `~/.actae/data-8003`. `ACTAE_AUTO_BUMP=0` fails loudly.
- Data-dir lock: `<data-dir>/.actae.lock` (`embedded_pg.rs:964`) — a
  cross-platform file lock so two starts of the same instance identity fail
  fast instead of racing for the PG port.
- `choose_instance` computes the (http_port, pg_port, data_dir) triple and
  classifies the PG port via `candidate_orphan` (`embedded_pg.rs:1038`):
  `Owned` (postmaster.pid records our data dir + port → reuse),
  `Foreign` (anything else — fail without signaling), or `OrphanExpected`
  (plausible crash orphan, final proof via `pg_ctl status`).

### 6.3 Orphan reclaim (`reclaim_orphan`, `embedded_pg.rs:1811`)

Before touching the cluster: read `postmaster.pid` (pid / canonicalized data
dir / port), require the recorded PID to match a live process reported by
`pg_ctl status`, then `pg_ctl stop` bounded by a timeout, kill+reap if needed.
Any mismatch → startup fails WITHOUT sending any signal. A restart can never
kill another instance's postgres.

### 6.4 Wipe & credentials

- `ACTAE_EMBEDDED_PG_WIPE=1|true` recreates the cluster from scratch and fails
  loudly if the wipe cannot fully complete (`embedded_pg.rs:1251,1301-1310`).
- Superuser password: generated once, persisted in `<data-dir>/pg.pwfile`
  (mode 0600); the persisted value is authoritative — a conflicting
  `ACTAE_EMBEDDED_PG_PASSWORD` on an existing cluster is a loud, actionable
  error (`embedded_pg.rs:1334,1400-1409`), never a silent re-login timeout.
- Embedded PG binds loopback with Unix sockets disabled (no stale
  `.s.PGSQL.*` artifacts in /tmp); scram auth.
- Startup banner (URL, PG port, data dir, password, psql command) prints once
  at the END of startup; each instance writes `<data-dir>/url` for
  `actae doctor` (which scans `ACTAE_DATA_DIR*` to list running instances).

---

## 7. Environment configuration reference

Selected defaults (all read in `config/mod.rs`; see the AGENTS.md table for
the control-plane and security set):

| Var | Default | Meaning |
|-----|---------|---------|
| `ACTAE_PORT` | 8002 | HTTP port |
| `ACTAE_HOST` | 127.0.0.1 (TLS off) / 0.0.0.0 (TLS on) | Bind host |
| `DATABASE_URL` / `CLIENT_DATABASE_URL` | — | Full DSNs, honored verbatim; a single `DATABASE_URL` serves both pools. Selects external storage. |
| `POSTGRES_*` / `CLIENT_POSTGRES_*` | — | Discrete external-PG settings (also select external storage). |
| `ACTAE_STORAGE` | inferred | `managed` \| `external` — forces the backend. |
| `DB_CONNECT_TIMEOUT` / `DB_IDLE_TIMEOUT` / `DB_POOL_TIMEOUT` | 30 / 300 / 30 (s) | Pool timeouts |
| `EVENT_RETENTION_DAYS` | 30 | Event purge horizon |
| `EVENT_ARCHIVE_RETENTION_DAYS` | 0 | 0 = archive kept forever |
| `STATE_RETENTION_DAYS` | 0 | Age-based purge of versioned state snapshots (0 = keep all); latest snapshot and fork-boundary/provenance anchors are always preserved |
| `TOOL_EXECUTION_RETENTION_DAYS` | 0 | Tombstone completed/failed tool executions older than this window: params/result/error bodies are nulled, the key identity + `expired_at` survive so a retried idempotency key fails loudly (`result_expired`) instead of silently re-executing (0 = keep bodies forever) |
| `RETENTION_CHECK_INTERVAL_SECS` | 3600 | Purge cadence |
| `EVENT_MAX_EVENTS_PER_MONTH` | 0 | Monthly event-count cap (0 = unlimited); enforced atomically |
| `EVENT_MAX_INGRESS_BYTES_PER_MONTH` | 0 | Monthly event payload+metadata ingress cap (0 = unlimited) |
| `EVENT_MAX_DURABLE_WRITTEN_BYTES_PER_MONTH` | 0 | Monthly durable-written flow meter cap across events, state snapshots, and tool params/results (0 = unlimited); writes stop when this OR the retained cap is hit first |
| `EVENT_MAX_RETAINED_BYTES` | 0 | Current logical retained-payload cap across events, state versions, and tool records, including fork-generated state copies (0 = unlimited). Computed from customer payload bytes, NOT PostgreSQL physical disk usage |
| `WS_MAX_CONNECTIONS` | 1000 | Global cap |
| `WS_CONNECTION_TIMEOUT` | 30 (s) | Handshake timeout |
| `WS_PING_INTERVAL` | 30 (s) | Server heartbeat |
| `WS_MAX_MESSAGE_SIZE` | 1048576 | 1 MB WS frame cap |
| `MESSAGE_BUFFER_SIZE` | 10000 | Per-connection queue |
| `ENABLE_SIGNUP` | false | 403 on signup/login when off |
| `JWT_EXPIRATION` | 86400 (s) | 24 h session |
| `JWT_ISSUER` / `JWT_AUDIENCE` | actae / actae-clients | JWT claims |
| `API_KEY_HEADER` | X-API-Key | Header name |
| `ENABLE_ANONYMOUS` | false | Anonymous read + write on HTTP and WebSocket |
| `REQUIRE_AUTH_FOR_PRIVATE` | true | Private channel policy |
| `ENABLE_COMPRESSION` / `BINARY_COMPRESSION_LEVEL` | true / 6 | WS compression |
| `BATCH_SIZE` / `BATCH_TIMEOUT_MS` / `WORKER_QUEUE_SIZE` | 100 / 10 / 10000 | Batching |
| `ACTAE_ACCESS_LOG_ALL` | false | Also log probes/static |
| `LOG_LEVEL` / `LOG_FORMAT` / `RUST_LOG` | build-dependent | Log level / format (`compact`\|`json`\|`pretty`). Release binaries default to `compact` and gate the startup sequence to WARN until operational; debug builds default to verbose `json`. An explicit value overrides the build default and disables the gate (`config/logging.rs`). |
| `ACTAE_IP_AUTH_RATE` / `ACTAE_IP_AUTH_CAPACITY` | 5 / 20 | Auth rate limit |
| `ACTAE_AUTO_BUMP` | 1 | Port bumping |
| `ACTAE_DATA_DIR` | `~/.actae/data` | Embedded data dir |
| `ACTAE_EMBEDDED_PG_PORT` | 5546 | Embedded PG port |
| `ACTAE_PG_BIN_DIR` / `ACTAE_PG_NO_HOST` | — / 0 | PG binary resolution |
| `ACTAE_CONTROL_PLANE_URL` | https://saas.diviga.dev | SaaS endpoint |
| `ACTAE_SYNC_INTERVAL_SECS` | 60 | Sync cadence |
| `ACTAE_SUBSCRIPTION_GRACE_SECS` | 259200 | 72 h offline grace |
| `ACTAE_CONFIG_DIR` | `~/.actae` | Enrollment file + SDK `client.json` location |
| `ACTAE_WRITE_CLIENT_CREDENTIALS` | 1 | Automatic on-boot publish of `client.json` (`0` disables) |
| `ACTAE_LOCAL_AUTO_LOGIN` | 1 | Loopback dashboard auto-login for a local standalone instance (`0` requires a sign-in even locally) |
| `GATEWAY_EXPERIMENTAL_ENABLE` | 0 | Experimental opt-in for multi-instance federation gateway features |
| `SAAS_EXPERIMENTAL_FLEET_ENABLE` | 0 | Experimental opt-in for SaaS fleet routes and UI |

---

## 8. CLI (`cli/mod.rs`)

`actae auth login|status|logout|sync` (device flow, 5 s poll),
`actae enroll <key>` (paste-to-enroll; `--install-id`/`--enroll-secret` for
air-gapped), `actae keys create|list|revoke`, `actae upgrade` (self-update via
the control-plane manifest), `actae doctor` (instance scan). `auth status`
uses `verify_license_signature` so an expired enrollment license is reported,
not fatal.

**Env loading**: `main.rs` calls `dotenvy::dotenv()` (loads `.env` from the
CWD) **before** the CLI dispatch, so CLI subcommands see the same
`JWT_SECRET`/`API_KEY_HASH_SECRET` as the server — a key minted by `keys
create` hashes with the identical secret the running server validates with.

**Embedded auto-detection**: `keys create|list|revoke` need no DB env vars
against a running managed-storage instance. `keys_pool()` calls
`discover_embedded_env()` when `POSTGRES_USER` is unset: it scans
`~/.actae/data*` dirs, reads each `url` file (`http://<host>:<http_port>`,
`pg_port`, `data_dir`), probes healthz to pick the live instance, and seeds
`POSTGRES_*`/`CLIENT_POSTGRES_*` from `pg.pwfile` (user `actae`, host
`127.0.0.1`, db `actae`, SSL off). It refuses when 0 or >1 instances are
live (operator must set `POSTGRES_*` explicitly).
