# Production Deployment Guide

How to deploy Actae for real. This complements the quick start in the root
`README.md` and the architecture docs; it covers the operational concerns
that "docker compose up" does not.

---

## 1. Distribution models and architecture at a glance

Actae ships three ways, with the **same security model** for all:

- **Self-contained binary** (`actae`) — a single file carrying the API, the
  dashboard and an embedded PostgreSQL 18 runtime. Run it with
  `./actae` (zero infrastructure) or point it at your own
  PostgreSQL via `DATABASE_URL` / `POSTGRES_*` (env always wins). See
  `docs/DISTRIBUTION.md` for the build/publish/update flow.
- **Docker image** — the released binary in a minimal Alpine image
  (`docker-compose.pull.yml`), or built from source against an external
  PostgreSQL container (`docker compose up -d --build`).

In all cases Actae runs on a **single port (8002)** serving HTTP API,
WebSocket, health, and metrics — no split ports:

```
  Clients (SDKs, dashboard, agents)
     │  X-API-Key: sk-…  (or dashboard JWT session)
     ▼
  Actae (port 8002 — TLS 1.3 when ENABLE_TLS=true)
     │
     ▼
  PostgreSQL 16+ (event store, auth, cursors)
```

### 1a. Managed storage (the default)

A bare `actae` runs a managed PostgreSQL child (scram auth, data persisted in
`ACTAE_DATA_DIR`, default `~/.actae/data`). It is the same code path for
development, CI and production. Passing an external database — `--database-url`,
`DATABASE_URL` / `POSTGRES_*`, or a `[database]` section in `--config` —
switches to that database instead (external always wins). See
`docs/STORAGE_CONFIGURATION.md`.

- **Ports**: `ACTAE_PORT` (default 8002) + `ACTAE_EMBEDDED_PG_PORT` (default
  5546). `ACTAE_AUTO_BUMP=1` (default) auto-bumps the pair when busy, deriving
  the data dir `~/.actae/data-<http_port>`; set `ACTAE_AUTO_BUMP=0` to fail
  loudly instead.
- **Credentials**: the postgres superuser password is generated once and
  stored in `<data_dir>/pg.pwfile` (mode 0600). After initialization the
  persisted value is authoritative: changing `ACTAE_EMBEDDED_PG_PASSWORD` on
  an existing cluster is a loud, actionable error (unset it, or recreate with
  `ACTAE_EMBEDDED_PG_WIPE=1`). The banner prints the password only on the
  boot that minted it.
- **PG binaries**: `ACTAE_PG_BIN_DIR` (explicit override — a zonky tree root
  with `bin/` and the `lib/`/`share/` rpath trees, or a plain bin dir) → the
  bytes baked into the binary (`pg-embedded` feature, release builds, verified
  against the SHA-256 pins in `actae/pg-checksums.json` before extraction) →
  a **host-installed PostgreSQL** (dev/bare builds only: `pg_config` → PATH →
  well-known install dirs; its binaries run Actae's own isolated cluster, a
  running host server is never touched, and an existing cluster's major must
  match, else the pinned download is used so data is never stranded) →
  one-time zonky download in dev/bare builds (also checksum-verified). Release
  binaries never download and never adopt host PG. `ACTAE_PG_NO_HOST=1`
  disables host detection.
- **Lifecycle**: graceful `pg_ctl stop` (bounded, then kill+reap) on
  SIGINT/SIGTERM/SIGHUP and on every post-spawn startup error; a SIGKILLed
  instance leaves an orphan that the next start reclaims **only after proving
  ownership** (validated `postmaster.pid` + `pg_ctl status` — never signals an
  unproven process); a data-dir lock (`.actae.lock`) prevents concurrent
  starts of the same identity. Unix sockets are disabled (loopback TCP only),
  so embedded PostgreSQL never leaves `/tmp/.s.PGSQL.*` artifacts. Portable
  across Unix and Windows.
- **First boot**: standalone instances auto-provision an initial API key
  (printed once) or honor `ACTAE_INITIAL_API_KEY`; enrolled instances pull
  keys from the control plane.

There is no separate health port. Run behind a TLS-terminating
reverse proxy (nginx / Caddy / LB) only if you want HTTP-level features
(rate limiting, WAF, domain routing) — Actae's own TLS is already enforced
end-to-end when enabled.

## 2. Onboarding (the first-run story)

Actae is SaaS-distributed: customers register on the portal, get a
**30-day trial install** plus one paste-to-enroll key, download the
binary from the portal (subscription-gated, presigned R2 URL), and start the
runtime. The API key they create in the portal is both their credential and
their license. Onboarding the binary:

```bash
# 1. Install the community build (no account required).
curl -fsSL https://raw.githubusercontent.com/DivigaActae/actae/main/install.sh | sh
# 2. Start it; the banner prints the dashboard URL and an API key.
./actae
# Keep it current: actae upgrade   (or actae upgrade --check)
```

**One-shot (docker / automation):** set `ACTAE_ENROLL_KEY` in the
environment and the instance exchanges the key at boot — no CLI step, no
second run:

```yaml
# docker-compose.yml
environment:
  ACTAE_ENROLL_KEY: sk-…
```

Behavior notes:

- **New installs are trial-active for 30 days** (granted once per account
  — only the first installation ever carries it): the runtime serves
  immediately, then 402-locks once the trial ends unless a paid Polar
  subscription is attached (see `docs/CONTROL_PLANE.md`).
- **First boot, standalone** (no control plane) → Actae auto-provisions an
  initial key (`source='local'`), printed once in the startup banner
  (`Initial API key: sk-…`), and serves. For a deterministic seed
  (CI/compose) set `ACTAE_INITIAL_API_KEY`, honored only when the
  `api_keys` table is empty.
- **Rejected keys fail loudly** at boot — never a half-dead API.
- **Standalone instances** (no control plane configured) run with no
  subscription enforcement; the startup banner prints a one-line nudge.
- The dashboard logs in with the same API key (see §5). Email/password
  auth is off by default (`ENABLE_SIGNUP=false`).
- The same key works for SDKs and the dashboard — one credential, period.

## 3. Secrets — do not ship the defaults

The compose file falls back to development values when the matching
environment variable is unset. **Every one of these must be overridden in
production** (via `.env` or the orchestrator's secret store):

| Secret | Dev default | Production requirement |
|--------|-------------|------------------------|
| `POSTGRES_PASSWORD` | `actae_dev_password` | Long random password (e.g. `openssl rand -base64 32`) |
| `JWT_SECRET` | `dev-jwt-secret-must-be-at-least-32-chars!!` | ≥ 64 random bytes (`openssl rand -base64 64`) |
| `API_KEY_HASH_SECRET` | inherits `JWT_SECRET` | Optional: separate random value (≥ 32 chars). See §7 for the trade-off |
| API keys | `test-api-key-for-integration-tests` (compose default) | `actae keys create` |

Generate a production `.env`:

```bash
cp .env.example .env
# edit .env — override POSTGRES_PASSWORD and JWT_SECRET
docker compose up -d --build
```

## 4. Transport security: two tiers

### Standard tier (default) — TLS + API keys + JWT

- **TLS**: `ENABLE_TLS=true` with `TLS_CERT_PATH`/`TLS_KEY_PATH` (the docker
  stack mounts `./certs` for this). Without TLS, Actae **binds the loopback
  interface by default** and warns loudly if you explicitly bind a
  non-loopback address — put it behind a TLS-terminating reverse proxy
  (Caddy/nginx) for public exposure instead.
- **Identity**: `X-API-Key` for machines; the **Actae portal account** for the
  dashboard (enrolled instances verify credentials through the control plane;
  the portal's "Open dashboard" auto-logs in via a one-time SSO handoff).
  Standalone instances use local email/password (first-boot admin). API keys
  are not a dashboard login method. All endpoints are authenticated;
  anonymous access is off by default.
- **Ops guards (always on)**: API keys hashed at rest (HMAC-SHA256, never
  plaintext), per-key/IP rate limiting, auth-failure lockout, key
  revocation (`actae keys revoke`).
- `/healthz` and `/readyz` stay open (liveness/readiness probes).
  `/metrics` and `/metrics.json` are **API-key-gated** — they accept
  `X-API-Key` **or** a dashboard session JWT (`Authorization: Bearer …`,
  minted by `/api/v1/auth/api-key-login` or `/api/v1/auth/login`); plain
  anonymous access (even with `ENABLE_ANONYMOUS=1`) is rejected.

### Hardened tier (opt-in) — mTLS

Client-certificate auth for deployments that want machine-bound
credentials or compliance:

```yaml
# docker-compose.yml
environment:
  ENABLE_MTLS: "true"
  CA_CERT_PATH: /app/certs/client/client-ca.crt   # CA that signs client certs
```

- Requires `ENABLE_TLS=true` — Actae fails fast (rather than silently
  downgrading) on `ENABLE_MTLS` without TLS or without `CA_CERT_PATH`.
- Issue one client certificate per operator/agent/CI:

```bash
./scripts/issue-client-cert.sh deploy-agent
./scripts/issue-client-cert.sh deploy-agent --install     # also drops it into ~/.actae
./scripts/issue-client-cert.sh deploy-agent --bundle deploy-agent.tar.gz   # for a remote client
```

  `--install` places `ca.crt`/`client.crt`/`client.key` in `~/.actae`
  (or `ACTAE_CONFIG_DIR`); the Python SDK then auto-loads them and connects
  with zero certificate configuration. `--bundle` produces a tarball for
  remote clients. It signs with the client CA (`CA_CERT_PATH`), so the new
  cert works immediately without a server restart. See `certs/README.md`
  for the full trust-chain explanation.

- **Rotation procedure**: 1) generate new certs, 2) `docker compose up -d
  --force-recreate actae`, 3) verify `/healthz` with the new client cert,
  4) revoke/delete old client certs. No downtime — the volume mount is read
  at startup.

> **Why mTLS is opt-in**: API keys already authenticate every client
> application-level. mTLS additionally binds the credential to a machine —
> valuable for compliance, but it costs a CA, per-operator certs, and
> rotation. AWS/Stripe-style API security does not require it; enable it
> when you need machine-binding. The dev PKI is **gitignored** — generate
> it with `./scripts/rotate-certs.sh` (development/CI only;
> use your existing PKI in production).

## 5. Dashboard authentication

The dashboard (served on the same port) logs in with an **API key**:
`POST /api/v1/auth/api-key-login` validates the key and mints a short-lived
JWT session. The user never sees a JWT or a password form.

- Email/password signup/login is **disabled by default**
  (`ENABLE_SIGNUP=false` → both endpoints return 403; the login page hides
  the Email tab). Enable it only for offline/admin deployments that want
  local password accounts.
- JWT sessions are internal (24h, HS256, signed with `JWT_SECRET`); they
  are exempt from control-plane subscription enforcement.

## 6. API key provisioning

API keys are stored hashed in Postgres (`api_keys`). Two sources, both
`source='local'` or `source='control_plane'`, all subject to subscription
enforcement:

- **Local keys**: `actae keys create` (the community build has no control
  plane; see `docs/CONTROL_PLANE.md`).
- **`actae keys` CLI** (host-side, no restart):

  ```bash
  docker compose exec actae actae keys create deploy-agent --user-id ci
  # → prints the raw key once (sk-…), never shown again
  docker compose exec actae actae keys list      # prefixes + status
  docker compose exec actae actae keys revoke sk-abc123def
  ```

> The compose default seeds the well-known test key
> (`test-api-key-for-integration-tests`) via `ACTAE_INITIAL_API_KEY` for the
> test stack (honored only when the `api_keys` table is empty). A standalone
> first boot without that seed auto-generates an initial key printed once in
> the banner.

## 7. Secret separation: `API_KEY_HASH_SECRET`

API keys are hashed with HMAC-SHA256 keyed by `API_KEY_HASH_SECRET` — a
separate secret from `JWT_SECRET` so that:

- A leak of one does not compromise the other (JWT tokens vs. key hashes).
- Rotating `JWT_SECRET` no longer invalidates every API key.

It **defaults to `JWT_SECRET`** so existing installs keep validating their
key hashes. Set it explicitly only when you accept that previously minted
key hashes are invalidated (recreate keys afterwards). The legacy plain
SHA-256 fallback keeps pre-HMAC keys working.

> Note: WebSocket API-key auth hashes with the **same** `API_KEY_HASH_SECRET`
> (audit item 4). A key that works over HTTP always works over WebSocket —
> there is no second `jwt_secret`-keyed hash path anymore.

## 7b. Reverse proxies: `ACTAE_TRUSTED_PROXIES` and client IP

Actae never trusts `X-Forwarded-For` unless the immediate peer is a
configured trusted proxy. When you run Actae behind Caddy/nginx/ALB, set:

```env
ACTAE_TRUSTED_PROXIES=10.0.0.0/8,172.16.0.0/12
```

- Without it, the socket peer address is authoritative for rate limiting
  and auth audit logging; a direct client cannot spoof another IP.
- With it, forwarding headers are honored only from peers in the list,
  using rightmost-untrusted chain semantics (the first hop that is not a
  trusted proxy wins).
- Do not list public IP ranges; only your actual proxy egress.

## 7c. WebSocket origin policy

Browser connections are **same-origin only by default**. If your dashboard
or SDK frontend is served from a different origin than the Actae port (for
example a marketing domain proxying the API), set the exact browser origins:

```env
WEBSOCKET_ALLOWED_ORIGINS=https://app.example.com
```

Matching is exact on (scheme, host, effective port) — never substring.
`*` is an explicit opt-in wildcard for trusted deployments. CLI and
server-side SDK clients that omit the `Origin` header are always allowed.

Embedded dev mode (`actae`, loopback host, no TLS) auto-allows
the Next.js dev server (`:3000`) **plus the server's own origin**
(`http://localhost:<port>` / `http://127.0.0.1:<port>`, effective
post-auto-bump port) — the embedded instance serves the dashboard from that
origin, and without the own-origin entries the browser WS upgrade fails
("Origin not allowed" → LiveToggle stuck on Offline). `ACTAE_HOST` on a
non-loopback, or TLS, or an operator-set `WEBSOCKET_ALLOWED_ORIGINS` all
skip the convenience list (same-origin policy / explicit list applies).
**Gotcha**: the auto-list matches only those two hostname spellings — a
dashboard accessed through a custom loopback-resolving hostname (e.g.
`i-xxxx.eu1.localhost` from a container port-forwarder) must set
`WEBSOCKET_ALLOWED_ORIGINS` to that exact origin or the WS upgrade is
rejected.

## 8. Control plane / subscription enforcement

Standalone instances run with no enforcement. When enrolled with the SaaS
portal, the instance enforces subscription state:

- Keys synced via `ACTAE_SYNC_INTERVAL_SECS` (default 60s)
- Subscription lapse → HTTP 402 for **every** API key (control-plane and
  local); only JWT (dashboard) sessions are exempt
- Offline grace: `ACTAE_SUBSCRIPTION_GRACE_SECS` (default 72h), then 503
- **Anti-fake-control-plane**: every enroll/device-flow/keys response carries
  an Ed25519 **license token** the SaaS signs and the binary verifies against
  a build-time embedded public key. A mock/substituted control plane cannot
  mint `subscription: active` without the SaaS private key.

License key lifecycle (one-time setup):

```bash
python -m actae_saas admin license-key
# → prints SAAS_LICENSE_SIGNING_KEY (keep secret, like the Fernet key) and
#   ACTAE_LICENSE_PUBKEY (bake into release builds of the Actae binary)
```

- Set `SAAS_LICENSE_SIGNING_KEY` in the SaaS environment (required in real
  mode; the backend refuses to start without it).
- Build Actae with `ACTAE_LICENSE_PUBKEY=<pubkey>` (base64url of the 32-byte
  public key) — `build.rs` validates it and **fails the build** on a bad
  value, so a release can never silently embed the dev/test key.
- Dev/test builds embed the shared test public key by default (the
  conformance stub signs with the matching test seed).
- **Rotation**: generate a new pair, embed the new public key in the next
  binary release, and keep signing with the old key until all deployed
  binaries have upgraded (tokens carry a `kid`). See
  `docs/CONTROL_PLANE.md` for the token format and verification rules.

See `docs/CONTROL_PLANE.md` for the full contract.

## 9. Backups and disaster recovery

Postgres data lives in the `postgres_data` volume (docker) or in your
managed database (binary). Back it up with `pg_dump` (or pgBackRest for
point-in-time recovery):

```bash
docker compose exec postgres pg_dump -U actae -d actae -Fc > actae.dump
# restore:
docker compose exec -T postgres pg_restore -U actae -d actae < actae.dump
```

Event retention is automatic (`EVENT_RETENTION_DAYS`, default 30) — tune it
against your storage budget. The event store is append-only; a dump of the
`agent_events`, `channel_states`, `channel_metadata`, `api_keys`,
`consumer_groups`/`consumer_offsets`, `scheduled_wakeups`, and
`tool_executions` tables captures the full system state.

For hosted plans, storage is metered with two caps enforced atomically in the
same transaction as each write: a monthly `durable_written_bytes` flow meter
(`EVENT_MAX_DURABLE_WRITTEN_BYTES_PER_MONTH`) and a current `retained_bytes`
gauge of logical payload bytes (`EVENT_MAX_RETAINED_BYTES`); writes stop when
either is reached. Retention extends to state versions
(`STATE_RETENTION_DAYS`) and tool-execution bodies (`TOOL_EXECUTION_RETENTION_DAYS`,
which tombstone — null bodies, keep the key identity so a retried idempotency
key fails with `result_expired`). See `docs/STORAGE_ACCOUNTING.md`.

## 10. Operational checklist

- [ ] `POSTGRES_PASSWORD` and `JWT_SECRET` overridden in `.env`
- [ ] Instance enrolled (`actae enroll <key>` / `actae auth login`), or
      keys provisioned via `actae keys create`; the seeded test key removed
      (override `ACTAE_INITIAL_API_KEY`)
- [ ] Embedded mode: `ACTAE_EMBEDDED_PG_PASSWORD` pinned (or recorded from
      the banner), `ACTAE_DATA_DIR` on durable storage, backups scheduled for
      `~/.actae/data`, `ACTAE_AUTO_BUMP` set explicitly for pinned ports
- [ ] Binary kept current: `actae upgrade --check` in cron / at boot
- [ ] TLS enabled for any non-loopback bind (own certs or reverse proxy)
- [ ] mTLS enabled only if you need machine-bound client certs (hardened tier)
- [ ] `ENABLE_SIGNUP=false` (default) — password auth stays disabled
- [ ] Rate limiting / lockout tuned: `AUTH_FAILURE_LOCKOUT_THRESHOLD`,
      `AUTH_FAILURE_LOCKOUT_DURATION_SECONDS`,
      `WEBSOCKET_MAX_CONNECTIONS_PER_IP`, `WEBSOCKET_MAX_MESSAGE_RATE`
- [ ] Postgres backups scheduled and restore-tested
- [ ] `docker compose` healthchecks green (`docker compose ps`)
- [ ] Dependency scans pass: `cd actae && cargo audit --ignore RUSTSEC-2023-0071`,
      `pip-audit -r tests/requirements.txt`, `cd sdks/go && govulncheck ./...`
- [ ] Monitoring: `/healthz`, `/readyz` (open), `/metrics` (API-key-gated)

## 11. Known limitations / out of scope

- **Single-node only.** There is no active multi-node clustering or HA
  failover today; run Postgres with your own HA/replication setup and point
  Actae at it (`DATABASE_URL`).
- **Horizontal scaling** of Actae itself is not supported (in-memory
  connection manager + single-writer Postgres). Vertical scaling or
  read-replica separation are the current options.
- `rsa` (transitive via `sqlx-mysql`, unused) has a known medium-severity
  advisory with no upstream fix — see AGENTS.md "Dependency security scans"
  for the rationale.

## 12. JSON input depth hardening

`sonic_rs::from_slice` / `from_str` recurse with no depth limit and overflow
the stack past ~40 nesting levels, which previously let any client crash the
server with a deeply nested JSON payload on any body-parsing endpoint
(record, transition, save_state, fork, WS publish/auth) or by saving deeply
nested state that crashed any later read. Hardened at every entry point:

- HTTP request bodies are depth-checked before parse (`parse_json_body`,
  `MAX_JSON_DEPTH = 24`) — deep bodies get `400 Invalid request body`.
- JSONB decode (`JsonValue::from_sql`) rejects documents deeper than 24
  levels as `{}`; JSONB encode (`JsonValue::to_sql`) serializes deep values
  as `null` instead of overflowing.
- WebSocket inbound messages (`Message::from_json`) and the pre-auth
  validation parse reject deep messages; the WS error-diagnosis re-parse is
  guarded too and logs are truncated.

The bound is a deliberate margin far below the overflow threshold (~40),
leaving room for legitimate but deep-ish payloads while eliminating the
crash vector. These are regression-tested in `json_compat.rs` unit tests and
`websocket/message.rs`.
