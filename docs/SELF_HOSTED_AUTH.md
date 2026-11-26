# Self-hosted dashboard authentication

How a user logs into the embedded dashboard on a **self-hosted** Actae
instance, how credentials are stored, and the hardening an operator should do.
This is the standalone/community path; the managed path is the Actae portal SSO
(`docs/CONTROL_PLANE.md`).

## Two modes

Actae decides the login mode from enrollment:

| Mode | When | Identity | Login |
|------|------|----------|-------|
| **Standalone** | no control plane configured, no `enrollment.json` | local accounts in `public.user` | email + password against the local table |
| **Enrolled** | `ACTAE_CONTROL_PLANE_URL`/`ACTAE_INSTALL_ID`/`ACTAE_ENROLL_SECRET` or `~/.actae/enrollment.json` | the **Actae portal account** (single identity) | credentials verified by the control plane, or the portal's one-time SSO handoff |

`GET /api/v1/auth/config` returns `{enable_signup, sso, setup_required,
local_auto_login}` so the login page shows the right thing (`sso: true` ⇒ "use
your Actae account"; `setup_required: true` ⇒ the first-run setup form;
`local_auto_login: true` ⇒ the dashboard signs in automatically, below).

## Local auto-login (no account, no login form)

A **standalone** instance that is being used from the same machine the dashboard
is served on should never show a login — that is the "no account required"
promise. On load, the dashboard calls `POST /api/v1/auth/local-session`; if the
request is a same-machine browser the server mints a full admin session for the
first local user and the dashboard proceeds. It also retires any pending
first-run setup token, so the setup form is skipped too.

The endpoint is deliberately narrow — it issues a session **only** when all of
these hold:

- the instance is **standalone** (not enrolled — enrolled uses portal SSO);
- the trusted peer IP is **loopback** (from the peer-IP middleware, never a
  spoofable header);
- the `Host` header names a loopback host; and
- any `Origin` header is a loopback origin.

The `Host`/`Origin` requirements defeat **DNS rebinding**: a page on a public
domain that re-resolves to `127.0.0.1` still sends its real `Host`/`Origin`, so
it is refused (`403`). A remote/LAN browser (non-loopback peer) is refused, so
exposing Actae on the network still requires a real sign-in. Operators who want
a login even locally set `ACTAE_LOCAL_AUTO_LOGIN=0`.

This changes nothing for **programmatic** access: API keys still govern SDK/HTTP
traffic. Only the co-located browser gets the convenience. Because the session
is returned in the response body (not a cookie), a cross-origin page cannot read
it.

Boundaries (all deliberate):

- **Remote / LAN / proxied access requires a sign-in.** Only a genuine loopback
  peer qualifies. A browser on the network, or a request proxied by nginx with a
  public `Host`, is refused (that is the point).
- **Docker does not auto-login.** With `-p 8002:8002` the container sees the
  connection from the Docker bridge gateway, not loopback, so the dashboard
  shows the normal sign-in. Use the container's admin credentials (or
  `docker compose exec actae actae passwd`). Native self-hosting is the path the
  auto-login is for.
- **Shared multi-user host.** Loopback trust is per-host, not per-OS-user: any
  local process/account can reach `127.0.0.1:<port>` and obtain a session, the
  same trust level as reading `~/.actae/client.json` or the startup banner. On a
  machine you do not fully trust, set `ACTAE_LOCAL_AUTO_LOGIN=0`.
- **Logout sticks.** The dashboard attempts auto-login at most once per page
  load, so signing out is not immediately undone; a reload re-authenticates
  (local no-account convenience).

## Standalone: first boot

On the first boot of a standalone instance whose `public.user` table is empty,
Actae provisions one dashboard admin (`role = 'admin'`):

- email: `ACTAE_INITIAL_ADMIN_EMAIL` (default `admin@actae.local`)
- **password**: `ACTAE_INITIAL_ADMIN_PASSWORD` if set, otherwise the admin is
  created **without a password** and a one-time **setup token** is minted and
  printed once in the startup banner:

  ```
  First-run setup: open the dashboard and set the admin password.
    account:      admin@actae.local
    setup token:  <96-hex>   (valid 24h, shown once)
    lost it? regenerate with `actae passwd` on the host.
  ```

  With a preset password the banner instead prints
  `Initial dashboard login: <email>  password: <password>`.

Login is **always** allowed; self-service **signup is opt-in** via
`ENABLE_SIGNUP=true` (`POST /api/v1/auth/signup`), so a fresh instance is never
open to registration by default. A first-boot API key is provisioned the same
way (`ACTAE_INITIAL_API_KEY`, or a generated `sk-…` printed once).

### First-run setup page

The setup token exists so a fresh instance has a real "choose your admin
password" page without ever leaving an unauthenticated account-claim endpoint
open. State lives in `public.instance_setup` (a singleton row:
`setup_complete`, `token_hash`, `token_expires_at`).

- `GET /api/v1/auth/config` reports `setup_required: true` while the singleton
  is incomplete (standalone only).
- The dashboard `/login` page renders a **setup form** (setup token + password +
  confirm, optional name) and calls `POST /api/v1/auth/setup` with
  `{token, password, name?}`. On success it sets the admin password, marks setup
  complete (single-use — a replay is `409`), mints a session and signs the
  operator in. Invalid/expired token ⇒ `401`; weak password ⇒ `400`; enrolled
  instance ⇒ `403`.
- The token is stored **SHA-256 hashed** (never plaintext) and expires after
  24h. `actae passwd` on the host is the recovery path if it is lost — setting a
  password also marks setup complete.

## User management and roles

Every dashboard user has a `role` in `public.user`:

| Role | Capabilities |
|------|--------------|
| `admin` | everything a member can, **plus** user management |
| `member` | read/write channels, events, forks, preferences |

Admins manage accounts over the dashboard **Team** page (Advanced nav) or the
API. All endpoints require an **active admin dashboard session** (Bearer JWT
only — API keys never grant administration) and the caller's role is read fresh
on every request:

| Method & path | Purpose |
|---------------|---------|
| `GET /api/v1/users` | list users `{id, email, name, role, email_verified, created_at}` |
| `POST /api/v1/users` | create `{email, password, name?, role?}` (role defaults `member`); duplicate email ⇒ `409` |
| `PATCH /api/v1/users/{id}` | update `{name?, role?}` |
| `DELETE /api/v1/users/{id}` | delete a user (also revokes their sessions) |
| `POST /api/v1/users/{id}/password` | reset a user's password (revokes their sessions) |

Guards prevent lockout: the **last admin** cannot be demoted or deleted, and you
cannot delete your own account (both `409`). The guard and the write run in one
transaction that locks the admin set (`SELECT … FOR UPDATE`), so concurrent
demotions cannot race the instance to zero admins. Passwords go through the same
policy as signup; the created/reset user must sign in again after a reset.
Non-admins get `403`. `role` is included in `/auth/me` and the login response so
the dashboard can gate the Team nav.

Hardening: every call is behind the per-IP auth rate limit and reports
success/failure to the security manager (IP/user lockout + threat monitor);
successful mutations emit an audit log line (`tracing::info!`, admin/target/ip);
and DB errors are logged server-side with a request id (never leaked).

## Credentials and sessions

- **Passwords** are hashed with **bcrypt** and stored in
  `public.user.password_hash`. The plaintext is never stored or returned.
  Policy: 8–128 characters.
- **Dashboard sessions** are a stateless **HS256 JWT** (24h), signed with
  `JWT_SECRET`. The browser stores it in `localStorage` (or `sessionStorage`
  when "Remember me" is off) under `actae_auth` and sends it as
  `Authorization: Bearer <jwt>` for HTTP and in the WebSocket `auth` frame.
  **Sessions are server-revocable**: every minted JWT records its `jti` in
  `public.dashboard_sessions`, and a request is only accepted while that row is
  live. `POST /api/v1/auth/logout` revokes the caller's session and
  `POST /api/v1/auth/logout-all` revokes every session for the user ("sign out
  everywhere" — the sidebar button). A password reset (`actae passwd`) also
  revokes the user's sessions. The check is cached ~30s per session.
- **API keys** are HMAC-SHA256 hashed at rest with `API_KEY_HASH_SECRET`
  (defaults to `JWT_SECRET`). `POST /api/v1/auth/api-key-login` still exchanges a
  key for a session JWT for programmatic use, but it is no longer the dashboard
  login method.

### SDK client credentials (`~/.actae/client.json`)

For local SDK use there is no separate login: on a standalone first boot the
instance publishes the endpoint + a fresh API key to
`<ACTAE_CONFIG_DIR>/client.json` (mode 0600, atomic), and the Python/Go/TS SDKs
read it automatically (explicit arg > env > file). `actae login` re-establishes
the file (minting a new key locally, or verifying a pasted key for a remote
instance) and `actae logout` removes it. See
[`CLIENT_CREDENTIALS.md`](CLIENT_CREDENTIALS.md) for the format, hardening
properties, and the recovery guarantee.

## Signing secret (secure by default)

When `JWT_SECRET` is not configured, Actae resolves it in order:

1. a generated secret persisted at `<ACTAE_CONFIG_DIR>/jwt.secret` (mode 0600);
2. the built-in default **only** when the instance already has API keys hashed
   with it (an upgrade — rotating it would invalidate every key); or
3. a freshly generated random secret, persisted for later boots.

So a fresh install never runs on the known default. The API-key hash secret
follows the JWT secret unless the operator sets it independently. If Actae keeps
the default (existing keys), the startup banner and `actae doctor` warn; set
`JWT_SECRET` + `API_KEY_HASH_SECRET` and re-create keys to move off it.

## Recovering / changing the admin password

There is no self-service password-change endpoint (an admin resets another
user's password from the Team page), so to recover/change the admin password use
the CLI on the Actae host:

```bash
actae passwd                                  # the only local user, generated password
actae passwd admin@actae.local                # pick the user, generated password
actae passwd admin@actae.local --password '<new>'   # explicit
```

`actae passwd` auto-detects a single live managed-storage instance (same discovery
as `actae keys`); otherwise set `POSTGRES_*`/`DATABASE_URL` as the server sees
them. It prints a generated password once and **revokes that user's existing
dashboard sessions** (and marks first-run setup complete, so the setup page
stops appearing).

## Hardening checklist

- **Set a real `JWT_SECRET`** (and `API_KEY_HASH_SECRET`) for explicit control.
  A fresh install auto-generates + persists one (above), but pinning it makes
  rotation and backups explicit:

  ```bash
  export JWT_SECRET="$(openssl rand -hex 32)"
  export API_KEY_HASH_SECRET="$(openssl rand -hex 32)"   # re-create existing API keys after changing
  ```

  Rotating `JWT_SECRET` invalidates existing dashboard sessions; rotating
  `API_KEY_HASH_SECRET` invalidates existing API-key hashes (re-create keys).
- **TLS**: without it, bind loopback (the default). Exposing a non-loopback host
  over plain HTTP logs a warning — put Actae behind a TLS-terminating proxy or
  set `ENABLE_TLS=true` (+ cert/key).
- Run `actae doctor` to surface the default-secret warning and connectivity
  issues.
- Keep `~/.actae/enrollment.json` (mode 0600) out of backups if you do not want
  the instance enrolled.

## Tests

`tests/test_self_hosted_auth.py` (opt-in) boots dedicated instances and drives
the real endpoints: the setup-token flow (single-winner under concurrent
submits, single-use replay `409`), admin user management (member `403`, reset
revokes sessions, last-admin/self guards), and the loud abort on an invalid
`ACTAE_INITIAL_ADMIN_PASSWORD`:

```bash
ACTAE_SELF_HOSTED_AUTH=1 python3 -m pytest tests/test_self_hosted_auth.py -v
```

Unit coverage: `actae::auth::setup` (constant-time compare, token matching) and
`api::users` role validation in `cargo test --lib`; the dashboard Team page in
`dashboard/` vitest.

## Known limitations (and the ideal)

- **Session token in `localStorage`** is readable by JS (XSS). This is a
  deliberate tradeoff for WebSocket auth (the WS handshake needs the token). An
  httpOnly cookie + a short-lived WS ticket would remove it.
- **No self-service password change** — a member must ask an admin (Team page)
  or the operator (`actae passwd`) to reset it. Ideal: a `change-password`
  endpoint requiring the current password.
- **No email invitations / self-service signup for teams** — admins create
  accounts with a temporary password. Ideal: emailed invites with one-time
  links.
- Revocation latency is bounded by the ~30s session cache; a just-revoked
  session can survive at most that long on a request path that already passed
  the check.
