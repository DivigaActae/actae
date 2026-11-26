# Client credentials (`~/.actae/client.json`)

The zero-copy onboarding path for self-hosted Actae: run the binary and your
SDKs are already authenticated. No account, no copy-paste, no environment file.

This is the Actae equivalent of `opencode login` — except, on the local machine,
the login is automatic.

## The model

* **Source of truth:** the server database (`public.api_keys.key_hash`). The raw
  key is a one-way HMAC at rest and is never recoverable from the DB.
* **Cache:** `$ACTAE_CONFIG_DIR/client.json` (default `~/.actae/client.json`),
  written on the boot that minted a key and refreshable with `actae login`.
* **Recovery:** `actae login` (mints a fresh key from the DB and rewrites the
  file). There is no state in the file the system cannot rebuild, so a lost or
  corrupt file costs one command — never a dead end.

## File format

```json
{
  "schema": "actae.client/v1",
  "endpoint": "http://127.0.0.1:8002",
  "api_key": "sk-...",
  "instance": {
    "http_port": 8002,
    "pg_port": 5546,
    "data_dir": "/home/me/.actae/data",
    "pid": 12345
  },
  "created_at": "2026-10-13T11:11:54.175936Z"
}
```

`endpoint` and `api_key` are the only fields the SDKs require; readers ignore
everything else (forward compatibility). A hand-written minimal file
`{"endpoint": "https://host", "api_key": "sk-..."}` is valid.

## How it is published

On a **standalone** instance (`ACTAE_CONTROL_PLANE_URL` unset) the boot that
mints a key writes the file, then the banner reports it:

```
  API key:     sk-dev-0000000000000000000000  (shown once — store it now)
  SDK setup:   saved to /home/me/.actae/client.json — Python/Go/TS SDKs
               authenticate automatically, no setup needed
```

Enrolled instances (control-plane-managed keys) and containers do not advertise
automatic setup: the key is printed for copy (in a container the file lives
inside the container, not on the host).

The automatic write can be disabled with `ACTAE_WRITE_CLIENT_CREDENTIALS=0`
(e.g. an operator who does not want a plaintext key under `$ACTAE_CONFIG_DIR`);
the banner still prints the key.

## Hardening

The writer is `actae/src/client_credentials.rs`. Properties:

* **Atomic + durable.** `create_new` in the same directory at mode `0600`
  (subject to a narrowing umask only), `fsync` the file, `rename` into place,
  `fsync` the parent directory, then re-assert `0600`. A crash mid-write leaves
  either the previous valid file or none — never a torn file.
* **Symlink-safe.** Reads and writes open with `O_NOFOLLOW` (Unix) so a symlink
  planted at the path can never redirect the secret. All three SDKs refuse a
  symlinked `client.json` (Rust via `O_NOFOLLOW`; Python/Go/TS via an
  `lstat`/`is_symlink` check).
* **Directory tightened** to `0700` (best effort).
* **Cross-process lock** (`fs2` advisory lock on `.client.json.lock`) serializes
  concurrent writers, so two instances booting at once cannot interleave.
* **No clobbering a live sibling.** Under the lock, if the existing default
  points at a *different* instance that is currently live (`GET /healthz`), this
  instance writes `client-<http_port>.json` instead.
* **Never logged.** `ClientCredentials` has a redacting `Debug` impl and a
  `redacted_summary()` used by the dashboard.
* **Never fatal.** A publish failure is a warning; the startup banner still
  prints the raw key.

On Windows the filesystem cannot express POSIX modes; the file inherits the
user-profile ACL of `~/.actae` — the same deliberate trade-off as
`enrollment.json`.

## SDK resolution order

All three SDKs resolve credentials **per field** as:

1. explicit argument / constructor option,
2. environment (`ACTAE_API_KEY`, `ACTAE_URL` / `ACTAE_ENDPOINT`, `ACTAE_WS_URL`),
3. `$ACTAE_CONFIG_DIR/client.json` (default `~/.actae/client.json`).

A missing / unreadable / symlinked / malformed / invalid file is treated as
absent, and the final error is actionable (mentions `actae login`). When
`client.json` itself is absent but **exactly one** per-instance
`client-<port>.json` exists (a machine running more than one managed instance),
that file is used; more than one is ambiguous and ignored (use
`actae login --url` to pick one).

| SDK | Zero-config entry point |
|-----|-------------------------|
| Python | `ActaeClient()` (or `ActaeClient.from_env()`) |
| Go | `actae.NewClientFromEnv(actae.ClientOptions{})` |
| TypeScript | `new ActaeClient({})` (or `newClientFromEnv()`) |

## CLI

```
actae login [--url <url>] [--api-key <key>]
actae logout [--all]
```

* `actae login` (no flags) — finds the single live local managed-storage
  instance, mints a fresh local key (`source='local'`, user `ops`, name
  `SDK login`), and writes `client.json`. No paste. The freshly minted key is
  **verified against the running server** before it is published (a signing-secret
  mismatch is a loud error, never a silently-401 credential), and a previous SDK
  key for the same server is **revoked** so repeat logins rotate instead of
  accumulating.
* `actae login --url <url> --api-key <key>` — verifies the key against
  `GET /api/v1/auth/capabilities` (the API-key-aware resolver; `/auth/me` is
  JWT-only) and saves it (remote instances). Without `--api-key` the key is
  prompted without echo. A network failure is a warning (the operator may log in
  before the server starts); a 401/403 is fatal.
* `actae login` always claims the default file — it is the explicit "make this
  my active credential" action.
* `actae logout` removes `client.json`; `--all` also removes per-instance
  `client-*.json` files.
* The dashboard reports the SDK login state (endpoint, never the key) and warns
  when the file is group/world-readable.

## Non-goals and known limitations

These are deliberate, not oversights:

* **No OS keychain.** A locked/headless keychain would break the "always works"
  guarantee, so the store is a plaintext `0600` file in a `0700` directory —
  the same model as `opencode`'s `auth.json`, AWS credentials, and the existing
  `~/.actae` mTLS keys. A keychain could be added later behind an explicit
  opt-in with the file as the fallback.
* **Enrolled instances do not auto-publish.** Control-plane-managed keys are the
  portal's to manage; silently caching them locally would blur the ownership
  boundary. Use `actae login --url` if you want one cached.
* **Docker.** The file is written inside the container; an SDK on the host must
  use `ACTAE_API_KEY` / `actae login --url` (the banner says so).
* **Windows.** No POSIX mode or `O_NOFOLLOW`; the file inherits the user-profile
  ACL and readers use a portable `lstat`/`is_symlink` check instead.
* **Liveness probe.** The "don't clobber a live sibling" probe runs just before
  the lock is taken; a concurrent boot can, in the worst case, flip which
  instance owns the default for one boot. It self-corrects on the next boot, and
  `actae login` (explicit) always wins.

## Related environment variables

| Var | Effect |
|-----|--------|
| `ACTAE_CONFIG_DIR` | Config directory (default `~/.actae`); overrides where `client.json` is read/written. |
| `ACTAE_WRITE_CLIENT_CREDENTIALS` | `0`/`false`/`no`/`off` disables the automatic on-boot publish. |
| `ACTAE_API_KEY`, `ACTAE_URL`, `ACTAE_ENDPOINT`, `ACTAE_WS_URL` | Env credentials; override the file. |

## Tests

* Rust: `actae/src/client_credentials.rs` unit tests — atomicity, `0600` perms
  (and the group/world-readable detector), symlink refusal, malformed-input
  handling, stale vs live-other ownership, force, concurrent writers, delete-all,
  redaction; `cli/mod.rs` unit tests for endpoint normalization / prefix / same-server.
* Python: `sdks/python/tests/test_client_credentials.py`.
* Go: `sdks/go/credentials_test.go` + opt-in live `credentials_live_test.go`.
* TypeScript: `sdks/typescript/test/credentials.test.ts` + opt-in live
  `credentials_live.test.ts`.
* Lifecycle: `tests/test_dev_lifecycle.py` (opt-in `ACTAE_LIFECYCLE=1`) boots an
  instance and asserts the file is written with `0600`, that `actae login` mints
  and **rotates** (the replaced key 401s), that `actae logout` removes it, and
  that the real Python SDK authenticates from the file alone.
