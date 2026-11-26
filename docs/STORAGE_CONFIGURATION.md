# Storage configuration

How an Actae instance chooses where to persist data, and how to configure it
with CLI flags, a TOML config file, or environment variables.

## Two storage backends

| Backend | What it is | When it is used |
|---------|------------|-----------------|
| **Managed** (default) | A bundled PostgreSQL child process that Actae starts, owns, and stops (scram auth, loopback TCP only, data under `ACTAE_DATA_DIR`). Release binaries have the PostgreSQL bytes baked in and work offline. | A bare `actae` with no external database configured. This is the self-hosted/free-tier default: one binary, no external dependencies. |
| **External** | An existing PostgreSQL you operate (local, container, managed cloud). Actae only connects. | Any of `--database-url`, `DATABASE_URL`, `POSTGRES_*`, `CLIENT_DATABASE_URL`, or a `[database]` section in `--config` is present. |

External configuration always wins over managed — a managed cluster is only
started when nothing external is selected.

## Selection precedence

1. `--database-url <DSN>` (CLI) — highest priority.
2. `--config <FILE>` file values.
3. Environment variables (`DATABASE_URL`, `POSTGRES_*`, `ACTAE_STORAGE`, …).
4. Built-in defaults → **managed** storage when still nothing is set.

`ACTAE_STORAGE=managed|external` overrides the inference:

- `external` — requires a database URL/host; fails loudly at startup otherwise.
- `managed` — forces the bundled PostgreSQL (never valid in hosted mode).

### Fail-closed rules

- `ACTAE_DEPLOYMENT_MODE=hosted` **forbids** managed storage: a hosted instance
  must be given an external database. Startup exits with a message otherwise.
- `ACTAE_STORAGE=external` without any database configuration exits with a
  message.
- The removed `--embedded` flag is rejected with migration guidance (managed
  storage is now the default).

### `.env` files and the bundled binary

A **downloaded release/community binary never auto-loads a `.env` file** — a
stray `.env` in the working directory (or one of its parents) must not silently
switch a first-time user into external-database mode. Debug builds (`cargo run`)
still load `.env` for development convenience; set `ACTAE_LOAD_DOTENV=1` to opt
a release build in. Production operators pass real environment variables
(systemd `EnvironmentFile=` / compose `environment:`), which are unaffected.

When an external database *is* selected from the real environment
(`DATABASE_URL` / `CLIENT_DATABASE_URL` / `POSTGRES_*`), startup prints a loud,
credential-free notice naming the target, and if the connection then fails it
prints the exact variables to unset (or `ACTAE_STORAGE=managed`) to fall back to
the bundled database — so a stray exported DSN can never leave a user staring at
connection retries with no explanation.

## CLI

```bash
# Self-contained: bundled PostgreSQL, data in ~/.actae/data
actae

# Existing PostgreSQL (sets both the main and client pools)
actae --database-url 'postgresql://actae:secret@db.internal:5432/actae?sslmode=require'

# Split pools
actae --database-url "$MAIN_DSN" --client-database-url "$CLIENT_DSN"

# Config file
actae --config /etc/actae/actae.toml

# Overrides
actae --host 0.0.0.0 --port 8002 --health-port 8002
```

`--database-url` is the same thing as exporting `DATABASE_URL`, but it removes
any discrete `POSTGRES_*`/`CLIENT_POSTGRES_*` variables from the environment so
a stale value cannot shadow the URL.

## Config file (`--config <file.toml>`)

A typed TOML file. Every field is optional; unset fields fall through to the
environment (and then the built-in default). Unknown fields are rejected
(`deny_unknown_fields`) so typos fail loudly. Because the file is applied to
the environment before the server reads it, the effective precedence is
**CLI > file > environment > default**.

```toml
[server]
host = "0.0.0.0"
port = 8002
storage = "external"          # managed | external

[managed-storage]             # tuning for the bundled PostgreSQL (default)
data-dir = "/var/lib/actae"   # ACTAE_DATA_DIR
# pg-port = 5546              # ACTAE_EMBEDDED_PG_PORT
# pg-password = "…"           # ACTAE_EMBEDDED_PG_PASSWORD (pinned once)
# pg-wipe = false             # ACTAE_EMBEDDED_PG_WIPE (recreate)
# pg-bin-dir = "/opt/pg"      # ACTAE_PG_BIN_DIR
# pg-no-host = false          # ACTAE_PG_NO_HOST

[database]
# Full DSN (sets both pools); or the discrete fields below.
url = "postgresql://actae:secret@db.internal:5432/actae?sslmode=verify-full"
# user = "actae"
# password = "…"
# host = "db.internal"
# port = 5432
# name = "actae"
# ssl-mode = "verify-full"
# ssl-root-cert = "/etc/actae/pg-ca.pem"
# client-url = "postgresql://…"   # optional separate client/auth database
# max-connections = 10
# min-connections = 2

[auth]
jwt-secret = "…"
api-key-hash-secret = "…"
enable-signup = false
# initial-api-key = "…"           # first-boot seed (empty table only)
# initial-admin-email = "admin@example.com"
# initial-admin-password = "…"

[tls]
enabled = true
cert-path = "/etc/actae/tls/cert.pem"
key-path = "/etc/actae/tls/key.pem"
# enable-mtls = true
# ca-cert-path = "/etc/actae/tls/ca.pem"
# ca-bundle = "/etc/actae/corp-ca.pem"   # outbound control-plane HTTPS

[retention]
event-days = 30
# archive-days = 0
# state-days = 0
# tool-execution-days = 0

[websocket]
allowed-origins = ["https://app.example.com"]
# max-connections = 1000

[logging]
level = "info"
format = "json"

[metrics]
enabled = true

[security]
trusted-proxies = ["10.0.0.0/8"]
http-allowed-origins = ["https://app.example.com"]

[control-plane]
url = "https://saas.diviga.dev"
install-id = "ins_…"
enroll-secret = "…"
```

### Escape hatch `[env]`

Any key without a typed field can be set as a raw environment variable. `[env]`
is applied first, so typed sections win on conflict:

```toml
[env]
BATCH_SIZE = 50
MEMORY_OPTIMIZER_ENABLED = false
ACTAE_FEDERATION_ENABLED = true
```

## Environment variables

| Variable | Meaning |
|----------|---------|
| `DATABASE_URL` | Full main-database DSN, honored verbatim (no longer ignored). |
| `CLIENT_DATABASE_URL` | Full client/auth-database DSN. Defaults to `DATABASE_URL`. |
| `POSTGRES_USER/PASSWORD/HOST/PORT/DB/SSL_MODE/SSL_ROOT_CERT` | Discrete main-database settings (a DSN is assembled). |
| `CLIENT_POSTGRES_*` | Discrete client-database settings. |
| `ACTAE_STORAGE` | `managed` or `external` — forces the backend. |
| `ACTAE_LOAD_DOTENV` | `1` makes a **release** build load a local `.env` (debug builds always do). Off by default so a stray `.env` cannot hijack a downloaded binary. |
| `ACTAE_DEPLOYMENT_MODE` | `self_hosted` (default) or `hosted` (forbids managed). |
| `ACTAE_CONFIG_FILE` | Not read; pass `--config`. |
| `ACTAE_DATA_DIR` | Managed-storage data dir (default `~/.actae/data`). |

Managed-storage tuning (unchanged): `ACTAE_EMBEDDED_PG_PORT`,
`ACTAE_EMBEDDED_PG_PASSWORD`, `ACTAE_EMBEDDED_PG_WIPE`, `ACTAE_PG_BIN_DIR`,
`ACTAE_PG_NO_HOST`, `ACTAE_AUTO_BUMP`.

## Backward compatibility

- `--embedded` is **removed**: it is intercepted and rejected with a migration
  message pointing at the new default. Scripts should simply drop the flag.
- `DATABASE_URL` used to be checked but not consumed; it is now consumed
  directly (a real fix — previously setting only `DATABASE_URL` skipped the
  embedded start and then failed with `POSTGRES_USER … is required`).
- Docker images shipped with `CMD ["actae", "--embedded"]` now use
  `CMD ["actae"]`; `POSTGRES_*` in Compose still selects external storage.

## See also

- `docs/SERVER_INTERNALS.md` — managed-PostgreSQL mechanics (binary resolution,
  orphan reclaim, wipe, port bumping).
- `docs/PRODUCTION.md` — deployment (TLS, secrets, backups).
- `docs/CONTROL_PLANE.md` — hosted/enrolled instances.
