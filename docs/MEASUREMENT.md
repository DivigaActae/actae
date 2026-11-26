# Free-tier measurement: downloads + link = consent

How Actae measures the free **Self-Hosted Community** edition without an
account, and how the optional free account turns "did anyone actually use it?"
into a measurable funnel. The design goal is **maximum reach, maximum trust**:
the download is anonymous, no binary phones home, and the only activation
signal comes from an explicit, opt-in action.

Canonical sources:

- Anonymous download events: `saas/backend/src/actae_saas/analytics.py`
  (`record_community_download`, `download_stats`) and the write in
  `downloads.py::community_download`.
- Table: `community_downloads` (`db.py`).
- Free plan: `state.FREE_PLAN_CODE`, `state.effective_status`,
  `state.create_installation`, and the portal free-install flow
  (`portal.py::create_free_installation`, `_provision_free_install`).
- Aggregates: `GET /metrics.json` (`community_downloads`),
  `GET /api/admin/downloads`, and the admin overview `free_tier` block.

## Why this shape (and what we deliberately did NOT do)

Industry practice splits on telemetry default: opt-out is common (Metabase,
n8n, Airbyte, Grafana, Elastic, VS Code) but carries a permanent trust tax with
engineers, while opt-in is the trust-maximizing pattern (Sentry asks at install;
Supabase's CLI defaults to **denied** in non-interactive shells; aider is
opt-in). Actae ships the most conservative option:

- **No automatic phone-home.** An unlinked self-hosted instance sends nothing
  to Actae — no beacon, no ping, no version check against our servers (the
  community build has no control plane).
- **Link = consent.** The only runtime-originated data Actae receives comes
  from an instance the operator *explicitly linked* to an account with
  `actae enroll <key>`. Linking is an authenticated action that creates a
  phone-home credential, so it is unambiguous consent.
- **No download gate.** The binary is anonymous and card-free; gating it would
  trade reach for leads (the Docker Hub / RHEL lesson). An optional free
  account is offered as value (key management, release/security notices), never
  as a toll booth.

## What is collected

### Anonymous download events (top of funnel)

Recorded **only** on a successful `GET /v1/community/download/{platform}`
(the artifact fetch — the intent to run). Manifest and checksum fetches are
not recorded. One row per successful download in `community_downloads`:

| Column | Meaning |
|---|---|
| `id` | `cdl_<hex>` event id |
| `created_at` | ISO-8601 UTC timestamp |
| `platform` | release platform key (`macos-arm64`, `linux-x86_64`, …) |
| `version` | release version |
| `country` | two-letter country code when the edge provides `CF-IPCountry`, else `NULL` |
| `ua_class` | coarse User-Agent bucket: `browser` / `cli` / `bot` / `other` / `unknown` |
| `ip_hash` | HMAC-SHA256 of the client IP keyed by the server's key-encryption secret, **or `NULL`** |

**Never stored:** the raw IP address, the raw User-Agent, any account
identifier (the downloader is anonymous), or anything the user typed. The
`ip_hash` is irreversible and exists only to count approximate unique clients
in a reporting window (the "unique_ips" floor). Recording is **best-effort**:
a write failure is logged and swallowed and can never fail, delay, or alter a
download.

### Linked-instance activation (the signal that matters)

A free account that links a self-hosted instance registers a
`plan_code = 'free'` installation. That linked instance then reports usage and
key acknowledgements through the normal control-plane sync — so activation is
measured as the existing funnel in `admin_panel.activation_metrics`:
signup → instance created → came online (first key ack) → first usage →
**reached differentiated value (a completed fork)** → day-14 active. Free
installations are counted in a dedicated `free` bucket (totals + rate)
alongside `paid`.

**The differentiated-value stage.** Because the free tier is meant to be
adopted for *forking and recovery*, not just event recording, Actae meters both
primitives server-side:

- **Fork** — a successful `POST /api/v1/channels/fork` (which also backs
  `resume(fork_at_step=)`) *or* a group fork (`execution-group-forks`) writes
  one `fork_operations` row to the instance's `usage_delivery_outbox` in the
  same transaction as the fork, and increments the local counter
  `actae_forks_total` (`forks.total` in `GET /metrics.json`).
- **Crash recovery** — either of:
  - a session-metadata transition back to `started` on the *same* channel from
    `stepping` (a **hard kill**: SIGKILL / `os._exit`, which never runs a crash
    handler) or `crashed` (a **caught exception**). All three SDKs persist the
    intermediate `stepping` state, so a hard kill is observable. This is how
    `AgentSession` is counted automatically; or
  - recording the explicit **`run.recovered` marker event** (see below), the
    opt-in signal for clients that do not use `AgentSession`.

  Either path writes one `recovery_operations` row and increments
  `actae_recoveries_total` (`recoveries.total` in `GET /metrics.json`), in the
  same transaction as the triggering write and guarded so a retry or replay
  never double-counts.

Both rows are delivered to the control plane **only when the instance is
linked**; a standalone instance keeps them locally and sends nothing.
`activation_metrics` surfaces them as the per-installation `forks_completed` /
`recoveries_completed` flags and `reached_differentiated_value` (either), the
cohort `totals.reached_differentiated_value` / `totals.fork_operations` /
`totals.recovery_operations`, and the
`rates_percent.reached_differentiated_value` rate. Both instruments are
**server-side only** — never in the MIT SDKs.

### The `run.recovered` marker (opt-in for non-AgentSession clients)

Recovery is a *claim about what happened between two appends*, so the server can
never infer it from raw appends. Clients that manage their own loop — raw
`record()` calls, framework adapters, `curl` — can declare it explicitly by
recording one event of type **`run.recovered`** on the channel when they resume
after a crash. The server meters it as `recovery_operations` in the same
transaction (idempotent per `operation_id`). It is an ordinary, auditable event;
it never affects state, step resolution, or replay. `AgentSession` does not
emit it (its metadata contract is detected automatically), so the two paths
never double-count.

The marker string is exported from all three SDKs — Python
`actae_client.RECOVERY_MARKER_EVENT`, Go `actae.RecoveryMarkerEvent`,
TypeScript `RECOVERY_MARKER_EVENT`:

```python
await client.record(
    "run-42", RECOVERY_MARKER_EVENT,
    payload={"reason": "worker killed"}, actor="my-agent",
)
```

**Metering boundary (honest limits).** Recovery is measured only where a client
declares it — via `AgentSession` (automatic) or the `run.recovered` marker
(explicit). A raw channel that simply resumes without either signal is counted
as ordinary events, by definition. A session hard-killed before its first step
(metadata still `started`, no `stepping`) is also not counted, since no durable
progress was lost. So `reached_differentiated_value` means "completed a fork
(channel or group) **or** a declarable same-channel crash recovery".

> Funnel targets (link rate, 5-minute activation, fork completion, day-14) are
> **internal operating hypotheses**, not published commitments; the anonymous
> download denominator counts successful artifact *fetches*, not unique users,
> and a "linked instance" is not necessarily a unique customer.

### Exactly what a linked instance reports (`actae enroll`)

Linking is the only runtime-originated data Actae receives, and it is an
explicit authenticated action ("link = consent"). A linked instance's sync
uploads only:

- installation identity + the enrollment credential (the phone-home credential);
- API-key acknowledgements (which keys the runtime has received);
- subscription/license status exchange (to enforce the correct plan);
- **aggregate usage counters per day**: `events_ingested`, `ingress_bytes`,
  `durable_written_bytes`, `retained_bytes`, `state_bytes`, `fork_operations`,
  and `recovery_operations`.

It does **not** upload event payloads, saved state contents, tool arguments or
results, or any model input/output. An unlinked instance sends none of the
above. This list, the privacy policy, and the implementation are kept in
lockstep.

## The free plan (never enforced)

`effective_status()` returns `"active"` for any installation with
`plan_code = 'free'`, with no Polar subscription and no trial. The synced
`subscription` status the runtime reads is therefore always `active`, so a
linked free instance is **never locked** (no `402`/`503`). `create_installation`
gives a free installation no `trial_ends_at`. Free installations may mint API
keys and claim tokens in production (the paid gate is bypassed for them by
design).

## Endpoints and how to read the numbers

| Surface | Auth | Returns |
|---|---|---|
| `GET /v1/community/download/{platform}` | anonymous | presigned URL (and records the event) |
| `GET /metrics.json` | `SAAS_METRICS_KEY` | `community_downloads`: `{window_days, total, unique_ips, by_platform}` (30-day window) |
| `GET /api/admin/downloads?days=30` | `SAAS_ADMIN_KEY` | full stats: `{window_days, total, unique_ips, by_platform, by_day}` |
| `GET /api/admin/overview` | `SAAS_ADMIN_KEY` | `free_tier` block (30-day download stats) |
| `GET /api/admin/activation-metrics?days=30` | `SAAS_ADMIN_KEY` | install funnel with a `free` bucket |
| `POST /api/portal/free-installation` | portal session | create/return the account's free install + one-shot claim token |
| `POST /api/auth/signup` with `deployment_mode:"self_hosted"` | none | card-free account + free install + claim token |

Client-side, the marketing site fires a consent-gated GA4 `community_download`
event (only after "Allow optional" cookies) as a page-level complement; it is a
biased sample by design and is never the source of truth.

## Privacy & compliance

- The SaaS privacy policy discloses anonymous download logging (coarse,
  non-identifying) and the optional linked-account telemetry.
- The `ip_hash` uses the server's `SAAS_KEY_ENCRYPTION_KEY` as the HMAC key; if
  it is unset (some test/dev configs), no `ip_hash` is stored at all.
- There is no opt-out because there is nothing to opt out of from an unlinked
  instance. A linked instance can be unlinked at any time with
  `actae auth logout` (revokes the enrollment) or by deleting the installation
  in the portal.
