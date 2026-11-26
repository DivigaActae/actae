# Control-plane split (done)

The commercial control-plane client has been removed from the community build.
This document records what changed and what remains in the tree.

## What was removed

- The enrollment/sync HTTP client (`client.rs` was 879 lines; `sync.rs`, 313).
- Device-flow login (`actae auth login|status|logout|sync`).
- Portal-key enrollment (`actae enroll`).
- Subscription enforcement (key and anonymous blocking).
- Usage outbox delivery and retained-bytes reporting to a control plane.
- The `doctor` command's control-plane reachability checks.

## What remains, and why

| File | Why it stays |
|---|---|
| `src/control_plane/mod.rs` | Keeps the `ControlPlaneManager` surface used by 20+ call sites; it now always reports standalone |
| `src/control_plane/enrollment.rs` | Local file format for `~/.actae`, used by the SDK credential path |
| `src/control_plane/license.rs` | Ed25519 verification for dashboard/license tokens; no network, public-key only |
| `src/control_plane/client.rs` | Three wire types and `full_access_permissions()`; no HTTP client |

## Behavior

- `ControlPlaneManager::new` never fails, never enrolls, never syncs.
- `is_enrolled()` is always `false`; `blocks_key()` / `blocks_anonymous()` are
  always `None`; `lock_imminent()` is always `false`.
- `federation_config()` and `effective_source_policy()` return local config only.
- `status_json()` reports `mode: "standalone"`.
- `actae auth` and `actae enroll` are gone from the CLI. `actae doctor` is gone.

## Verification

```bash
cargo check
cargo check --tests
cargo build
strings target/debug/actae | grep -Ei 'saas\.diviga|diviga_'
```

Expected: the build succeeds; the only remaining match is the local API-key
prefix fixture in tests. There are no `saas.diviga.dev` strings and no client
endpoints.

## Private build

The managed build keeps its own copy of the removed client and sync loop in the
private `diviga/cloud` repository. The two trees share the event store and the
HTTP API; only the licensing/enforcement layer differs.
