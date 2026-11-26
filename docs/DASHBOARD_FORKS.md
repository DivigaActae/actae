# Dashboard Forks

This document describes how flow forking works in the Actae dashboard and the
invariants maintained by the frontend and backend.

## User-visible semantics

Forks are created in code (SDK `AgentSession.fork`, see
`docs/DASHBOARD_WORKFLOW.md`); the dashboard only displays them. When a
channel is forked at cursor `N`, the fork's Flow page must display:

1. The full inherited history from the parent (and all ancestors, for nested
   forks) up to and including cursor `N`.
2. Any events recorded on the fork itself after the fork point.

Example: in a 10-node parent run, forking at node 5 must show nodes 1-5 from
the parent, plus any new nodes created by the fork. The dashboard discovers
the new channel via its 15s channel-list poll — no reload needed.

## Why this is non-trivial

A forked channel starts its own per-channel cursor sequence at 1. That means
inherited parent events and fork-owned events can share cursor values (e.g.
both have a cursor-3 event). If the canvas builds React Flow node ids from
cursor alone, the two events collide and one silently disappears.

## Frontend implementation

### Node identity

`dashboard/src/lib/flow-layout.ts` generates globally unique React Flow node ids:

- Events with an `id` field: `evt-{channel}-{eventId}`
- Events without an `id`: `event-{channel}-{cursor}-{index}`
- State snapshots: `state-{channel}-{cursor}-{index}`

Because the id includes the channel (and the event id when available), inherited
and fork-owned events with the same cursor can coexist on the canvas.

### Ancestry resolution

`dashboard/src/lib/flow-helpers.ts::resolveAncestry()` walks the
`channel_metadata` parent chain from the active channel up to the root run. It
detects cycles and retries once on 404 (useful immediately after a fork is
created).

### Inherited timeline

`loadInheritedTimeline()` builds the full inherited history in a single pass
over the caller-resolved ancestry chain (post-audit hardening — the old
version re-resolved ancestry for every element AND appended parent events
before grandparent events, i.e. reverse chronology):

- The caller passes the **fully resolved** chain (immediate → root).
- It is reversed to root → leaf and each channel is replayed **exactly once**
  for the segment it contributes: `[1, hop.forkCursor]` (the cursor in that
  channel's stream at which the next-deeper channel split).
- Every inherited event is marked `isInherited: true`.

This guarantees a nested fork shows the complete root timeline oldest-first:
`root segment → intermediate segments → immediate parent segment`.

### Pagination

`paginateReplay()` pages through the backend replay API using the `cursor`
offset and `limit` parameters, so parent histories larger than the backend's
default 100-event page are fully loaded.

### Event merging

`mergeEvents()` combines inherited and own events:

- Inherited timeline is preserved in chronological order.
- Own events are sorted by cursor and appended.
- Duplicate event ids are removed.

The resulting order is:

```
root events 1..N -> parent events 1..M -> fork events 1..K
```

which matches the expected fork view.

## Backend implementation

### `fork_channel`

`actae/src/events/store.rs::fork_channel()` now inserts a row into
`channel_cursors` for the new channel:

```sql
INSERT INTO channel_cursors (channel_id, last_cursor) VALUES ($1, 0)
```

`last_cursor = 0` means:

- The fork is immediately discoverable via `list_channels()`.
- The first event published to the fork receives `cursor = 1`, starting
  the fork's own cursor sequence.

### Discoverability

`list_channels()` queries `channel_cursors`. Before this fix, a fork did not
appear in the channel selector until the first event was published.

## Files involved

| File | Responsibility |
|------|----------------|
| `dashboard/src/lib/flow-helpers.ts` | Ancestry resolution, replay pagination, event merging |
| `dashboard/src/lib/flow-layout.ts` | React Flow node/edge construction with unique ids |
| `dashboard/src/lib/flow-store.ts` | Zustand store that orchestrates loading |
| `dashboard/src/components/flow/EventFlowCanvas.tsx` | Renders the React Flow canvas |
| `dashboard/src/lib/api.ts` | API layer, including mocked `replayEvents`/`loadState` |
| `actae/src/events/store.rs` | Backend `fork_channel` implementation |

## Running tests

Dashboard unit tests:

```bash
cd dashboard
npm install
npm test
```

Rust backend tests (requires running PostgreSQL):

```bash
cd actae
cargo test --test event_store_test fork_channel
```
