# Dashboard Pages

## Visual system

The operational dashboard shares the public SaaS website's technical-editorial
theme. Its base is near-black (`#090b0a`) with warm-white text, neutral
green-gray surfaces, restrained grid/aurora depth, and acid lime (`#c8ff3d`)
as the Actae brand, focus, live-status, selection, and primary-action color.
The authenticated shell uses the same Actae app icon and uppercase ACTAE
wordmark as the SaaS control plane; the brand in the sidebar (and on the login
page) links back to the product site (`NEXT_PUBLIC_SITE_URL`, default
`https://actae.dev`). The same icon set (`favicon.ico`, `favicon-16/32x32.png`,
`apple-touch-icon.png`, `android-chrome-*.png`) is served from `dashboard/public`
and declared in `app/layout.tsx` metadata, so the browser tab shows the real
favicon instead of a missing one.

This theme is centralized in `dashboard/tailwind.config.ts` and
`dashboard/src/app/globals.css`. Shared `card`, `input`, `button`, `badge`,
table, skeleton, React Flow control, and shell styles should be reused rather
than introducing page-local brand colors. Red, amber, blue, purple, and green
remain available for semantic errors, warnings, event categories, fork
comparison, and state nodes; acid lime must not erase those meanings.

The sidebar collapses to an icon rail on narrow screens. The login page mirrors
the SaaS story/form split while remaining specific to the self-hosted dashboard:
the identity is the **Actae portal account** (email/password), verified by the
control plane when enrolled; the portal's "Open dashboard" button auto-logs in
via a one-time `#sso=…` handoff. Standalone instances use local email/password
(signup opt-in via `ENABLE_SIGNUP`), and API keys are not a dashboard login
method.

The authenticated routes are deliberately not copies of one generic card grid:

- **Overview** is an editorial command center: large product statement, direct
  Flow/Event actions, numbered operational chapters, a runtime pulse deck, and
  telemetry grouped by operator question rather than API response shape.
- **Events** is an immutable ledger: its filters sit in a single instrument bar,
  the chronological index and sticky payload inspector share one bounded
  workspace, and event density remains suitable for long runs.
- **Experiments** is a fork laboratory: a persistent, height-bounded family
  index sits beside a gridded evidence stage for rankings, outcomes, and
  promotion; the combined **Fork diff** is a full-width section at the **end of
  the page** (below the bounded workbench), so it never competes with the list
  or summary for vertical space.
- **Flow** is a full-height workstation, not a document page. Its command bar,
  execution-map stage, activity drawer, live strip, and resizable inspector use
  the entire authenticated viewport.

`DashboardPageHeader.tsx` owns the shared editorial heading contract, while
`console-*`, `overview-*`, `experiment-*`, and `flow-*` classes in
`globals.css` define the separate page compositions. Responsive rules stack
page headers and workbench panes below 1024 px; the existing sidebar becomes an
icon rail below 760 px.

Structure of the Actae dashboard (`dashboard/src/app/dashboard/`) and the
navigation/deep-link conventions between pages.

## Pages

| Route | File | Purpose |
|-------|------|---------|
| `/dashboard/` | `page.tsx` | **Overview** — the home surface: polls `/metrics.json` every 5s for the status row (System + Mode pill showing `dev · embedded PG` vs `production`, uptime, updated-at), stat cards (**Connections**, **Channels**, **Total Events**, **Live Topics** — DB-backed, not WS counters), a **dismissible Getting-started panel** (intent cards; dismissal persisted per identity as `prefs.onboarding.seen`), and a **Recent runs** table (capped at 8, `View all runs` link) built from the additive `stats` field on `GET /api/v1/channels`. **Signal telemetry** (connections + WS message charts, queue health, topic subscriptions) is demoted behind a collapsed **System details** disclosure. Also renders a **subscription banner** when `/metrics.json` `subscription` reports `locked`/`unreachable`/`lock_imminent`. |
| `/dashboard/runs/` | `runs/page.tsx` | **Runs** — the primary run index (a “channel” is a run). Lists every run from `GET /api/v1/channels` `stats` (event count, latest cursor, last activity) with a recency status dot, a **fork** badge + parent (best-effort `GET /channels/{id}/metadata` enrichment for the first 60 runs), a client-side filter, and **Open run** / **Events** actions. 15s poll. This is the intended entry point; the Flow page is the run *detail*. |
| `/dashboard/flow/` | `flow/page.tsx` | **Run detail** (former “Flow”) — opens as a **single readable Execution Timeline** with a persistent **Timeline / Graph** switch and nothing else competing for attention. **Progressive disclosure**: the **Inspector starts closed** and opens automatically when a step is selected (a floating *Inspector* button reopens it); the **activity panel starts collapsed** (*Show activity*); and the inspector shows only the selected event, with the run’s State / Channel Stats / Event Types / Current Fork / Forks / State Diff / Decision Trail grouped under one collapsed **Run details** disclosure. Clicking a row uses the same selected-node contract as the graph. **Graph** retains React Flow for spatial fork topology. The toolbar keeps Run / Timeline-Graph / Types / Fields / Show-from-event / Export-Copy visible and moves Layout, grid-columns, and zoom into a real `…` overflow menu; the header has a real breadcrumb (`Runs / [parent] / <run>`) and a working **Rename run** pencil (`PUT /channels/{id}/metadata`). Reads `?channel=` to deep-link; without one it auto-selects the most recently active run. Live mode defaults on and the run list refreshes every 15s. |
| `/dashboard/experiments/` | `experiments/page.tsx` | **Forks** (route unchanged) — flagship fork laboratory. Merges explicit experiment groups with ordinary forks derived from durable `fork.started` events. An embedded GitHub-style red/green Fork diff compares any two members across state, configuration/manifest, tools, output, and metrics. This is the canonical compare home; the run detail’s Compare tab cross-links here (`Open Forks lab →`). |
| `/dashboard/events/` | `events/page.tsx` | **Events** — compact master-detail explorer. Dense oldest-first index + sticky right inspector (Tree/JSON). Search covers type, channel, actor, id and payload; filters, pagination, deep-links and live deduplication remain supported. Now also reads `?q=` (used by the command palette). |
| `/dashboard/approvals/` | `approvals/page.tsx` | **Approvals** — durable human-in-the-loop surface. Joins `approval.requested` / `approval.decided` / `approval.expired` events with pending `scheduler.wakeups` deadlines. Pending requests have **Approve/Reject** actions that record an `approval.decided` event as the signed-in dashboard user; Actae never resumes the agent itself. |
| `/dashboard/groups/` | `groups/page.tsx` | **Coordination** (former “Groups”, demoted under the sidebar’s **Advanced** section) — durable cross-machine coordination. Member cards, a causal-message timeline, and a counterfactual-fork summary render as UI; the raw fork/graph JSON now lives behind a per-section **View raw JSON** toggle instead of always-on `<pre>` dumps. |
| `/dashboard/states/` | `states/page.tsx` | Legacy compatibility route; immediately redirects to the Events ledger. State snapshots are inspected in the run detail’s Metadata/Compare surfaces. |

### Shell navigation, search and help

- `Sidebar.tsx` groups destinations by intent — **Monitor** (Overview, Runs,
  Events), **Compare** (Forks), **Review** (Approvals) — each with a one-line
  description, and puts **Coordination** behind a collapsed **Advanced**
  section. Routes are unchanged (labels only); `/dashboard/flow/` is a run
  detail that highlights **Runs**.
- `CommandPalette.tsx` (Cmd/Ctrl-K, mounted in `ProtectedLayout`) searches runs
  via `listChannels` and always offers an event-search fallback; state lives in
  `src/lib/ui-store.ts` (`useUiStore`).
- `HelpDialog.tsx` (sidebar footer `?`, also `useUiStore`) renders a
  plain-language glossary (run, event, cursor, fork, snapshot, approval, tool
  execution, coordination group) and closes on Escape.


Removed pages: **Users** (`/dashboard/users/`), **Health** (`/dashboard/health/`),
and **API Keys** (`/dashboard/api-keys/`) were deleted — health lives in
Overview, the users/API-key admin surface no longer exists (keys are managed
by the SaaS control plane, see `docs/CONTROL_PLANE.md`). The tenant system
was purged from the dashboard entirely (see git history).

## Node-type filtering (Flow + Events)

Runs of 3k+ events render as thousands of nodes, so both the Flow canvas and
the Events page expose a shared multi-select type filter
(`dashboard/src/components/EventTypeFilter.tsx`):

- **Flow** (`?channel=` deep-link, `flow/page.tsx`): the toolbar's **Types**
  dropdown lists every distinct event type present in the loaded timeline,
  with counts (long names wrap in the dropdown rather than truncating).
  Selecting a subset re-renders the React Flow canvas with only the matching
  nodes — `buildFlowNodes` (`src/lib/flow-layout.ts`) filters *before*
  layout, so the surviving nodes are re-chained sequentially in cursor order
  (links stay correct). State snapshots group under the pseudo-type
  `"state"`. Selection lives in the store (`flow-store.visibleEventTypes`,
  `Set<string> | null`, `null` = show all; reset on channel switch). "Show
  all types" button appears when the filter hides every node. The header
  keeps its own stacking context (`relative z-40`) so the dropdown paints
  above the React Flow canvas and stays clickable. The **grid layout is
  adaptive**: columns scale with the number of nodes actually displayed
  (`gridColumnsForCount`: 4 for ≤12 nodes → 16 for >500), so 100 nodes come
  out as rows of 8 and filtering narrows the grid too; the Layout pill shows
  the current column count.
  The Execution Timeline consumes the same filtered event collection but keeps
  chronological document layout; Graph-only layout and zoom controls are hidden
  until **Graph** is selected.

## Timeline / Graph visualization preference

`flow-store.visualizationMode` is `"timeline" | "graph"` and defaults to
`"timeline"`. `VisualizationSelector.tsx` exposes the two modes as an
accessible pressed-button group. The choice is stored in
`prefs.flow.visualizationMode` alongside `layoutMode`, `visibleEventTypes[]`,
and `visibleFields[]`; unknown stored values are ignored.

`ExecutionTimeline.tsx` is deliberately independent of React Flow. Semantic
classification lives in `src/lib/execution-timeline.ts`: state snapshots always
win, then fork, recovery, failure, completion, tool, active, and neutral event
families. Custom event types that do not match a known family remain neutral;
they are never incorrectly promoted to a recovery or failure boundary. Large
histories use browser `content-visibility` containment per row to avoid paying
full layout/paint cost for off-screen events without truncating evidence.
- **Events** (`events/page.tsx`): the same dropdown filters the rows
  **client-side** (the API call never sends `event_type` — offset pagination
  counts against the unfiltered, newest-first stream). The header shows
  "N of M events shown" while filtering, with a recovery button when nothing
  matches. The type + field filters **persist per identity** (see
  `docs/DASHBOARD_WORKFLOW.md` — `prefs.events.visibleEventTypes[]` /
  `visibleFields[]`, restored on mount, debounced save on change).

## Payload-field filtering (Flow + Events)

A second multi-select, next to **Types**, controls which **payload keys**
are displayed (same `EventTypeFilter` component, label "Fields"). It does
not filter nodes/events — only what the payloads render, so nodes can show
just the fields the user cares about (e.g. hide `description`,
`updated_at`…):

- **Counts** are per payload key across the current event set, computed by
  `computeFieldCounts` (`src/lib/payload-fields.ts`); the dropdown lists
  keys with how many events carry them.
- **Flow** (`flow/page.tsx`): selection lives in the store
  (`flow-store.visibleFields`, `Set<string> | null`, `null` = show all;
  reset on channel switch like `visibleEventTypes`). It applies to:
  - `EventNode` summary rows — `summarizePayload(payload, visibleFields)`
    drops hidden keys from both the known-key rows (model/status/tool/…)
    and the `key=value` fallback; a node whose payload has no visible keys
    shows "no details".
  - `FlowBottomPanel` (all three tabs): the timeline's Input/Output and
    Duration cells, the event-log payload `<pre>`, and the Metadata tab's
    `JsonTree` all render `filterPayload(payload, visibleFields)` (shallow
    key subset). State snapshots in the Metadata tab stay **unfiltered**
    (they are not event payloads).
  - `InspectorPanel` Selected Event detail (Text + JSON views) renders the
    filtered payload too; the Context Delta / Full Context sections are
    untouched (they are reconstructed context, not payload fields).
- **Live events** at or below an active replay cursor are dropped from the
  canvas and the live bar (`addLiveEvent` guard) so a filtered timeline
  never gets stale history resurfaced.
- **Events** (`events/page.tsx`): the Fields dropdown is local state (same
  as its type filter); card payloads (`<pre>` + `JsonTree`) render the
  filtered subset. Top-level event metadata (`id`, `channel_cursor`,
  `depends_on`) and the type filter are unaffected.

## Event log ↔ Events page deep link

Clicking an event row in the Flow page's bottom-panel **Event Log** tab
(`dashboard/src/components/flow/FlowBottomPanel.tsx` `openInEventsPage`)
navigates to:

```
/dashboard/events/?channel={channel}&cursor={cursor}
```

`cursor` is the **per-channel** cursor (replay returns it as `e.cursor`; the flow
helpers keep it as `DashboardEvent.cursor`). The Events page:

1. Sets the channel filter from `?channel=` (matching events via
   `channel_id`).
2. Fetches a window around the target: `cursor_start = cursor - 99`,
   `cursor_end = cursor`, `limit = 100` (per-channel cursors are sequential
   (1,2,3… per channel), so the target is guaranteed inside the window).
3. Highlights the matched event (ring + `scrollIntoView`) and, on click, syncs
   the URL via `router.replace` so the event is shareable.

The Timeline tab keeps its node-selection behavior; only the Event Log tab
navigates.

## Display order

- **Events page** renders events **oldest → newest** (cursor ascending), even
  though the server returns `ORDER BY cursor DESC`. The client sorts the
  fetched pages before rendering; "Load more" still pages through the
  newest-first stream underneath.
- **Flow canvas** lays out nodes in cursor order; the Flow page's auto-selected
  channel is the most recently active one (server-ordered).

## Node selection linkage (canvas ↔ Timeline)

Clicking a node in the React Flow canvas selects the matching row in the
bottom-panel Timeline and vice versa. Selection (`flow-store.selectedNode`)
carries `type` + per-channel `cursor` + `channel`:

- The Timeline highlights rows where `channel`+`cursor`+`type` all match, so
  inherited parent events and fork-owned events sharing a cursor never both
  light up, and the selected row is **scrolled into view** (`scrollIntoView
  block: nearest`) — with thousands of rows the match used to be invisible.
- The canvas mirrors the store selection by marking the matching node
  `selected` (blue ring) via `nodeMatchesSelection` (`src/lib/flow-layout.ts`).

## Channel selector overflow

Long channel ids (e.g. `chchchch…`) used to push the flow toolbar's
right-hand controls off-screen because native `<select>` elements size to
their longest `<option>` and no ancestor in the flex chain could shrink. The
chain now has `min-w-0` at every level (toolbar group → channel pill →
selector wrapper) plus `max-w-[240px]` on the flow selector (`max-w-[320px]`
on the Events page one); the full id is exposed via a `title` tooltip. The
flow selector no longer renders its own "Channel" label (the header pill
provides it).

## Forks

Forks are **created in code** (SDK `AgentSession.fork` / `fork_channel` — see
`docs/DASHBOARD_WORKFLOW.md`), not from the dashboard: the old **Fork from
here** button (`ForkButton.tsx`) was removed because inspecting a flow and
forking from one of its nodes is a code-level decision, not a viewer
feature. The dashboard is a viewer + inspector:

- Forks appear as new entries in the channel selector when the channel list
  auto-refreshes (Flow page 15s polling, Events page 15s polling).
- Selecting a fork renders its full inherited history (see
  `docs/DASHBOARD_FORKS.md` for the ancestry/merge machinery).
- The inspector's **Execution Tree** (`ForkTree.tsx`) shows the nested
  tree: display name or truncated id, reason, event count, latest cursor and
  a "forked @N" badge; the child channel starts with the server's
  `fork.started` marker so its Flow view carries the full inherited
  history.
- The toolbar's **Show from event** control (`ReplayControls.tsx`) is a pure
  *view filter*: N skips the first N events of the loaded timeline (the
  input is a 1-based position, translated to the per-channel cursor of the Nth
  event — the `cursor` column is per-channel (each channel numbers 1,2,3…
  independently), so raw cursor input would be meaningless on a timeline that
  spans multiple channels; values past the end of the
  timeline clamp to the last event, yielding an empty canvas). Placeholder
  "0" = full timeline, "Show all" resets. It does not fork or mutate
  anything. Next to it, **Export JSON** (download) and **Copy JSON**
  (clipboard) export the channel's record — metadata + ancestry + the
  current filtered view (Type/Fields filters and the Show-from-event
  cursor applied exactly like the canvas) — as pretty-printed, valid JSON
  for LLM investigation — see `docs/DASHBOARD_WORKFLOW.md` ("Exporting a run").

## API notes

- `listChannels` (`dashboard/src/lib/api.ts`) reads `GET /api/v1/channels`.
  The response is `{channels: [<id strings>], stats: {<id>: {event_count,
  latest_cursor, last_event_at, display_name, parent_channel_id}}}` — the
  `stats` object is additive (SDKs only read `channels`) and lets the Overview
  and Runs pages render names, counts and fork lineage in **one round-trip,
  with no per-channel metadata lookups**. `display_name`/`parent_channel_id`
  come from the `channel_metadata` LEFT JOIN; older servers that lack them make
  the Runs page fall back to bounded `GET /channels/{id}/metadata` enrichment.
- `getMetrics` reads `/metrics.json`, which includes DB-backed counters:
  `events: {total, channels, state_snapshots}` (the "Total Events" stat is
  this number, not the WS message counters), plus `mode`/`embedded_pg` and
  the `websocket` section. The old `resources` section was removed (it was
  never implemented and rendered a confusing red "unavailable").
- `queryEvents` (`dashboard/src/lib/api.ts`) posts to `/api/v1/events/query`
  with `channel_ids?: string[]` (omitting it queries **all** channels; the old
  singular `channel_id` field was silently ignored by the server — do not use
  it). Other supported filters: `event_type`, `actor`, `cursor_start`,
  `cursor_end`, `from`, `to`, `limit` (max 1000), `offset`.
- The query response returns both `cursor` (per-channel) and `channel_cursor`
  (deprecated alias, same value). The dashboard displays the per-channel
  `cursor` to stay consistent with the replay-based Flow log.
- `getMetrics` reads `/metrics.json`; `getLatestCursor(channelId)` reads
  `/api/v1/events/cursor/{channel}` (response field `latest_cursor`).
- `/metrics.json` also exposes `subscription` (control-plane enforcement
  state: `mode`, `status`, `lock_imminent`, `remaining_grace_secs`, ...) —
  the Overview banner renders from it. See `docs/CONTROL_PLANE.md`.
- Experiments use `GET /api/v1/experiments`, `GET
  /api/v1/experiments/{group_id}`, and `GET
  /api/v1/experiments/{group_id}/rank`. Outcome and promotion actions use
  `PATCH /api/v1/channels/{channel_id}/outcome` and `POST
  /api/v1/channels/{channel_id}/promote` respectively.
  Ordinary fork discovery also queries `POST /api/v1/events/query` for up to
  1000 `fork.started` events and reconstructs root families using
  `payload.source_channel_id`; explicit group baselines take precedence.
- The inline Fork diff calls `GET /api/v1/channels/compare` automatically
  when either selected member changes. Its aligned before/after cells render
  the authoritative structural diff; Flow remains the deeper event trace.
