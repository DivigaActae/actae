# Dashboard Workflow

How the Actae dashboard is meant to be used: forks are created in code, the
dashboard is a live viewer, channel lists auto-refresh, and the replay
control is a view filter. This document is the "plain English" companion to
the more mechanical `docs/DASHBOARD_PAGES.md`, `docs/DASHBOARD_FORKS.md` and
`docs/DASHBOARD_TESTS.md`.

## The mental model

The dashboard does **not** create anything. It observes:

- **Agents are seeded/forks are created from SDK code** (`AgentSession`),
  never from the UI. The old "Fork from here" button was removed: forking
  from node 5 of a 10-node flow is a code-level decision with access to
  context, and the dashboard has no business doing it silently.
- **The dashboard is live by default.** Both the Flow and Events pages
  subscribe to the WebSocket event stream and append new events as they
  arrive; a running agent renders itself without a manual refresh.
- **Channel lists auto-refresh** every 15s on both pages, so channels and
  forks created by SDK code appear without a reload.

## Operator surfaces

The interface maps each operator intent to a distinct surface. Start with the
**Runs** list (or the **Overview** home) and open a run to trace it. Use the
**Events** ledger when the question is “what was recorded?” and the **Run**
detail (`/dashboard/flow/`, the former Flow workstation) when the question is
“how did this execution unfold?” Use the **Forks** laboratory
(`/dashboard/experiments/`) when the question is “which fork won, and why?”
**Approvals** holds human decisions; **Coordination** (behind the sidebar’s
**Advanced** section) holds multi-agent groups. This presentation layer does
not change the read-only dashboard model or any of the API, live-stream, filter,
replay, comparison, and promotion contracts documented below.

Three shell affordances orient returning users:

- **Command palette** (Cmd/Ctrl-K, `CommandPalette.tsx`): jump to any run by
  name/id, or type a query to open the Events ledger with `?q=`.
- **Help & glossary** (`HelpDialog.tsx`, the sidebar `?`): plain-language
  definitions of run/event/cursor/fork/snapshot/approval, etc.
- **Getting started** (Overview, dismissible): four intent cards, dismissal
  persisted as `prefs.onboarding.seen` via the preferences API.

Long or dense surfaces use progressive disclosure: the Overview hides signal
telemetry behind **System details**; a run detail opens as **just the
timeline** — the **Inspector starts closed** (it opens when a step is selected)
and the **activity panel starts collapsed**, so a first-time user isn't shown
three views of the same run at once; the inspector shows the selected event and
keeps the run's State / Channel Stats / Event Types / Forks / State Diff /
Decision Trail behind one collapsed **Run details** disclosure; and the toolbar
moves Layout/zoom/columns into a `…` overflow menu.


Flow opens in **Timeline** mode: a chronological, live-following execution
record designed for reading. Failure, recovery, state, fork, tool, completion,
and active events receive distinct but deterministic treatment; unknown event
types stay neutral. Select any row to inspect the same payload, context, state,
lineage, and decision evidence exposed from a graph node. Switch to **Graph**
when fork topology matters more than chronology. The choice persists per
identity, while graph layout controls only appear in Graph mode.

**Framework internals are hidden by default.** A framework checkpointer (e.g.
LangGraph's `ActaeCheckpointSaver`) emits `langgraph.checkpoint` /
`langgraph.pending_writes` events that are infrastructure, not agent steps.
When a run ALSO has business events, the timeline opens with those internal
types hidden (`isFrameworkInternalType`, `flow-store.loadEvents`) so a newcomer
reads the agent's actual steps — "Selected 8 of 40 papers → Extracted 7
findings → …". The **Types** chip still lists them (unchecked; e.g. `4/6`), so
one click restores them; the default is never applied when the user already
chose a filter, and never when it would leave the timeline empty (a run with
only internals still shows everything).

## Fork entry points (code-first)

Forks are created by the SDKs, in three flavors:

1. **Python** — `session.fork(reason=..., display_name=...)` after a step;
   the fork inherits the parent's context up to the fork cursor and starts
   recording its own events.
2. **Go** — `agentSession.Fork(...)` (see `sdks/go/` and
   `docs/GOLANG_SDK.md`), identical semantics.
3. **Adapters** — LangGraph checkpoint saver, CrewAI, Claude Agent SDK,
   OpenAI Agents SDK adapters surface the same fork primitives
   (`docs/ADAPTERS.md`).

Every fork creates a new channel (`<parent>-fork-<ts>-<rand>` or a custom
name) that starts with the server's `fork.started` marker. The dashboard
picks it up via the 15s channel poll; opening it renders the full inherited
timeline (parent events up to the fork cursor + fork-owned events) — see
`docs/DASHBOARD_FORKS.md` for the ancestry/merge machinery and the
`forked @N` badge in the Execution Tree.

## Live topology

| Mode | API origin | WS origin | How the dashboard connects |
|------|-----------|-----------|---------------------------|
| Dev | `http://localhost:8002` (Actae, managed storage) | `ws://localhost:8002/ws` | `NEXT_PUBLIC_API_BASE=http://localhost:8002 npm run dev` (Next on :3000). `getWsUrl()` derives `ws` from the API base. |
| Embedded self-serve (dashboard on the same port) | `http://localhost:<port>` | `ws://localhost:<port>/ws` | Dashboard served from the same Actae port — `getWsUrl()` falls back to the page origin, no env var. The embedded loopback WS origin policy auto-allows the server's own origin (`http://localhost:<port>` / `127.0.0.1:<port>`) plus the Next dev server (`:3000`); without the own-origin entries the browser WS upgrade is rejected ("Origin not allowed" → LiveToggle stuck on Offline). |
| Production (docker) | `https://host:8002` (TLS) | `wss://host:8002/ws` | Dashboard served from the same Actae port, so `getWsUrl()` falls back to the page origin — no env var. |

### Preview the dashboard locally against a remote instance

The cloud API does **not** send `Access-Control-Allow-Origin`, so a browser at
`localhost:3000` cannot call it cross-origin (and the Next dev server cannot
upgrade the WS). To see the current dashboard source against a **hosted**
instance without deploying, build the static export and serve it through a tiny
same-origin reverse proxy (`dashboard/scripts/serve-cloud.mjs`, Node builtins
only — no deps):

```bash
cd dashboard
NEXT_PUBLIC_API_BASE= npm run build            # out/ calls /api on its own origin
ACTAE_CLOUD_ORIGIN=https://<instance>.cloud.actae.dev npm run serve:cloud
# open http://127.0.0.1:4180 and log in with your normal account
```

The proxy serves `out/` and forwards `/api`, `/ws`, `/healthz`, `/readyz`,
`/metrics` to `ACTAE_CLOUD_ORIGIN` (same topology as production, so WS and auth
work). `PORT` overrides `4180`. For **pure-local** development (hot reload,
local data) use `NEXT_PUBLIC_API_BASE=http://localhost:8002 npm run dev` against
a running `cd actae && cargo run` (the dev binary also serves `dashboard/out`
directly on the Actae port when present — `dashboard/dashboard.rs` resolution
order: `ACTAE_DASHBOARD_DIR` → embedded → `../dashboard/out`).

The shared WebSocket client (`dashboard/src/lib/live.ts`) is a singleton:
consumers subscribe to topics (channel ids) and register message listeners;
the client keeps one connection alive, authenticates with the session JWT
from `actae_auth` storage, reconnects with exponential backoff (1.5x, 30s cap,
20 attempts) and re-subscribes all topics after a reconnect.

The WS auth frame is `{"type":"auth","token":<session-jwt>,"payload":{}}` —
the JWT must go in the **`token`** field, never `api_key`: the server
resolves `api_key` only against the `api_keys` table (HMAC lookup), and a JWT
there fails and silently downgrades the connection to anonymous (dev) or
rejects it (prod), and anonymous principals are denied every read (no live
events). When no token is stored the field is omitted, so public instances
fall back to anonymous auth.

Wire format (mirrors the Python SDK `_normalize_msg`): the auth ack is
`{"type":"connection","event":"authenticated","payload":{...}}`; broadcasts
are `{"type":"broadcast","topic":...,"payload":{"event":{id,type,payload,
cursor,...}}}` (inner event wrapper, or event fields directly on `payload`);
messages may also arrive externally tagged (`{"Broadcast": {...}}`). Server
`{"type":"error","message":...}` frames are surfaced into the connection
status (`connection.error`), so LiveToggle/ExecutionTimeline labels show the
real failure reason instead of a stuck "Connecting".

### What is live vs not

- **Live (WS push):** events published to subscribed channels — Flow canvas
  nodes, Events page rows, connection status.
- **Polled (15s):** the channel list — catches code-created forks and new
  runs. The Events page additionally subscribes to *all* channels when "All
  channels" is selected, growing its topic set as the poll discovers more.
- **Not live:** state snapshots (state saves are not broadcast over WS; the
  Flow page needs a reload to pick up new `State Snapshot` nodes).

## Seeding patterns (getting data into the dashboard)

Three ways to have something worth looking at:

1. **Run an agent, watch it live.** Start the agent with
   `AgentSession(...).start(...)` and keep the Flow page open: nodes appear
   as steps are recorded.
2. **Replay a past run.** Use the toolbar's **Show from event** control
   (input + Apply): N skips the first N events of the loaded timeline
   (position semantics — the input is translated to the per-channel cursor of
   the Nth event, since the server-side `cursor` column is per-channel and
   gapless). Placeholder `0` = full timeline, **Show all** resets. This
   is a pure view filter — nothing is forked or mutated. The Events page
   deep-link `?channel=X&cursor=N` jumps straight to an event window.
3. **Fork + compare.** In code, fork a run at a specific step (see above),
   then open the fork's channel: the canvas shows inherited parent nodes
   (muted/dashed, `forked from <parent>`) chained into the fork's own
   events. The Execution Tree (`ForkTree`) gives the per-fork summary
   (reason, event count, latest cursor, `forked @N`). Open the bottom
   panel's **Compare** tab for the full-width, server-authoritative "What
   changed?" workspace. It compares the current channel with another fork
   across structural state, fork manifest/configuration, idempotent tool
   executions, final output, and recorded latency/token/cost metrics. The
   header names the lowest shared ancestor and reports each side's own
   divergence cursor (important when sibling forks split at different
   boundaries). The Inspector's compact **State Diff** remains available for
   a quick state-only check. The **Current fork** label shows the run's
   display name (resolved from channel metadata), not the raw channel id.

   Two readability rules apply to the **State** tab (`meaningfulStateEntries`):
   framework-adapter envelope paths (`_actae_adapter.*`, `checkpoint`,
   `checkpoint_id`, `parent_checkpoint_id`, `metadata`, `pending_writes`,
   `event_cursor`, `event_id`, `thread_id`, `checkpoint_ns`) are removed so a
   diff isn't just `channel_id`/`checkpoint_id` noise. When that leaves nothing
   (a framework stores its state as an opaque checkpoint), the tab says so and
   points at **Config / Output / Metrics**, where the business change lives.
   Populate those tabs by recording `experiment_metadata`
   (`metrics` + `result.output`) and a fork `manifest` from the SDK — see
   `docs/RESEARCH_AGENT_DEMO.md`.
4. **Declutter payloads.** The **Fields** dropdown (next to **Types** in the
   Flow toolbar and on the Events page) lists every payload key present in
   the current events with counts. Hiding a key removes it everywhere:
   node summary rows, the timeline's Input/Output + Duration cells, the
   event-log payload dump, the Metadata tab's tree, and the Events page
   cards (`filterPayload` / `summarizePayload(payload, visibleFields)`).
   Like the type filter it resets on channel switch; **Show all** restores
   every field. It's purely a display filter — nothing is fetched or
   mutated.

## Experiment workspace

The sidebar's **Forks** destination (route `/dashboard/experiments/`) is the
outcome-oriented layer over forks. It discovers ordinary SDK forks from durable
`fork.started` events, groups nested descendants under their root run, and
labels those families as **fork lineage**. Family and member names resolve
through channel metadata (`listChannels().stats.display_name`, with the
`fork.started` record's `display_name` as a fallback), so the lab shows
"Fork family · Research baseline · …" and "Fork A · +fact-check" rather than
hashed channel ids — consistent with the Runs list. When the SDK also registers a first-class experiment group,
that richer record takes precedence and the dashboard presents baseline/variant roles,
declared deltas, scores, outcomes, and the current leader. **Inspect** opens a
member in Flow, **Reject** records a rejected outcome, and **Promote** asks for
confirmation before invoking the append-only server promotion that copies the
winner's latest state onto its parent. No ranking or score is recomputed in
the browser.

The page ends with a full-width GitHub-style red/green **Fork diff** (below the
viewport-bounded workbench, so the family list and evidence stage scroll
internally rather than pushing the diff down).
Before/After selectors automatically request the authoritative combined
channel comparison and expose structural state, configuration/manifest, tool
execution, output, and metrics tabs. Operators only need **Inspect** when they
want the full event-by-event Flow trace.

## Exporting a run (JSON for LLM investigation)

The Flow toolbar has two buttons (between **Show from event** and the
more-options menu): **Export JSON** (downloads a `.json` file) and **Copy
JSON** (writes the same document to the clipboard). Both produce a
pretty-printed, always-valid-JSON document you can paste straight into an
LLM prompt for investigation.

The export **mirrors the current filtered view** — it is exactly what the
canvas renders with the active filters applied:

- **Events** are the store's loaded timeline (the merged inherited + own
  events the canvas shows, including the **Show from event** replay-cursor
  truncation), then the **Type filter** is applied with the canvas's exact
  semantics (`nodeTypeKey`: state snapshots group under the pseudo-type
  `"state"`), and each event's **payload is trimmed by the Fields filter**
  (`filterPayload`). `event_count` counts the filtered result.
- The header records which filters shaped the export:
  `filters: { event_types: [...] | null, payload_fields: [...] | null }`
  (null = no filter) — so an LLM knows the export is a filtered view.
- Each event carries `id`, `channel_id` (the source channel — inherited
  events keep their ancestor's id), `type`, `cursor`, `timestamp` (ISO
  string), `payload`, plus `is_state: true` for state snapshots (payload
  from the snapshot's `state`) and `inherited: true` for events from
  ancestor channels.
- Header fields: `channel_id`, `display_name`, `parent_channel_id`,
  `origin_run_id`, `forked_at_cursor` / `forked_at`, `fork_reason`,
  `experiment_metadata`, and `ancestry` (ancestor channel ids, immediate
  parent → root run).

Both buttons show inline state (`Exporting…`/`Copying…` → `Exported`/
`Copied` or `… failed`, with the error in the tooltip); they are disabled
while busy or when no channel is selected. Copy uses `navigator.clipboard`
with a legacy `execCommand` fallback for non-secure contexts. Mock mode
exports the `MOCK_EVENTS` timeline.

## Auto-refresh specifics

- Flow page: `flow-store.startChannelPolling()` (15s default) calls
  `loadChannels()` quietly — it only updates the list and auto-selects the
  most recently active channel when none is active. It **never** resets the
  current channel, events, selection or replay cursor. Cleanup on unmount.
- Events page: its own 15s `listChannels()` poll; the channel selector is
  fed from the page-owned list (`externalChannels`), and the live
  subscription topic set follows it.

## User preferences (view-state persistence)

Both the Flow page and the Events page persist their view state as a
per-identity preference so reopening the dashboard restores the same view.

**Flow page** (`prefs.flow.{layoutMode, visibleEventTypes[], visibleFields[]}`):

- `flow-store.loadPreferences()` runs on Flow page mount (after
  `loadChannels()`), fetches `GET /api/v1/user/preferences`, and applies the
  flow fields — empty arrays mean "show everything" (null filters), and a
  failed fetch is a silent no-op (never blocks the view).
- Every filter/layout mutation (`setVisibleEventTypes`, `toggleVisibleEventType`,
  `resetVisibleEventTypes`, `setVisibleFields`, `toggleVisibleField`,
  `resetVisibleFields`, `setLayoutMode`) calls `savePreferences()`, which
  debounces (300 ms) and PUTs the serialized `flow` object (sets → arrays)
  back to the server — rapid changes coalesce into one request so out-of-order
  completions can't persist stale state.
  Best-effort: a failed save is non-fatal.

**Events page** (`prefs.events.{visibleEventTypes[], visibleFields[]}`):

- On mount it fetches preferences and restores the type + field filters
  (empty arrays → show-all). A failed fetch is a silent no-op.
- Filter changes (the two `EventTypeFilter` dropdowns and the "Show all
  types" recovery button) go through a component-local debounced (300 ms)
  PUT of the `events` object; the timer is cleared on unmount so a pending
  save never fires after navigation.

Shared behavior:
- Identity is the dashboard session JWT (via `api.ts`'s `getPreferences` /
  `putPreferences` → Bearer token); the server shares the same `user_id`
  namespace with `X-API-Key` SDK clients.
- Mock mode (`NEXT_PUBLIC_MOCK_MODE`) short-circuits both directions.

## Files

- `dashboard/src/lib/live.ts` (+ `live.test.ts`) — shared WS singleton
- `dashboard/src/lib/flow-store.ts` — `startChannelPolling`/`stopChannelPolling`,
  `loadPreferences`/`savePreferences`
- `dashboard/src/hooks/useWebSocket.ts` — thin Flow-page wrapper over `@/lib/live`
- `dashboard/src/app/dashboard/events/page.tsx` — live log + poll + Live pill
- `dashboard/src/lib/api.ts` `getWsUrl()` — WS URL derivation
- `dashboard/src/lib/payload-fields.ts` — `filterPayload` + `computeFieldCounts` (Fields dropdown)
- `dashboard/src/components/flow/ReplayControls.tsx` — view-filter control
- `dashboard/src/components/flow/FlowHeader.tsx` — toolbar incl. **Export JSON** / **Copy JSON**
- `dashboard/src/lib/api.ts` `buildChannelExport()` (filter-aware export doc) / `downloadJsonFile()` / `copyToClipboard()`
- `dashboard/src/components/flow/CompareView.tsx` — full fork comparison workspace
