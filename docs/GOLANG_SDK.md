# Go SDK — Copilot Integration (`actae/copilot`)

`Runtime` is the preferred application/orchestrator entry point. It creates
run scopes in `context.Context`, records idempotent lifecycle boundaries,
routes external effects through the execution ledger, and provides a
credential-free `RunCarrier` for cross-process activities. The lower-level
`Client` remains available as `runtime.Client`. See
[SDK Integration](SDK_INTEGRATION.md).

Core Go SDK support for distributed execution groups, durable waits, ownership fencing, causal graphs, and Group Fork is documented in [Execution Groups](EXECUTION_GROUPS.md).

The `actae/copilot` package makes Actae work **fully** with the GitHub Copilot
SDK for Go (`github.com/github/copilot-sdk/go`, module tag `go/v1.0.8` →
`v1.0.8` in the Go proxy). Every Copilot session becomes a first-class Actae
channel: its events are recorded, replayable, forkable at any event, and
resumable with cursor-aligned state snapshots.

```
module github.com/divigaactae/actae/sdks/go   (the go.mod at sdks/go/)
imports: actae (root package) + github.com/github/copilot-sdk/go
```

`actae/copilot` is a sub-package of the single `sdks/go` module — import it
as `github.com/divigaactae/actae/sdks/go/copilot`; it is not a separate
module.

## Concepts

| Piece | Responsibility |
|-------|----------------|
| `Recorder` | One per Copilot session. Registers a `Session.On` handler, asynchronously persists every `SessionEvent` to the session's Actae channel, tracks `eventID → Actae cursor` for fork resolution. Never blocks the SDK's event loop. |
| `RecordingHooks` | Wraps a user `*copilotsdk.SessionHooks`; records each hook invocation as `copilot.hook.<name>` before chaining to the user's handler. Pass the result as `SessionConfig.Hooks`. |
| `Manager` | Tracks recorders by session ID, maps `sessionID → channel`, forks sessions at an event, snapshots/loads state, wires `StartSession`/`ResumeSession` end-to-end. |

## Runtime and deterministic orchestrators

`Runtime` is framework-neutral; it does not own a worker, scheduler, or
agent loop. `Runtime.Run` and `Runtime.Observe` place a `Scope` in
`context.Context`; `Scope.Effect` / typed `actae.Do` use the execution ledger
for mutations, while `scope.Run` exposes the regular event, state, fork,
experiment, wake-up, and execution-group APIs.

For Temporal or another deterministic scheduler, do not call `Runtime.Run`
from replayed workflow code. Construct only a `RunCarrier` there and open it
inside an activity:

```go
temporal, _ := runtime.Orchestrator("temporal")
// Workflow code: no network I/O; pass carrier in the activity payload.
carrier, _ := temporal.Carrier(workflowID)

// Activity/worker code:
err := temporal.Activity(ctx, carrier, actae.RunOptions{
    ID: activityID, Attempt: actae.Ptr(activityAttempt),
}, func(ctx context.Context, scope actae.Scope) error {
    _, err := actae.Do(ctx, scope, "order:"+orderID+":charge", "charge",
        map[string]any{"order_id": orderID}, charge)
    return err
})
```

`RunCarrier` is the strict cross-SDK `actae.run-carrier/v1` JSON schema. It
contains lineage only and rejects unknown fields; place business input and
credentials in separate activity payload fields.

## Channel naming

`<prefix>:<session-id>` with prefix default `"copilot"` (override via
`ManagerOptions.ChannelPrefix` or a recorder's `ChannelID`).

## Event model

Every Copilot `SessionEvent` becomes one Actae event:

- **event type**: `copilot.<SessionEventType>` — e.g.
  `copilot.user.message`, `copilot.tool.execution_start`,
  `copilot.assistant.message`, `copilot.session.error`,
  `copilot.subagent.completed`.
- **payload**: `{"event_type": string, "data": <JSON of the typed data>}`
  (the `data` object is the SDK's typed struct marshalled verbatim — field
  names are the SDK's camelCase wire names).
- **metadata**: `session_id`, `event_id` (Copilot event UUID — the dedup
  key), `parent_event_id`, `agent_id` (sub-agent events), `ephemeral`.
- **lifecycle events**: `copilot.session.started` (on `Attach`),
  `copilot.session.info` (session metadata — summary, start/modified
  times, working directory, repository, fork; recorded by
  `StartSession`/`ResumeSession`), `copilot.session.ended` (once, via the
  `session_end` hook or `EndSession`), `copilot.hook.<hook_name>` (via
  hooks).

### Coverage — "do we get everything?"

The recorder is **fully generic**: all 113 `SessionEventData` types in the
Copilot SDK flow through it with no type switch and no special-casing, so
nothing new added by the SDK is ever missed. The `parent_event_id` chain
lets you reconstruct the complete session timeline from Actae.

What is *not* recorded by default, and why:

| Not captured by default | How to enable |
|-------------------------|---------------|
| **Streaming deltas** — `assistant.message_delta`, `assistant.reasoning_delta`, `assistant.streaming_delta`, `assistant.tool_call_delta` are marked `Ephemeral` by the runtime (transient; not persisted to the session's on-disk event log) | `RecorderOptions.IncludeEphemeral: true` — live capture only; these events are **not** in `GetEvents` history, so they cannot be backfilled |
| Events excluded via `EventTypes` filter | remove the filter |
| Session-level metadata (summary/context) | automatic via `copilot.session.info` (see above) |

Dedup: events are deduplicated by Copilot event ID (bounded window of 5000).
Transient events are skipped unless `RecorderOptions.IncludeEphemeral`.
`RecorderOptions.EventTypes` restricts recording to a subset of types.

## Quick start

```go
import (
    actae "github.com/divigaactae/actae/sdks/go"
    copilot "github.com/divigaactae/actae/sdks/go/copilot"
    copilotsdk "github.com/github/copilot-sdk/go"
)

db, _ := actae.NewClient(actae.ClientOptions{APIKey: "…", Endpoint: "http://localhost:8002"})
cli, _ := copilotsdk.NewClient(copilotsdk.ClientOptions{Connection: "/path/to/copilot"})
defer cli.Stop()

mgr := copilot.NewManager(db, copilot.ManagerOptions{})
defer mgr.StopAll()

// Creates the session with recording hooks wired, tracks it, records
// copilot.session.started.
handle, err := mgr.StartSession(ctx, cli, &copilotsdk.SessionConfig{})
// ... user hooks are chained: config.Hooks is REPLACED by the wrapper
_, err = handle.Session.SendPromptAndWait(ctx, copilotsdk.MessageOptions{Prompt: "…"})

// Fork the session at a specific event:
newChannel, err := mgr.Fork(ctx, handle.Session.SessionID, eventID,
    copilot.ForkOptions{NewChannelID: "experiment/refactor-v2"})

// State snapshots for crash recovery:
version, _ := mgr.Snapshot(ctx, handle.Session.SessionID, map[string]any{"task": "…"})
state, _ := mgr.LoadSnapshot(ctx, handle.Session.SessionID)
```

## Manager API

| Method | Behavior |
|--------|----------|
| `NewManager(db, opts)` | prefix + default recorder options |
| `ChannelFor(sessionID)` | `prefix:sessionID` |
| `NewRecorder(opts)` | unattached recorder preconfigured with prefix |
| `StartSession(ctx, client, *SessionConfig)` | wire hooks → `CreateSession` → `Attach` → record `copilot.session.info` → track; returns `*SessionHandle{Session, Recorder, Channel}` |
| `ResumeSession(ctx, client, sessionID, *ResumeSessionConfig)` | same for `ResumeSession` |
| `Track(ctx, *copilotsdk.Session)` | attach+track an existing session (idempotent) |
| `RecorderFor(sessionID)` | lookup |
| `EndSession(ctx, sessionID, reason)` | records `copilot.session.ended` (idempotent — see below) |
| `Untrack(sessionID)` / `StopAll()` | stop recorder(s) (Copilot session untouched) |
| `Fork(ctx, sessionID, atEventID, opts)` | resolve cursor for the event, `actae.Fork` (**strict boundary** — `SnapshotBoundaryError` propagates rather than silently forking from later state); returns new channel ID |
| `ContinueSession(ctx, client, sourceSessionID, atEventID, *SessionConfig, opts)` | **fork + continue in one step** (see below) |
| `Snapshot` / `LoadSnapshot` / `ListSnapshots` | `StateManager` over the session channel |

`Fork` cursor resolution: (1) recorder in-memory `eventID → cursor` map,
(2) replay of the channel matching `metadata.event_id`. Fork metadata
records `forked_from`, `forked_at_event`, `forked_at_cursor`, `status`.

## Forking and continuing

Forking in the Copilot world has two steps, both first-class:

1. **`Manager.Fork`** — forks the session's Actae channel at a specific
   event (any point in the session, resolved via the recorder's
   event→cursor map, falling back to a replay of the channel). The server
   sets `parent_channel_id`, so the fork shows up in the execution tree /
   dashboard. Fork metadata (`forked_from`, `forked_at_event`,
   `forked_at_cursor`) is written to the fork channel. When no saved state
   boundary exists exactly at the event cursor, the fork falls back to the
   latest state (`at_cursor=0`) — same semantics as `AgentSession.fork`.

2. **`Manager.ContinueSession`** — starts a **new Copilot session that
   records directly into the fork channel**, mirroring the Python
   `AgentSession.fork()` contract where the fork channel *is* the new
   session. The channel then carries the complete lineage:
   - source events up to the fork event (copied by the server fork),
   - `copilot.session.forked` (links `source_session_id`,
     `source_channel`, `forked_at_event`),
   - the continuation's `copilot.session.started` + all new events,
   - any `copilot.session.info` / snapshots.

```go
// fork the tracked session at a real event and continue there
cont, err := mgr.ContinueSession(ctx, cli, handle.Session.SessionID, eventID,
    &copilotsdk.SessionConfig{Model: "gpt-5", ClientName: "experiment"},
    copilot.ContinueSessionOptions{ForkChannelID: "exp/refactor-v2"})
// cont.Session is a live Copilot session; cont.Channel is the fork channel

// seed the continuation from the fork point:
history, _ := db.Replay(ctx, cont.Channel, actae.ReplayOptions{Limit: 100}) // or LoadSnapshot
_, _ = cont.Session.SendPromptAndWait(ctx, "Continue from: " + summarize(history))
```

Note the Copilot SDK itself has no forking concept — sessions are
linear. The fork lives in Actae (the fork channel + event chain); the
continuation is a fresh Copilot session whose history is the fork channel.
Use `LoadSnapshot`/replay on the fork channel to seed the new session's
first prompt or `SystemMessage`.

## Lifecycle events and backfill

- **`copilot.session.started`** — recorded synchronously by `Recorder.Attach`.
- **`copilot.session.ended`** — recorded **at most once** per session
  (`Recorder.RecordSessionEnded` guards against duplicates). Two sources
  feed it, safely idempotent against each other:
  1. the `session_end` hook (automatically wired by `RecordingHooks` — fires
     whenever the Copilot session ends, using the hook's `reason`), and
  2. an explicit `Manager.EndSession(ctx, sessionID, reason)` call.
- **`Recorder.Backfill(ctx) (int, error)`** — live-attach recovery: pulls the
  session's full event history via the SDK's `Session.GetEvents(ctx)` and
  records everything not already seen. Dedup is by Copilot event ID, so
  events delivered both live and via history are recorded exactly once.
  Call it right after `Attach`/`Track` when resuming a session that has been
  running without a recorder (e.g. after a restart). Same
  `Ephemeral`/`EventTypes` filters apply; history-fetch failures return an
  error without losing or duplicating anything already recorded.
  **Ordering caveat**: backfilled events are appended at the end of the
  channel, so if live events have already flowed, Actae cursor order no
  longer matches chronological order. Call `Backfill` immediately after
  `Attach` (before the first live event) to keep the channel chronological;
  the `parent_event_id` chain always preserves true causality regardless.

## Recorder threading model

- `Session.On` handlers and session hooks run on the SDK's serial dispatch
  goroutine / JSON-RPC loop → the recorder only marshals + pushes to a
  buffered queue (`QueueSize`, default 1024; drop-with-log when full).
- A single worker goroutine persists jobs to Actae (30s per-record timeout;
  failures logged and dropped — recording must never stall the agent).
- `Stop()` unsubscribes, closes the queue and **drains** pending jobs.
- Hooks can fire before `Attach` (the session-start hook runs inside
  `CreateSession`): jobs are queued and the worker starts lazily, deriving
  the channel from the hook invocation's `session_id` if needed.
- `Recorder.Attach` is idempotent; the recorder is safe for concurrent use.

## Hook recording types

`copilot.hook.pre_tool_use`, `copilot.hook.post_tool_use`,
`copilot.hook.post_tool_use_failure`, `copilot.hook.user_prompt_submitted`,
`copilot.hook.session_start`, `copilot.hook.session_end`,
`copilot.hook.error_occurred`, `copilot.hook.pre_mcp_tool_call`.

Payload: `{"hook": "<name>", "input": <hook input JSON>}` (the SDK's typed
input structs, camelCase wire names). Hook outputs are never modified by
the recorder; a nil user handler becomes a record-only handler.

## Testing

```bash
cd sdks/go && go test ./copilot/ -race
```

Unit tests use an in-memory fake Actae and drive the recorder through its
public API. Coverage: event recording + dedup + payload/metadata shape,
ephemeral filtering, EventTypes filter, Stop draining, hook recording
(pre-attach included), custom channel IDs, hook chaining + record-only
fallback, session.ended idempotency (explicit + hook-fired), Backfill
(dedup against live events, error paths), session.info metadata recording
(+ failure tolerance), Manager fork (in-memory + replay fallback + error
paths), snapshots, End/Untrack, Track idempotency, and channel naming.

### Live integration test

`copilot/live_test.go` (`TestLiveEndToEnd`) drives the full lifecycle
against a **real** Copilot runtime + Actae: `StartSession` → real prompt →
events streaming into Actae → fork at a real recorded event →
`session.ended`. Skipped unless provisioned:

```bash
ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
  go test ./copilot/ -run TestLive -v
# COPIT_CLI=/path/to/copilot overrides the runtime binary (default: the
# SDK's bundled CLI). Needs the Actae server itself to accept API keys.
```
