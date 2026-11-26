# Actae TypeScript SDK

Distributed execution groups, durable waits, ownership fencing, causal graphs, and Group Fork policies/receipts are documented in [Execution Groups](EXECUTION_GROUPS.md).

Feature-parity port of the Python SDK (`actae-client`) and the Go SDK. Node
20+ ESM-only, single runtime dependency (`ws`). Source at `sdks/typescript/`,
published as `@actae/sdk`.

For application integration, `Actae.fromEnv()` is the preferred entry point.
It adds ambient `AsyncLocalStorage` run scopes, `effect`/`tool` wrappers,
native framework providers, serializable orchestrator carriers, and OpenAI
Agents `RunState` persistence while retaining the lower-level client as
`actae.client`. See [SDK Integration](SDK_INTEGRATION.md).

**Why TypeScript was chosen as the third SDK**: TypeScript is the most-used
language on GitHub (Octoverse 2025), every agent framework Actae integrates
with ships a TS SDK (LangGraph.js, LangChain.js, Claude Agent SDK, OpenAI
Agents JS, Google ADK), and Anthropic — the closest agent-native analog — is
the only major provider where JS SDK downloads exceed Python. The TS SDK
reaches the web/edge/serverless segment that Python and Go do not. Rationale
and market data in `docs/TYPESCRIPT_SDK_PLAN.md`.

**Parity gaps (deliberate, documented, not silent)**: no sync facade (JS is
natively async — `await` is the parity story); no CrewAI adapter (CrewAI has
no TypeScript port); Node-only in v1 (mTLS + `ws`); int64 values outside
±2^53 are returned as `bigint`, never silently precision-rounded.

## Package layout (`sdks/typescript/src/`)

| File | Contents |
|------|----------|
| `index.ts` | Public exports (mirrors Go package surface) |
| `client.ts` | `ActaeClient` — full HTTP API + WS delegations + facades |
| `client_ws.ts` | `WebSocketConnection` — wire protocol, reconnect, stream |
| `session.ts` | `AgentSession` — step/fork/resume lifecycle |
| `state_manager.ts` | `StateManager` — versioned state |
| `types.ts` | Typed models + `fromResponse` factories |
| `json.ts` | `parseJson` (BigInt-aware), `unwrapSonic`, `stringify`, accessors |
| `errors.ts` | Typed error hierarchy |
| `options.ts` | Options interfaces + channel-id validation + TLS resolution |
| `uuid.ts`, `version.ts` | UUIDv4, `SDK_VERSION` |
| `adapters/{langgraph,langchain,claude,openai}.ts` | Framework adapters |

The Go SDK (`sdks/go/`) is the closer structural reference (static typing +
big-int64 wire concerns already solved); the Python SDK is the behavioral
reference.

## High-level integration facade

`Actae.fromEnv()` keeps the full client at `actae.client` while adding scoped
application integration. `run` and `observe` establish an `AsyncLocalStorage`
scope; `effect` is the fail-closed, replayable mutation wrapper; `tool` is
read-only observability. `actae.langgraph`, `langchain`, `claude`, `openai`,
`copilot`, and `codex` expose native adapters without adding framework
dependencies to the core package.

For a deterministic scheduler such as Temporal or Azure Durable Functions,
workflow replay must not call the networked facade. Use the pure carrier in
the workflow and resume Actae in the activity/worker:

```ts
const temporal = actae.orchestrator('temporal');
// Workflow: serializable lineage only.
const carrier = temporal.carrier(workflowId);
// Activity/worker: network I/O is permitted here.
await temporal.activity({ id: activityId, parent: carrier, attempt }, runActivity);
```

`ActaeCarrier` uses the strict, credential-free `actae.run-carrier/v1`
schema. `parseCarrier` rejects unknown fields, so application payload and
secrets must travel in their own activity input fields.

## `ActaeClient`

Constructor: `new ActaeClient({ apiKey, endpoint?, wsEndpoint?, tls?,
timeout?, autoReconnect?, maxReconnectFailures?, keepAliveInterval?,
echoSelf? })`. `apiKey` is optional and resolved (explicit → `ACTAE_API_KEY` →
`~/.actae/client.json`) when omitted; `endpoint` or `wsEndpoint` is required
(same resolution). `tls` takes PEM `{ ca?, cert?, key?, rejectUnauthorized? }`
(mTLS / custom CA). `newClientFromEnv(opts)` reads `ACTAE_URL`, `ACTAE_WS_URL`,
`ACTAE_API_KEY`, then the client-credentials file (see
`docs/CLIENT_CREDENTIALS.md`).

`wsEndpoint` is derived from `endpoint` (`http→ws`, `https→wss`, `/ws`
appended) unless given explicitly.

### HTTP transport

`node:http`/`node:https` wrapper (no fetch/undici dependency). Request engine:
`do(method, path, query?, body?, headers?, signal?)`. Response bodies parsed
with `parseJson` (BigInt-aware); `text/*` bodies returned as strings; empty
bodies → `{}`. Path segments are percent-encoded (channel IDs validate to
`[A-Za-z0-9._:-]`).

### HTTP API methods (all `Promise<...>`, `X-API-Key` auth)

**Events** — `record(channelId, eventType, payload, {actor?, agentId?,
userId?, metadata?, operationId?, stepNumber?}) → Event` (POST
`/api/v1/events/record`; `operationId` idempotent; returned Event's
metadata/agentId/userId filled from opts); `replay(channelId, {cursor?,
limit?, eventType?}) → Event[]` (GET `/api/v1/events/replay/{id}`,
cursor-exclusive, limit default 100); `query({channelIds?, eventType?,
actor?, cursorStart?, cursorEnd?, from?, to?, limit?, offset?}) → Event[]`;
`getCursor/latestCursor(channelId) → number | undefined`; `transition(channelId,
eventType, payload, state, {actor?, operationId?, expectedVersion?,
expectedCursor?}) → {event, stateVersion}`.

**State** — `saveState(channelId, cursor, state, {expectedVersion?,
expectedCursor?}) → number` (version); `latestState(channelId) → StateSnapshot
| undefined`; `listStates(channelId, {limit?, offset?}) → StateVersionInfo[]`;
`getState(channelId, version) → StateSnapshot | undefined`; `deleteState`.

**Channels/forks** — `listChannels() → string[]`; `fork(source, child,
atCursor, {displayName?, reason?, experimentMetadata?, operationId?,
manifest?, toolPolicies?, expectedVersion?, expectedCursor?}) → ForkReceipt`
(POST `/api/v1/channels/fork`; `at_cursor=0` = latest; SnapshotBoundaryError
when no boundary; idempotent via `operation_id`; `toolPolicies` chooses the
child's side-effect policy `replay|block|live|auto`); `getForkReceipt`,
`resolveStep` (GET `/api/v1/channels/{id}/steps/{n}` → `StepResolution |
undefined` on 404), `latestStepNumber` (GET `/api/v1/channels/{id}/steps` →
highest step, O(1) recovery probe), `getChannelMetadata` (undefined on 404),
`listForks`, `getForkTree`,
`updateMetadata` (PUT partial), `deleteChannel(channelId, recursive)`,
`setOutcome(channelId, outcome, score?)`, `promoteChannel`, `diffStates(left,
right) → StateDiff`, `decisionTrail(channelId) → DecisionTrail`,
`compareChannels(left, right)`.

**Experiments** — `createExperiment(name, {description?, baselineChannelId?})`,
`listExperiments()`, `addExperimentMember(groupId, channelId, {role?,
declaredDelta?})` (role default `variant`), `rankExperiment(groupId)`.

**Tool executions** (claim-token-guarded) — `claimExecution(channelId,
keyName, toolName, params?, {dedupFields?, leaseSeconds?, emitReplayEvent?,
actor?}) → ExecutionClaim`; `completeExecution(id, claimToken?, result?)`,
`failExecution(id, message, {claimToken?, errorType?, stack?})`,
`heartbeatExecution(id, claimToken?, leaseSeconds?)`, `cancelExecution(id,
claimToken?)`, `getExecution(id)`, `listExecutions(channelId, limit?)`,
`deleteExecution(id)`.

**Consumer groups** — `createGroup(groupId, channelId, metadata?)`,
`listGroups({channelId?})`, `deleteGroup`, `joinGroup(groupId, consumerId,
leaseSeconds?)` (default 60), `claimWork(groupId, consumerId, limit?)`,
`ackWork(groupId, consumerId, cursor)` (CHANNEL cursor watermark),
`heartbeat`, `groupOffsets`.

**Wake-ups** — `scheduleWakeup(channelId, runAt, payload?)` (RFC 3339),
`listWakeups({channelId?, status?})`, `getWakeup(id)` (undefined on 404),
`cancelWakeup(id) → boolean` (throws `WakeupAlreadyFiredError` on 409).

**Health/metrics** — `healthCheck()`, `readinessCheck()`,
`getMetricsText() → string`, `getMetricsJson()`.

**Auth (JWT)** — `signup({email, password, name?})`, `login(email, password)`,
`logout(jwtToken)` (Bearer), `getMe(jwtToken)`.

### Facades

Namespaced read-only subsets (Python parity):
`client.events`, `client.state`, `client.channels`, `client.executions`,
`client.groups`, `client.wakeups`, `client.health`, `client.auth`,
`client.ws`. `client.events.record(...) === client.record(...)`.

## WebSocket (`client_ws.ts`)

`connect(signal?)` → auth frame `{"type":"auth","api_key":…}` → waits for
`authenticated` (timeout → `AuthError`; handshake failure → `ConnectionError`).
`disconnect()` disables auto-reconnect permanently. `isConnected()`,
`isAuthenticated()`, `connectionId()`, `subscribedTopics()`.

**Subscribe** — `subscribe(topic, cursor?, wait=true, signal?)` sends
`{"type":"subscribe","topic","cursor"}`; server catch-up replays
cursor-exclusive then live; `wait` blocks for the `subscribed` confirmation
(10s timeout). Per-topic `subscribed` map tracks the cursor.

**Publish** — `publish(topic, payload, {operationId?}, signal?) → Event`.
Sends `{"type":"broadcast",…,"request_id","operation_id"}`; awaits the
per-`requestId` Ack (correlated, so concurrent publishes and delayed acks can
never cross) with a FIFO fallback for legacy servers; returns the persisted
Event from the Ack. `echoSelf` delivers own publishes to local `onMessage`
callbacks (the server never echoes to the publisher).

**Stream** — `stream(topic, cursor?, signal?)` → `EventStream` (async
iterator of live events; ends on disconnect or `close()`).

**Message normalization** — externally-tagged frames
(`{"Connection":…}`, `{"Broadcast":…}`, `{"Subscription":…}`, `{"Ack":…}`,
`{"CursorSync":…}`, `{"Error":…}`, `{"Nack":…}`) are flattened to
`{"type":"connection"|"broadcast"|…}`. Broadcast payloads carry the event
nested as `payload.event`.

**Reconnect** — on drop: fires `onDisconnected`, then reconnects with backoff
(0.5s → 30s cap, `maxReconnectFailures` default 10) and resubscribes all
topics, then fires `onReconnect`. Keepalive: JSON ping every
`keepAliveInterval` (default 30s; negative disables).

**Callbacks** — `onMessage` accumulates; `onError`, `onSubscribed(topic,
cursor)`, `onDisconnected`, `onReconnect` are single-slot. Callback panics are
caught and routed to `onError`.

## `AgentSession` (`session.ts`)

`new AgentSession(actae, name, {displayName?, stateFn?, snapshotInterval?,
params?})`. Lifecycle `created → started → stepping → completed | crashed`.

- `start()` — records `session.started`, persists metadata.
- `step(stepType, {input?, output?, context?, metadata?}) → Event` — per-step
  `operationId` is a DETERMINISTIC UUIDv5 (`deterministicOperationKey` +
  `canonicalStepContent` in `src/deterministic.ts`, exported from the
  package root; byte-identical to the Go/Python SDKs — a crash-recovery
  re-drive of the same step replays server-side, changed content records
  anew; parity vectors pinned in `test/deterministic.test.ts`),
  `stepNumber`, 3 retries with backoff
  0.5→5s on `ConnectionError`/5xx/429, snapshot every `snapshotInterval`
  via `stateFn` (panic-safe, non-blocking).
- `fork(atStep, name, {displayName?, params?, stateFn?, reason?,
  boundaryMode?, manifest?, intervention?, toolPolicies?}) → AgentSession`
  (unstarted). Resolves the owning channel + cursor via the server step index
  (`resolveStep`), walking the lineage for inherited steps and falling back to
  replay with cursor-monotonicity validation. `boundaryMode` `'exact'`
  (default) raises `NoRestorableCheckpointError` on `SnapshotBoundaryError`;
  `'approximate'` falls back to `at_cursor=0` and reports drift; `'lineage_only'`
  no state copy. Primes the fork (stepCount=N, parent cursors prefix,
  inherited state; refuses contaminated state when
  `resolvedBoundary > requestedBoundary`). `intervention` is an opaque
  descriptor (application-applied) exposed via `session.intervention`;
  `toolPolicies` chooses the child's side-effect policy
  (`replay|block|live|auto`, default `auto`) and is exposed via
  `session.toolPolicies`. A refused call throws `ForkToolBlockedError`.
- `AgentSession.resume(actae, channelId, {forkAtStep?, name?, params?,
  stateFn?, snapshotInterval?, boundaryMode?, manifest?, intervention?,
  toolPolicies?})` — crash recovery (in-place) or fork-from-existing; raises
  `SessionCompletedError` on completed non-fork resume. Crash recovery probes
  `latestStepNumber` (O(1)) and reconstructs step progress from the event log
  when metadata lags, so a **hard** process death does not resume back at
  step 1.
- Human approval — `client.requestApproval(channel, {summary, details?,
  timeoutSeconds?, requester?, requestId?}) → request id`,
  `client.decideApproval(channel, requestId, {decision, actor, reason?})`
  (idempotent per `(requestId, decision)`), `client.waitForApproval(channel,
  requestId, {timeoutSeconds?, pollInterval?}) → Event | undefined`.
- Experiment helpers — `setOutcome(outcome, score?)`, `promote()`,
  `createExperiment(name, opts)`, `addToExperiment(groupId, opts)`.

Provenance getters: `forkReceipt`, `boundaryRestorable`, `requestedBoundaryCursor`,
`resolvedBoundaryCursor`, `sourceStateVersion`, `sourceStateSha256`,
`reproducibility`, `inheritedState`.

Manifest: `buildManifest(caller?)` merges caller fields over an environment
fingerprint (`node`/`platform`/`actae` SDK version).

## `StateManager` (`state_manager.ts`)

`new StateManager(actae, channel)`; `save(state) → version` (aligns to latest
cursor), `load()`, `resume(default?)`, `loadSnapshot()`, `listVersions()`,
`getVersion(v)`, `deleteVersion(v)`,
`fork(newChannel, {reason?}) → StateManager` (forks the latest state into a
new channel; the returned manager's `load()` returns the inherited state —
a `SnapshotBoundaryError` at the latest cursor retries once at `at_cursor=0`,
matching Python `StateManager.fork`).

## Adapters (`src/adapters/`, subpath exports)

Each adapter is structural (frameworks load lazily; absence surfaces a
helpful error). The LangGraph, LangChain, Claude and OpenAI adapters are all
**verified against their real frameworks** (devDependencies): `@langchain/langgraph`,
`@langchain/core`, `@anthropic-ai/claude-agent-sdk`, `@openai/agents`.

- **`@actae/sdk/adapters/langgraph`** — `ActaeCheckpointSaver` implements the
  LangGraph.js checkpoint-saver protocol (`getNextVersion`, `put`,
  `putWrites`, `getTuple`, `list`, `deleteThread`, `get`,
  `getDeltaChannelHistory`, `toJSON`) plus `forkThread`. Storage:
  cursor-aligned state snapshots on channel `{prefix}:{sha256(thread_id \0
  ns)[:16]}`; envelopes `{v:1, thread_id, checkpoint_ns, checkpoint_id,
  parent_checkpoint_id, pending_writes, event_cursor, event_id, checkpoint,
  metadata}` with serde-packed base64 blobs (pass LangGraph's own
  `JsonPlusSerializer` via `{ serde }` for full fidelity; default is a JSON
  fallback). `putWrites` merges idempotently by `(task_id, idx)` and buffers
  writes for not-yet-persisted checkpoints, flushed on the next `put`.
  Verified against a real `StateGraph`: compile+run, accumulation, `getState`
  resume, restart durability, `forkThread`, `deleteThread`.
- **`@actae/sdk/adapters/langchain`** — `ActaeContextSaver` (LangChain.js
  callback handler; `handleLLMEnd`/`handleToolEnd`/`handleChainEnd`, saves
  `{context: {messages, tool_outputs, chain_steps}}` every N callbacks) and
  `ChainResumer` (`resume(chain, defaultInput)` seeds from the last assistant
  message; `fork(newChannel, {reason})`). Verified against a real
  `CallbackManager` from `@langchain/core`.
- **`@actae/sdk/adapters/claude`** — `ActaeClaudeSessionStore` implements the
  Claude Agent SDK `SessionStore` protocol (`append`, `load`, `forkSession`,
  `delete`, `listSessions`, `listSubkeys`, `listSessionSummaries` — camelCase,
  per the real contract; snake_case aliases for Python parity). Dedups
  transcript entries by `uuid` (upsert) per the SessionStore idempotency
  contract; `load` returns `null` for absent/emptied keys; summaries are
  folded with the real `foldSessionSummary`. Deterministic channels
  `claude:{sha256(project \0 session)[:16]}` (+ `:idx:` project index
  channel); every append is an atomic `transition` recording a
  `claude.session.append` event. Verified against `InMemorySessionStore`
  behavior + compile-time `SessionStore` conformance.
- **`@actae/sdk/adapters/openai`** — `ActaeTracingProcessor` implements the
  real `@openai/agents` `TracingProcessor` (camelCase async `onTraceStart`/
  `onSpanStart`/`onSpanEnd`/`onTraceEnd`/`shutdown`/`forceFlush`; snake_case
  aliases for Python parity). `installActaeTracing(actae)` registers it via
  `addTraceProcessor`. Spans → `openai.span.end` events; each completed run
  persisted as a snapshot with the full span tree.
  `forkTrace(traceId, newChannel, {reason?})` pages the channel's state
  versions newest-first until a snapshot whose `state.trace_id` matches,
  forks at that snapshot's cursor into `newChannel` (fork-for-comparison —
  the trace snapshot), and throws `TypeError` when no snapshot
  matches (Python `fork_trace` parity). Verified against the real SDK:
  `withTrace` drives the lifecycle, real `Span` objects exercise
  extraction. (Note: the npm package `openai-agents` is an unrelated library;
  the real JS OpenAI Agents SDK is `@openai/agents`.)
  The high-level `actae.openai.run/resume/fork` API separately stores and
  restores the SDK's native serializable `RunState`; tracing is not used as a
  substitute checkpoint.
- **`@actae/sdk/adapters/codex`** — `CodexOTLPReceiver` hosts an OTLP/HTTP log
  endpoint (`POST /v1/logs`) mirroring Codex CLI `codex.*` events into
  per-conversation Actae channels (`codex:{conversation.id}`), with
  accumulating token/tool state snapshots so diff/trail/dashboard work on
  Codex sessions.
- **`@actae/sdk/adapters/copilot`** — `CopilotRecorder` (queue + worker,
  event-id dedup, event→cursor map, backfill via `session.getEvents`) and
  `CopilotManager` (startSession/resumeSession/fork/endSession/untrack/
  stopAll, snapshot/loadSnapshot/listSnapshots) for the `@github/copilot-sdk`
  — Go `actae/copilot` parity. Sessions record to `copilot:{session-id}`.
- **`@actae/sdk/adapters/otel`** — `ActaeOtelBridge` mirrors a channel's
  durable events into any OTel-shaped tracer as one span per event (GenAI
  semconv + `actae.*` identity attributes); `traceContext(traceId, spanId)`
  attaches an external trace context to event metadata. Dependency-free
  (duck-typed tracer). See `docs/OBSERVABILITY_INTEROP.md`.

## JSON / int64 model (`json.ts`)

- `JsonValue = null | boolean | number | string | bigint | JsonValue[] |
  JsonObject`.
- `parseJson(raw)` — number-aware parser; integers outside
  `±(2^53-1)` → `bigint`; sonic-rs markers
  (`{"$sonic_rs::private::JsonNumber":"…"}`) unwrapped in the same pass.
- `unwrapSonic(v)` — recursive marker replacement (number when safe,
  bigint when not, untouched on failure).
- `stringify(v)` — JSON serializer emitting `bigint` as raw number tokens
  (round-trip safe, no `JSON.stringify` TypeError), `Date` → ISO, cyclic
  detection, `undefined`/`function`/`symbol` handled.
- Accessors: `asInt64(v, def)`, `asBigInt(v)`, `asFloat(v, def)`,
  `asString(v, def)`, `asMap(v)`, `asList(v)`, `asBool(v, def)`,
  `asStringList(v)`.

## Errors (`errors.ts`)

`ActaeError` base. `AuthError`, `LockError` (402), `ConnectionError`,
`RateLimitError` (`retryAfterSeconds`), `APIError` (`statusCode`),
`NotFoundError`, `ServerError`, `SnapshotBoundaryError`,
`VersionConflictError`, `NoRestorableCheckpointError`,
`IdempotencyConflictError`, `ChannelConflictError`, `ConsumerError`,
`IdempotencyKeyMismatchError`, `ExecutionNotOwnedError`,
`ExecutionNotFoundError`, `ForkToolBlockedError` (409 `fork_tool_blocked` — a
forked channel's tool policy refused a call), `SessionError`,
`SessionCompletedError`, `WakeupAlreadyFiredError`.

## Testing

- Unit (`npm test`, no server): **336 tests across 23 files** — JSON model,
  error hierarchy, types, HTTP client (against `node:http` fakes), parity
  matrix (content-type/error-code/URL/payload/normalize), WebSocket (against
  a real `ws` server: auth, subscribe, publish/ack, broadcast, stream,
  reconnect, keepalive, error frames), AgentSession (against an in-memory
  `FakeActaeClient`), StateManager, adapters, env factory. Coverage
  (`npm run test:coverage`) ≈ 77% lines (src ≈ 80%).
- **Real-framework integration tests** (each framework is a devDependency):
  - `test/adapters_langgraph_real.test.ts` — a real `StateGraph` runs through
    `ActaeCheckpointSaver`: compile+run, accumulation, `getState` resume,
    restart durability (fresh saver over the same Actae data), `forkThread`
    independence, `deleteThread`.
  - `test/adapters_langchain_real.test.ts` — a real `@langchain/core`
    `CallbackManager` drives `ActaeContextSaver` (LLM/tool/chain callbacks)
    and `ChainResumer` seeds a real invoke from the saved context.
  - `test/adapters_claude_real.test.ts` — `ActaeClaudeSessionStore` satisfies
    the real `SessionStore` type (compile-time), matches
    `InMemorySessionStore` behavior, honors the uuid idempotency contract,
    folds summaries with the real `foldSessionSummary`, cascades deletes.
  - `test/adapters_openai_real.test.ts` — real `@openai/agents` `withTrace`
    drives the processor lifecycle (tracing re-enabled via
    `setTracingDisabled(false)`, since the SDK disables it under
    `NODE_ENV=test`); real `Span` objects exercise extraction + errors;
    `installActaeTracing` registers against the real SDK.
  - `test/adapters_codex.test.ts` — real HTTP `POST /v1/logs` OTLP exports
    are mirrored to channels with accumulating token/tool snapshots.
  - `test/adapters_copilot.test.ts` — recorder (dedup, ephemeral/type
    filters, backfill, event→cursor) and manager (start/resume/fork/end/
    snapshot) against a fake Copilot session.
- Live (`ACTAE_LIVE=1 npm run test:live`, needs `cd actae && cargo run`): record/replay/state/fork round-trip, WS two-client delivery,
  AgentSession step→fork with inherited state.
- **Live adapter-fork tests** (`test/adapters_fork_live.test.ts`, same
  `ACTAE_LIVE=1` gating) — every framework adapter's fork primitive against a
  real Actae: `ActaeCheckpointSaver.forkThread` (fork at an intermediate
  checkpoint, resume re-runs only the tail with the inherited message context),
  `ActaeClaudeSessionStore.forkSession` (fork discoverable via `listSessions`),
  `ChainResumer.fork` (inherited context on the new channel), and
  `CopilotManager.fork` (fork at a recorded event's cursor with inherited
  snapshot). Note: `ChainResumer` exposes `resume`/`fork` but no `loadContext`
  — read a fork's context with a fresh `ActaeContextSaver({ channel:
  fork.channel })`. A forked LangGraph config remains authoritative over the
  inherited snapshot's source `thread_id`, so continuation checkpoints stay
  isolated in the fork channel, including with a custom `channelResolver`.
- `npm run smoke` — a single end-to-end script against a running dev
  instance (health → events → state/fork → WS → session).
- CI: `.github/workflows/ci.yml` `typescript-sdk` job runs
  `npm ci && npm test && npm run typecheck && npm run build && npm pack
  --dry-run` on every push.
- After any change: `npm test && npm run typecheck && npm run build`.

## Known limitations

- Node-only in v1 (mTLS via `node:https`, `ws`). A browser/edge build would
  drop mTLS and swap in the native `WebSocket` (the `WebSocketConnection` is
  the single seam).
- No CrewAI adapter (no TS port upstream).
- No sync facade (JS is natively async).
- int64 > 2^53 arrives as `bigint`; `asInt64` returns the default when the
  value cannot be represented as a JS number.
