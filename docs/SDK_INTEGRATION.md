# SDK Integration: the Short Path

Actae never executes an agent from its dashboard or backend. The SDK runs in
the application process, the framework/orchestrator remains the execution
owner, and Actae stores the durable history, checkpoints, lineage and effect
ledger produced at those boundaries.

## The default API

Python:

```python
from actae_client import Actae

actae = Actae.from_env()

@actae.effect("order:{order_id}:charge")
async def charge(order_id: str, amount: int):
    return await payments.charge(order_id, amount)

async with actae.run(ticket.id, framework="support-agent"):
    result = await agent(ticket)
```

Synchronous Python preserves synchronous callables:

```python
@actae.effect("email:{message_id}")
def send_email(message_id: str):
    return mailer.send(message_id)

with actae.run_sync(job.id, framework="worker"):
    send_email(job.message_id)
```

TypeScript:

```typescript
import { Actae } from '@actae/sdk';

const actae = Actae.fromEnv();
const charge = actae.effect(
  { key: 'order:{orderId}:charge', name: 'charge' },
  async ({ orderId, amount }) => payments.charge(orderId, amount),
);

await actae.run({ id: ticket.id, framework: 'support-agent' }, async () => {
  await charge({ orderId: ticket.orderId, amount: 2500 });
});
```

Go:

```go
runtime, err := actae.NewRuntimeFromEnv(actae.ClientOptions{})
if err != nil { return err }

return runtime.Run(ctx, actae.RunOptions{
    ID: ticket.ID, Framework: "support-agent",
}, func(ctx context.Context, run actae.Scope) error {
    _, err := actae.Do(ctx, run, "order:"+ticket.OrderID+":charge", "charge",
        map[string]any{"order_id": ticket.OrderID, "amount": 2500},
        charge)
    return err
})
```

`ACTAE_URL` and `ACTAE_API_KEY` are the only required environment variables.
The facade owns shutdown helpers but does not connect until it performs work.

If a native execution can be delivered more than once, pass its stable
`execution_id` / `executionId` / `ExecutionID`. If the orchestrator exposes an
attempt number, pass `attempt`; the SDK derives `attempt:N` as the invocation
identity. Without either one, each entry receives a new UUID and is treated as
a new invocation. This identity affects lifecycle deduplication only—effect
keys must still describe the logical business operation and exclude attempts.

## Existing entry points can be one wrapper

Python:

```python
@actae.observe(lambda request: request.request_id, framework="support-api")
async def handle(request):
    return await existing_handler(request)
```

TypeScript:

```typescript
export const handle = actae.observe(
  { runId: request => request.requestId, framework: 'support-api' },
  existingHandler,
);
```

Go uses `Runtime.Observe`, which accepts a `RunOptions` resolver and a normal
`func(context.Context) error`.

## Native framework setup

The high-level object exposes one provider per installed framework. Providers
compose with existing hooks and callbacks; they do not replace application
configuration.

### LangGraph

```python
actae = Actae.from_env()
graph = builder.compile(checkpointer=actae.langgraph.checkpointer())
await graph.ainvoke(input, actae.langgraph.config(ticket.id))
```

```typescript
const graph = builder.compile({ checkpointer: actae.langgraph.checkpointer() });
await graph.invoke(input, actae.langgraph.config(ticket.id));
```

### Claude Agent SDK

```python
options = ClaudeAgentOptions(session_store=actae.claude.session_store())
```

```typescript
const options = { sessionStore: actae.claude.sessionStore() };
```

### LangChain

```python
callbacks = actae.langchain.callbacks(existing=existing_callbacks)
await chain.ainvoke(input, {"callbacks": callbacks})
```

```typescript
const callbacks = actae.langchain.callbacks(existingCallbacks);
await chain.invoke(input, { callbacks });
```

### OpenAI Agents

The tracing provider remains available for global span mirroring:

```python
processor = actae.openai.tracing()
```

The runner facade additionally persists the SDK's native serializable
`RunState`, allowing a process restart or an Actae fork to continue it:

```python
result = await actae.openai.run(agent, prompt, run_id=ticket.id)
result = await actae.openai.resume(agent, run_id=ticket.id)

fork_channel = await actae.openai.fork(ticket.id, "ticket-42-model-b")
result = await actae.openai.resume(
    agent_b, run_id="ticket-42-model-b", channel_id=fork_channel
)
```

TypeScript provides the same `actae.openai.run`, `resume`, `fork`, and
`tracing` concepts using `RunState.toJSON/fromString`.

### CrewAI, Copilot and Codex

Python exposes `actae.crewai.hook()/resumer()` and
`actae.codex.receiver()`. TypeScript exposes `actae.copilot.manager()` and
`actae.codex.receiver()`. Go Copilot users create a manager with
`copilot.NewManagerFromRuntime(runtime, options)`.

## Orchestrators

Actae is never a second scheduler:

| Owner | Responsibility |
|---|---|
| Orchestrator | scheduling, task queues, timers and execution retries |
| Agent framework | model loop, handoffs and native framework state |
| Application | business logic and external effects |
| Actae | event lineage, agent checkpoints and effect deduplication |

For orchestrators where workflow code may perform I/O:

```python
prefect = actae.orchestrator("prefect")
async with prefect.workflow(flow_run_id, attempt=attempt):
    await run_flow()
```

Temporal-style deterministic workflows are different: workflow replay must
not call Actae or any other network service. Construct a pure carrier in the
workflow, pass it through the activity input, and establish the Actae scope
inside the activity:

```python
temporal = actae.orchestrator("temporal")

# Workflow code: no I/O.
carrier = temporal.carrier(workflow_id).to_dict()
await workflow.execute_activity(process_order, {"carrier": carrier, "order": order})

# Activity/worker code: I/O is allowed.
async with temporal.activity(
    activity_id,
    parent=payload["carrier"],
    attempt=activity_attempt,
):
    await process_order(payload["order"])
```

The carrier contains channel/run lineage only—never credentials. Python,
TypeScript and Go use the same strict `actae.run-carrier/v1` JSON field names.
Unknown fields are rejected, so credentials or application payload cannot be
silently smuggled under the carrier schema.

An effect identity is based on the logical task/tool/business operation and
must exclude the retry attempt. `orchestrator.effect_key/effectKey/EffectKey`
provides a deterministic cross-SDK key. The attempt remains lifecycle
metadata, so dashboards can show retries without treating them as new
payments, emails or writes.

## Failure and privacy behavior

- Lifecycle writes default to `raise`: a durable run boundary is durable or
  it fails visibly. Set `lifecycle_errors="warn"` / `lifecycleErrors: 'warn'`
  only for telemetry-style adoption.
- Effect ownership is always fail-closed. A failed claim never executes the
  external operation.
- Application/tool errors are never masked by a secondary failure-recording
  error.
- Cancellation records `run.cancelled` / `tool.cancelled`, not a false
  failure. TypeScript effects receive an `AbortSignal` with `passSignal`;
  Go effects receive the normal `context.Context`; Python effects can opt in
  to a final `asyncio.Event` argument with `pass_cancel_event=True`. External
  clients must honor that signal for prompt cooperative cancellation.
- Losing an effect lease prevents completion and fails the execution ledger.
  An uncooperative blocking external call cannot be forcibly undone by any
  SDK, so business-level idempotency keys remain mandatory for payment,
  email and database APIs that support them.
- `@actae.tool` records read-only tool inputs/results. Disable capture for
  sensitive data; effect parameter mappers should retain only identity fields.
- Every effect result and checkpoint must be JSON-serializable. The SDK fails
  before claiming/completing ambiguous data.
- Existing LangChain/Copilot hooks are composed rather than overwritten.

## CLI onboarding and coding agents

```bash
actae doctor --framework --path services/agent
actae add                                  # detect + preview; no files changed
actae add --path services/agent --write    # monorepo-safe target
actae add --language python --framework langgraph
actae explain integration --json          # machine-readable integration contract
```

`actae add --write` uses create-new semantics and refuses to overwrite an
existing setup or environment example. It prints the exact install command,
supports `pyproject.toml`, `requirements.txt`, `setup.py`, `setup.cfg`,
`Pipfile`, `package.json` and `go.mod`, refuses ambiguous polyglot roots, and
never writes a real API key.

The lower-level `ActaeClient`, `ActaeRunContext`, adapters and service APIs
remain supported. The facade is additive, so existing integrations can
migrate one boundary at a time.
