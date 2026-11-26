# Durable Human Approval

A long-running agent often needs to stop and wait for a person: approve a
contract, release a payment, sign off on a deployment. The agent process may
not stay alive for days, and the request must not be lost if a worker dies.
Actae packages this as a small, durable pattern over primitives that already
exist — **no server-side execution, no callback into the agent**.

Actae records the request and the decision; the **application** decides what an
approval means and resumes itself.

## The three events

| Event | Recorded when | Payload |
|---|---|---|
| `approval.requested` | the agent asks for a decision | `{request_id, summary, details?, timeout_seconds?}` |
| `approval.decided` | a human/service resolves it | `{request_id, decision: "approved"\|"rejected", reason?}` (the event `timestamp` is the decision time) |
| `approval.expired` | the wait timed out | `{request_id}` |

They are ordinary channel events (visible in the dashboard's Flow/Events
pages, replayable, WebSocket-deliverable). When a timeout is supplied, Actae
also schedules a **persisted wake-up** so the deadline survives restarts.

## SDK helpers

Python (`ActaeClient`):

```python
request_id = await actae.request_approval(
    channel, summary="Send the contract to legal", details={"amount": 1200},
    timeout_seconds=86400,
)
# ... the agent can exit or move on; a human (dashboard/service) later calls:
await actae.decide_approval(channel, request_id, decision="approved", actor="alice")
# ... another worker (or the same one after restart) observes the verdict:
event = await actae.wait_for_approval(channel, request_id, timeout_seconds=86400)
# event is None on timeout (an approval.expired event is recorded)
```

`AgentSession` convenience:

```python
result = await session.require_approval("Ship this release?", timeout_seconds=3600)
if result["approved"]:
    ...
```

Go:

```go
requestID, _ := client.RequestApproval(ctx, channel, actae.RequestApprovalOptions{
    Summary: "Send the contract", TimeoutSeconds: actae.Ptr(86400.0),
})
client.DecideApproval(ctx, channel, requestID, actae.DecideApprovalOptions{
    Decision: "approved", Actor: "alice",
})
event, _ := client.WaitForApproval(ctx, channel, requestID, time.Hour, time.Second, 0)
```

TypeScript:

```ts
const requestId = await client.requestApproval(channel, { summary: 'Send the contract' });
await client.decideApproval(channel, requestId, { decision: 'approved', actor: 'alice' });
const event = await client.waitForApproval(channel, requestId, { timeoutSeconds: 3600 });
```

## Semantics and guarantees

- **Durable across restarts.** The request, decision and timeout wake-up live
  in PostgreSQL. A crashed agent that re-drives `require_approval` records the
  same `approval.requested` event (`request_id` is the `operation_id`) instead
  of duplicating it.
- **The agent does not have to stay alive.** It can exit after
  `approval.requested`; a worker that later replays the channel sees the
  `approval.decided` event and continues. `wait_for_approval` is a convenience
  that polls — replay/`stream()` are equally valid.
- **Decisions are idempotent per `(request_id, decision)`.** `decide_approval`
  derives a deterministic operation id, so a retried or accidentally duplicated
  decision replays the original event instead of writing a second one. A
  *different* decision derives a different id and is recorded as a new event —
  the application applies last-wins. (The payload carries no timestamp; the
  event `timestamp` is the decision time, which is what keeps retries
  byte-identical.)
- **Actae never resolves or resumes anything.** There is no server callback and
  no automatic continuation. A human/service must call `decide_approval`; the
  application must read the verdict and act.
- **Timeouts are a wake-up, not an execution.** `approval.expired` is recorded
  by the waiter on timeout; the persisted `scheduler.wakeup` is the durable
  deadline that survives a process restart.
- **Not an authorization system.** Actae records that a decision was made; it
  does not authenticate the approver or enforce who may approve. Put that in
  your application/control plane.

## Where it fits

This combines the existing [wake-ups](COORDINATION.md) (durable timers) and
[cursor replay](FORK_RUNTIME.md) primitives. Pair it with a fork's
`tool_policies` (`block`/`replay`) so an approved fork cannot re-fire the
side effect the approval was about — see
[FORK_RESUME.md](FORK_RESUME.md#side-effect-aware-forking-and-interventions).

## Tests

`sdks/python/tests/test_approval_pattern.py` (opt-in live):
request → decide → wait, timeout → `approval.expired` + scheduled wake-up, and
the `AgentSession.require_approval` convenience.
