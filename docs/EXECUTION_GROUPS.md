# Actae Execution Groups

Execution Groups coordinate agents that run independently in different processes, languages, frameworks, or machines. Actae is the durable rendezvous and causality plane; it does not spawn or execute agents.

## Model

An execution group maps stable member identities to ordinary Actae channels. Sending a group message atomically writes a `message.sent` event to the sender channel, a `message.received` event to the recipient channel, a durable message envelope, and a cross-channel dependency edge. Offline recipients recover the same envelope through history before waiting for new work.

Members are exclusively owned through a renewable lease `(owner_id, generation, lease_until)`. A takeover after expiry increments the generation; stale heartbeats and acknowledgements fail with `409 member_fenced`. Release a lease when a worker exits normally.

Events accept additive `dependencies` alongside the compatibility `depends_on` field. `GET /api/v1/events/{id}/causal-graph` traverses all parents and returns the bounded causal DAG.

## Python quick start

```python
group = await client.create_execution_group("support-42")
await client.add_execution_group_member("support-42", "router", "support-42-router")
await client.add_execution_group_member("support-42", "research", "support-42-research")

session = client.execution_group("support-42")
async with session.claim_member("router", "router-host-a") as router:
    await router.send("research", "research.request", {"question": "..."})
    answer = await router.wait_for("research.completed", timeout=60)
    await router.acknowledge(answer)
```

`wait_for`, `wait_any`, and `wait_all` query durable delivery history first, so a restart cannot miss a message sent while the process was offline. TypeScript exposes `executionGroup().member().waitFor()` and Go exposes `ExecutionGroup`, `WaitFor`, `WaitAny`, and `WaitAll` with the same semantics.

For low-latency consumers, each SDK also exposes a group-specific WebSocket
stream (`stream_ws`, `streamWebSocket`, or `Stream`). It resolves the member's
delivery channel, requests cursor replay, filters `message.received` envelopes,
and then yields live deliveries. Persist the last event cursor and pass it back
after reconnect; the server's exclusive cursor replay prevents gaps or duplicate
delivery at the reconnect boundary. `subscribe_ws`/`unsubscribe_ws` (Python),
`Subscribe`/`Unsubscribe` (Go), and `subscribeWebSocket`/`unsubscribeWebSocket`
(TypeScript) are provided for callback-oriented consumers.

## Group Fork

Create a counterfactual with an immutable source boundary and a policy for every member:

```json
{
  "fork_group_id": "support-42-alt",
  "source_group_id": "support-42",
  "intervention_member_id": "router",
  "intervention_cursor": 18,
  "member_policies": {"router":"live","research":"frozen","writer":"reactive"},
  "tool_policies": {"web.search":"live","stripe.charge":"replay","email.send":"block"}
}
```

The fork is created atomically, clones member identities into isolated channels, restores the intervention state at the boundary, and materializes recorded inbound messages with `replay_source` causal edges.

- `frozen`: cannot claim, send, or execute tools. An unrecorded inbound request is a persisted `COUNTERFACTUAL_BLOCKED` boundary.
- `live`: the explicitly changed member; its first new message is the fork's first divergence.
- `reactive`: can run after divergence.
- tool `replay`: only an exact canonical source-ledger match is returned; a miss or changed request blocks and never invokes the tool.
- tool `block`: always blocks and persists the attempted boundary.
- tool `live`: uses the normal idempotent ledger and is classified in the receipt.

Promoting a frozen member is an explicit, audited `frozen → reactive` transition:

```text
POST /api/v1/execution-group-forks/{fork}/members/{member}/promote
```

The evolving receipt classifies replayed/live events, first divergence, promotions, blocked boundaries, replayed tool results, and live tool executions without mutating source history. The dashboard's **Groups** page shows members, messages, policies, receipts, and causal graphs.

## HTTP API

| Operation | Endpoint |
|---|---|
| Create/list groups | `POST/GET /api/v1/execution-groups` |
| Add/list members | `POST/GET /api/v1/execution-groups/{group}/members` |
| Claim/heartbeat/release | `POST .../members/{member}/claim|heartbeat|release` |
| Send message | `POST /api/v1/execution-groups/{group}/messages` |
| Durable inbox | `GET /api/v1/execution-groups/{group}/members/{member}/messages` |
| Acknowledge | `POST /api/v1/execution-group-messages/{message}/ack` |
| Causal graph | `GET /api/v1/events/{event}/causal-graph` |
| Create/get fork | `POST /api/v1/execution-group-forks`, `GET /api/v1/execution-group-forks/{fork}` |
| Promote member | `POST /api/v1/execution-group-forks/{fork}/members/{member}/promote` |

Use a stable UUID `operation_id` for message retry safety. Reusing it with different content returns `409 idempotency_conflict`. This system provides durable coordination and idempotent tool replay; it does not claim exactly-once external side effects.

## Testing

`tests/test_execution_groups.py` (44 live tests, run with the rest of the
pytest suite) covers the full surface end-to-end: group lifecycle and upsert
semantics, member add/list, the lease lifecycle (claim → heartbeat → release,
exclusive ownership, takeover-after-expiry bumps the generation, stale
heartbeat/ack → `409 member_fenced`), message send/ack with the dual
`message.sent`/`message.received` events, durable-inbox replay and
`after=<message_id>` pagination, `operation_id` idempotency
(`409 idempotency_conflict` on reuse with different content), causal-graph
traversal of the cross-channel message edge, group forks (member/tool policy
validation, cloned channels, replayed `replayed:true` history, frozen-member
blocking with a persisted `blocked_boundaries` receipt entry, promote →
`reactive`, idempotent re-creation, `409 channel_conflict` on conflicting
params), parallel-send stress, and live WS delivery of `message.received`.

The Rust unit suite (`cargo test --bin actae`) locks the HTTP JSON shapes for
`group_json`/`member_json`/`lease_json`/`message_json`/`fork_json` and the
`err_to_status` mapping for every execution-group error variant.

Idempotency is **canonical (key-order-insensitive)**: jsonb storage normalizes
object key order, so both the message `operation_id` replay check and the
group-fork re-creation check compare payloads/policies canonically
(`json_values_equal` in `src/events/store.rs`). A retry that re-serializes
its payload or policies with a different key order replays; genuinely
different content fails loudly with `409 idempotency_conflict` /
`409 channel_conflict`.
