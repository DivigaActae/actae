# Limitations

What Actae does not do. Read this before adopting it.

## Exactly-once is a contract, not a guarantee

A durable ledger prevents duplicate effects only when the downstream honors an
idempotency key. If a crash happens after the effect landed but before the
result was recorded, the retry re-fires once. This is measured and published in
the harness; it is not a bug that will be fixed by more bookkeeping.

## Single node

The community build runs one node with an embedded PostgreSQL. There is no
replication, no failover, no clustering, no multi-region story. If the machine
dies, you restore from your backups.

## No workflow DSL

Actae does not define your control flow. Your loop, prompts, and model calls
stay yours. If you want a workflow definition language, use Temporal or Inngest.

## Framework-neutral, not framework-aware

Adapters cover LangGraph, the OpenAI Agents SDK and the Claude Agent SDK. Inside
a framework, fork granularity is bounded by what that framework exposes as a
step. Deep framework internals are not instrumented automatically.

## Pre-1.0

Interfaces may change between minor versions. There is no long-term support
policy yet. The event schema and HTTP API are documented and versioned; the Rust
crate API is not stable.

## Operational surface

No built-in alerting, no dashboards beyond the local UI, no log shipping. Bring
your own observability; OpenTelemetry export is on the roadmap, not shipped.
