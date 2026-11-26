# Observability Interop

Actae is the **execution layer under observability**, not a replacement for it.
Its durable events are the operational record you recover, fork, and coordinate
from; tools like Langfuse, Phoenix, and plain OpenTelemetry are where you chart
latency, tokens, cost, and quality. This document describes how the two
correlate without making either one authoritative for the other.

## Identity mapping

An Actae execution is addressable at several levels; an observability backend
usually addresses a trace/span. The bridge is a metadata convention, not a
schema change:

| Actae | Meaning | External |
|---|---|---|
| `channel_id` | the durable execution stream | resource/grouping key |
| `cursor` | ordered position within the channel | stable event id for dedup |
| event `id` | immutable event UUID | span id (optional) |
| `metadata.trace_id` | caller-supplied external trace id | trace id |
| `metadata.span_id` | caller-supplied external span id | parent span id |
| `ForkReceipt.fork_id` | fork identity | correlation tag |
| `ExecutionInfo.id` | tool-execution ledger record | correlation tag |

Attach the external trace context when you record, so an Actae event can be
found from the trace and vice versa:

```python
from actae_client.adapters.otel import trace_context

await session.step("llm.call", output=..., metadata=trace_context(trace_id, span_id))
```

Any `metadata` is accepted; `trace_id`/`span_id` are simply the keys the bridge
lifts into span attributes.

## Exporting Actae events as OTel spans

`ActaeOTelBridge` replays a channel and emits one span per event. In Python and
TypeScript it is **dependency-free**: it drives any tracer exposing
`start_as_current_span(name, attributes=...)`, so the core SDK carries no
OpenTelemetry dependency. Go uses the real `go.opentelemetry.io/otel/trace` API
(already a transitive dependency via the Copilot SDK). Pass a tracer to ship
spans to your collector:

```python
from opentelemetry import trace
from actae_client.adapters.otel import ActaeOTelBridge

bridge = ActaeOTelBridge(trace.get_tracer("actae"))
await bridge.export_channel(actae, "run-123")   # one span per event
```

```ts
import { ActaeOtelBridge } from '@actae/sdk/adapters/otel';
const bridge = new ActaeOtelBridge(tracer);
await bridge.exportChannel(client, 'run-123');
```

```go
// tracer is your OpenTelemetry trace.Tracer (e.g. tp.Tracer("actae")); any
// type with Start(ctx, name, opts...) satisfies actae.TracerStarter.
spans, err := client.ExportChannelToOTel(ctx, "run-123", tracer, actae.ReplayOptions{})
// or event-by-event:
bridge := actae.NewOTelBridge(tracer)
span := bridge.ExportEvent(ctx, event)
```

Mapping (OpenTelemetry GenAI semantic conventions where applicable):

| Attribute | Source |
|---|---|
| span name | the Actae `event_type` |
| `actae.channel_id`, `actae.cursor`, `actae.event_type`, `actae.actor` | event identity |
| `actae.trace_id`, `actae.span_id` | event metadata (`trace_context`) |
| `gen_ai.request.model` / `gen_ai.response.model` | payload `model` / `response_model` |
| `gen_ai.usage.input_tokens` / `gen_ai.usage.output_tokens` | payload `input_tokens`/`prompt_tokens`/`tokens`, `output_tokens`/`completion_tokens` |
| `gen_ai.tool.name` | payload `tool` / `tool_name` |
| `gen_ai.operation.name` | payload `operation` / `operation_name` |
| `actae.latency_ms` | payload `latency_ms` / `duration_ms` |

Non-primitive payload fields are ignored (attributes must be primitives); the
bridge never mutates events and never executes anything.

## Importing existing traces

Actae already ships two importers so framework telemetry lands in Actae without
changing the framework:

- **OpenAI Agents SDK** — `ActaeTracingProcessor` (Python) / `ActaeTracingProcessor`
  (TS) implements the tracing-processor lifecycle and mirrors spans into Actae
  events; `fork_trace` / `forkTrace` copies one run's snapshot for structural
  diffing.
- **Codex CLI** — `CodexOTLPReceiver` (Python) / `CodexOtlpReceiver` (TS) runs a
  local OTLP HTTP receiver and mirrors Codex execution events into Actae
  channels.

See `docs/ADAPTERS.md`.

## What is not shipped

- **No server-side OTel export.** Actae does not push to a collector itself; the
  bridge runs in your process. This keeps the server free of observability
  egress and credentials.
- **Go uses the OTel API directly.** `actae.NewOTelBridge(tracer)` /
  `Client.ExportChannelToOTel` take any `TracerStarter` (a real
  `trace.Tracer` satisfies it); there is no duck-typed shim because the Go
  `trace.Tracer` interface carries a private marker.
- **No automatic span parenting from Actae lineage.** Fork/`depends_on` lineage
  is available as `actae.*` attributes and the decision trail; mapping it onto
  OTel parent links is left to the backend.

## Limitations (deliberate, not gaps)

- The bridge emits **point-in-time** spans from replayed events; it does not
  reconstruct start/end timestamps or a nested parent/child waterfall. That is
  by design: Actae's model is an ordered event log, and the bridge hands a
  backend the execution facts — latency, tokens, tool names, errors — rather
  than inventing timing the log does not have. Automatic span parenting from
  Actae lineage is intentionally left to the backend (lineage is exposed as
  `actae.*` attributes and the decision trail).
- Python/TS bridges carry no OpenTelemetry dependency (duck-typed tracer); Go
  uses the real API. The Go bridge passes attributes both as a span start
  option (so samplers observe them) and via `SetAttributes` (so any tracer,
  including a test double, records them) — an intentional, harmless double-set.
- Trace/span ids are opaque strings carried in metadata; Actae does not
  validate them and does not guarantee they are parseable by a given backend.
