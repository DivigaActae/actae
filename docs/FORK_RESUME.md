# Fork & Resume — the flagship workflow

The pitch, in one sentence:

> **Fork at step N, refine step N+1, and never re-run steps 1..N.**

If you have a 10-step agent pipeline where steps 1-5 are good and you want
to iterate on step 6 (its prompt, tool, or logic), Actae lets you fork at
step 5 and continue from step 6. The fork **inherits steps 1-5's real
output** (state), so step 6 runs against that data without re-invoking the
steps before it — saving the LLM calls, tokens, and wall-clock time of
steps 1-5 on every iteration.

This is **framework-agnostic**: it works the same for a plain `async for`
loop, LangGraph, CrewAI, Claude Agent SDK, or any other orchestrator,
because it operates purely on the Actae channel + step + state primitives.

---

## The mechanism (what actually happens)

Actae's fork is a **state snapshot + lineage** operation, not a process
clone:

1. `POST /api/v1/channels/fork` (`fork_channel` on the server) resolves the
   source channel's state at-or-before the fork cursor and copies that
   snapshot into the child channel. The child's `load_state()` returns the
   inherited snapshot. The child's event log starts with a single
   `fork.started` lineage marker (`depends_on` → parent's boundary event).
2. `AgentSession.resume(actae, "parent", fork_at_step=5, name="fork")`
   (Python) / `Resume(ctx, actae, "parent", ResumeOptions{ForkAtStep, Name})`
   (Go) forks the channel, then **primes the fork session**:
   - `step_count = 5` → the next `step()` records as step 6 on the fork.
   - restores the parent's cursors (up to the fork step) so nested forks
     and step→cursor resolution keep working,
   - loads the fork's inherited state and exposes it via
     `session.inherited_state` (Python) / `session.InheritedState()` (Go),
   - makes a `state_fn()` call right after resume return the inherited
     snapshot (steps 1-5's data), so the fork's first step genuinely sees
     the parent's accumulated output.

So on the fork you run *only* the remaining steps, seeded with the inherited
state. Steps 1-5 are never re-executed.

---

## Proven behavior (with real data)

`examples/fork_resume_demo.py` is a framework-agnostic demo that runs this
workflow against the DeepSeek API (real LLM calls) and prints the proof,
including a **token & wall-clock report** whose figures are summed directly
from the provider's `usage` fields (see `examples/fork_token_report.py` for
the honesty contract — nothing is modelled or extrapolated). One measured run:

```
[baseline] steps 1-10 complete — 10 LLM calls
[fork] LLM calls by step: {'recommendations': 1, 'summary': 1, 'next_steps': 1, 'final': 1, 'complete': 1}

  ✓ steps 1-5 'research' NOT re-invoked on the fork
  ✓ steps 1-5 'insights' NOT re-invoked on the fork
  ... (all of steps 1-5 skipped)

  fork event log (8 events): ['fork.started', 'session.started',
    'step.6.recommendations', 'step.7.summary', 'step.8.next_steps',
    'step.9.final', 'step.10.complete', 'session.completed']

  ✓ step 6 received inherited context: research=True insights=True metrics=True
  ✓ step 6 output differs between baseline and fork (refinement applied)

  LLM calls: baseline=10 fork=5 → saved 5 calls (steps 1-5)

  ── Token & wall-clock report ──
  source: real DeepSeek usage (measured)

  step                baseline       fork
  ---------------------------------------
  research               1,060          —
  insights               2,036          —
  stakeholders           3,052          —
  risks                  4,077          —
  metrics                3,049          —
  recommendations        5,019      5,126
  summary                3,284      3,303
  next_steps             2,950      3,088
  final                  1,617      1,529
  complete               1,349      1,330
  ---------------------------------------
  TOTAL                 27,493     14,376

  tokens saved on the fork run: 13,117 (47.7% of the baseline)
  wall-clock (LLM calls): baseline 86.2s · fork 29.1s · 57.1s saved

  Per-iteration economics (from the measured numbers):
    • naive re-run of all steps:          27,493 tokens / iteration
    • fork run (inherited prefix + tail): 14,376 tokens / iteration
    • each refinement iteration avoids ~13,117 tokens — the prefix the fork never re-runs

  Note: the fork's tail steps ran with the inherited prefix output in their
  prompts, so they cost 157 tokens more than baseline's same steps (visible in the rows above).
  The saving is the skipped prefix — it is not inflated by the tail.
```

Exact totals vary run to run (the model is not deterministic), but the shape
is stable: **forking in the middle of a pipeline skips the whole prefix, so
each refinement iteration pays only the tail** — here ~14k tokens instead of
~27k (~48% per iteration). The report never hides the tradeoff: the fork's
tail steps carry the inherited prefix in their prompts and can cost slightly
more than baseline's same steps (the "Note" line above prints this
automatically when it happens).

The fork's step-6 event carries the **full inherited text** from steps 1-5
(research ≈ 4k chars, insights ≈ 3k, etc.) — real LLM output that was
inherited, not regenerated.

### Fidelity — does the fork change the output?

Savings are only worth anything if the fork produces the *same* output a
no-fork re-run would. There are two layers of guarantee, and the demo
measures the second one live:

1. **By construction (exact).** The fork copies the actual stored snapshot of
   steps 1-5 into the child channel (`load_state_at_or_before` →
   `inherited_state`). Steps 1-5 on the fork **are** the baseline's output
   bytes — copied, not regenerated. The demo verifies this byte-for-byte: the
   fork's step-6 event input equals the baseline's step-6 input
   (`✓ inherited prefix byte-identical to baseline`). The deterministic live
   suite proves the stronger claim: a full run and a same-prompt fork are
   **byte-identical end-to-end** (`test_fork_resume_deep.py::_full_vs_fork`),
   and a modified-prompt fork **differs only at the refined step**
   (`_full_vs_fork_modified`).

2. **Real LLMs (measured, honest).** Models sample, so two runs of *any*
   pipeline with the same prompt are not byte-identical — a no-fork re-run
   included. The demo therefore runs a **no-fork re-run control** (the full
   10-step pipeline again, unchanged) and compares tail (steps 6-10)
   similarity to baseline for both paths:

   ```
   ── Fidelity: is the fork's output like a no-fork re-run? ──
   controls: no-fork re-run (full 10-step re-run, same prompts) vs same-prompt fork
   ✓ inherited prefix byte-identical to baseline: research, insights, stakeholders, risks, metrics
     (steps 1-5 on the fork ARE the baseline's stored output — copied, not regenerated)

   tail similarity to baseline, same prompts (1.00 = identical text):
     step             no-fork re-run     fork
     recommendations            0.04     0.03
     summary                    0.08     0.12
     next_steps                 0.05     0.08
     final                      0.07     0.07
     complete                   0.04     0.36
   --------------------------------------------
     mean                       0.06     0.13
   ✓ fork tail divergence (0.87) is not worse than a plain no-fork re-run (0.94) — forking does not
     degrade output fidelity; the gap to the baseline is model sampling.
   (pipeline temperature 0.4-0.7; lower temperature → higher similarity)
   ```

   The per-run numbers vary, but the claim that matters is checked every run:
   **the fork's tail diverges from the baseline no more than a plain re-run
   does** (it is often *closer*, because the fork's inputs are byte-identical
   while a re-run's steps 1-5 drift). If a run ever violates that, the demo
   prints `⚠` instead of `✓` — the check is not ornamental. In `--dry-run`
   mode the pipeline is deterministic, so both divergences are exactly 0.00.

   This demo uses temperature 0.4-0.7 (deliberately divergent). Pipelines
   that need deterministic output should use low/zero temperature — forking
   changes nothing about that; it just stops re-paying the skipped prefix.

> **The in-demo check above is a quick smoke signal.** The rigorous,
> marketing-grade fidelity evidence — an LLM judge (DeepSeek, temp 0) scoring
> semantic equivalence across **five unrelated pipelines**, with repeat runs
> and a no-fork re-run control — is `examples/fork_fidelity_suite.py`
> (`--repeats 3`), documented in **`docs/FORK_FIDELITY.md`**. Its committed
> evidence report (2026-08-10): 5/5 scenarios pass, mean fork fidelity 8.6,
> mean re-run control 7.67 — the fork is on average *closer* to the baseline
> than a fresh re-run is.

---

## How to run the demo

```bash
# 1. Start Actae (dev mode)
cd actae && cargo run

# 2. DEEPSEEK_API_KEY is read from actae/.env automatically.
# 3. Run the framework-agnostic demo (real LLM calls + measured report):
python examples/fork_resume_demo.py

# 4. No API key handy? Deterministic fake usage (reproducible report, clearly
#    labelled DRY-RUN — do not present these figures as real):
python examples/fork_resume_demo.py --dry-run

# 5. Skip the extra 5-call fidelity check (quick run):
python examples/fork_resume_demo.py --no-fidelity
```

The demo defines a 10-step report pipeline; step 6 (recommendations) is
refined on the fork with a different style prompt. It records execution
counters per step and routes every LLM call's `usage` object into
`StepRegistry` (`examples/fork_token_report.py`), so the printed savings are
the summed real figures — per step, in total, in % and in wall-clock time.

---

## Using it in your own code (Python)

```python
from actae_client import ActaeClient
from actae_client.session import AgentSession

actae = ActaeClient(api_key=..., endpoint="http://localhost:8002")

# First run: steps 1-10, snapshots saved every step.
async with AgentSession(actae, "exp-v1", state_fn=lambda: live_state) as s:
    for i in range(1, 11):
        live_state = await step(i, live_state)
        await s.step(f"step.{i}", input=..., context=live_state)

# Fork at step 5, refine step 6 — steps 1-5 are NOT re-run.
fork = await AgentSession.resume(
    actae, "exp-v1", fork_at_step=5, name="exp-v1-refined",
    state_fn=lambda: fork_state,
)
inherited = fork.inherited_state          # steps 1-5's real output
async with fork:
    for i in range(6, 11):                # steps 6-10 only
        fork_state = await step(i, inherited)  # step 6 sees inherited data
        await fork.step(f"step.{i}", input=..., context=fork_state)
```

---

## Using it in your own code (Go)

```go
sess, _ := actae.NewAgentSession(client, "exp-v1", actae.AgentSessionOptions{
    StateFn: func() map[string]any { return liveState },
})
// ... run steps 1-10 ...

step := 5
fork, err := actae.Resume(ctx, client, "exp-v1", actae.ResumeOptions{
    ForkAtStep: &step, Name: "exp-v1-refined",
    StateFn: func() map[string]any { return forkState },
})
inherited := fork.InheritedState() // steps 1-5's real output
fork.Start(ctx)
for i := 6; i <= 10; i++ {
    forkState = step(i, inherited)
    fork.Step(ctx, fmt.Sprintf("step.%d", i), actae.StepOptions{})
}
```

---

## Side-effect-aware forking and interventions

Forking at step N inherits steps 1..N's state, so the prefix is not re-run —
but a fork still reaches *new* code that may call tools. Two additions make
"change the decision at step N" safe:

- **`tool_policies`** — how the child handles tool executions
  (`{"tool": "replay"|"block"|"live"|"auto", "*": default}`, default `auto`).
  `auto` replays an exact inherited result and runs new/changed work live;
  `replay`/`block` refuse on a miss/mismatch and persist a `tool.blocked`
  boundary; `live` is today's behavior. Full table in
  `docs/TOOL_EXECUTIONS.md` → "Fork tool policies".
- **`intervention`** — an opaque descriptor of the intended change recorded on
  the fork metadata. **The application reads it and applies it**; Actae never
  interprets it.

```python
# Change the decision at step 5 without re-firing an inherited side effect.
fork = await parent.fork(
    5,
    "exp-v1-candidate",
    intervention={"model": "candidate-model"},
    tool_policies={"email.send": "block", "*": "auto"},
)

# The application applies the intervention (Actae does not execute anything).
model = fork.intervention.get("model", "default-model")
async with fork:
    ...  # steps 6..10 run with `model`; email.send calls are blocked, not re-sent
```

```go
fork, _ := sess.Fork(ctx, 5, "exp-v1-candidate", actae.ForkSessionOptions{
    Intervention: map[string]any{"model": "candidate-model"},
    ToolPolicies: map[string]any{"email.send": "block", "*": "auto"},
})
model := fork.Intervention()["model"]  // application applies it
```

`resume(..., fork_at_step=N, intervention=..., tool_policies=...)` forwards
both. `session.intervention` / `session.toolPolicies` (Go:
`Intervention()` / `ToolPolicies()`; TS: `session.intervention` /
`session.toolPolicies`) expose them, and `fork_receipt.toolPolicies` echoes the
policy. A blocked call raises `ForkToolBlockedError`. See the runnable
`examples/fork_intervention_demo.py` (deterministic, no LLM).

---

## Framework integration — how the LLM context is maintained

Actae has two fork paths. Which one you use depends on where the LLM context
lives:

### 1. `AgentSession` (framework-agnostic step recorder)

For plain loops and custom orchestrators, the "LLM context" is whatever your
`state_fn` returns — **you must include it in the state dict**. Steps record
`context=` deltas and `state_fn()` returns the live accumulated state, which
is what gets snapshotted and inherited by the fork. Example: if your context
is a conversation, return `{"messages": [...], "metadata": ...}` from
`state_fn` so the fork inherits the full transcript.

### 2. LangGraph (`ActaeCheckpointSaver`) — the context lives in the graph state

In LangGraph the LLM context is the **graph state itself** (e.g.
`MessagesState["messages"]`). `ActaeCheckpointSaver` persists every
checkpoint (the full graph state, messages included) as a snapshot. Forking
that channel inherits the checkpoint envelope, and resuming from it
re-executes only the nodes after the fork point with the full context:

```python
from actae_client.adapters.langgraph import ActaeCheckpointSaver

saver = ActaeCheckpointSaver(actae, channel="lg")
cfg = {"configurable": {"thread_id": "run-1"}}
await graph.ainvoke(input, cfg)                  # full run

# Fork the thread (optionally at an intermediate checkpoint_id) into a new
# thread. The returned config resumes from the fork point.
fork_cfg = await saver.fork_thread(
    {"configurable": {"thread_id": "run-1"}},
    new_thread_id="run-1-fix", reason="refine node 5",
)
fork_graph = build(saver=saver)                  # same saver; node 5 modified
res = await fork_graph.ainvoke(None, fork_cfg)   # nodes after 5 re-run, context intact
# graph.invoke users: use saver.fork_thread_sync(...)
```

Verified against a real Dev space: forking a linear `MessagesState` graph at
node 2 and re-running node 3 with a modified prompt preserved the full
`[user, n1, n2]` history and produced a different n3. Loops (revision
condition re-evaluated) and parallel fan-out (`Send`) also preserve their
partial results across the fork, because the checkpoint envelope holds the
whole state.

> **Which to use?** If you use LangGraph's checkpoint saver, use
> `fork_thread` — it is the native, context-preserving path. If you drive
> steps yourself, use `AgentSession.resume(fork_at_step=)` and make sure your
> `state_fn` captures the full context.

### 3. Claude Agent SDK (`ActaeClaudeSessionStore`)

The Claude adapter persists the session transcript (`entries`) as a snapshot
on a per-session channel. `fork_session` copies the full transcript into a
new session and registers it, so a resumed session has the complete
conversation:

```python
from actae_client.adapters.claude import ActaeClaudeSessionStore

store = ActaeClaudeSessionStore(actae)
# ... run a conversation with session_store=store ...
new_key = await store.fork_session(
    project_key="my-project", session_id="run-1",
    new_session_id="run-1-fix", reason="refine the last turn",
)
# Resume the fork with the full inherited transcript:
async for message in query(prompt="Actually, change the conclusion",
                           options=ClaudeAgentOptions(
                               session_store=store, resume=True,
                               session_id=new_key["session_id"])):
    ...
```

### 4. Codex CLI (`CodexOTLPReceiver`) — observability mirror

Codex (the OpenAI coding agent) has **native local fork/resume** via its
rollout JSONL + SQLite index — Actae deliberately does not re-implement that.
Actae's value for Codex is **shared observability and cross-session
comparison**: Codex exports OpenTelemetry log events (`codex.api_request`,
`codex.sse_event` with token counts, `codex.user_prompt`,
`codex.tool_result`, …) describing each run. The `CodexOTLPReceiver` hosts
the OTLP endpoint and mirrors those events into per-conversation Actae
channels, persisting an accumulating token/tool snapshot so the dashboard,
structural state-diff and decision-trail work on Codex sessions exactly like
any other channel.

Enable Codex export in `~/.codex/config.toml`:

```toml
[otel]
environment = "dev"
log_user_prompt = false
exporter = { otlp-http = {
  endpoint = "http://127.0.0.1:4319/v1/logs",
  protocol = "json"
}}
```

```python
from actae_client.adapters.codex import CodexOTLPReceiver

receiver = CodexOTLPReceiver(actae, host="127.0.0.1", port=4319)
await receiver.start()
# ... run `codex` with the [otel] config above ...
await receiver.stop()
```

Each `codex.*` event lands on channel `codex:<conversation.id>`, and the
accumulated state snapshot (tokens, tool ledger) means
`GET /api/v1/channels/diff?left=<run-a>&right=<run-b>` compares two Codex
runs. Codex's rollout JSONL stays the source of truth for local resume; this
is a read-only observability mirror.

### 5. Any framework (`StateManager`) — the generic primitive

If your framework has no dedicated adapter (or you want to manage state
yourself), `StateManager.fork` copies an **opaque state dict** to a new
channel — the same primitive every adapter is built on:

```python
from actae_client.adapters.base import StateManager

mgr = StateManager(actae, channel="run-1")
await mgr.save({"messages": [...], "step": 5})   # any dict is the "context"
forked = await mgr.fork("run-1-fix", reason="refine from here")
inherited = await forked.load()                  # steps 1-5's data, verbatim
```

---

## Is the fork framework-agnostic?

**The fork is.** At the core, forking is a server operation: copy an opaque
state snapshot at a point in time into a new channel, with lineage metadata.
Actae never inspects the state — it is an opaque dict (or LangGraph
checkpoint envelope, or Claude transcript). That is why the *same* fork
semantics work identically for plain loops, LangGraph, Claude, CrewAI, or a
hand-rolled `StateManager`.

**The resume is framework-specific, and that is unavoidable.** "Continue from
here" means different things per framework:

- LangGraph: re-run the graph nodes after the fork checkpoint.
- Claude: continue the same conversation with the inherited transcript.
- Codex: resume is native (local JSONL) — Actae mirrors the run for
  observability and cross-session diff, not resume.
- AgentSession / StateManager: keep calling `step()` / `save()` from the
  inherited state.

This is not an Actae limitation — it is the framework's own notion of
"what to do next". Actae's job is to make the *state* fork cleanly and give
each framework the inherited context in its native shape:
`fork_thread` (LangGraph), `fork_session` (Claude),
`CodexOTLPReceiver` (Codex observability),
`AgentSession.resume(fork_at_step=)` / `StateManager.fork` (generic).

### The unified mental model

```
state → (snapshot per step) → fork copies snapshot → resume continues from it
```

Every adapter maps its "context" onto a state snapshot; the fork is the one
universal operation; resume is the framework's native continuation.

---

## What the server guarantees

- The fork copies the **state snapshot at-or-before the fork cursor** into
  the child, choosing the **closest cursor** (`resolve_fork_state`).
- The child gets a `fork.started` lineage marker; `channel_metadata`
  records `parent_channel_id`, `origin_run_id`, and the full immutable
  provenance: `forked_at_cursor` (the **resolved** boundary),
  `requested_at_cursor`, `resolved_state_cursor`, `resolved_event_id`,
  `source_state_version`, `source_state_sha256`, `restorable`, `manifest`,
  `reproducibility`.
- `POST /fork` returns an **immutable fork receipt** (also at
  `GET /channels/{id}/receipt`).
- Forks are idempotent with **standard semantics**: identical replay returns
  the original child; request drift is `409 idempotency_conflict`;
  child-id reuse under a different definition is `409 channel_conflict`.
- **Strict-by-default boundary policy** in the SDKs: `AgentSession.fork` /
  `resume(fork_at_step=)` default to `boundary_mode="exact"` and raise
  `NoRestorableCheckpointError` when no snapshot exists at the step — they
  NEVER silently inherit later state. `"approximate"` (fall back + report the
  drift via `resolved_boundary_cursor`) and `"lineage_only"` (no state copy)
  are explicit opt-ins. Save state every step (`state_fn` +
  `snapshot_interval=1`) to make every step forkable.
- The dashboard surfaces it: open the fork channel → Inspector → **State
  Diff** (compare against a sibling, with honest truncation) and **Decision
  Trail** (lineage + immutable fork-time boundary + tool-execution ledger).

## Tests

- `sdks/python/tests/test_session.py` — unit: fork-resume sets step_count,
  inherited_state, state_fn priming (no server); strict/approximate/lineage_only
  boundary policy; receipts; **contamination guard** (approximate fallback that
  resolves BEYOND the requested boundary nulls `inherited_state`; exact-boundary
  forks inherit normally; a `requested=0` "latest" fork is never contaminated).
- `sdks/go/session_test.go` — parity: `TestSessionForkContinuesAtStepAndInheritsState`,
  `TestSessionResumeForkInheritsState`,
  `TestResumeForkExcludesSessionLifecycleMarkers`, strict/approximate tests,
  `TestResumeForkApproximateRefusesContaminatedState` (resolved > requested →
  `InheritedState()` nil), `TestResumeForkApproximateExactBoundaryInherits`,
  `TestResumeForkLineageOnlyNoStateCopy`.
- `sdks/typescript/test/session.test.ts` — parity: exact raises
  `NoRestorableCheckpointError`, approximate fallback, `lineage_only`,
  boundary_mode validation, and the contamination guard (resolved beyond
  boundary → `inheritedState` undefined; exact boundary inherits).
- `sdks/python/tests/test_fork_resume_live.py` — live against a dev
  instance: skip-1-5, fork-of-fork, crash recovery.
- `sdks/python/tests/test_fork_resume_deep.py` — live deep comparison: a
  **full run and a fork run with the same prompt are byte-identical**; a
  **full run vs a fork with a modified step-6 prompt differ only at step 6**
  while steps 1-5 stay identical; fork-at-every-step; concurrent forks;
  idempotent forks; no-state fallback; raw-channel (outside AgentSession)
  replay-fallback; step-beyond-pipeline raises; snapshot_interval>1 fallback;
  **live `lineage_only`** (event-only channel forks with restorable=False,
  no inherited state, and parent lineage recorded in the child metadata);
  **live `StateManager.fork`** and **live Claude `fork_session`**.
- `sdks/go/fork_deep_live_test.go` — **the Go port of the deep suite** (15
  cases, env-gated): byte-identical full-vs-fork and modified-prompt
  byte-compare, fork-at-every-step, fork-of-fork, strict/approximate/
  lineage_only boundary policy, contamination guards, raw-channel fork,
  idempotent forks, concurrent forks, inherited-step lineage resolution.
- `sdks/typescript/test/fork_deep_live.test.ts` — **the TS port of the deep
  suite** (15 cases, `ACTAE_LIVE=1` gated): the same byte-identical
  assertions and edge cases through the TS SDK.
- `sdks/python/tests/test_adapter_fork_live.py` — live fork proof through the
  framework adapters: `CrewAIResumer.fork` (inherits `completed_tasks`
  verbatim, `get_remaining_tasks` reflects it), `ChainResumer.fork` (inherits
  the full `{"context": {...}}` dict), `ActaeTracingProcessor.fork_trace`
  (picks the requested run's snapshot, not the newest; ValueError on unknown).
- `sdks/typescript/test/adapters_fork_live.test.ts` — live fork proof through
  the TS adapters: LangGraph `forkThread` (re-runs only the tail with context
  preserved), Claude `forkSession` (full transcript + discoverability),
  `ChainResumer.fork` (context inherited), `CopilotManager.fork` (recorded
  session forked at a real event).
- `sdks/go/smoke/smoke.go` — live Go smoke asserts the fork's StepCount and
  InheritedState.
- `sdks/example-tests/python/test_fork_token_savings.py` — **unit (no server)**: the
  token-report arithmetic behind the demo. Registry sums are exact,
  "saved" = baseline − fork, % and wall-clock are correct, skipped steps
  render as "—", negative savings are shown honestly (never clamped), and
  the fork-tail-heavy note only appears when it's true.
- `sdks/example-tests/python/test_fork_resume_tokens_live.py` — opt-in live
  dry-run: runs the actual demo pipeline against a dev instance in `--dry-run`
  mode (deterministic fake usage, no API key) and asserts the reported totals
  (17,660 / 9,400 tokens) and headline savings (8,260 tokens, 46.8%) end-to-end.
- `sdks/example-tests/python/test_fork_fidelity_lib.py` — **unit (no server, no LLM)**:
  the LLM-judged fidelity infrastructure (judge JSON parsing incl. fences and
  loose output, judge averaging + retry, tail/prefix extraction, pass/fail
  thresholds, repeat aggregation, transient-error classification, report
  serialization, scenario validity/unrelatedness).
- `sdks/example-tests/python/test_fork_fidelity_suite_live.py` — opt-in live: dry-run
  plumbing (needs only Actae) + real DeepSeek judge run (needs
  DEEPSEEK_API_KEY) that asserts the marketing claim on the means and writes
  the evidence report to `examples/fidelity-reports/`. See `docs/FORK_FIDELITY.md`.

## Edge cases found and fixed

- **Off-by-one in the replay fallback**: position-based step resolution
  previously counted `session.started` / `session.completed` (and only
  excluded `fork.started`) as user steps, so forking at the last step or
  beyond resolved to the wrong cursor. Both SDKs now exclude all three
  system markers and raise a clear error for a step beyond the recorded
  steps.
- **Unknown-channel metadata**: `GET /api/v1/channels/{id}/metadata` now
  returns **404** for a genuinely-unknown channel (no events/state/metadata/
  cursors) instead of a synthesized default, so `AgentSession.resume` on a
  nonexistent channel raises "not found" instead of silently misbehaving.
- **Nested-fork cursor alignment**: a fork's `_cursors` is indexed by step
  number (pre-filled for inherited steps, appended for the fork's own steps)
  so fork-of-fork resolves the right cursor.
- **Fork-of-fork at an inherited step**: forking a fork at a step it
  *inherited* (≤ its `forked_at_step`) now walks up the lineage to the
  ancestor that actually recorded that step and forks from there — instead
  of silently mapping the position against the fork's own event log (which
  only has the fork's own steps). Both SDKs share a `_resolve_step_owner` /
  `resolveForkSource` helper used by both `resume(fork_at_step=)` and the
  instance `fork()`.
- **Crash-recovery inherited state**: resuming a crashed channel now exposes
  the last saved state via `inherited_state`, so the caller can continue
  from where the run stopped.
- **Fork `state_fn` never wrapped (Go/TS parity, found by the deep-suite
  ports 2026-08-12)**: Go and TS `primeForkSession` used to replace the
  caller's `StateFn` with a frozen closure returning only the inherited
  prefix — a fork channel persisted only steps 1..N, never its own evolving
  steps, and nested forks inherited only the prefix. Both SDKs now match
  Python (the caller's `state_fn` is intentionally left unwrapped so it
  reflects the fork's live state). Locked in by parity unit tests and the
  byte-identical deep live suites. See `docs/FORK_FIDELITY.md`.

## Related

- `docs/COUNTERFACTUAL.md` — falsifiable debugging (fork + diff proves which
  fix worked).
- `docs/FORK_RUNTIME.md` §9 — the diff / trail endpoints.
