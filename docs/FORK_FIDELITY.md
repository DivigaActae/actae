# Fork & Resume — LLM-judged output fidelity

> **Audit record:** `docs/FORK_FIDELITY_AUDIT.md` is the self-contained,
> reproducible account of exactly what was run, what the results were, the
> development history (including every failed attempt and the fix), and the
> threats to validity an auditor should scrutinize.

The benchmark hypothesis, made measurable:

> For the same prompt, a fork produces output **equivalent** to a full no-fork
> re-run — judged by an LLM (DeepSeek, temperature 0), across **multiple
> unrelated pipelines**, with per-scenario repeat runs and a no-fork re-run
> control. The benchmark tests whether forking degrades output beyond ordinary
> re-running in the measured scenarios; it is not a universal guarantee for
> arbitrary agents or future model versions.

This is the evidence engine: `examples/fork_fidelity_lib.py` (infrastructure),
`examples/fork_fidelity_scenarios.py` (the five scenarios),
`examples/fork_fidelity_suite.py` (the CLI), and the committed evidence
reports in `examples/fidelity-reports/`.

---

## Why an LLM judge and not string similarity

Real models sample: two runs of the same pipeline with the same prompt
produce **differently-worded but semantically-equivalent** text. Character
similarity (difflib) measures 0.06-0.20 for such runs — a figure that looks
terrible and means nothing. Only a judge that understands the content can
distinguish "same facts, different words" (fine) from "missing/wrong key
points" (a real problem).

The judge is the **only** fidelity metric in this suite. No difflib, no token
overlap, no hand-written heuristics gate the result.

## What is measured

For each scenario (one self-contained, unrelated pipeline):

1. **baseline** — the full pipeline run on a fresh channel,
2. **re-run** — the full pipeline again, unchanged ("the run without the
   fork"),
3. **fork** — fork at step N (inherit steps 1..N byte-identically), re-run
   only steps N+1..M.

The **tail** outputs (steps N+1..M) are compared, not the whole output: the
fork's prefix IS the baseline's stored output (copied), so comparing whole
outputs would trivially score the prefix identical and mask tail drift. The
tail is exactly what forking recomputes vs what a re-run recomputes.

Two scores per scenario, both from the judge (DeepSeek, temperature 0):

- `fork_fidelity`  = judge(baseline tail, **fork** tail)
- `rerun_fidelity` = judge(baseline tail, **re-run** tail)  ← the control

### The claims under test

Per scenario, **both** must hold:

1. `fork_fidelity  >= threshold`                     (default 7.0) — the fork
   output is semantically equivalent to the baseline, and
2. `fork_fidelity  >= rerun_fidelity - tolerance`    (default 1.5) — forking
   is **not worse than re-running**.

Claim 2 is the one that isolates the fork's effect: any difference between a
fork and the baseline is also present (and usually larger) when you just
re-run the pipeline. Claim 1 is the absolute equivalence bar.

### Statistical honesty: repeats

A single LLM sample is noisy — one bad draw can fail a scenario that is fine
on average (observed live: `s5-product-brief` failed once on a single sample,
then passed 5/5 with repeats). Therefore the suite runs each scenario
`repeats` independent times (evidence runs use 3) and evaluates the claims on
the **mean** scores. Every per-repeat score is kept in the report, so a
reader can see the spread, not just the average.

### Structural check (exact, no judge needed)

The inherited scenario outputs are checked for exact deserialized-value
equality (`prefix_identical`). Live evidence runs also require the fork receipt
to report a restorable boundary, equal requested/resolved cursors, and a
non-empty source-state SHA-256. “Byte-identical” should be reserved for the
server-recorded snapshot fingerprint, not Python dictionary equality.

## The judge rubric

The judge prompt (`JUDGE_SYSTEM` in `fork_fidelity_lib.py`) is the heart of
the measurement. Key rules:

- Judge **facts, conclusions, coverage** — never wording/style/length.
- **Differing invented values are not contradictions**: when a step is asked
  to propose prices/targets/estimates, two runs legitimately propose
  different values; the judge penalizes missing items or wrong reasoning, not
  different proposed numbers. (This rule was added after the first evidence
  run: the market-analysis scenario scored 6.3 solely because baseline and
  fork proposed different euro prices — a sampling artifact, not a fidelity
  gap. The re-run control scored *lower* at 5.0.)
- Output is strict JSON: `{"score": 1-10, "verdict", "rationale"}`.
  `score 9-10` equivalent, `7-8` minor differences, `4-6` substantive,
  `1-3` substantially different. Judge temperature is always 0.

## The five scenarios (deliberately unrelated)

| id | domain | steps | fork@ | input |
|----|--------|-------|-------|-------|
| `s1-market-entry` | market analysis brief | 5 | 2 | topic |
| `s2-code-review` | Python payment-handler review | 5 | 3 | code snippet |
| `s3-localization` | EN→FR technical translation | 3 | 1 | source paragraph |
| `s4-incident-analysis` | outage postmortem | 4 | 1 | incident report |
| `s5-product-brief` | offline-mode feature spec | 4 | 3 | topic |

Different domains, step counts, fork points and temperature profiles — the
claim is not pinned to one shape. Scenario inputs that admit genuinely
different-but-valid conclusions were deliberately avoided (the incident
scenario originally allowed two defensible root causes; it was clarified so
the fidelity comparison measures the fork, not interpretive variance).

## How to run

```bash
# 1. Start Actae (dev mode). Raise the auth rate limits for heavy runs:
cd actae && ACTAE_IP_AUTH_RATE=100 ACTAE_IP_AUTH_CAPACITY=200 cargo run

# 2. Real evidence run (DeepSeek key from actae/.env; ~10-20 min):
python examples/fork_fidelity_suite.py --repeats 3 --judge-reps 1

# 3. Plumbing only (deterministic fake pipeline + judge, no API key):
python examples/fork_fidelity_suite.py --dry-run

# 4. Subset / tuning:
python examples/fork_fidelity_suite.py --scenarios s3-localization,s5-product-brief \
    --repeats 2 --threshold 7.0 --tolerance 1.5 --judge-reps 2
```

Exit code is 0 (all scenarios passed) or 1 (any failed / hard error). Reports
are written to `examples/fidelity-reports/` (`fork-fidelity-<timestamp>.json`
+ `latest.json`).

### Go port

A feature-parity Go port lives in `sdks/go/examples/fidelity` (same five
scenarios byte-for-byte, same Go `AgentSession`/`Resume` fork machinery, same
report JSON schema; the DeepSeek judge client is stdlib-only). Flags are
identical to the Python CLI. Reports use the `-go` suffix to avoid clobbering
Python evidence:

```bash
cd sdks/go/examples/fidelity
ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
  go run . --dry-run                  # plumbing only, no API key
go run . --repeats 3 --judge-reps 1   # real evidence run (DeepSeek key from actae/.env)
go run . --scenarios s3-localization,s5-product-brief --repeats 2
# writes examples/fidelity-reports/fork-fidelity-go-<run-id>.json + latest-go.json
```

### TypeScript port (session + LangGraph-native adapters)

The TypeScript port lives in `sdks/typescript/examples/fidelity/index.ts`
(same five scenarios byte-for-byte, same report JSON schema). `--adapter`
selects the fork machinery:

- `--adapter session` (default) — `AgentSession.fork`/`resume`, identical to
  the Python/Go ports.
- `--adapter langgraph` — proves the **native LangGraph.js fork path**: each
  scenario is compiled as a real `@langchain/langgraph` `StateGraph` (one node
  per step, a single last-wins `{ acc: Record<string,string> }` channel) with
  `new ActaeCheckpointSaver(client, { channel })`. The fork point is the
  newest `graph.getStateHistory()` entry whose `values.acc` holds exactly
  `fork_at_step` step keys; the fork uses
  `saver.forkThread({configurable:{thread_id, checkpoint_id}}, {newThreadId,
  reason})` and the tail re-runs via `graph.invoke(null, forkCfg)`.
  Provenance comes from `actae.getForkReceipt(saver.channelForConfig(forkCfg))`
  and must satisfy the same loud assertions as the session mode (restorable,
  requested==resolved, non-empty source-state SHA-256). Reports carry
  `adapter: "langgraph"`, a per-scenario `langgraph: {thread, forkChannel,
  forkedCheckpointId, receipt}` block, and use the `-langgraph` file suffix
  (`fork-fidelity-langgraph-ts-<run-id>.json` + `latest-langgraph.json`).

```bash
cd sdks/typescript && npm run build
ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
  node examples/fidelity/index.ts --adapter langgraph --dry-run
node examples/fidelity/index.ts --adapter langgraph --repeats 3 --judge-reps 1  # real evidence
```

The TS `ActaeCheckpointSaver` treats the requested fork configuration as
authoritative when it loads an inherited envelope. Continuation checkpoints
therefore land on the new fork channel, not the parent thread's channel; the
source history remains unchanged. Dry-run (deterministic) mode scores every
comparison 10.0 and reports `prefix_identical: true` for both adapters.

### LangGraph variant (the framework-native fork path)

`examples/fork_fidelity_langgraph.py` runs the SAME five scenarios and the
SAME judge/claims, but the fork happens through LangGraph's own machinery
(`ActaeCheckpointSaver.fork_thread`) instead of the AgentSession manual loop.
Each scenario step becomes ONE node of a linear `StateGraph` whose state is a
single `acc` dict; the inherited prefix is carried by LangGraph checkpoints,
so "fork at step N" = "fork the checkpoint whose state contains exactly N
step keys". Provenance is verified server-side: the fork channel's first
event is `fork.started` and the immutable fork receipt must report a
restorable boundary with equal requested/resolved cursors and a non-empty
source-state SHA-256. Report files use the `-langgraph` suffix and carry
`adapter: "langgraph"` plus a per-scenario `langgraph` provenance block
(`fork_channel`, `forked_checkpoint_id`, `thread`, `receipt`).

```bash
python examples/fork_fidelity_langgraph.py --dry-run                  # plumbing, no API key
python examples/fork_fidelity_langgraph.py --repeats 3 --judge-reps 1 # real evidence run
python examples/fork_fidelity_langgraph.py --scenarios s1-market-entry,s4-incident-analysis \
    --repeats 2 --dry-run                                             # subset + repeats
# writes examples/fidelity-reports/fork-fidelity-langgraph-<run-id>.json + latest-langgraph.json
```

## How to read the report

```json
{
  "generated_at": "...", "pipeline_model": "deepseek-chat",
  "judge_model": "deepseek-chat", "judge_temperature": 0.0,
  "threshold": 7.0, "tolerance": 1.5, "repeats": 3,
  "scenarios": [
    {
      "name": "s2-code-review",
      "fork_fidelity": {"score": 8.3, "verdict": "minor_differences",
                         "rationale": "...", "samples": [...]},
      "rerun_fidelity": {"score": 8.0, ...},
      "repeats": [{"fork_fidelity": 8.0, "rerun_fidelity": 8.0, "prefix_identical": true}, ...],
      "prefix_identical": true, "pass": true, "fail_reason": null,
      "channels": {"baseline": "fid-s2-code-review-...", "rerun": "...", "fork": "..."}
    }, ...
  ],
  "overall": {"pass": true, "passed": 5, "scenarios": 5,
               "mean_fork_fidelity": 8.6, "mean_rerun_fidelity": 7.67,
               "worst_fork_fidelity": 8.33, "total_llm_calls": 159, "total_tokens": 136266}
}
```

- `pass` per scenario is the conjunction of the two claims on the **mean**.
- `repeats` shows every score and, in newly generated reports, preserves the
  complete baseline, re-run, and fork state, step trace (prompts, generation
  settings, and output), compared tail, channel IDs, and fork provenance.
  Reports produced before this evidence-schema change lack those observations.
- `rationale` is the judge's own words, verbatim — auditable.
- `channels` lets you open the runs in the dashboard and inspect the events.

## The committed evidence (2026-08-10)

`examples/fidelity-reports/fork-fidelity-2026-08-10-12-21-48.json` —
`--repeats 3 --judge-reps 1`, real DeepSeek:

```
OVERALL: PASS  5/5 scenarios
s1-market-entry       mean fork= 8.3  mean rerun= 5.0
s2-code-review        mean fork= 8.3  mean rerun= 8.0
s3-localization       mean fork= 9.7  mean rerun= 9.7
s4-incident-analysis  mean fork= 8.3  mean rerun= 8.3
s5-product-brief      mean fork= 8.3  mean rerun= 7.3
mean fork=8.6  worst fork=8.33  mean re-run control=7.67
```

Notes a reader should keep in mind (they are honest features, not bugs):

- The **mean re-run control is lower** than the mean fork (7.67 vs 8.6) —
  a fork is on average *closer* to the baseline than a fresh re-run, because
  the fork's tail inputs are byte-identical to baseline's while a re-run's
  prefix drifted.
- Absolute scores are pipeline-dependent: `s3-localization` (translation) is
  near-perfect (9.7), while pipelines that synthesize open-ended analysis
  (market entry) sit lower (8.3). The claim is that **forking is not the
  source of the variance** — the re-run control is equal or worse in every
  scenario.
- These are live-model measurements. The suite must be re-run (and the report
  re-committed) if the claim is presented with new model versions or dates.
  The thresholds are defaults; changing them must be deliberate and visible
  in the report.

## The cross-SDK committed evidence (2026-08-12)

On 2026-08-12 the same five scenarios were run through **every supported SDK
and the native LangGraph adapter**, each producing a committed report. All
are `--repeats 3 --judge-reps 1` real DeepSeek runs except the two TypeScript
reports (see the note below):

| Suite | Adapter | Report | repeats | result | mean fork | mean re-run | worst fork |
|-------|---------|--------|---------|--------|-----------|-------------|------------|
| Python | `AgentSession` (manual) | `fork-fidelity-2026-08-12-09-50-12.json` | 3 | 5/5 PASS | 8.47 | 7.07 | 7.67 |
| Python | LangGraph `fork_thread` | `fork-fidelity-langgraph-2026-08-12-10-00-14.json` | 3 | 5/5 PASS | 8.40 | 6.87 | 7.33 |
| Go | `AgentSession`/`Resume` | `fork-fidelity-go-2026-08-12-10-10-13.json` | 3 | 5/5 PASS | 8.20 | 7.67 | 7.67 |
| TypeScript | `AgentSession` | `fork-fidelity-ts-2026-08-12-10-33-21.json` | 1 | 5/5 PASS | 8.80 | 8.20 | 8.00 |
| TypeScript | LangGraph `forkThread` | `fork-fidelity-langgraph-ts-2026-08-12-10-38-47.json` | 1 | 5/5 PASS | 8.40 | 7.80 | 8.00 |

Every scenario in every suite reported `prefix_identical=true` (the inherited
prefix is byte-identical to the baseline) and every fork receipt proved a
restorable, exact boundary (requested == resolved cursor) with a non-empty
source-state SHA-256. Every suite had a mean fork fidelity above the re-run
control — the fork is on average *closer* to the baseline than a fresh re-run,
in all three SDKs and through the LangGraph adapter.

**A parity bug the ports caught and fixed (2026-08-12):** the first Go and
TypeScript ports failed their byte-identical fork checks. Root cause: both
SDKs' `primeForkSession` *replaced* the caller's `stateFn` with a frozen
closure returning only the inherited prefix, so a fork channel never persisted
its own evolving steps (nested forks inherited only the prefix). Python
deliberately never wraps `state_fn` ("it must reflect the fork's evolving
live state"). Both SDKs now match Python; the fix is locked in by parity unit
tests (`TestForkStateFnNotWrapped`, TS `does NOT wrap stateFn…`) and the
byte-identical deep live suites (`sdks/go/fork_deep_live_test.go`,
`sdks/typescript/test/fork_deep_live.test.ts`).

**TypeScript `repeats` note (honest reporting):** the two TS evidence runs
were captured with `--repeats 1` — the shared dev server was being restarted
by a concurrent lifecycle-test workstream every few minutes, and a single
repeat completes inside that window while three does not. They are real,
single-sample evidence; the deterministic byte-identical deep suites and the
unit suites give the TS SDK its structural certainty. Re-run at
`--repeats 3` on a stable server to match the Python/Go statistical bar.

**What this proves per SDK/framework:**

- **Python** — manual (`AgentSession`), LangGraph (`fork_thread`), Claude
  (`fork_session`), CrewAI, LangChain and OpenAI Agents (`fork_trace`) all
  have live fork tests; the manual and LangGraph paths now also have
  LLM-judged fidelity evidence.
- **Go** — the deterministic deep suite (`sdks/go/fork_deep_live_test.go`,
  15 cases incl. byte-identical full-vs-fork, fork-of-fork at an inherited
  step, lineage_only, contamination guards) + LLM-judged fidelity + Copilot
  live fork (`sdks/go/copilot/live_test.go`).
- **TypeScript** — the deterministic deep suite
  (`sdks/typescript/test/fork_deep_live.test.ts`, 15 cases) + live adapter
  forks for LangGraph, Claude, LangChain and Copilot
  (`test/adapters_fork_live.test.ts`) + LLM-judged fidelity (session and
  LangGraph adapters).
- **API parity closed** — `StateManager.fork` now exists in Go and
  TypeScript (Python already had it), and the TS OpenAI adapter gained
  `forkTrace` (Python `fork_trace` parity).

## Tests

- `sdks/example-tests/python/test_fork_fidelity_lib.py` — 19 unit tests (no server,
  no LLM): judge JSON parsing (plain/fenced/loose/malformed), deterministic
  judge, LLM judge averaging + retry, tail/prefix extraction, pass/fail
  threshold logic, repeat aggregation, `_is_transient`, report
  serialization, and validation that the five scenarios are well-formed and
  genuinely unrelated.
- `sdks/example-tests/python/test_fork_fidelity_suite_live.py` — opt-in live:
  `test_fidelity_suite_dry_run_plumbing` (needs only ACTAE_URL/KEY, runs the
  full suite deterministically, asserts structure + pass) and
  `test_fidelity_suite_real_judge` (needs DEEPSEEK_API_KEY, runs the real
  suite with `repeats` and asserts the marketing claim; writes the evidence
  report).

Related: `docs/FORK_RESUME.md` (the workflow), `docs/FORK_RUNTIME.md`
(server semantics), `examples/fork_resume_demo.py` (the token-savings +
in-demo fidelity demo).

## Interpretation limits

The five original scenarios were used while developing the rubric and are not
an untouched holdout set. Their results are regression evidence, not a
population-wide proof. The pass rule permits the fork mean to trail the re-run
mean by `tolerance` (default 1.5); a PASS therefore must not be paraphrased as
“the fork was never worse” unless the recorded paired results establish that
separate statement.

The scenarios exercise agents whose complete continuation state is a
JSON-compatible dictionary. They do not establish fidelity for uncaptured
provider threads, mutable external services, nondeterministic tools, retrieval
drift, or side effects. Strong general claims require a frozen, preregistered
holdout evaluation covering those cases, independent/blinded judges, and
confidence intervals over paired fork-versus-rerun outcomes.

### Independent follow-up result

The strengthened 2026-08-10 run (`--repeats 3 --judge-reps 2`) is stored in
`examples/fidelity-reports/fork-fidelity-independent-audit-2026-08-10.json`.
All five scenario means passed (fork 8.27, re-run 7.57), and all 15 fork
receipts proved exact/restorable boundaries. One individual fork comparison
scored 5.0, however, and the fork trailed its paired re-run twice. This is
positive aggregate evidence for exact checkpoint restoration, but it rules
out absolute “always highly similar” and “never worse” wording.
