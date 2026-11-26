# Fork Showcase — a claims-to-evidence index

Forking is Actae's most under-marketed capability: it is easy to demo badly and
hard to claim credibly. This document is the single surface that maps each
public fork claim to the artifact that backs it and the command that reproduces
it, plus one runnable end-to-end demo.

## The runnable demo

`examples/fork_showcase.py` — deterministic, LLM-free, Python SDK only:

1. A baseline run **fails** at a verify step (recorded immutably in state).
2. Two forks are created at that **exact boundary** with different
   interventions (more retries vs a fallback tool).
3. `diff_states` proves structurally what changed between the forks.
4. Both fork receipts are shown to be **restorable, exact** boundaries.
5. The parent is shown to be **unaffected** by either fork.
6. A deterministic token report shows the prefix (3 of 5 steps) was not re-run.
7. A **fidelity-bound** check: a ledger-recorded nondeterministic effect
   replays on the fork (no re-execution), while the same effect re-run on a
   fresh channel re-executes and diverges — the fork is only as faithful as
   what the ledger captured.

```bash
cd actae && cargo run
python3 examples/fork_showcase.py
```

Exit 0 when all 11 checks pass. Unit tests for the pure helpers:
`sdks/example-tests/python/test_fork_showcase.py`.

## Claims → evidence → reproduce

| Claim | Evidence | Reproduce |
|-------|----------|-----------|
| A fork inherits an **exact, restorable boundary** with immutable requested/resolved cursors and a source-state SHA-256. | `docs/FORK_RUNTIME.md` §1/§10; `docs/COUNTERFACTUAL.md`; `examples/fork_showcase.py` receipt checks | `python3 examples/fork_showcase.py` |
| The inherited prefix is **byte-identical** to the baseline; the fork recomputes only the tail. | `docs/FORK_FIDELITY.md` (structural `prefix_identical`); deep deterministic suites in all three SDKs | `python3 examples/fork_fidelity_suite.py --dry-run` |
| Fork output is **semantically equivalent to a no-fork re-run** (LLM-judged, with a re-run control). | Committed reports in `examples/fidelity-reports/`; `docs/FORK_FIDELITY.md` | `python3 examples/fork_fidelity_suite.py --repeats 3 --judge-reps 1` |
| **Counterfactual debugging**: fork twice from a failure, diff the states, prove which fix worked. | `docs/COUNTERFACTUAL.md`; `examples/fork_showcase.py` | `python3 examples/fork_showcase.py` |
| Forking **saves the prefix cost** (measured tokens/wall-clock on a real pipeline). | `examples/fork_resume_demo.py` + `examples/fork_token_report.py`; `docs/FORK_RESUME.md` | `python3 examples/fork_resume_demo.py --dry-run` |
| A fork does **not silently re-fire an inherited side effect** (`auto` replays, `block` refuses, `live` runs). | `docs/TOOL_EXECUTIONS.md` "Fork tool policies"; reliability `fork` suite; `examples/fork_intervention_demo.py` | `python3 examples/reliability/runner.py --suites fork` |
| The **server owns the step index** and resolves a fork's inherited steps through the lineage. | `docs/FORK_RUNTIME.md` §1 "Server-owned step index"; `tests/test_step_index.py` | `python3 -m pytest tests/test_step_index.py` |
| Crash recovery does not lose or duplicate the prefix. | `docs/RELIABILITY_EVIDENCE.md` (`crash`, `atomicity`); `docs/DURABILITY_BENCHMARK.md` | `python3 examples/reliability/runner.py --suites crash,atomicity` |
| The same fork semantics hold in **every SDK and framework adapter**. | `docs/FORK_FIDELITY.md` "cross-SDK committed evidence"; deep live suites (`fork_deep_live`, `adapters_fork_live`) | see the SDK test commands in `AGENTS.md` |
| Recorded execution is **tamper-evident**. | `docs/EXECUTION_INTEGRITY.md` | `GET /api/v1/events/verify/{channel}` |

## A realistic fork workload

`examples/repo_agent/demo.py` (`docs/REPO_AGENT_DEMO.md`) forks a real
repo-maintenance agent at a failing-test step and shows the fork applying a
corrective fix without re-running the prefix or re-firing the inherited report.

## Honest limits

- The fidelity scenarios exercise agents whose continuation state is a
  JSON-compatible dictionary; they do not establish fidelity for uncaptured
  provider threads, mutable external services, nondeterministic tools, or
  retrieval drift (`docs/FORK_FIDELITY.md` "Interpretation limits").
- The token report in the showcase is a deterministic fixture, not measured
  provider usage; the measured version is `examples/fork_resume_demo.py`
  (dry-run reports are clearly labelled).
- The dashboard never creates forks (forks are code-first); it visualizes and
  compares them.
