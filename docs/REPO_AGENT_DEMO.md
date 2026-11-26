# Repo-Maintenance Agent Demo (real workflow, real crash, real fork)

`examples/repo_agent/` runs a plausible software-engineering agent — inspect a
repository, locate a bug, edit code, run the real test suite, react to the
failure, submit a report — against a real copied repo with real tools, a real
external side effect, a real process crash, and a real fork.

> **This is a demo, not the durability proof.** The proof lives in
> `examples/reliability` and `examples/durability_benchmark.py`
> (`docs/RELIABILITY_EVIDENCE.md`, `docs/DURABILITY_BENCHMARK.md`). This demo
> shows the same guarantees holding for a realistic workload; it is not a
> throughput or model-capability benchmark.

## Layout

| File | Role |
|------|------|
| `fixture_repo/` | A tiny Python package with a genuine bug in `calc/stats.py::median` and a failing test in `tests/test_stats.py`. Never mutated (the workspace is a copy). |
| `tools.py` | Real tools: `RepoWorkspace.read_file` / `search` / `edit_file` / `run_tests` (subprocess) / `file_hash` / `diff`; `seed_workspace`; the known-correct `CORRECT_MEDIAN_FIX`. |
| `agent.py` | `RepoAgent` (the 8-step pipeline) and the policies: `DryRunPolicy` (deterministic) and `LLMPolicy` (real model chooses the fix). |
| `worker.py` | Runs the agent as a subprocess and kills it with `os._exit(137)` at `--crash-at N`, after that step's side effect and before the step is recorded. |
| `demo.py` | Orchestrates baseline → crash+resume → fork, checks the invariants, writes a report. |
| `reports/` | Committed run reports (`latest.json`, `repo-agent-<run-id>.json`) and throwaway workspaces. |

## The pipeline (8 steps)

`inspect → locate → plan → edit (first attempt) → test → react (fork-specific
edit) → test → submit_report`

Every step is recorded through `AgentSession.step`. The side-effecting
operations are guarded by Actae's idempotent execution ledger:

- edits use keys `edit-<step>` (params include a `variant`),
- test runs use keys `test-<step>` (params include `subject_sha256`, the hash
  of the code under test),
- the report uses key `report-1`.

The report is submitted to a local `HttpEffectService`
(`examples/durability_benchmark.py`) with `honors_key=True`, so the exact
effect count is independently observable.

## Scenarios and what they prove

1. **baseline** — the agent makes a plausible first edit; the tests still fail;
   the report is submitted **exactly once** (task-service applications == 1).
2. **crash + resume** — a worker is killed with `os._exit(137)` right after the
   report lands but before the ledger records completion. After the lease
   lapses, a resume **reclaims** the report, queries the task service, sees the
   report already landed, and **completes without re-submitting** (applications
   stays 1). Steps 1..7 are not re-invoked (`invoked == [8]`).
3. **fork** — the baseline is forked at the failing-test step with a corrective
   `intervention`. The fork applies the correct fix, the tests pass, the
   prefix (1..5) is not re-run (`invoked == [6, 7, 8]`), and the fork's
   re-issued `report-1` **replays** the parent's recorded result (`auto`
   policy) — the inherited side effect never fires again. The fork receipt is a
   restorable, exact boundary (requested cursor == resolved cursor).

Deterministic invariants gate the exit code. In LLM mode the model's baseline
outcome is reported informationally (it may fix the bug itself, in which case
the fork outcome is not required to differ).

## Design lessons the demo encodes

- **Idempotency identity must cover the inputs it stands for.** The test step's
  key includes `subject_sha256`; otherwise a fork would replay the parent's
  stale test result instead of re-testing changed code.
- **Fork-specific work must be request-distinct.** The react-edit `variant`
  differs per fork, so a fork does not replay the parent's (no-op) edit under
  `auto`; a genuinely new edit runs live.
- **The report is the irreversible effect.** It is what crashes test: apply
  then die before recording, and reconcile by query on recovery.

## Run

```bash
pip install -e ./sdks/python
cd actae && cargo run          # Actae on :8002

# Deterministic (no API key) — the committed evidence path:
python3 examples/repo_agent/demo.py

# Real model (DeepSeek) for the baseline's fix decision:
DEEPSEEK_API_KEY=... python3 examples/repo_agent/demo.py --llm
# (the key is also auto-loaded from actae/.env when present; override the model
#  with DEEPSEEK_MODEL, the endpoint with DEEPSEEK_BASE_URL)
```

Env: `ACTAE_URL` / `ACTAE_API_KEY` (defaults `http://localhost:8002` /
`sk-dev-0000000000000000000000`). `--reports-dir DIR` changes the output dir.

Exit code is 0 when every check passes. The report is written to
`examples/repo_agent/reports/repo-agent-<run-id>.json` plus `latest.json`:

```json
{
  "run_id": "...", "actae_url": "...", "mode": "dry-run" | "llm",
  "baseline_channel": "...", "fork_channel": "...",
  "checks": [{"name": "...", "ok": true, "detail": ""}],
  "passed": 12, "total": 12, "verdict": "PASS"
}
```

## Tests

```bash
python3 -m pytest sdks/example-tests/python/test_repo_agent_tools.py -v
```

Unit tests (no server, no LLM) pin the real filesystem/test behavior, the
deterministic policy, that seeding never mutates the fixture, and that the
correct fix produces the expected file hash. They also drive the **full 8-step
`RepoAgent` orchestration** against an in-memory ledger and a real local task
service (asserting the report fires exactly once and replays on a re-run), and
pin `LLMPolicy` determinism via an injected `chat_fn` (median → correct fix,
average → wrong edit, malformed → median).

## Honest limitations

- The fixture is small and the fix is known; the demo measures durability and
  fork isolation, not model capability.
- LLM mode is non-deterministic by nature: the model may fix the bug on the
  baseline, so the "baseline fails" and "fork differs" checks are informational
  there. The deterministic dry-run is the reproducibility path; `LLMPolicy`
  is deterministically testable through its injectable `chat_fn`.
- Runs use a throwaway workspace copy; nothing here proves behavior for a
  multi-service production agent (that is the point of the semantic suites).
