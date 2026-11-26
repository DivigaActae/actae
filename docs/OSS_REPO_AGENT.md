# OSS Repo Agent — a real agent on a real public repository

`examples/oss_repo_agent/` is the "real agent, public code" example: it clones a
pinned public GitHub repository, reintroduces a small deterministic bug, and
runs a real software-engineering agent whose oracle is the repo's **own** test
file. It exists because testing Actae with an agent on private/work code raises
data-handling questions; public code removes that blocker.

## What runs

`examples/oss_repo_agent/demo.py`:

| Phase | Steps | Result |
|-------|-------|--------|
| baseline | `clone(1) survey(2) locate(3) edit(4) test(5) react_edit(6) react_test(7) report(8)` | the first edit is plausible but wrong → tests fail |
| fork | forked at `test(5)`, runs `react_edit(6) react_test(7) report(8)` | the correct fix → tests pass |
| compare | `GET /api/v1/channels/compare` | structural state diff + output diff between the runs |
| promote | `POST /api/v1/channels/{id}/promote` | the winning fork is merged into its parent |

The default task targets `pallets/itsdangerous` (BSD-3-Clause, pure Python,
stdlib-only runtime). The reintroduced bug is `int_to_bytes` dropping its
`.lstrip(b"\x00")`; the plausible-but-wrong attempt is `.rstrip(...)`
(no-op here); the correct fix restores `.lstrip(...)`. The repo's
`tests/test_itsdangerous/test_encoding.py::test_int_bytes` is the oracle.

## Run

```bash
pip install -e ./sdks/python && pip install pytest
cd actae && cargo run        # :8002
python3 examples/oss_repo_agent/demo.py    # deterministic (no key)
DEEPSEEK_API_KEY=... python3 examples/oss_repo_agent/demo.py --llm
```

`--task <name>` selects an `oss_tasks.py` entry; `--refresh` re-clones;
`--reports-dir <dir>` changes outputs (default `examples/oss_repo_agent/reports/`,
gitignored, along with `.cache/`).

## Actae features it exercises

- **`AgentSession` steps** with `state_fn` + `snapshot_interval=1`, so every step
  is an exact, forkable checkpoint.
- **Idempotent execution ledger** for edits (`edit-<step>`), test runs
  (`test-<step>`, identity includes the target file's SHA-256) and the report
  (`oss-report-1`). The fork's inherited report **replays** (`tool.replayed`)
  instead of firing again.
- **Fork from an existing session** (`AgentSession.resume(fork_at_step=…)`) — the
  fork inherits the prefix and only re-runs steps 6–8; the immutable receipt is
  checked for `restorable` + exact `requested == resolved` cursor.
- **Server-side compare + promote** (`compare_channels`, `AgentSession.promote`).

## Policies

- `DeterministicPolicy(correct=False|True)` — scripted give-up vs correct fix
  (the reproducibility path, no key).
- `LLMPolicy` — DeepSeek receives the real pytest output and the target source;
  replies strict JSON `{"fix": "lstrip"|"rstrip"}`. Everything else
  (scaffolding, verification, ledger, fork) is unchanged and deterministic.

## Sensitivity boundary

The agent stores its own steps (including the tiny diffs and test output) in
your Actae instance's Postgres. Egress beyond your machine: the `git clone` of
public code (GitHub) and, in `--llm` mode, the failing test output + target file
sent to DeepSeek. Before pointing this at private code, decide on: a
zero-retention/no-training provider or a self-hosted model, Actae's retention
env vars (`EVENT_RETENTION_DAYS` etc.), the 1 MiB payload cap, and redaction.
New tenants can also run with `ENABLE_ANONYMOUS=false` and stay fully on-prem.

## Tests

Offline (no network, no server): `sdks/example-tests/python/test_oss_repo_agent.py`
covers task-anchor integrity, `prepare_workspace` isolation, `RepoWorkspace`
search/edit/hash/diff, and both policies (including the LLM choice via an
injected `chat_fn`).

```bash
python3 -m pytest sdks/example-tests/python/test_oss_repo_agent.py -v
```
