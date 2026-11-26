# OSS Repo Agent · Actae Example

A **real agent on a real public repository**. It clones a pinned public GitHub
repo (`itsdangerous`, BSD-3-Clause), reintroduces a small deterministic bug, and
runs an 8-step software-engineering agent with the repo's **actual test file**
as the oracle. No customer or work data is involved — only public,
permissively licensed code.

```
baseline   clone → survey → locate → edit (plausible but wrong) → test (fails)
           → react (gives up) → test (fails) → report
fork       forked at the failing-test step → applies the correct fix
           → test (passes) → report (inherited report replays, not re-fires)
compare    server-side "what changed?" between the two runs
promote    the winning fork is promoted into its parent
```

## Run it

```bash
pip install -e ./sdks/python
pip install pytest                 # the target repo's test runner

cd actae && cargo run          # Actae on :8002
# in another shell, from the repo root:
python3 examples/oss_repo_agent/demo.py                 # deterministic, no key
DEEPSEEK_API_KEY=... python3 examples/oss_repo_agent/demo.py --llm   # real model decides
```

`--task` selects a registry entry (`examples/oss_repo_agent/tasks.py`);
`--refresh` re-clones; `--reports-dir` changes the output location.

## What it proves

- **Real tools, real code.** `git clone`, file read/search/edit, and the repo's
  own `pytest` file run as subprocesses. The oracle is `itsdangerous`'
  `tests/test_itsdangerous/test_encoding.py`.
- **Idempotent side effects.** Edits, test runs and the report are guarded by
  Actae's execution ledger. The fork's inherited report **replays** rather than
  firing again (`tool.replayed`).
- **Exact-boundary forks.** The fork inherits a restorable checkpoint at the
  failing-test step; only steps 6–8 re-run (the prefix is not re-invoked).
- **Server-side comparison + promotion.** `GET /api/v1/channels/compare` shows
  the structural diff between the two runs; `POST .../promote` merges the
  winner into its parent.

The model's role in `--llm` mode is narrow and honest: given the real failing
`pytest` output and the source file, DeepSeek returns strict JSON
`{"fix": "lstrip" | "rstrip"}`. All scaffolding and verification stay
deterministic.

## Files

| File | Role |
|------|------|
| `oss_tasks.py` | Task registry: pinned repo + mutation / wrong-fix / correct-fix anchors + test path. |
| `oss_tools.py` | `clone_repo`, `prepare_workspace`, `apply_mutation`, and `RepoWorkspace` (read/search/edit/run_tests/diff/hash). |
| `oss_agent.py` | `OssRepoAgent` (8-step pipeline) + `DeterministicPolicy` / `LLMPolicy`. |
| `demo.py` | Orchestrates baseline → fork → compare → promote, checks invariants, writes a report. |
| `reports/` | Run reports + throwaway workspaces (gitignored). `.cache/` holds the pinned clone. |

## Data safety

The agent clones **public** code and stores its own steps in your Actae
instance's database. Nothing leaves your machine except the `git clone` (public
GitHub) and, in `--llm` mode, the failing pytest output + the target source file
sent to DeepSeek. That is the boundary to decide on before pointing this at any
private code — see the "sensitivity boundary" note in
`docs/OSS_REPO_AGENT.md`.

Offline unit tests (no network, no server): `sdks/example-tests/python/test_oss_repo_agent.py`.
