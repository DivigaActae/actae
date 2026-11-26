# LangGraph ReAct agent + Actae · durable & forkable

A **real LangGraph tool-calling agent** (`create_react_agent`) compiled with
Actae's `ActaeCheckpointSaver` as its checkpointer. Actae *is* the agent's
durable state store, so the same thread survives a hard process kill and can be
forked at an exact checkpoint. This is the "thread the Actae SDK into a real
agent framework" demo.

```
baseline        agent calls a side-effecting tool, then answers
crash + resume  worker killed (exit 137) inside the tool, after the effect but
                before the ledger records it -> a NEW process resumes the graph
                from its Actae checkpoint; the effect does NOT fire twice
fork            fork the thread at the exact checkpoint after the tool, change
                the final decision, run only the tail (the tool is not re-run),
                compare baseline vs fork server-side, promote the winner
```

## Run

```bash
pip install -e ./sdks/python
pip install "langgraph>=0.6,<2" "langchain-core>=0.3,<2"
pip install langchain-openai        # only for --llm

cd actae && cargo run        # :8002
python3 examples/langgraph_actae_agent/demo.py                 # deterministic, no key
DEEPSEEK_API_KEY=... python3 examples/langgraph_actae_agent/demo.py --llm
```

## Why this shows Actae's power (not just its primitives)

- **Durable execution is the framework's own loop.** The agent is LangGraph's
  prebuilt ReAct graph; `checkpointer=ActaeCheckpointSaver(...)` makes every
  super-step durable. A `SIGKILL`-style exit (`os._exit(137)`) mid-tool leaves a
  resumable checkpoint; `ainvoke(None, config)` continues the *same* thread in a
  new process.
- **Idempotent side effects.** The tool claims an execution ledger key
  (`reading:<label>`); on resume the claim is *reclaimed*, the effect store is
  checked, and completion is recorded **without re-applying** the effect.
- **Forking an agent at a step.** `ActaeCheckpointSaver.fork_thread(config,
  new_thread_id=...)` copies the checkpoint (the full graph state, including the
  LLM message history) to a new channel. Resuming there re-runs only the tail —
  the tool does not re-fire — and the two runs are diffed with
  `GET /api/v1/channels/compare` and merged with `POST .../promote`.

## Files

| File | Role |
|------|------|
| `lg_agent.py` | Scripted deterministic ReAct model, the idempotent ledger-guarded tool + pure tool, `build_graph`, effect-store helpers. |
| `worker.py` | Runs the agent once in a subprocess; `--crash` kills it inside the tool. |
| `demo.py` | baseline → crash+resume → fork+compare+promote, invariant checks, JSON report. |
| `reports/` | Reports + the effect log (gitignored). |

## Deterministic vs `--llm`

Default mode uses a history-aware `ScriptedSurveyModel` (no key) so the
durability and fork guarantees are reproducible and asserted. `--llm` runs the
**baseline** with DeepSeek (the crash and fork branches stay deterministic so the
checks are exact). LLM mode needs `langchain-openai` + `DEEPSEEK_API_KEY`.

Offline unit tests (no server/network): `sdks/example-tests/python/test_langgraph_actae_agent.py`.

Live torture suite (needs a running Actae): `python3 examples/langgraph_actae_agent/torture.py`
— 10 scenarios (concurrent threads, crash at every point in the tool, immediate
resume, resume-after-complete, fork at *every* checkpoint, fork-of-fork,
concurrent forks, parallel tool calls, tool failure + retry, time-travel, and a
sequential battery) with hard invariants. It found and drove the fix for a real
server deadlock under concurrent writes (see `docs/LANGGRAPH_ACTEA_AGENT.md`).
