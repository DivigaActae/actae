# LangGraph + Actae: durable, forkable agents

`examples/langgraph_actae_agent/` threads the Actae SDK into **LangGraph**, the
canonical agent framework, at its native durability boundary: the checkpointer.
The agent is a real `create_react_agent` (tool-calling ReAct graph); Actae's
`ActaeCheckpointSaver` is its `BaseCheckpointSaver`. This is the difference
between "an agent that records events to Actae" and "an agent whose execution
*is* durable in Actae".

## What the demo proves

Three scenarios over one real agent, with the side-effecting tool guarded by
Actae's execution ledger:

| Scenario | Steps | Asserted |
|---|---|---|
| **baseline** | agent → `record_reading` tool → final answer | the tool fired exactly once |
| **crash + resume** | worker `os._exit(137)` inside the tool, after the effect, before the ledger completes it; then a **new process** resumes | effect count stays 1 (the reclaimed claim reconciles by checking the effect store); the graph continues from its checkpoint |
| **fork** | `fork_thread` at the checkpoint after the tool, before the final answer; resume with a modified decision | the final answer differs; the tool is **not** re-run; `GET /channels/compare` shows the divergence; `POST /channels/{id}/promote` merges the winner |

Run: `python3 examples/langgraph_actae_agent/demo.py` (deterministic) or
`--llm` for a DeepSeek baseline. Deps: `langgraph`, `langchain-core`
(+ `langchain-openai` for `--llm`).

## Integration (the whole point)

```python
from actae_client.adapters.langgraph import ActaeCheckpointSaver
from langgraph.prebuilt import create_react_agent

saver = ActaeCheckpointSaver(client, channel="lg-survey")
graph = create_react_agent(model, tools, checkpointer=saver)

cfg = {"configurable": {"thread_id": "run-1"}}
await graph.ainvoke({"messages": [HumanMessage("…")]}, cfg)   # durable run
await graph.ainvoke(None, cfg)                                 # resume after a crash

fork_cfg = await saver.fork_thread(cfg, new_thread_id="run-1-fix",
                                   reason="refine the final decision")
await graph.ainvoke(None, fork_cfg)                            # tail-only re-run
```

That is a **3-line change** to a normal LangGraph program — `checkpointer=…`
plus `fork_thread`. Everything else (the model's loop, tool calling, message
state) stays LangGraph's.

## Mapping to Actae features

- **Durable execution** — each LangGraph super-step is persisted as an Actae
  event + state snapshot (`aput`/`aput_writes`); `aget_tuple`/`alist` reconstruct
  the checkpoint history; the checkpoint's event cursor anchors replay and fork.
- **Resume after a hard kill** — no in-memory state; the graph rebuilds from the
  latest Actae checkpoint and re-runs only the pending tasks.
- **Forking** — `fork_thread` copies the checkpoint envelope to a derived channel
  and returns a resume config; the fork inherits the exact prefix (including the
  LLM message history) and re-runs only the tail.
- **Idempotent side effects** — the tool claims/completes an execution key, so a
  resume (reclaimed claim) or a fork never fires an inherited effect twice.
- **Comparison + promotion** — `GET /api/v1/channels/compare` structural diff and
  `POST /api/v1/channels/{id}/promote`.

## Other frameworks

The same boundary exists for the other adapters (`docs/ADAPTERS.md`,
`sdks/python/src/actae_client/adapters/`): CrewAI (`ActaeCrewStateHook` +
`CrewAIResumer`), Claude Agent SDK (`SessionStore` durable transcripts), OpenAI
Agents (`ActaeTracingProcessor`), LangChain (`ActaeContextSaver` /
`ChainResumer`), and the OTel bridge. They are tested against the real SDKs.

## Tests

`sdks/example-tests/python/test_langgraph_actae_agent.py` (offline; skipped without
`langchain-core`) pins the scripted model, the effect store, and the idempotent
ledger-guarded tool (apply / reconcile-reclaimed / replay). The live durability
+ fork story is the demo itself.

## Torture suite (`torture.py`)

`examples/langgraph_actae_agent/torture.py` is a repeatable, hard-asserting
battery run against a live Actae — the "test the hell out of it" pass. It mimics
what users (and hostile CI) actually do and asserts exact invariants:

| Scenario | Asserts |
|---|---|
| `isolation` | 6 concurrent threads → 6 distinct channels, one effect each |
| `crash_resume` | kill inside the tool at `before_effect`, `after_effect`, `after_complete`, and an **immediate** resume (live lease) → effect fires exactly once end-to-end every time |
| `resume_after_complete_is_noop` | resuming a finished thread changes nothing |
| `fork_every_checkpoint` | fork at **every** checkpoint of a thread; all resume; the effect is never re-fired |
| `fork_of_fork` | nested fork + a fork receipt |
| `concurrent_forks` | 5 simultaneous `fork_thread` + resume; distinct channels, independent |
| `parallel_tool_calls` | two tool calls in one AI message; the side-effecting one fires once |
| `tool_failure_then_retry` | a failing tool surfaces, writes no effect; a retry applies once |
| `time_travel` | resume from an older `checkpoint_id`; no double effect |
| `many_sequential` | 15 sequential runs, each one effect |

```bash
python3 examples/langgraph_actae_agent/torture.py                 # all scenarios
python3 examples/langgraph_actae_agent/torture.py --scenario concurrent_forks
```

This suite found a real server bug: concurrent writes deadlocked on the
storage-accounting triggers (HTTP 500). Fixed in migration **v31** (advisory-lock
serialization; see `docs/STORAGE_ACCOUNTING.md`). Server-side, internal errors
(HTTP 500) are now logged with their cause instead of being silently masked
(`err_to_client_message`).

## Data-sensitivity boundary

The agent's messages and tool payloads are stored in **your** Actae instance's
Postgres; the model call (in `--llm`) is the only other egress. For work data,
decide on a zero-retention/self-hosted model and Actae retention
(`EVENT_RETENTION_DAYS`, the 1 MiB payload cap) first — see
`docs/OSS_REPO_AGENT.md`'s boundary note.
