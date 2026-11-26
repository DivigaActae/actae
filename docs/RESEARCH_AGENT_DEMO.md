# Research-agent demo (LangGraph + Actae + real arXiv, fork flagship)

`examples/research_agent/` is a real research agent whose defining
demonstration is **forking a live run to add a step without recomputing the
expensive prefix**. It ingests **real papers from the open arXiv API**, and is
built on LangGraph with Actae's `ActaeCheckpointSaver` as the graph's
checkpointer, OpenAI `gpt-4o-mini-2024-07-18`, and a stdlib-only arXiv client.

A **cloud twin** lives at `examples/research_agent_cloud/` (see "Cloud twin & the
dashboard story" below): the same pipeline pointed at a **hosted** instance so
the run and its forks render live in the hosted dashboard.

## The data

`source.py` queries `https://export.arxiv.org/api/query` (Atom XML, no key,
permissive terms; a descriptive User-Agent is sent). A topic search returns up
to 40 papers; each becomes an addressable chunk `[<arxiv-id>] title :: abstract`.
Results are **cached** to `examples/research_agent/data/arxiv-<slug>.json` and
committed, so offline runs and tests use the same inputs; `--refresh` re-fetches
and `--topic` retargets. Default topic: `large language model agents`
(40 papers, ~7.5k words). Pure helpers `parse_atom`, `chunks`, `word_count` are
unit-tested.

## The pipeline

```
load_source ──▶ filter_source ──▶ extract_notes ──▶ [fact_check] ──▶ write_report
  (arXiv)          (LLM)             (LLM)           (LLM, added)      (LLM)
```

`filter_source`/`extract_notes` are the expensive prefix (they read the papers
to the model). `fact_check` is the step we add: it verifies each finding against
the arXiv paper it cites and flags hallucinated citations.

## What it demonstrates

| Scenario (`--scenario`) | Shows |
|---|---|
| `fork` (default in `examples/research_agent/`) | Run `load → filter → extract → brief`; then insert `fact_check`, **fork at the checkpoint after `extract_notes`**, and run only the tail. The fork inherits the real papers + findings; the token report shows zero `filter_source`/`extract_notes` calls on the fork. |
| `tree` (default in `examples/research_agent_cloud/`) | Run the baseline **once**, then fork it **twice from the same checkpoint** — Fork A adds `fact_check`, Fork B swaps model + prompt. One root, two sibling counterfactuals: the cleanest thing to see in the dashboard. |
| `variant` | Fork after `extract_notes` and re-run only the tail with a **different model** (`--fork-model`, default `gpt-4o`) and a **different report prompt** (`--fork-style brief\|exec\|skeptical`, from `pipeline.REPORT_STYLES`). Same papers/findings, tail-only change — the "swap any downstream knob without replaying the past" beat. |
| `crash` | A separate worker runs to the checkpoint after `extract_notes` and is hard-killed (`os._exit(137)`). A new process resumes the same thread from that checkpoint and writes the brief — the prefix is not recomputed. |
| `replay` | The durable event history Actae kept for the run, cursor by cursor. |

## How the fork is done (the non-obvious part)

`ActaeCheckpointSaver` persists every LangGraph super-step as a durable Actae
checkpoint (an event + a state snapshot). `demo.py`:

1. locates the checkpoint produced by `extract_notes` via
   `pipeline.checkpoint_after(graph, config, "extract_notes")` — it walks the
   checkpoint history with `graph.aget_state(...).parent_config` (LangGraph
   checkpoint metadata in 1.x does not carry node names);
2. forks it with `saver.fork_thread(boundary, new_thread_id=...)`;
3. resumes the **enhanced** graph, re-deriving the next step via
   `graph.aupdate_state(resume_cfg, {}, as_node="extract_notes")`.

Step 3 matters: a checkpoint records its **next** step, so resuming the
modified graph would otherwise replay the baseline's successor instead of the
new node. `aupdate_state(as_node=<last completed step>)` on the fork's latest
checkpoint (no pinned `checkpoint_id`) makes LangGraph recompute the next step
from the new graph shape.

## Files

| File | Purpose |
|---|---|
| `demo.py` | Driver: ingest arXiv → baseline → add fact-checker → fork → token report; `replay`/`crash` scenarios; `--topic/--max-results/--refresh` |
| `pipeline.py` | `ResearchState`, the swappable `Model` (`OpenAIModel` / `ScriptedModel`), `TokenLedger`, the nodes + `build_graph`, `checkpoint_after` |
| `source.py` | The arXiv client: `fetch_papers`/`parse_atom`/cache + `chunks` (stdlib only) |
| `worker.py` | Separate process for the durability scenario |
| `data/arxiv-*.json` | Committed snapshots for reproducible offline runs |

The model is swappable behind `Model.complete(system, user, step)`. The
`ScriptedModel` is **extractive** — it parses the real abstracts out of each
prompt and returns real claims with no API key, injecting exactly one fabricated
citation so the checker has a hallucination to catch (the dry-run tells the same
story as a real run). JSON-returning nodes use `_call_json`, which retries once
with a stricter instruction if the model wraps JSON in prose.

## Cloud twin & the dashboard story

`examples/research_agent_cloud/` is a self-contained copy that points at a
**hosted** instance instead of localhost. It adds three things so the run reads
well in the dashboard:

1. **`cloud.env`** (next to `demo.py`, **not committed**) holds `ACTAE_URL` +
   `ACTAE_API_KEY`; `demo.py`/`worker.py` load it automatically (an exported
   variable or `--url`/`--api-key` still wins).
2. **First-class business events.** The LangGraph checkpointer only records
   opaque `langgraph.checkpoint` / `langgraph.pending_writes` events. The cloud
   `pipeline.py` nodes also `record(...)` named step events —
   `load_source`, `filter_source`, `extract_notes`, `fact_check`,
   `write_report` — each with `{title, model, duration_ms, tokens, rows}`
   (and the report under `output` on `write_report`). The dashboard timeline
   then reads as the agent's actual steps. `_emit` pins the event to the run's
   **root** checkpoint namespace (`checkpoint_ns=""`) so steps land on the run
   channel, not a per-node sub-channel.
3. **Run metadata + fork manifests.** After each run the demo publishes
   `experiment_metadata` (`steps`, `metrics` = tokens/duration/cost,
   `result.output`) via `update_metadata`, and each fork carries a declared
   `manifest` (`fork_thread(..., manifest=...)`) — model, prompt style, added
   step. That is what populates the dashboard's **Metrics**, **Output** and
   **Config** compare tabs.

Run it (default scenario is `tree`):

```bash
python examples/research_agent_cloud/demo.py --dry-run        # no OpenAI key
python examples/research_agent_cloud/demo.py                  # real LLM + arXiv
```

Then open the instance's dashboard: **Runs** shows the three named runs
(`Research baseline · <topic>`, `Fork A · +fact-check`, `Fork B · …`, each fork
tagged FORK with its parent); a run's **Compare** tab shows "What changed?"
(state/config/tools/output/metrics); the **Forks** nav (Fork laboratory)
auto-derives the "Fork family" with named members and a side-by-side **Fork
diff**. See `docs/DASHBOARD_WORKFLOW.md` for the dashboard-side behaviour.

## Run

```bash
pip install -e ./sdks/python
pip install "langgraph>=0.6,<2" langchain-core openai

export ACTAE_URL=http://127.0.0.1:8002   # or --url / --port
export ACTAE_API_KEY=sk-...              # or --api-key
export OPENAI_API_KEY=sk-...             # real model only; omit with --dry-run
cd actae && cargo run

python examples/research_agent/demo.py [--dry-run] [--topic "…"] [--refresh]
python examples/research_agent/demo.py --scenario fork|variant|crash|replay

# cloud twin (reads examples/research_agent_cloud/cloud.env):
python examples/research_agent_cloud/demo.py --dry-run
```

There are **no hardcoded endpoint or keys**: the demo reads `ACTAE_URL` /
`ACTAE_API_KEY` / `OPENAI_API_KEY` (export preferred) or the `--url`/`--port`/
`--api-key` flags, and fails with a clear, actionable message when one is
missing or rejected. `ACTAE_API_KEY` must be valid for the target instance
(mint one with `./target/debug/actae keys create demo`, or run a clean
standalone instance: `ACTAE_DATA_DIR=… ACTAE_CONFIG_DIR=… ./target/debug/actae`).
The pipeline's checkpointer uses the HTTP API only, so the demo does not open a
WebSocket.

## Tests

`sdks/example-tests/python/test_research_agent.py` (offline, no server/network;
skipped without `langgraph`): Atom parsing/normalization, the committed snapshot
+ chunker, the token ledger + report arithmetic, `_extract_json` leniency,
the extractive scripted model (real claims + the caught fabricated citation),
and the graph shape with/without `fact_check`. Optional live fetch gated on
`ACTAE_ARXIV_LIVE=1`. The live fork and durability behaviour is the demo itself.
