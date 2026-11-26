# Research agent demo — fork is the flagship (on real arXiv data)

A real research agent: it ingests a topic from the **open arXiv API**, filters
the papers that matter, extracts the key findings, and writes a brief. Then we
**add a step** — a fact-checker that verifies each finding against the paper it
cites and flags hallucinated citations — and show that Actae lets us **fork the
run at the checkpoint after extraction**, so the new step runs without
re-paying for the ingest, filter and extract LLM calls. The token report proves
the saving.

This is the demo used in the video. It is deliberately small and legible: the
pipeline has four named steps and a fifth we add, and the fork boundary is a
specific step, so the audience can see exactly what is skipped.

Built on **LangGraph** (a `StateGraph`) with **Actae's `ActaeCheckpointSaver`**
as the graph's checkpointer, and **OpenAI `gpt-4o-mini-2024-07-18`** by default.

## The data (real, open, no key)

`source.py` queries the public **arXiv API** (`https://export.arxiv.org/api/query`,
Atom XML) — no account, no key, permissive terms. A topic search returns up to
40 papers; each becomes an addressable chunk `[2401.12345] Title :: abstract`
that the agent filters, cites, and verifies against.

The fetched result is **cached** to `data/arxiv-<topic>.json` (committed), so
the demo and its tests re-run on the *same* inputs offline. `--refresh` re-hits
arXiv; `--topic "…"` points it at anything. A descriptive User-Agent is sent, as
arXiv requests. Default topic: `large language model agents` (40 papers, ~7.5k
words of abstracts).

## The pipeline

```
load_source ──▶ filter_source ──▶ extract_notes ──▶ [fact_check] ──▶ write_report
  (arXiv)          (LLM)             (LLM)           (LLM, added)      (LLM)
```

The expensive part is `filter_source` + `extract_notes` — they read the papers
to the model and read back structured output. That is the prefix a fork
inherits instead of recomputing.

## The story the demo tells

1. **Baseline** — run `load → filter → extract → brief`; print the brief (with
   real findings from real abstracts). It may contain a fabricated citation.
2. **Add a step** — insert a `fact_check` node between `extract_notes` and
   `write_report` (verify each finding against the paper it cites; flag
   unsupported ones).
3. **Fork** — `saver.fork_thread(...)` at the checkpoint *after* `extract_notes`;
   the fork inherits the real papers and findings and runs only
   `fact_check → write_report`. The hallucinated citation is caught and the
   brief is corrected.
4. **Prove it** — a measured token report shows the fork re-issued **zero**
   `filter_source` / `extract_notes` calls; the saved tokens are the prefix.

Two more scenarios round out the story:

- **Durability** (`--scenario crash`) — a worker process runs to the checkpoint
  after `extract_notes` and is hard-killed (`os._exit(137)`, exit code 137). A
  new process resumes the same thread from that checkpoint and writes the brief;
  the prefix is restored from durable state, never recomputed.
- **Replay** (`--scenario replay`) — the durable event history Actae kept for
  the run, cursor by cursor.

And a second fork demo:

- **Change the model and the prompt** (`--scenario variant`) — fork after
  `extract_notes` and re-run **only the tail** with a different model
  (`--fork-model`, default `gpt-4o`) and a different report prompt
  (`--fork-style brief|exec|skeptical`). Same papers, same extracted findings,
  different downstream model/prompt — and the expensive prefix is not re-run.
  This is the "swap any downstream knob without replaying the past" beat:
  changing the model or the prompt is a tail-only change.

## Run it

Everything is explicit — **no hardcoded endpoint or keys**. Export the ones you
need (preferred) or pass the matching flag. The demo talks to a **running Actae
instance**.

```bash
pip install -e ./sdks/python
pip install "langgraph>=0.6,<2" langchain-core openai

# --- config (export) ---
export ACTAE_URL=http://127.0.0.1:8002      # or --url / --port 8002
export ACTAE_API_KEY=sk-...                 # or --api-key
export OPENAI_API_KEY=sk-...                # real model only; omit with --dry-run

# --- start Actae ---
cd actae && cargo run

# --- run the demo ---
python examples/research_agent/demo.py                       # real model
python examples/research_agent/demo.py --dry-run             # no OpenAI key needed
python examples/research_agent/demo.py --topic "retrieval augmented generation" --refresh
python examples/research_agent/demo.py --scenario variant --fork-model gpt-4o --fork-style exec
python examples/research_agent/demo.py --scenario crash      [--dry-run]
python examples/research_agent/demo.py --scenario replay     --run-id <id>
```

### Getting `ACTAE_API_KEY`

The key must be valid for the target instance. On a fresh standalone instance it
is the dev key printed in the banner; on a used or control-plane-enrolled
instance, mint one and export it — or run a clean standalone instance:

```bash
# mint a key against the running managed instance, then export it
cd actae && ./target/debug/actae keys create demo    # prints sk-… once
export ACTAE_API_KEY=sk-...

# or a fully clean, standalone instance (fresh data + config dir; dev key seeded):
ACTAE_DATA_DIR=/tmp/actae-demo/data ACTAE_CONFIG_DIR=/tmp/actae-demo/cfg \
ACTAE_EMBEDDED_PG_PORT=5701 ACTAE_PORT=8111 ./target/debug/actae
export ACTAE_URL=http://127.0.0.1:8111
```

A missing endpoint/key, or one Actae rejects, prints a clear actionable error
(no traceback) rather than crashing. `--model` overrides the OpenAI model id
(default `gpt-4o-mini-2024-07-18`).

## Files

| File | Purpose |
|---|---|
| `demo.py` | The driver: ingest arXiv, run the baseline, fork, print the token report; the `replay` and `crash` scenarios |
| `pipeline.py` | The LangGraph graph, the swappable model (`OpenAIModel` / `ScriptedModel`), the token ledger, the checkpoint helpers |
| `source.py` | The real arXiv client (Atom parse + cache) and the chunker (stdlib only) |
| `worker.py` | Separate process for the durability scenario (runs, pauses at a checkpoint, hard-kills itself) |
| `data/` | Committed arXiv snapshots (reproducible offline runs) |

## How the fork works (the part worth reading)

`ActaeCheckpointSaver` stores every LangGraph super-step as a durable Actae
checkpoint. `demo.py` locates the checkpoint produced by `extract_notes`
(`pipeline.checkpoint_after`, which walks the checkpoint history), then:

```python
await saver.fork_thread(boundary, new_thread_id="research-...-factcheck")
# Resume the ENHANCED graph from the fork's latest checkpoint and tell it the
# last completed step was extract_notes, so it schedules its NEW successor.
await fork_graph.aupdate_state(resume_cfg, {}, as_node="extract_notes")
await fork_graph.ainvoke(None, resume_cfg)   # runs only fact_check -> write_report
```

Two subtleties worth knowing:

- A LangGraph checkpoint records its **next** step. After forking, resuming the
  *modified* graph would replay the baseline's recorded successor (skipping the
  new node), so the demo re-derives the next step with
  `aupdate_state(..., as_node=<last completed step>)` on the fork's own latest
  checkpoint. That is what makes "insert a node and resume" work.
- The fork inherits the exact papers and findings — including the extracted
  claims and their cited arXiv ids — so `fact_check` runs against the real
  prefix, not a re-creation.

## Tests

```bash
python3 -m pytest sdks/example-tests/python/test_research_agent.py -v
# opt-in live fetch: ACTAE_ARXIV_LIVE=1 python3 -m pytest ... -k live
```

Offline (no server, no network): the Atom parser, the committed snapshot +
chunker, the token ledger + report arithmetic, JSON parsing, and the
deterministic scripted model (extractive over the real abstracts, and it catches
its one fabricated citation). The live fork and durability paths are the demo.
