# Camera walkthrough — research agent fork demo

A small, teleprompter-style script. Read the **[SAY]** lines out loud; do the
**[SCREEN]** / **[RUN]** actions. Line numbers are hints — menu-search by the
function name if they drift. Target: **6–8 minutes**.

Legend: **[SCREEN]** = what to open / point at · **[SAY]** = the line ·
**[RUN]** = the command to type.

---

## Pre-flight (before you hit record)

```bash
# one terminal, big font; start fresh so the story is reproducible
export ACTAE_URL=http://127.0.0.1:8010
export ACTAE_API_KEY=sk-dev-0000000000000000000000          # from the banner
export OPENAI_API_KEY=sk-...                                # your key
cd actae
ACTAE_DATA_DIR=/tmp/walk/data ACTAE_CONFIG_DIR=/tmp/walk/cfg \
  ACTAE_EMBEDDED_PG_PORT=5710 ACTAE_PORT=8010 ./target/debug/actae
```
Leave that running. Open a second terminal in the repo root, and an editor with
`examples/research_agent/` open. Have `demo.py`, `pipeline.py`, `source.py` tabs
ready.

> If you'd rather not burn tokens on camera, add `--dry-run` to every demo
> command; the story is identical (deterministic model, real arXiv data).

---

## 0. Cold open — the promise (~30s)

**[SAY]** "This is a real research agent. It pulls papers from arXiv, extracts
their findings, and writes a brief. The catch: the expensive part — reading and
filtering all those papers — happens *before* the interesting part. So when I
want to change the final step, I don't want to pay for that again. I'm going to
fork the run and change only the tail. Watch."

---

## 1. The data (real, open) (~40s)

**[SCREEN]** `source.py` → `fetch_papers` (~L99) and `parse_atom` (~L61).

**[SAY]** "The source is the open arXiv API — no key, real papers. One query for
a topic returns forty papers; each becomes an addressable chunk: an id, the
title, the abstract. And it's cached on disk, so the demo and its tests re-run
on the same inputs."

**[SCREEN]** `pipeline.py` → `load_source` (inside `build_graph`, ~L455).

**[SAY]** "`load_source` is the only node that touches the data — everything
after it is the model."

**[RUN]**
```bash
python examples/research_agent/demo.py --dry-run --run-id demo   # or drop --dry-run for real
```
Point at the top: **"40 papers · 7,514 words of abstracts."**

---

## 2. The pipeline (~40s)

**[SCREEN]** `pipeline.py` → `build_graph` (~L427), the four `add_node` calls and
edges near the bottom.

**[SAY]** "Four steps: filter the papers, extract the findings, optionally
fact-check, and write the brief. It's a LangGraph `StateGraph`, and Actae's
`ActaeCheckpointSaver` is the checkpointer — hold that thought, it's the whole
trick."

**[SCREEN]** `ResearchState` (~L62): `chunks → selected → notes → fact_check →
report`.

**[SAY]** "Each step reads the previous step's output from the graph state. That
state is what a fork inherits."

---

## 3. Baseline run (~30s)

**[RUN]** (same command) — let the baseline brief print, scroll through it.

**[SAY]** "Real findings, each citing its arXiv id. And notice it confidently
asserts one thing the source doesn't support. Keep that in mind."

---

## 4. Add a step (~40s)

**[SAY]** "Now I want a fact-checker — verify each finding against the paper it
cites, and flag hallucinated citations. That's a new node."

**[SCREEN]** `pipeline.py` → `fact_check` node (~L517). Point at the system
prompt ("strict fact-checker… which paper supports it").

**[SCREEN]** The `if with_fact_check:` edge block in `build_graph` — show
`extract_notes → fact_check → write_report`.

**[SAY]** "Insert one node. Now — do I re-run the whole pipeline to use it? No."

---

## 5. Fork the run — the flagship (~90s)

**[SCREEN]** `demo.py` → `run_fork` (~L141). Walk the three lines that matter:

1. `checkpoint_after(baseline_graph, baseline_cfg, "extract_notes")` — "find the
   durable checkpoint right after extraction."
2. `saver.fork_thread(boundary, new_thread_id=…)` — "fork the run there into a
   new thread."
3. `await fork_graph.aupdate_state(resume_cfg, {}, as_node="extract_notes")` —
   **[SAY]** "this tells the enhanced graph its new next step is the
   fact-checker — a LangGraph checkpoint remembers its next step, so we
   re-derive it on the fork."

**[RUN]** scroll to the fork output on screen:
- **Fact-check results** — one line is `!! UNSUPPORTED  A single approach now
  dominates the field (0000.00000)`, `evidence: no retrieved paper`.
- **Forked brief (fact-checked)** — ends with a **Corrections:** note removing
  it.

**[SAY]** "The fork inherited the real papers and findings and ran only the tail.
And here's the proof —" *(point at the token report)* "— the fork made **zero**
`filter_source` and `extract_notes` calls. That prefix was never recomputed."

---

## 6. Swap the model and the prompt (~60s)

**[SAY]** "Adding a step is one thing. But you can change *any* downstream knob
the same way."

**[SCREEN]** `pipeline.py` → `REPORT_STYLES` (~L40): `brief`, `exec`,
`skeptical`.

**[SCREEN]** `demo.py` → `run_variant` (~L222) — same fork, but resumes with a
*different model* and a *different `report_style`*.

**[RUN]**
```bash
python examples/research_agent/demo.py --dry-run --scenario variant \
  --fork-model gpt-4o --fork-style exec
```

**[SAY]** "Same papers, same extraction. Different model, different prompt — a
purely tail change. The brief came out as an **executive one-pager**, and the
prefix was skipped again." *(point at the token report: baseline vs fork.)*

---

## 7. Durability — kill it, resume it (~45s)

**[SCREEN]** `worker.py` (~L33) — the `os._exit(137)`.

**[SAY]** "Same run, but the worker dies before it finishes. Actae stored the
filtered papers and findings as a checkpoint."

**[RUN]**
```bash
python examples/research_agent/demo.py --dry-run --scenario crash
```

**[SAY]** "Worker exits 137 — a hard kill, no cleanup. A **new process** resumes
the same thread and writes the brief — and the resume ran only the last step.
The prefix came back from durable state, not from the model."

---

## 8. Replay (~20s)

**[RUN]**
```bash
python examples/research_agent/demo.py --scenario replay --run-id demo
```

**[SAY]** "And Actae kept the whole durable event history — cursor by cursor.
This is the substrate under the graph."

---

## 9. Close (~30s)

**[SAY]** "That's Actae: your agent loop stays yours — LangGraph here — but every
step is a durable, addressable checkpoint. So you can add a step, swap a model,
or rewrite a prompt and re-run only the tail, because the expensive prefix
already happened and it's durable. Fork it, don't rerun it."

Point at: `README.md` and `docs/RESEARCH_AGENT_DEMO.md`.

---

## If something goes wrong on camera

- **Auth error:** your `ACTAE_API_KEY` isn't valid for that instance. It prints
  the fix; or mint one: `cd actae && ./target/debug/actae keys create demo`.
- **Connection refused:** Actae isn't running on `$ACTAE_URL`; check the port.
- **Want zero token cost / no key:** add `--dry-run` to any command.
- **Reset the story:** use a fresh `ACTAE_DATA_DIR` + `ACTAE_CONFIG_DIR` (see
  pre-flight) and a new `--run-id`.

## Talking points to hit (and not over-claim)

- ✅ "The fork re-issued **zero** prefix LLM calls" — it's asserted in code.
- ✅ Token numbers are the provider's own counts.
- ⚠️ The dry-run model is a deterministic extractive stand-in that *injects one
  fabricated citation* to exercise the checker — say so if asked.
- ⚠️ Real models vary; the first run may not hallucinate — that's why the
  checker is the point.
