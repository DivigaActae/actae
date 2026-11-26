# LangGraph Multi-Agent Research Pipeline

Enterprise-grade comparison of **pure LangGraph** vs **Actae-enhanced** agent execution.

## Architecture

```
                   Agent Logic (src/agents.py)
                                          
                                           
    Plain LangGraph                   Actae-enhanced
    (01_plain_langgraph.py)           (02_actae_langgraph.py)
                                      (03_actae_websocket.py)
                                             
                                             
                             
     stdout                            Actae    
     only                              Postgres
                              Events  
                                         WS pub/ 
                                         sub     
                                        
```

## The Pipeline

A 4-agent research system: **Researcher → Analyst → Writer → Critic → (loop)**

| Agent | Responsibility | Output |
|-------|---------------|--------|
| Researcher | Generates 5-7 key findings on a topic | `research_findings` |
| Analyst | Extracts 3-5 strategic insights | `key_insights` |
| Writer | Produces a structured markdown report | `draft` |
| Critic | Scores 1-10, provides revision notes | `review_score`, `revision_notes` |

If `score < 7`, Writer revises (max 2 revisions).

## Project Structure

```
examples/langgraph-comparison/
 01_plain_langgraph.py      # Pure LangGraph (no Actae)
 02_actae_langgraph.py       # Same graph + Actae persistence
 03_actae_websocket.py       # Real-time WS event streaming
 README.md
 .env.example
 src/
     __init__.py
     agents.py              # Agent logic (framework-agnostic, tool-calling loop)
     config.py              # Typed, validated configuration
     logging_setup.py       # Structured logging with correlation IDs
     resilience.py           # Retry, circuit breaker
     telemetry.py           # Metrics, timing, tracing
     tools.py               # Executable tools the researcher can call
     mcp_bridge.py          # MCP servers → tools (optional, via MCP_SERVERS)
     actae_integration.py    # Actae client wrapper + session management
```

## What Actae Adds

| Capability | Plain | Actae |
|-----------|-------|------|
| State persistence | In-memory (lost) | Postgres (survives restart) |
| Dashboard | None | Real-time WebSocket dashboard |
| Replay | Not possible | From any cursor position |
| Forking | Not possible | Fork execution at any step |
| Crash recovery | Start over | Resume from last checkpoint |
| Version tracking | None | Every N steps saved |
| Real-time streaming | None | Pub/sub via WebSocket |
| Audit trail | Print statements | Every event with cursor + timestamp |
| Circuit breaker | None | Actae calls protected |
| Metrics | None | Token usage, latency, throughput |
| **Agent context capture** | None | **Every LLM call's full prompt + response + token usage persisted** |
| **State snapshots** | None | **Full ResearchState (topic, findings, insights, draft, scores) saved after every agent step** |
| **Execution tree** | None | **Fork hierarchy visible via `get_fork_tree`** |
| **Tool calls + results** | `run_tool` return swallowed | **Every tool call and result (builtin + MCP) emitted as `tool.<name>.started` / `.completed` dashboard nodes, stored uncapped** |

## Tool Calling

The researcher agent grounds its findings in current facts by calling real,
executable tools via DeepSeek's OpenAI-compatible function-calling API. The
tool loop (`_call_llm_with_tools` in `src/agents.py`) sends messages with
`tools` attached, executes any requested tools, feeds results back to the
model, and repeats until a final written answer is produced.

| Tool | Purpose | Backend |
|------|---------|---------|
| `web_search` | Search the web for up-to-date info | DuckDuckGo HTML + Google News RSS (no key, with engine fallback) |
| `fetch_page` | Fetch + strip a URL's readable text | `urllib` (stdlib) |
| `calculate` | Safe AST-based arithmetic, no `eval` | `ast` (stdlib) |

`web_search` runs two no-key engines and merges results: DuckDuckGo's HTML
endpoint first (general web results with snippets), then Google News RSS
(dated, attributed headlines — `news.google.com/rss/search`, which serves a
real RSS feed and never captchas). DDG occasionally serves its anti-bot page
to scrapers; when that happens the news results still come through, so the
tool always returns something usable.

Builtin tools live in `src/tools.py`. **Results are stored uncapped** — the
full text a tool returns is recorded as-is, with no truncation. The only
practical bounds are Actae's hard 1 MiB per-event payload limit
(`actae/src/events/handler.rs` `MAX_EVENT_SIZE`) and the HTTP read ceiling for
page fetches (`MAX_FETCH_BYTES = 900_000`); both are documented in code.
The previous 200-character preview caps on agent input context were also
removed — `.started` events and `llm_prompt` in state snapshots now carry the
complete values.

### Seeing tool calls in the dashboard

Every tool invocation is recorded to Actae as an event pair on the pipeline
channel, so you can watch calls happen in real time and click any node to
inspect its arguments/result in the inspector:

```
tool.<name>.started     payload.input  → { name, arguments, tool_call_id }
tool.<name>.completed   payload.output → { name, arguments, result, duration_ms }
```

The researcher **system prompt instructs the model to always call `web_search`
(usually multiple times) before writing findings**, so a run reliably produces
visible tool nodes. The console summary also prints a **Tool Calls** table
listing every recorded tool event with its arguments and full result.

To view in the dashboard: open `http://localhost:8002`, select the channel
(`langgraph-research-<timestamp>`), and flow nodes / the event log will render
tool events distinctly (pink). Because results are uncapped, the Timeline,
Event Log, and Inspector collapse long values by default with a **show more**
toggle — expand to read the entire tool output, no re-fetch.

## MCP Tools

MCP servers expose tools to agents; those calls are captured too. Configure
one or more stdio MCP servers via the `MCP_SERVERS` environment variable and
their tools are merged into the researcher's toolset and recorded as the same
`tool.<name>.started` / `.completed` events:

```bash
export MCP_SERVERS='[{"name":"github","command":"npx",
                      "args":["-y","@modelcontextprotocol/server-github"],
                      "env":{"GITHUB_TOKEN":"ghp_..."}}]'
python 02_actae_langgraph.py "Research topic"
```

Schema of a server entry (`src/config.py` → `MCP_SERVERS`):

| Key | Required | Meaning |
|-----|----------|---------|
| `name` | no | Display name used in logs/events |
| `command` | yes | Binary to launch (default `npx`) |
| `args` | no | Args, e.g. `["-y", "<server-package>"]` |
| `env` | no | Extra environment variables (API keys, tokens) |
| `cwd` | no | Working directory |

Mechanics (`src/mcp_bridge.py`, `MCPTools`):
- Each server is connected over stdio (`mcp` Python SDK), initialized, and
  its tools listed via `list_tools()`; `inputSchema` maps directly to the
  OpenAI/DeepSeek function calling format.
- The researcher runner dispatches through `MCPTools.execute(name, args)`:
  MCP names go to the owning server session, everything else falls back to the
  builtin `run_tool`. So `web_search` keeps working when MCP is configured.
- Text results are returned in full. Binary content blocks
  (image/audio/embedding/resource) can't be inlined — a single Actae event or
  state snapshot is capped at 1 MiB — so they're recorded as a compact JSON
  note (`{type, mime_type, size_bytes}`) instead of the raw bytes.
- If the `mcp` Python package isn't installed, MCP is silently disabled.

## Full Agent Context Capture

Every agent step in `02_actae_langgraph.py` records to Actae:

| What | How | Where it lives |
|------|-----|----------------|
| **Input state** (what the agent received) | `trace_step(input=...)` → `{agent}.started` event | Actae event payload |
| **Output state** (what the agent produced) | `trace_step(output=...)` → `{agent}.completed` event | Actae event payload |
| **LLM system prompt** (truncated) | `llm_prompt.system` in the agent's return dict | State snapshot |
| **LLM user message** (truncated) | `llm_prompt.user` in the agent's return dict | State snapshot |
| **LLM response** | Agent's output fields (`research_findings`, `key_insights`, `draft`) | State snapshot |
| **Token usage** | `llm_response.prompt_tokens`, `llm_response.completion_tokens` | State snapshot |
| **Tool calls** | `tool.<name>.started`/`.completed` events + `llm_response.tool_calls` (name, args, result, duration) | Actae events + state snapshot |
| **Full ResearchState** | `save_state()` called after every agent node | State snapshot (versioned) |
| **Latency** | `elapsed_ms` in `.completed` event | Actae event metadata |

The pipeline prints a **State Snapshots** table at the end showing every saved version
with score, revision count, and content lengths.

### Viewing the full agent context after a run

```bash
# Load the latest state snapshot (full research state with all agent outputs)
python -c "
import asyncio
from actae_client import ActaeClient

async def main():
    async with ActaeClient(
        api_key='sk-dev-0000000000000000000000',
        endpoint='http://localhost:8002',
        ws_endpoint='ws://localhost:8002/ws'
    ) as c:
        state = await c.latest_state('langgraph-research-<CHANNEL_ID>')
        if state:
            d = state['state']
            print(f'score: {d.get(\"review_score\")}/10')
            print(f'revisions: {d.get(\"revision_count\")}')
            print(f'findings ({len(d.get(\"research_findings\",\"\"))} chars)')
            print(f'draft ({len(d.get(\"draft\",\"\"))} chars)')
            print(f'llm_prompt: {d.get(\"llm_prompt\")}')
            print(f'llm_response: {d.get(\"llm_response\")}')

asyncio.run(main())
"

# List all state versions
python -c "
import asyncio
from actae_client import ActaeClient

async def main():
    async with ActaeClient(...) as c:
        for v in await c.list_states('langgraph-research-<CHANNEL_ID>'):
            print(f'v{v[\"version\"]} cursor={v[\"cursor\"]}')
asyncio.run(main())
"
```

## Running

### 1. Start Actae (dev mode)

```bash
cd actae && cargo run -- --dev
# Actae running at http://localhost:8002
```

### 2. Set environment variables

```bash
export DEEPSEEK_API_KEY=sk-...
# Optional overrides:
export ACTAE_URL=http://localhost:8002
export LOG_LEVEL=INFO
export LOG_FORMAT=json          # JSON for production, console for dev
export PIPELINE_TOPIC="Your custom research topic"
```

### 3. Run the pipelines

```bash
cd examples/langgraph-comparison

# Plain version (no Actae)
python 01_plain_langgraph.py "Why Rust is the future of systems programming"

# Actae version (same pipeline + dashboard)
python 02_actae_langgraph.py "Why Rust is the future of systems programming"

# WebSocket demo (real-time event streaming)
python 03_actae_websocket.py
```

### 4. View results

```bash
# Open the Actae dashboard
open http://localhost:8002

# Replay events from a specific channel
python -c "
import asyncio
from actae_client import ActaeClient
async def main():
    async with ActaeClient(api_key='sk-dev-0000000000000000000000', endpoint='http://localhost:8002', ws_endpoint='ws://localhost:8002/ws') as c:
        evts = await c.replay('langgraph-research-1234567890', cursor=0)
        for e in evts:
            print(f'[{e.cursor}] {e.event_type}')
asyncio.run(main())
"
```

## Enterprise Features

| Feature | Implementation |
|---------|---------------|
| Structured config | `src/config.py` — typed dataclasses, env-based, validated |
| Logging | `src/logging_setup.py` — correlation IDs, JSON output |
| Resilience | `src/resilience.py` — retry with backoff, circuit breaker |
| Telemetry | `src/telemetry.py` — span trees, timing, execution tracing |
| Agent isolation | `src/agents.py` — framework-agnostic, pure functions |
| Actae wrapper | `src/actae_integration.py` — retry, structured events, channels |
| LLM observability | `src/agents.py` `LLMResult` — captures prompt, response, tokens, duration |
| State snapshots | `AgentSession.save_state()` — versioned full-state persists after every step |
| Fork / execution tree | `02_actae_langgraph.py` forks a fork at midpoint and displays the tree |

## Viewing the Agent's "Thoughts" (LLM Context)

The `LLMResult` dataclass in `agents.py` captures the full prompt/response context
of every DeepSeek call. Each agent function includes `llm_prompt` (truncated system +
user messages) and `llm_response` (model, tokens, duration) in its return value.

These are persisted in two ways:
1. **In Actae events**: The `input` field of `.started` events shows the state preview
   (topic, current_agent, truncated content fields).
2. **In state snapshots**: Every agent step saves the full `ResearchState` including
   `llm_prompt` and `llm_response` keys — so the complete LLM context (what was asked,
   what was returned, how many tokens used) survives restarts.

```bash
# After a run, inspect the state snapshot for LLM context:
python -c "
import asyncio
from actae_client import ActaeClient
async def main():
    async with ActaeClient(api_key='sk-dev-...', endpoint='http://localhost:8002') as c:
        state = await c.latest_state('langgraph-research-<ID>')
        s = state['state']
        print('Researcher prompt:', s.get('llm_prompt'))
        print('Researcher tokens:', s.get('llm_response'))
asyncio.run(main())
"
```

## Key Design Decision

**Actae doesn't change agent logic.** The same `src/agents.py` functions
power both pipelines. Actae adds an observability layer around the graph
execution — it records events, manages state, and provides real-time
visibility without modifying the agent code.

Think of it as:
- **LangGraph**: The execution engine (state machine, routing)
- **Actae**: The persistence + observability layer (events, state, streaming)
