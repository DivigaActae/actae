# Actae Test Suite

Quick tests to verify your Actae instance is working correctly.

## Setup

```bash
cp .env.example .env
# Edit .env with your API key and endpoint

pip install httpx python-dotenv
```

## Running

```bash
# Basic health + event + state + fork test
python test_actae.py

# Full agent lifecycle test (session, 5 steps, fork, alt path)
python test_agent.py
```

## What's tested

**test_actae.py** — Instance health and core features:
- Health/readiness endpoints
- Record & replay events
- Cursor tracking
- State save/load/versions
- Fork (fork) a channel
- Metrics endpoint

**test_agent.py** — Agent workflow simulation:
- 5-step agent session with state tracking
- Fork at step 3, run alternative steps on fork
- Verify original channel is unaffected
- Cross-channel query
- Metrics update
