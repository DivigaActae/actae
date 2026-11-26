# Agent Support System · Actae Example

A realistic test application that simulates an AI customer support agent
and exercises all Actae features: record, subscribe, replay, query.

## Architecture

```
simulator.py record() Actae  Postgres
                               
dashboard.py replay()
                               
cli.py replay/query
```

## Quick start

```bash
# Start Actae + Postgres
cd ../.. && docker compose up -d

# Install example deps
cd examples/agent-support-system
pip install -r requirements.txt

# Run one conversation with full detail
python simulator.py single

# Open the real-time dashboard
python dashboard.py
# → http://localhost:9090

# Watch execution history in terminal
python cli.py channels
python cli.py show <channel-id>
python cli.py watch <channel-id>
```

## Simulator modes

```
python simulator.py single           # One conversation, verbose
python simulator.py stream           # Continuous random conversations
python simulator.py load             # N concurrent conversations
```

## Dashboard

Opens at `http://localhost:9090`. Shows:

- Live list of recent agent conversations
- Full execution timeline with event types
- System stats and event type breakdown
- Auto-refreshes every 3 seconds

## CLI debugger

```
python cli.py channels                          # List conversations
python cli.py show <channel>                    # Full timeline
python cli.py replay <channel> [--auto]         # Step through events
python cli.py search --type tool.completed      # Cross-channel search
python cli.py inspect <channel> <cursor>        # Single event detail
python cli.py watch <channel>                   # Live event tail
```

## Load testing

```bash
# Steady state: 50 workers, 2 minutes
python load_test.py --profile=steady

# Spike: 200 workers burst, 1 minute
python load_test.py --profile=spike

# Endurance: 25 workers, 10 minutes
python load_test.py --profile=endurance

# Max throughput: 500 workers
python load_test.py --profile=max

# Manual config
python load_test.py --workers=100 --duration=300 --think-min=0.01 --think-max=0.05
```

## Example output

```
   agent · started                          # Search for order-12345
   tool · search_orders                      # Querying orders API
   tool · search_orders · completed          # Found 1 order, status: shipped
   tool · check_carrier                      # FedEx tracking
   tool · check_carrier · completed          # Delivery June 27th
   message · sent                            # Informed customer
   agent · completed                         # Resolution: informed
```

## What it tests

| Actae feature | How it's tested |
|---|---|
| `POST /api/v1/events/record` | Every conversation step records an event |
| `GET /api/v1/events/replay/{channel}` | Dashboard and CLI replay conversations |
| `POST /api/v1/events/query` | CLI search across all channels |
| `GET /api/v1/events/cursor/{channel}` | Watch mode resumes from last cursor |
| `GET /healthz` | Dashboard health indicator |
| Concurrency | Load test with up to 500 parallel workers |
| Error handling | 3 retries with backoff on record failures |
