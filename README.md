# Actae

Durable execution for AI agents. Fork a production run at the exact step it went
wrong, change one decision, continue — without re-running the prefix or re-firing
a side effect.

One binary. Embedded PostgreSQL. No account, no card, no phone-home.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/DivigaActae/actae/main/install.sh | sh
# or
brew install divigaactae/tap/actae
# or build from source
git clone https://github.com/DivigaActae/actae.git
cd actae/actae && cargo build --release && ./target/release/actae
```

Open http://localhost:8080. The first screen walks you through recording a run,
killing it, and forking it.

Building from source:

```bash
cargo build --release
./target/release/actae
```

## What it does

- **Record** an agent run as an ordered event log with atomic state.
- **Resume** from the exact cursor after a crash.
- **Fork** at a boundary, change the next decision, and keep the prefix.
- **Protect** side effects with a ledger: claims, leases, fencing tokens,
  per-consumer offsets. A retry reuses the recorded result instead of re-sending.

Framework-neutral: LangGraph, OpenAI Agents SDK, Claude Agent SDK, CrewAI, or a
plain loop.

## Documentation

- Quickstart: https://actae.dev/docs/quickstart
- Concepts: https://actae.dev/docs/concepts
- API: https://actae.dev/docs/api
- Limitations: [docs/limitations.md](docs/limitations.md)
- Security: [SECURITY.md](SECURITY.md)
- Network behavior: [docs/network.md](docs/network.md)

## When not to use Actae

- Runs shorter than a few minutes with no external side effects.
- You already run Temporal/Inngest/Restate and have no need to fork executions.
- You need a workflow DSL. Actae is not a workflow engine.

## Status

Pre-1.0. Single-node community build. No HA, no clustering, no managed control
plane in this repository. Interfaces may change between minor versions; see
[CHANGELOG.md](CHANGELOG.md).

## Security

Report vulnerabilities privately: see [SECURITY.md](SECURITY.md). Do not open a
public issue.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). The name and logo are
covered by [TRADEMARK.md](TRADEMARK.md).
