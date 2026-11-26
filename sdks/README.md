# Actae SDKs

Open-source client SDKs for [Actae](https://actae.dev) — the framework-neutral
layer for forking, debugging, and resuming production AI-agent runs. Actae keeps
your execution loop; these SDKs record durable events, versioned state, and
idempotent tool executions, and let you fork a run at an exact step.

The SDKs and the community runtime are **Apache-2.0**. The managed service is
operated separately. See `LICENSE` and `NOTICE` at the repository root.

| SDK | Package | Public source | Install |
|---|---|---|---|
| Python | `actae-client` | <https://github.com/actae-dev/actae/tree/main/sdks/python> | `pip install actae-client` |
| Go | `github.com/actae-dev/actae/sdks/go` | <https://github.com/actae-dev/actae/sdks/go> | `go get github.com/actae-dev/actae/sdks/go` |
| TypeScript | `@actae/sdk` | <https://github.com/actae-dev/actae/tree/main/sdks/typescript> | `npm install @actae/sdk` |

Each SDK has its own `README`. Framework adapters ship **inside** the SDKs:

- Python extras: `pip install 'actae-client[langgraph]'` (also `langchain`,
  `crewai`, `claude`, `openai-agents`, or `[all]`).
- TypeScript subpaths: `@actae/sdk/adapters/langgraph` (also `langchain`,
  `claude`, `openai`, `codex`, `copilot`, `otel`).

## Repository layout (this monorepo)

- `sdks/python/` — Python SDK source (mirrored to `actae-python`).
- `sdks/typescript/` — TypeScript SDK source (mirrored to `actae-ts`).
- `sdks/go/` — Go SDK source (mirrored to `actae-go`).
- `sdks/example-tests/` — monorepo-only tests that couple the SDKs to repo
  fixtures/examples; kept **outside** the SDK directories so those stay
  verbatim-copyable into their public repos. Run with `scripts/test-sdk-examples.sh`.

## Publishing a release

Public repos are mirrored from the monorepo and tagged by scripts:

```bash
scripts/publish-go-sdk.sh v0.1.0        # mirror + tag the Go module repo
scripts/publish-python-sdk.sh v1.3.0    # mirror + tag actae-python; optional PyPI upload
scripts/publish-ts-sdk.sh v0.2.0        # mirror + tag actae-ts; optional npm publish
scripts/publish-sdks.sh                 # all three
```

Registry uploads need credentials: `npm login` (or `NODE_AUTH_TOKEN`) for npm,
`~/.pypirc` (twine) for PyPI.
