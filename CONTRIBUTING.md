# Contributing

Thanks for looking. A few rules that keep this project reviewable by one person.

## Before you write code

Open an issue describing the problem. Large unsolicited PRs are usually closed
with a pointer back to the issue — not because the work is bad, but because the
maintainer cannot review it properly.

## What is welcome

- Bug reports with a minimal reproduction.
- Documentation fixes (typos, wrong commands, missing steps).
- Adapter bugs for LangGraph / OpenAI Agents / Claude SDK.
- Benchmark harness improvements that make results more reproducible.

## What is out of scope

- New SDK languages.
- Workflow-DSL features.
- Multi-tenant or enterprise features.

## Development

```bash
cargo build
cargo test -- --test-threads=1
(cd sdks/go && go test -race ./...)
python3 -m pytest sdks/python/tests/
(cd dashboard && npm ci && npm test)
```

Run the same checks CI runs before opening a PR:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo deny check licenses bans sources
```

## Sign-off (DCO)

Every commit must be signed off:

```
Signed-off-by: Your Name <you@example.com>
```

`git commit -s` does this for you. By signing off you certify the Developer
Certificate of Origin 1.1 (https://developercertificate.org).

## Style

`cargo fmt`, `cargo clippy -- -D warnings`, `gofmt`, `ruff`, `prettier`.
CI enforces all of them.

## Security

Do not open a public issue for a vulnerability. See [SECURITY.md](SECURITY.md).
