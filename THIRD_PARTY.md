# Third-party components

This file is generated at release time by `scripts/generate-notice.sh` and
supplemented by the SPDX SBOM attached to each GitHub release.

## Bundled binaries

| Component | Version | License | Source |
|---|---|---|---|
| PostgreSQL | bundled with the community build | PostgreSQL License | https://www.postgresql.org/about/licence/ |

The embedded PostgreSQL archives are redistributed unmodified. Their license
text is included in the archive and reproduced in the release SBOM.

## Language ecosystems

| Ecosystem | Manifest | How licenses are checked |
|---|---|---|
| Rust | `actae/Cargo.lock` | `cargo deny check licenses` in CI |
| Go | `sdks/go/go.sum` | `go-licenses` at release time |
| Python | `sdks/python/pyproject.toml` | `pip-licenses` at release time |
| JavaScript | `dashboard/package-lock.json`, `sdks/typescript/package-lock.json` | `license-checker` at release time |

## Policy

- No GPL, AGPL, SSPL or Commons-Clause dependency may ship in a binary or a
  published package.
- Every dependency with an attribution requirement is listed in the release
  `NOTICE` and SBOM.
- Adding a dependency with a new license requires a note in the pull request.

If you believe a component is missing or misattributed, open an issue with the
package name and version.
