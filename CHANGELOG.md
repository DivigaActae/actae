# Changelog

All notable changes are documented here. Format: Keep a Changelog.
Versioning: SemVer, with the caveat that pre-1.0 minor versions may break.

## [0.3.0] - 2026-11-26

First public release.

### Added

- Public source release under Apache-2.0.
- `install.sh` with checksum verification and macOS/Linux release artifacts.
- `docs/limitations.md`, `docs/network.md`, `docs/security/threat-model.md`.
- SPDX headers on source files.

### Changed

- The community build no longer contains the control-plane client, enrollment,
  sync loop, subscription enforcement, or the `auth`/`enroll`/`doctor` commands.
  See `docs/control-plane-split.md`.
- Dashboard on Next 15 / React 19.
- Go module path is `github.com/divigaactae/actae/sdks/go`.

### Fixed

- `zip` upgraded to 9.8.2 (critical parser advisories).
- Python SDK: canonical step content now applies the documented
  `<`, `>`, `&`, U+2028/U+2029 escaping (the block was unreachable).

## [Unreleased]

### Added

- Public source release under Apache-2.0.
- `docs/limitations.md` and `docs/network.md`.
- SPDX headers on source files.

### Changed

- Repository URLs point at the public organization.
- The community build no longer contains the control-plane client, enrollment,
  sync loop, subscription enforcement, or the `auth`/`enroll`/`doctor` commands.
  See `docs/control-plane-split.md`.
- Go module path is `github.com/divigaactae/actae/sdks/go`.

### Security

- Removed development TLS material from the tree; certificates are generated
  locally on first run.

## [0.2.3] - 2026-10-21

Previous internal release. See git tags for the full history.
