# Changelog

All notable changes are documented here. Format: Keep a Changelog.
Versioning: SemVer, with the caveat that pre-1.0 minor versions may break.

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
