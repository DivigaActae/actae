# Release signing and anti-virus trust

How to make the Actae binaries trusted: macOS Gatekeeper, Windows
SmartScreen/Authenticode, and Linux checksum signatures. Signing is what stops
"unknown developer"/AV false positives and, for Windows, builds SmartScreen
reputation.

**Status:** documented, automatable parts implemented. macOS binaries are
**ad-hoc signed for free** on every release path (`scripts/sign-macos.sh`) —
the paid Apple tier is deliberately not part of this project (§2). Windows
Authenticode is the remaining paid tier: it needs a certificate you obtain and
keep in CI secrets, and the GitHub Actions release workflow
(`.github/workflows/release.yml`) is the right place to run it (it must be
signed on a Windows runner).

---

## 1. What actually causes AV/SmartScreen warnings

- **Unsigned binaries** have no publisher identity, so heuristics and
  reputation systems default to "unknown".
- **Low reputation**: Windows SmartScreen warns until a signed binary
  accumulates download reputation — unless it is signed with an **EV**
  certificate or **Azure Trusted Signing** (immediate reputation).
- **macOS Gatekeeper** blocks any downloaded file that is not signed with a
  **Developer ID** and **notarized** by Apple — but only when the file carries
  the `com.apple.quarantine` xattr (see §2).

Signing does not require obfuscation and is orthogonal to the embedded
PostgreSQL. Sign the **`actae` executable** (inside each archive), not just the
tarball.

---

## 2. macOS

**One tier, and it is free.** Every macOS release binary is ad-hoc signed;
there is no Apple Developer Program membership and none is planned.

| Tier | Cost | What users see | Shipped |
|---|---|---|---|
| **Ad-hoc signature** | free | `curl` / `actae upgrade` installs run untouched; a **browser** download shows the standard one-time Gatekeeper prompt | **yes** |

### 2.1 What actually blocks a Mac user

Gatekeeper is triggered by the `com.apple.quarantine` extended attribute, which
is set by **browsers, Mail and AirDrop** — not by the code signature:

- `curl` / `wget` / `gh release download` and **`actae upgrade`** never set the
  xattr, so those installs run with no prompt, ad-hoc signature or not.
- A **browser** download is quarantined: Gatekeeper lets a notarized
  Developer ID binary through silently, and prompts for or blocks everything
  else.

So the free tier is genuinely free *and* usable — the only friction is the
browser-download path.

### 2.2 What we ship today (free)

Every release signs macOS binaries on exactly one path —
`scripts/sign-macos.sh` — called by `scripts/release-r2.sh` (manual releases,
`release.sh`, `saas/ops/release-and-deploy.sh`) and by the CI release
workflow, always **after** `strip` (strip invalidates any signature):

```bash
scripts/sign-macos.sh actae/target/aarch64-apple-darwin/release/actae
# ==> macos signing: ad-hoc (free, no Apple account)
#     ...
#     Signature=adhoc
```

Ad-hoc guarantees the arm64 binary carries a **valid** code signature (macOS
kills invalidly-signed arm64 executables outright) and costs nothing. It
cannot — and does not try to — silence Gatekeeper for browser downloads.

Verify a shipped binary:

```bash
codesign --verify --strict --verbose=2 ./actae
codesign -dv ./actae          # look for: Signature=adhoc
```

### 2.3 If a browser download gets blocked

User-facing fixes, in order of preference (all free):

1. **Drop the quarantine flag** — works for any downloaded file:

   ```bash
   xattr -dr com.apple.quarantine ./actae
   ```

2. **Right-click → Open** (macOS 15+: System Settings → Privacy & Security →
   Open Anyway).
3. **Reinstall with `curl`** instead of the browser button — `curl` never sets
   the quarantine xattr. The download page publishes the exact one-liner.

### 2.4 Why there is no paid Apple tier

A **Developer ID + notarization** membership (~$99/year) buys exactly one
thing: a browser-downloaded binary that opens on double-click with no prompt.
It adds no integrity (the archive's SHA-256 + the GPG-signed `SHA256SUMS`
already cover that), and it changes nothing for `curl`/`actae upgrade`
installs, which are never quarantined anyway. §2.3 closes the remaining gap
for free, so the paid tier was removed from the release path:
`scripts/sign-macos.sh` is ad-hoc only and no identity/notary environment
variables exist.

Reversing that decision is a ~20-line re-add to `scripts/sign-macos.sh`
(`codesign --sign "Developer ID Application: …"` + `xcrun notarytool`),
gated on a build-host secret.

### 2.5 Optional: Homebrew (zero-friction, still free)

If we later want install-without-prompt **and** without an Apple account, a
Homebrew tap is the standard solution: a **formula** builds from source (the
result is never quarantined) and a **cask** has the quarantine xattr stripped
by Homebrew itself. Publishing a tap is pure repo work — no certificates:

```bash
brew install <org>/tap/actae
```

Until a tap exists, `curl` + `xattr` (§2.3) is the documented path.

---

## 3. Windows (required for SmartScreen trust)

Two options:

| Option | Cost (approx.) | Reputation | Notes |
|---|---|---|---|
| **Azure Trusted Signing** | ~$10/month | Immediate | Cloud signing, no USB token; best fit for CI. |
| **OV code-signing cert** | ~$200–400/year | Builds over time | File or cloud HSM. |
| **EV code-signing cert** | ~$300–600/year | Immediate | Highest trust; USB token/HSM. |

Sign on a Windows runner (the release workflow's `windows-latest` job):

```powershell
signtool sign /fd SHA256 /tr http://timestamp.digicert.com /td SHA256 `
  /f certificate.pfx /p $env:CERT_PASSWORD actae.exe
signtool verify /pa /v actae.exe
```

- **Azure Trusted Signing** (recommended for CI, no token):
  `trusted-signing-cli -e <endpoint> -a <account> -c <profile> -d "Actae" actae.exe`
  (or the `azure/trusted-signing-action` GitHub Action) using an Azure service
  principal secret.
- Set the PFX/token and password as repository **secrets** and reference them
  only in the Windows job. Never commit certificates.

---

## 4. Linux (no OS gate; sign the artifacts)

Linux does not enforce code signing, but distributors and cautious users verify
checksums/signatures. Publish `SHA256SUMS` plus a **detached GPG signature**:

```bash
# one-time: create a release signing key
gpg --quick-generate-key "Actae Releases <hello@actae.dev>" ed25519 sign 2y

# per release (implemented in scripts/release-r2.sh when ACTAE_GPG_KEY is set)
sha256sum actae-*.tar.gz actae-*.zip > SHA256SUMS
gpg --armor --detach-sign --local-user "$ACTAE_GPG_KEY" SHA256SUMS
# publish SHA256SUMS + SHA256SUMS.asc next to the archives
```

Users verify:

```bash
gpg --verify SHA256SUMS.asc SHA256SUMS      # with the published public key
sha256sum --check SHA256SUMS
```

Publish the public key at `https://actae.dev/.well-known/actae-release-signing-key.asc`
and in the repo. (minisign is a lighter alternative; the same detached-signature
model applies.)

---

## 5. What is already built in

- The **paid** `actae upgrade` path already verifies the release archive's
  **SHA-256** against the manifest, and the manifest itself is **Ed25519-signed**
  by the control plane (`scope=upgrade-manifest`) and verified by the binary
  before any download (see `docs/DISTRIBUTION.md`, `control_plane/license.rs`).
- The **community** channel publishes `SHA256SUMS` (from the manifest) at
  `GET /v1/community/SHA256SUMS`, each download response carries `sha256`, and
  the **GPG signature** is served at `GET /v1/community/SHA256SUMS.asc`.
- `scripts/init-release-signing-key.sh` generates the Ed25519 signing key
  (passphrase-less, for batch signing) for `viganotti.exe@gmail.com` and writes
  the public key to
  `saas/frontend/public/.well-known/actae-release-signing-key.asc`
  (served at `https://actae.dev/.well-known/actae-release-signing-key.asc`).
- `scripts/release.sh` builds the embedded dashboard, builds the host's
  platform(s) (macOS + Linux via `cargo-zigbuild`), **GPG-signs** `SHA256SUMS`,
  and uploads the archives + manifest + checksums (+ `.asc`) to R2 — no CI.
- `scripts/release-r2.sh` (called by `release.sh` and by
  `saas/ops/release-and-deploy.sh`) writes `actae/SHA256SUMS` always and
  `actae/SHA256SUMS.asc` when `ACTAE_GPG_KEY` is set, and code-signs macOS
  binaries via `scripts/sign-macos.sh`.
- `scripts/sign-macos.sh` is the single macOS signing path: a **free ad-hoc
  signature**, with no identity/notary configuration to manage. Called by
  `scripts/release-r2.sh` (so `release.sh` and `saas/ops/release-and-deploy.sh`
  inherit it) and by the CI release workflow (always after `strip`, which
  invalidates signatures), and it fails the release if
  `codesign --verify --strict` does not pass.

Verify a release locally:

```bash
curl -fsS https://actae.dev/.well-known/actae-release-signing-key.asc | gpg --import
curl -fsS https://actae.dev/v1/community/SHA256SUMS      -o SHA256SUMS
curl -fsS https://actae.dev/v1/community/SHA256SUMS.asc  -o SHA256SUMS.asc
gpg --verify SHA256SUMS.asc SHA256SUMS
sha256sum --check SHA256SUMS
```


**Back up the private key.** The signing key lives only in the workstation's GPG
keyring; if it is lost, a new key must be published and old signatures become
unverifiable. Export it and store it offline (never commit it):

```bash
gpg --armor --export-secret-keys 14D17393777C6A74CE39EF6EC8D86996F1B7BBAD \
  > actae-release-signing-key.secret.asc   # store securely / offline
```

`scripts/release.sh` refuses to build a release without a production license
public key unless `--allow-dev-key` is given, so a signed release can never
silently embed the dev key.

## 6. Environment / secrets to wire

| Var | Where | Purpose |
|---|---|---|
| `ACTAE_GPG_KEY` | release host / CI | GPG key id/fingerprint for `SHA256SUMS.asc` |
| Windows cert / Azure Trusted Signing creds | CI secrets (Windows job only) | Authenticode signing |

macOS needs **no** signing environment: `scripts/sign-macos.sh` is ad-hoc
only (§2.4 explains why the identity/notary variables were removed).

## 7. Adjacent hardening (not required for AV trust)

- **Reproducible builds** (pinned toolchain, `--locked`) so a third party can
  rebuild and compare hashes.
- **SBOM** (`cargo sbom` / cyclonedx) and `cargo audit` (already run in CI).
- **cosign** signatures on the container images (sigstore keyless) for the
  hosted runtime and fleet gateway.

## 8. Cost summary

- macOS: **free** (ad-hoc signature — what we ship; `curl`/`actae upgrade`
  installs are prompt-free, browser downloads need the one-line `xattr` fix).
  The Apple Developer Program tier is deliberately not bought (§2.4).
- Windows: **~$10/month** (Azure Trusted Signing) or **~$200–600/year** (OV/EV).
- Linux: **free** (GPG/minisign).
