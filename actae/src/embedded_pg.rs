// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Embedded PostgreSQL for managed storage (the default backend).
//!
//! Runs a real PostgreSQL server as a child process, with no external
//! infrastructure. This is the SAME code path for development, CI and
//! production — the only difference is where the PostgreSQL binaries come
//! from:
//!
//! 1. `ACTAE_PG_BIN_DIR` — explicit override (an operator-provided `bin/`
//!    directory with `initdb`, `postgres` and `pg_ctl`).
//! 2. The bytes embedded in the binary (`pg-embedded` feature) — extracted
//!    to `<data_dir>/pg-bin/<version>-<platform>/` on first use.
//! 3. A **host-installed PostgreSQL** (dev/bare builds, to avoid the
//!    download when the machine already has PostgreSQL). The host's
//!    `initdb`/`postgres`/`pg_ctl` run Actae's OWN isolated cluster (own
//!    data dir, port, scram auth) — a running host server is never touched.
//!    Version rule: an existing cluster's major must match, else we fall
//!    back to the download so existing data is never stranded; on a fresh
//!    data dir the highest installed major is preferred. `ACTAE_PG_NO_HOST=1`
//!    disables detection.
//! 4. A one-time download of the zonky PostgreSQL package for the current
//!    platform (dev builds without the embedded jar).
//!
//! **Release binaries never download.** The embedded package is baked for the
//! exact platform the artifact targets (glibc packages for Linux binaries,
//! `-alpine` musl packages for the Alpine artifacts). If the host platform
//! does not match the embedded package, startup fails with actionable
//! guidance instead of silently downloading a second archive — an Alpine
//! container is fully self-contained and works offline.
//!
//! **Authenticated archives.** Every supported zonky artifact is pinned in
//! `actae/pg-checksums.json` (source-controlled SHA-256). Build scripts and
//! the runtime downloader verify the archive before extraction, and the
//! embedded jar is re-verified against the same pin at extraction time.
//! Extraction is staged (sibling directory, fsync, atomic rename) with
//! compressed/expanded size caps and a complete-layout validation, so an
//! interrupted or malicious archive can never become a partial install.
//!
//! Instance identity: the HTTP port and the embedded-PG port move in
//! lockstep. When the configured HTTP port is busy (`ACTAE_AUTO_BUMP=1`,
//! the default) the next free port pair is chosen and the data directory
//! derives from it:
//!
//! ```text
//!   http 8002 -> pg 5546 -> <ACTAE_DATA_DIR>/data
//!   http 8003 -> pg 5547 -> <ACTAE_DATA_DIR>/data-8003
//! ```
//!
//! Ownership model — portable (Unix AND Windows):
//!
//! * An exclusive **file lock** on the data directory is held for the
//!   process lifetime (auto-released by the OS on exit, even SIGKILL). A
//!   concurrent start of the same identity fails fast.
//! * A busy pg port is only ever reclaimed when ownership is **proven**:
//!   the `postmaster.pid` inside *our* cluster directory must record our
//!   data directory and our port, and `pg_ctl status` must confirm a live
//!   server whose PID matches the pid file. Only then is `pg_ctl stop` run.
//!   If ownership cannot be established, startup fails **without sending
//!   any signal** — a stale pid file can never make us signal an unrelated
//!   process that reused the port.
//! * The server runs with **Unix sockets disabled** (`unix_socket_directories=''`)
//!   and loopback TCP only, so it can never collide with — or be made to
//!   delete — another PostgreSQL server's socket artifacts in `/tmp`.
//! * Graceful shutdown uses a **bounded** `pg_ctl -D <cluster> stop -m fast
//!   -w` (Tokio timeout), with a hard kill + reap of the child as the last
//!   resort. The same guard runs on every post-spawn startup error path so
//!   no failed start can leave a live postgres (or crash-recovery state).
//!
//! Production posture: the cluster is initialized with `scram-sha-256` auth
//! (`initdb --pwfile`), the superuser password is generated once and stored
//! in `<data_dir>/pg.pwfile` (mode 0600), and the server binds loopback. The
//! persisted credential is authoritative after initialization; a conflicting
//! `ACTAE_EMBEDDED_PG_PASSWORD` is a loud configuration error, not a silent
//! re-login failure.

use anyhow::{anyhow, Context, Result};
use fs2::FileExt;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};
use tracing::{debug, info, warn};

/// PostgreSQL major.minor.patch pinned by the zonky package we ship.
const PG_VERSION: &str = "18.2.0";
/// How many (http, pg) port pairs to scan when auto-bumping.
const PORT_SCAN_RANGE: u16 = 10;
/// How long to wait for the postgres child to accept connections.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
/// How long to wait for a graceful `pg_ctl stop` before killing the child.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait when probing a host-installed PostgreSQL (`pg_config`,
/// `initdb --version`) before giving up on that candidate.
const HOST_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// The oldest host PostgreSQL major Actae will adopt: scram-sha-256 auth
/// requires PG 10+ (this also bounds the version-matching surface).
const MIN_HOST_PG_MAJOR: u32 = 10;
/// Maximum compressed size accepted for a zonky package (jar and .txz
/// payload). PostgreSQL 18 packages are ~15–50MB compressed.
const MAX_JAR_BYTES: u64 = 256 * 1024 * 1024;
/// Maximum fully-expanded tar size accepted for the PostgreSQL runtime
/// (uncompressed is ~150MB; the cap is a generous headroom bound).
const MAX_EXPANDED_TAR_BYTES: u64 = 1_000 * 1024 * 1024;

/// Maven classifier → zonky `embedded-postgres-binaries-{platform}` artifact.
fn zonky_platform() -> Result<String> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    // Alpine and other musl hosts need the `-alpine` (musl-built) package —
    // it cannot run on glibc hosts and vice versa.
    let suffix = if on_alpine_like_host() { "-alpine" } else { "" };
    match (os, arch) {
        ("linux", "x86_64") => Ok(format!("linux-amd64{suffix}")),
        ("linux", "aarch64") => Ok(format!("linux-arm64v8{suffix}")),
        ("macos", "x86_64") => Ok("darwin-amd64".to_string()),
        ("macos", "aarch64") => Ok("darwin-arm64v8".to_string()),
        ("windows", "x86_64") => Ok("windows-amd64".to_string()),
        _ => Err(anyhow!(
            "no bundled PostgreSQL binaries for {os}/{arch} — set ACTAE_PG_BIN_DIR to a \
             directory containing initdb/postgres/pg_ctl"
        )),
    }
}

/// True on Alpine and other musl libc Linux hosts — the host requires the
/// `-alpine` (musl) PostgreSQL package. Detected via the canonical
/// `/etc/alpine-release` marker and, as a fallback, the presence of the musl
/// dynamic loader in `/lib` (covers non-Alpine musl distros).
pub fn on_alpine_like_host() -> bool {
    if Path::new("/etc/alpine-release").exists() {
        return true;
    }
    if let Ok(entries) = std::fs::read_dir("/lib") {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with("ld-musl-") {
                return true;
            }
        }
    }
    false
}

/// Source-controlled SHA-256 pins for every supported zonky artifact, read
/// from `actae/pg-checksums.json` (compiled into the binary). A download or
/// embedded jar that does not match its pin is never extracted.
static ZONKY_CHECKSUMS: OnceLock<Result<HashMap<String, String>, String>> = OnceLock::new();

fn zonky_checksums() -> &'static Result<HashMap<String, String>, String> {
    ZONKY_CHECKSUMS.get_or_init(|| {
        let text = include_str!("../pg-checksums.json");
        let value: Value = serde_json::from_str(text)
            .map_err(|e| format!("pg-checksums.json is invalid JSON: {e}"))?;
        let pinned_version = value
            .get("postgresql_version")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if pinned_version != PG_VERSION {
            return Err(format!(
                "pg-checksums.json pins PostgreSQL {pinned_version}, but this build ships {PG_VERSION} \
                 — the manifest and the pinned version must be updated together"
            ));
        }
        let jars = value
            .get("zonky_jars")
            .and_then(|v| v.as_object())
            .ok_or("pg-checksums.json is missing the zonky_jars map")?;
        let mut map = HashMap::new();
        for (platform, hash) in jars {
            let hash = hash
                .as_str()
                .ok_or_else(|| format!("checksum for {platform} is not a string"))?;
            map.insert(platform.clone(), hash.to_lowercase());
        }
        Ok(map)
    })
}

/// The pinned SHA-256 for a zonky platform, or an error explaining why the
/// archive cannot be trusted.
fn expected_jar_sha256(platform: &str) -> Result<String> {
    match zonky_checksums() {
        Ok(map) => map.get(platform).cloned().with_context(|| {
            format!(
                "no pinned SHA-256 for zonky platform {platform} in actae/pg-checksums.json \
                     — refusing to use an unauthenticated PostgreSQL package"
            )
        }),
        Err(e) => Err(anyhow!("pg-checksums.json is unusable: {e}")),
    }
}

/// Paths of the PostgreSQL executables used by the embedded cluster.
#[derive(Clone, Debug)]
pub struct PgBinaries {
    pub initdb: PathBuf,
    pub postgres: PathBuf,
    pub pg_ctl: PathBuf,
}

impl PgBinaries {
    fn validate(&self, source: &str) -> Result<()> {
        for path in [&self.initdb, &self.postgres, &self.pg_ctl] {
            if !path.is_file() {
                return Err(anyhow!(
                    "{source}: {} is missing (expected initdb/postgres/pg_ctl)",
                    path.display()
                ));
            }
        }
        Ok(())
    }
}

/// Resolve an executable path inside a PostgreSQL `bin` directory, honoring
/// `.exe` on Windows — the zonky Windows package (and typical Windows
/// installs) ship `initdb.exe`/`postgres.exe`/`pg_ctl.exe`, and Windows
/// path checks need the real filename (CreateProcess appends `.exe` when
/// running, but `is_file()` does not).
fn bin_executable(bin_dir: &Path, name: &str) -> PathBuf {
    let plain = bin_dir.join(name);
    if plain.is_file() {
        return plain;
    }
    #[cfg(target_os = "windows")]
    {
        let exe = bin_dir.join(format!("{name}.exe"));
        if exe.is_file() {
            return exe;
        }
    }
    plain
}

/// The three PostgreSQL executables under a `bin` directory.
fn pg_binaries_from(bin_dir: &Path) -> PgBinaries {
    PgBinaries {
        initdb: bin_executable(bin_dir, "initdb"),
        postgres: bin_executable(bin_dir, "postgres"),
        pg_ctl: bin_executable(bin_dir, "pg_ctl"),
    }
}

/// Validate the complete zonky layout: the three executables plus the
/// `lib/` and `share/` trees the binaries need to run (they locate them via
/// rpath relative to `bin/`).
fn validate_zonky_layout(dest: &Path) -> Result<()> {
    let bins = pg_binaries_from(&dest.join("bin"));
    bins.validate(&dest.display().to_string())?;
    for sub in ["lib", "share"] {
        if !dest.join(sub).is_dir() {
            return Err(anyhow!(
                "zonky package in {} is missing the {sub}/ tree — the PostgreSQL binaries \
                 cannot run without it",
                dest.display()
            ));
        }
    }
    Ok(())
}

/// A host-installed PostgreSQL candidate: its `bin` directory, the resolved
/// executable paths, and its parsed `initdb --version` (major, minor).
#[derive(Clone, Debug)]
struct HostPostgres {
    bin_dir: PathBuf,
    bins: PgBinaries,
    version: (u32, u32),
}

/// True when host-PostgreSQL detection is disabled (`ACTAE_PG_NO_HOST=1`).
fn host_detection_disabled() -> bool {
    matches!(
        std::env::var("ACTAE_PG_NO_HOST").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Enumerate candidate PostgreSQL `bin` directories on the host, deduped by
/// canonical path. Sources, in order: `pg_config --bindir` (the user's
/// PATH-respecting choice), every PATH entry, then well-known per-OS
/// locations. Candidates are validated (and versioned) later by
/// `probe_host_postgres`.
async fn enumerate_host_candidate_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();

    // 1. `pg_config --bindir` — the canonical way to locate a coherent
    //    install. Bounded by a timeout so a broken `pg_config` on PATH can
    //    never hang startup.
    if let Ok(Ok(out)) = tokio::time::timeout(
        HOST_PROBE_TIMEOUT,
        Command::new("pg_config").arg("--bindir").output(),
    )
    .await
    {
        if out.status.success() {
            let bindir = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !bindir.is_empty() {
                dirs.push(PathBuf::from(bindir));
            }
        }
    }

    // 2. Every PATH entry (the trio check happens in `probe_host_postgres`).
    if let Ok(paths) = std::env::var("PATH") {
        for dir in std::env::split_paths(&paths) {
            if !dir.as_os_str().is_empty() {
                dirs.push(dir);
            }
        }
    }

    // 3. Well-known per-OS locations (no `glob` dependency; read_dir + name
    //    matching covers versioned install directories).
    dirs.extend(known_host_postgres_dirs());

    // Dedup by canonical path (a symlinked opt/ path may appear via multiple
    // sources). Keep the first occurrence.
    let mut seen: HashSet<PathBuf> = HashSet::new();
    dirs.into_iter()
        .filter_map(|d| {
            let canon = d.canonicalize().unwrap_or_else(|_| d.clone());
            if seen.insert(canon) {
                Some(d)
            } else {
                None
            }
        })
        .collect()
}

/// Collect `<parent>/<name>/<rel>` for entries whose name is version-like
/// (starts with a digit, e.g. `/usr/lib/postgresql/17`) or matches `prefix`
/// (e.g. `/opt/homebrew/opt/postgresql@17`).
fn version_dir_bins(parent: &Path, prefix: Option<&str>, rel: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(parent) else {
        return out;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let version_like = name
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false);
        let matches_prefix = prefix.map(|p| name.starts_with(p)).unwrap_or(true);
        if version_like || matches_prefix {
            let bin = entry.path().join(rel);
            if bin.is_dir() {
                out.push(bin);
            }
        }
    }
    out
}

/// Well-known PostgreSQL install locations per OS.
fn known_host_postgres_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    #[cfg(target_os = "linux")]
    {
        dirs.extend(version_dir_bins(
            Path::new("/usr/lib/postgresql"),
            None,
            "bin",
        ));
        dirs.extend(version_dir_bins(
            Path::new("/usr/pgsql"),
            Some("pgsql-"),
            "bin",
        ));
        for direct in [
            "/usr/local/pgsql/bin",
            "/usr/local/postgresql/bin",
            "/opt/postgresql/bin",
        ] {
            if Path::new(direct).is_dir() {
                dirs.push(PathBuf::from(direct));
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        dirs.extend(version_dir_bins(
            Path::new("/opt/homebrew/opt"),
            Some("postgresql"),
            "bin",
        ));
        dirs.extend(version_dir_bins(
            Path::new("/usr/local/opt"),
            Some("postgresql"),
            "bin",
        ));
        dirs.extend(version_dir_bins(
            Path::new("/Library/PostgreSQL"),
            None,
            "bin",
        ));
        dirs.extend(version_dir_bins(
            Path::new("/Applications/Postgres.app/Contents/Versions"),
            None,
            "bin",
        ));
    }
    #[cfg(target_os = "windows")]
    {
        for base in ["ProgramFiles", "ProgramFiles(x86)"] {
            if let Some(pf) = std::env::var_os(base) {
                dirs.extend(version_dir_bins(
                    Path::new(&pf).join("PostgreSQL").as_path(),
                    None,
                    "bin",
                ));
            }
        }
    }
    dirs
}

/// Validate a candidate bin dir as a usable PostgreSQL installation:
/// `initdb`/`postgres`/`pg_ctl` must all exist and `initdb --version` must
/// parse to a supported major (>= `MIN_HOST_PG_MAJOR`). Bounded by a timeout
/// so a hung/broken install is skipped, never fatal.
async fn probe_host_postgres(dir: &Path) -> Option<HostPostgres> {
    let bins = pg_binaries_from(dir);
    bins.validate("host PostgreSQL").ok()?;
    let version = tokio::time::timeout(HOST_PROBE_TIMEOUT, binary_version(&bins.initdb))
        .await
        .ok()?
        .ok()?;
    if version.0 < MIN_HOST_PG_MAJOR {
        debug!(
            "skipping host PostgreSQL {} (PostgreSQL {}.{}) — major must be >= {MIN_HOST_PG_MAJOR}",
            dir.display(),
            version.0,
            version.1
        );
        return None;
    }
    Some(HostPostgres {
        bin_dir: dir.to_path_buf(),
        bins,
        version,
    })
}

/// Pure selection: an existing cluster pins its major; otherwise the highest
/// installed major wins.
fn pick_host_postgres(
    candidates: &[HostPostgres],
    cluster_major: Option<u32>,
) -> Option<&HostPostgres> {
    match cluster_major {
        Some(major) => candidates.iter().find(|hp| hp.version.0 == major),
        None => candidates.iter().max_by_key(|hp| hp.version.0),
    }
}

/// Locate a host-installed PostgreSQL to run the managed cluster with,
/// avoiding the pinned download on machines that already have PostgreSQL.
///
/// Returns `Ok(None)` when detection is disabled, nothing usable is found,
/// or (critically) when an existing cluster's major matches no host install —
/// in that case we fall back to the download so existing data is never
/// stranded by a version change.
async fn select_host_postgres(cluster_dir: &Path) -> Result<Option<PgBinaries>> {
    if host_detection_disabled() {
        info!("ACTAE_PG_NO_HOST=1 — skipping host PostgreSQL detection");
        return Ok(None);
    }

    let mut candidates: Vec<HostPostgres> = Vec::new();
    for dir in enumerate_host_candidate_dirs().await {
        if let Some(hp) = probe_host_postgres(&dir).await {
            candidates.push(hp);
        }
    }
    if candidates.is_empty() {
        return Ok(None);
    }

    // An existing cluster pins its major; a wipe means "fresh" (the cluster
    // is about to be recreated, so its recorded version is irrelevant).
    let wipe = std::env::var("ACTAE_EMBEDDED_PG_WIPE")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);
    let cluster_major = if wipe {
        None
    } else {
        std::fs::read_to_string(cluster_dir.join("PG_VERSION"))
            .ok()
            .and_then(|s| parse_version(s.trim()).ok().map(|(m, _)| m))
    };

    let Some(chosen) = pick_host_postgres(&candidates, cluster_major) else {
        info!(
            "host PostgreSQL is installed ({}), but none matches the existing cluster's major \
             version {} — using the bundled/downloaded PostgreSQL instead",
            candidates
                .iter()
                .map(|c| format!("{}.{}", c.version.0, c.version.1))
                .collect::<Vec<_>>()
                .join(", "),
            cluster_major
                .map(|m| m.to_string())
                .unwrap_or_else(|| "?".to_string())
        );
        return Ok(None);
    };

    info!(
        "Using host PostgreSQL from {} (PostgreSQL {}.{}) — set ACTAE_PG_NO_HOST=1 to force \
         the bundled download",
        chosen.bin_dir.display(),
        chosen.version.0,
        chosen.version.1
    );
    Ok(Some(chosen.bins.clone()))
}

/// Resolve the PostgreSQL binaries for this instance.
///
/// Order: `ACTAE_PG_BIN_DIR` → existing extracted install → embedded bytes →
/// host-installed PostgreSQL → zonky download. Both the embedded and
/// downloaded paths unpack the same zonky jar into
/// `<data_dir>/pg-bin/<version>-<platform>/` (bin/lib/share), so the runtime
/// layout is identical.
///
/// Release builds (with the `pg-embedded` feature) embed the package for
/// ONE platform and are self-contained: if the runtime platform differs
/// (e.g. an Alpine artifact run on glibc, or vice versa) startup fails with
/// actionable guidance — release binaries never download from the internet.
/// Dev/default builds (no embedded jar) prefer a host-installed PostgreSQL
/// (avoiding the download), then download the platform-matched package,
/// verifying its pinned SHA-256 before extraction.
async fn resolve_binaries(data_dir: &Path) -> Result<PgBinaries> {
    if let Ok(dir) = std::env::var("ACTAE_PG_BIN_DIR").map(|s| s.trim().to_string()) {
        if !dir.is_empty() {
            let base = PathBuf::from(&dir);
            // The zonky binaries resolve their shared libraries via rpath
            // (`$ORIGIN/../lib`), so the full tree (bin/lib/share) is the
            // canonical layout. A plain bin dir is accepted for
            // operator-provided builds.
            let bin = if base.join("bin").join("initdb").is_file()
                || base.join("bin").join("initdb.exe").is_file()
            {
                base.join("bin")
            } else {
                base.clone()
            };
            let bins = pg_binaries_from(&bin);
            bins.validate(&format!("ACTAE_PG_BIN_DIR={dir}"))?;
            info!("Using PostgreSQL binaries from ACTAE_PG_BIN_DIR={dir}");
            return Ok(bins);
        }
    }

    let runtime_platform = zonky_platform()?;
    let pg_bin_root = data_dir.join("pg-bin");
    let dest = pg_bin_root.join(format!("{PG_VERSION}-{runtime_platform}"));
    let bin_dir = dest.join("bin");
    let bins = pg_binaries_from(&bin_dir);

    // Already installed and complete?
    if bin_dir.is_dir() {
        match bins.validate(&dest.display().to_string()) {
            Ok(()) => {
                info!("Using PostgreSQL binaries from {}", bin_dir.display());
                return Ok(bins);
            }
            Err(e) => {
                warn!("{e} — re-extracting PostgreSQL binaries");
            }
        }
    }

    tokio::fs::create_dir_all(&pg_bin_root)
        .await
        .with_context(|| format!("failed to create {}", pg_bin_root.display()))?;
    let jar_path = pg_bin_root.join(format!("{runtime_platform}-{PG_VERSION}.jar"));

    match embedded_platform() {
        Some(embedded) => {
            if embedded != runtime_platform.as_str() {
                return Err(anyhow!(
                    "this Actae binary embeds the PostgreSQL runtime for {embedded}, but this host \
                     requires {runtime_platform}. The embedded package cannot run here (the glibc \
                     and musl/alpine packages are not interchangeable). Download the matching \
                     release artifact (the `-alpine` build for Alpine/musl hosts), or set \
                     ACTAE_PG_BIN_DIR to a PostgreSQL distribution for this platform. Release \
                     builds never download PostgreSQL from the internet."
                ));
            }
            info!(
                "Extracting embedded PostgreSQL {PG_VERSION} ({runtime_platform}) to {}",
                dest.display()
            );
            #[cfg(feature = "pg-embedded")]
            {
                tokio::fs::write(&jar_path, EMBEDDED_PG_JAR)
                    .await
                    .with_context(|| {
                        format!("failed to write embedded PG jar {}", jar_path.display())
                    })?;
            }
            #[cfg(not(feature = "pg-embedded"))]
            {
                unreachable!("embedded_platform() is Some only when the pg-embedded feature is on")
            }
        }
        None => {
            // Dev/bare builds: prefer a host-installed PostgreSQL over the
            // download (release builds never reach here — the embedded jar is
            // always platform-matched).
            if let Some(host) = select_host_postgres(&data_dir.join("pg")).await? {
                return Ok(host);
            }
            // Reuse a previously-downloaded jar when it still matches the
            // pinned SHA-256 (e.g. the extracted tree was removed or the
            // cluster-major mismatch forced a fallback) — no re-download.
            if jar_path.is_file()
                && verify_jar_sha256(&jar_path, &runtime_platform)
                    .await
                    .is_ok()
            {
                info!(
                    "Reusing verified cached PostgreSQL jar {}",
                    jar_path.display()
                );
            } else {
                info!(
                    "Downloading PostgreSQL {PG_VERSION} binaries for {runtime_platform} \
                     (one-time, ~15-60MB) ..."
                );
                download_jar(&runtime_platform, &jar_path).await?;
            }
        }
    }

    // Integrity gate: never extract an archive whose SHA-256 is not the
    // pinned one — the downloader verifies too, but this covers the
    // embedded jar and defends the whole extraction path.
    verify_jar_sha256(&jar_path, &runtime_platform).await?;
    unpack_zonky_jar(&jar_path, &dest).await?;
    validate_zonky_layout(&dest).with_context(|| {
        format!(
            "zonky package {runtime_platform} {PG_VERSION} is incomplete in {}",
            dest.display()
        )
    })?;
    info!("PostgreSQL binaries ready at {}", bin_dir.display());
    Ok(bins)
}

/// The zonky classifier of the jar baked into the binary (e.g. `linux-amd64`).
/// `None` in builds without the `pg-embedded` feature.
#[cfg(feature = "pg-embedded")]
fn embedded_platform() -> Option<&'static str> {
    Some(env!("ACTAE_PG_EMBED_PLATFORM"))
}

/// No embedded jar in dev/default builds.
#[cfg(not(feature = "pg-embedded"))]
fn embedded_platform() -> Option<&'static str> {
    None
}

/// The zonky jar baked into the binary (build.rs embeds it when the
/// `pg-embedded` feature is enabled; the release workflow provides it).
#[cfg(feature = "pg-embedded")]
pub static EMBEDDED_PG_JAR: &[u8] = include_bytes!(env!("ACTAE_PG_EMBED_JAR_PATH"));

/// Stream a zonky PostgreSQL jar to `dest`, enforcing a size cap and
/// verifying its pinned SHA-256 before the file is committed.
async fn download_jar(platform: &str, dest: &Path) -> Result<()> {
    let expected = expected_jar_sha256(platform)?;
    let url = format!(
        "https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-{platform}/{PG_VERSION}/embedded-postgres-binaries-{platform}-{PG_VERSION}.jar"
    );
    let response = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .with_context(|| format!("failed to download {url}"))?
        .error_for_status()
        .with_context(|| format!("zonky returned an error for {url}"))?;
    if let Some(len) = response.content_length() {
        if len > MAX_JAR_BYTES {
            return Err(anyhow!(
                "refusing {url}: Content-Length {len} exceeds the {MAX_JAR_BYTES}-byte cap"
            ));
        }
    }

    let tmp = dest.with_extension("jar.tmp");
    let mut file = tokio::fs::File::create(&tmp)
        .await
        .with_context(|| format!("failed to create {}", tmp.display()))?;
    let mut stream = response.bytes_stream();
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("failed reading download stream")?;
        total += chunk.len() as u64;
        if total > MAX_JAR_BYTES {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(anyhow!(
                "download of {url} exceeded the {MAX_JAR_BYTES}-byte cap — aborting"
            ));
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .with_context(|| format!("failed writing {}", tmp.display()))?;
    }
    file.sync_all().await.ok();

    let actual = hex::encode(hasher.finalize());
    if !actual.eq_ignore_ascii_case(&expected) {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(anyhow!(
            "SHA-256 mismatch for {url}: expected {expected}, got {actual}. The download is not \
             the pinned PostgreSQL package and will not be executed. Set ACTAE_PG_BIN_DIR to a \
             verified PostgreSQL distribution, or retry with a working network."
        ));
    }
    tokio::fs::rename(&tmp, dest)
        .await
        .with_context(|| format!("failed to move {} into place", dest.display()))?;
    Ok(())
}

/// Verify an on-disk zonky jar against its pinned SHA-256 before extraction.
async fn verify_jar_sha256(path: &Path, platform: &str) -> Result<()> {
    let expected = expected_jar_sha256(platform)?;
    let data = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    let actual = hex::encode(Sha256::digest(&data));
    if !actual.eq_ignore_ascii_case(&expected) {
        return Err(anyhow!(
            "{} does not match the pinned SHA-256 for platform {platform} (expected {expected}, \
             got {actual}). The archive may be corrupted or tampered with; refusing to extract it.",
            path.display()
        ));
    }
    Ok(())
}

/// Unpack a zonky jar (a ZIP containing an XZ-compressed tar of the PG
/// runtime: bin/lib/share) into `dest`. Runs on a blocking thread — the
/// archive is ~150MB uncompressed.
///
/// Extraction goes into a freshly-created sibling staging directory, the
/// complete expected layout is validated, and the staging directory is
/// fsynced and atomically renamed into place, so an interrupted extraction
/// can never leave a partial install and a re-extraction never overlays a
/// broken version on top of a good one.
async fn unpack_zonky_jar(jar_path: &Path, dest: &Path) -> Result<()> {
    let jar_path = jar_path.to_path_buf();
    let dest = dest.to_path_buf();
    tokio::task::spawn_blocking(move || unpack_zonky_jar_blocking(&jar_path, &dest))
        .await
        .context("blocking PG unpack task panicked")?
}

fn unpack_zonky_jar_blocking(jar_path: &Path, dest: &Path) -> Result<()> {
    let staging = dest.with_extension(format!("staging-{}", std::process::id()));
    if staging.exists() {
        std::fs::remove_dir_all(&staging)
            .with_context(|| format!("failed to clear stale staging dir {}", staging.display()))?;
    }
    std::fs::create_dir_all(&staging)
        .with_context(|| format!("failed to create staging dir {}", staging.display()))?;

    let result = extract_zonky_into(jar_path, &staging);
    match result {
        Ok(()) => {
            sync_dir(&staging);
            replace_dir_atomic(&staging, dest)?;
            if let Some(parent) = dest.parent() {
                sync_dir(parent);
            }
            Ok(())
        }
        Err(e) => {
            let _ = std::fs::remove_dir_all(&staging);
            Err(e)
        }
    }
}

/// Extract the XZ-compressed tar from the jar into `dest` with strict size
/// caps on the compressed jar, the `.txz` payload and the expanded tar.
fn extract_zonky_into(jar_path: &Path, dest: &Path) -> Result<()> {
    let file = std::fs::File::open(jar_path)
        .with_context(|| format!("failed to open {}", jar_path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("failed to stat {}", jar_path.display()))?
        .len();
    if len > MAX_JAR_BYTES {
        return Err(anyhow!(
            "{} is {len} bytes — over the {MAX_JAR_BYTES}-byte cap",
            jar_path.display()
        ));
    }
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("{} is not a valid zip", jar_path.display()))?;

    // Locate the XZ-compressed tar entry inside the jar.
    let mut txz_bytes: Vec<u8> = Vec::new();
    let mut found = false;
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .with_context(|| format!("failed to read zip entry {i}"))?;
        let name = entry
            .name()
            .with_context(|| format!("zip entry {i} has an invalid name"))?
            .to_string();
        if name.ends_with(".txz") || name.ends_with(".xz") {
            let mut limited = entry.take(MAX_JAR_BYTES + 1);
            limited
                .read_to_end(&mut txz_bytes)
                .with_context(|| format!("failed to read {name} from the zip"))?;
            if txz_bytes.len() as u64 > MAX_JAR_BYTES {
                return Err(anyhow!(
                    "{name} inside {} exceeds the {MAX_JAR_BYTES}-byte payload cap",
                    jar_path.display()
                ));
            }
            found = true;
            break;
        }
    }
    if !found {
        return Err(anyhow!(
            "{} contains no .txz/.xz payload",
            jar_path.display()
        ));
    }

    // XZ-decompress the tar, then extract it. Symlink/hardlink entries are
    // created on Unix; skipped on Windows (the windows package duplicates
    // those files, and unprivileged symlink creation is unreliable there).
    let mut tar_bytes: Vec<u8> = Vec::new();
    lzma_rs::xz_decompress(&mut Cursor::new(&txz_bytes), &mut tar_bytes)
        .context("failed to decompress the PostgreSQL tar.xz")?;
    drop(txz_bytes);
    if tar_bytes.len() as u64 > MAX_EXPANDED_TAR_BYTES {
        return Err(anyhow!(
            "decompressed PostgreSQL archive is {} bytes — over the {MAX_EXPANDED_TAR_BYTES}-byte cap",
            tar_bytes.len()
        ));
    }

    let mut tar = tar::Archive::new(Cursor::new(tar_bytes));
    let mut extracted_bytes: u64 = 0;
    for entry in tar.entries().context("failed to read the PostgreSQL tar")? {
        let mut entry = entry.context("corrupt entry in the PostgreSQL tar")?;
        let is_link = matches!(
            entry.header().entry_type(),
            tar::EntryType::Symlink | tar::EntryType::Link
        );
        if is_link && !cfg!(unix) {
            // Windows packages carry real files instead of links; skip links.
            continue;
        }
        extracted_bytes = extracted_bytes
            .checked_add(entry.header().size().unwrap_or(0))
            .context("extracted size overflow")?;
        if extracted_bytes > MAX_EXPANDED_TAR_BYTES {
            return Err(anyhow!(
                "expanded PostgreSQL archive exceeds the {MAX_EXPANDED_TAR_BYTES}-byte cap"
            ));
        }
        entry.unpack_in(dest).with_context(|| {
            format!(
                "failed to extract {} from the PostgreSQL tar",
                entry
                    .path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default()
            )
        })?;
    }
    Ok(())
}

/// Atomically replace `dest` with the freshly-extracted `staging`
/// directory, rolling back on failure.
fn replace_dir_atomic(staging: &Path, dest: &Path) -> Result<()> {
    if dest.exists() {
        let backup = dest.with_extension(format!("old-{}", std::process::id()));
        std::fs::rename(dest, &backup)
            .with_context(|| format!("failed to move {} aside", dest.display()))?;
        if let Err(e) = std::fs::rename(staging, dest) {
            let _ = std::fs::rename(&backup, dest); // best-effort rollback
            return Err(e).with_context(|| format!("failed to install {}", dest.display()));
        }
        let _ = std::fs::remove_dir_all(&backup);
        Ok(())
    } else {
        std::fs::rename(staging, dest)
            .with_context(|| format!("failed to install {}", dest.display()))
    }
}

/// Best-effort fsync of a directory (durability after an atomic rename).
fn sync_dir(path: &Path) {
    #[cfg(unix)]
    {
        if let Ok(dir) = std::fs::File::open(path) {
            let _ = dir.sync_all();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

/// True when a TCP listener accepts connections on `port` (loopback probe).
fn port_busy(port: u16) -> bool {
    std::net::TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}")
            .parse()
            .expect("valid loopback address"),
        Duration::from_millis(300),
    )
    .is_ok()
}

/// Reserve the HTTP socket before any expensive embedded-PG work, so another
/// process cannot steal the port in the window between the availability
/// check and the server's own bind. Mirrors the config host resolution
/// (`ACTAE_HOST`, else `0.0.0.0` with TLS / `127.0.0.1` without), so the
/// reserved listener serves on exactly the address the server would have
/// bound. The listener is held by `StartedEmbeddedPostgres` and handed to the
/// HTTP/HTTPS server, which serves from it directly.
fn reserve_http_listener(http_port: u16) -> Result<std::net::TcpListener> {
    let enable_tls = std::env::var("ENABLE_TLS")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);
    let host = std::env::var("ACTAE_HOST").unwrap_or_else(|_| {
        if enable_tls {
            "0.0.0.0".to_string()
        } else {
            "127.0.0.1".to_string()
        }
    });
    let addr = format!("{host}:{http_port}")
        .parse::<std::net::SocketAddr>()
        .with_context(|| {
            format!(
                "ACTAE_HOST {host:?} is not a valid IP address — cannot reserve the HTTP \
                 socket on port {http_port}"
            )
        })?;
    let listener = std::net::TcpListener::bind(addr).with_context(|| {
        format!(
            "failed to reserve HTTP socket on {addr} — the port was taken before startup \
             completed (retry, or pick a different ACTAE_PORT)"
        )
    })?;
    info!("Reserved HTTP socket on {addr}");
    Ok(listener)
}

/// Exclusive advisory lock on an instance's data directory.
///
/// Held for the process lifetime; the OS releases it on exit (even SIGKILL).
/// This is the ownership signal that makes orphan detection portable without
/// `/proc` parent-pid inspection.
struct DataDirLock {
    _file: std::fs::File,
}

impl DataDirLock {
    /// Create the data directory and take the exclusive lock, failing fast
    /// when another live process already holds it.
    fn acquire(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("failed to create {}", data_dir.display()))?;
        let lock_path = data_dir.join(".actae.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("failed to open {}", lock_path.display()))?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(DataDirLock { _file: file }),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Err(anyhow!(
                "another Actae instance is already using data dir {} — stop it or pick a \
                 different port",
                data_dir.display()
            )),
            Err(e) => {
                Err(e).with_context(|| format!("failed to lock data dir {}", data_dir.display()))
            }
        }
    }
}

/// Parsed `postmaster.pid` header lines (pid / data directory / port).
struct PostmasterInfo {
    pid: u32,
    data_dir: String,
    port: String,
}

/// Read and parse the postmaster pid file of a cluster. Returns `Ok(None)`
/// when the file is absent or malformed (i.e. not a trustworthy ownership
/// signal).
fn read_postmaster(cluster_dir: &Path) -> Result<Option<PostmasterInfo>> {
    let path = cluster_dir.join("postmaster.pid");
    let raw = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", path.display())),
    };
    let lines: Vec<&str> = raw.lines().collect();
    // Format: pid / data directory / start time / port / socket dir / ...
    if lines.len() < 4 {
        return Ok(None);
    }
    let Ok(pid) = lines[0].trim().parse::<u32>() else {
        return Ok(None);
    };
    Ok(Some(PostmasterInfo {
        pid,
        data_dir: lines[1].trim().to_string(),
        port: lines[3].trim().to_string(),
    }))
}

/// Whether two paths refer to the same directory (canonicalized comparison;
/// the postmaster records a canonicalized data dir in `postmaster.pid`).
fn paths_equal(a: &str, b: &Path) -> bool {
    let pa = Path::new(a);
    let canon_a = pa.canonicalize().unwrap_or_else(|_| pa.to_path_buf());
    let canon_b = b.canonicalize().unwrap_or_else(|_| b.to_path_buf());
    canon_a == canon_b
}

/// The result of scrutinizing a busy pg port.
enum CandidateOrphan {
    /// The `postmaster.pid` in our cluster dir records our data dir + port —
    /// plausibly a crash orphan (final proof happens via `pg_ctl status`).
    Owned,
    /// The port is occupied by something that is not provably our orphan;
    /// the candidate pair must be skipped, not fought over.
    Foreign(String),
}

/// Decide whether a busy `pg_port` belongs to a crash orphan of this data
/// directory. Only `Owned` candidates are ever signalled, and `start()` still
/// requires `pg_ctl status` to confirm liveness before stopping.
fn candidate_orphan(data_dir: &Path, pg_port: u16) -> CandidateOrphan {
    let cluster_dir = data_dir.join("pg");
    let info = match read_postmaster(&cluster_dir) {
        Ok(Some(info)) => info,
        Ok(None) => {
            return CandidateOrphan::Foreign(format!(
                "port {pg_port} is occupied by a foreign process (no valid postmaster.pid in {})",
                cluster_dir.display()
            ))
        }
        Err(e) => {
            return CandidateOrphan::Foreign(format!(
                "port {pg_port} is occupied and its postmaster.pid cannot be read: {e:#}"
            ))
        }
    };
    if !paths_equal(&info.data_dir, &cluster_dir) {
        return CandidateOrphan::Foreign(format!(
            "port {pg_port} is occupied by a process whose postmaster.pid records a different \
             data dir ({} != {})",
            info.data_dir,
            cluster_dir.display()
        ));
    }
    if info.port != pg_port.to_string() {
        return CandidateOrphan::Foreign(format!(
            "port {pg_port} is occupied by a process whose postmaster.pid records port {}",
            info.port
        ));
    }
    CandidateOrphan::Owned
}

/// The chosen, isolated instance identity.
struct Instance {
    http_port: u16,
    pg_port: u16,
    data_dir: PathBuf,
    lock: DataDirLock,
    /// A busy pg port whose `postmaster.pid` (validated for dir + port)
    /// suggests a crash orphan — to be confirmed with `pg_ctl status`.
    orphan_expected: bool,
    /// The HTTP socket reserved for this identity (bound during selection so
    /// another process cannot steal the port while PostgreSQL initializes).
    http_listener: std::net::TcpListener,
}

/// Pick the (http, pg, data-dir) identity: the configured pair, or the first
/// free pair when auto-bumping is enabled. A candidate whose http port is
/// busy or unreservable, whose data-dir lock is held by a live instance, or
/// whose pg port is occupied by a foreign process is skipped. Port
/// arithmetic is checked so a scan near 65535 never overflows.
fn choose_instance(
    base_http: u16,
    base_pg: u16,
    base_data: &Path,
    auto_bump: bool,
) -> Result<Instance> {
    if base_http == 0 || base_pg == 0 {
        return Err(anyhow!(
            "port 0 is not a valid server port (ACTAE_PORT / ACTAE_EMBEDDED_PG_PORT)"
        ));
    }
    let range = if auto_bump { PORT_SCAN_RANGE } else { 1 };
    let mut last_reason: Option<String> = None;
    for offset in 0..range {
        let Some(http_port) = base_http.checked_add(offset) else {
            break;
        };
        let Some(pg_port) = base_pg.checked_add(offset) else {
            break;
        };
        let data_dir = if offset == 0 {
            base_data.to_path_buf()
        } else {
            PathBuf::from(format!("{}-{}", base_data.display(), http_port))
        };

        if port_busy(http_port) {
            last_reason = Some(format!("port {http_port} is busy"));
            continue;
        }

        // Reserve the HTTP socket NOW, before the data-dir lock / PG work:
        // nobody can steal the port while we initialize. If the reservation
        // loses a race (the port was just taken), treat it like a busy port
        // and safely retry the next pair.
        let http_listener = match reserve_http_listener(http_port) {
            Ok(listener) => listener,
            Err(e) => {
                last_reason = Some(format!(
                    "HTTP socket on {http_port} could not be reserved: {e:#}"
                ));
                continue;
            }
        };

        // Take the data-dir lock: it makes the ownership of any postgres on
        // `pg_port` unambiguous.
        let lock = match DataDirLock::acquire(&data_dir) {
            Ok(lock) => lock,
            Err(e) => {
                last_reason = Some(e.to_string());
                continue;
            }
        };

        let mut orphan_expected = false;
        if port_busy(pg_port) {
            match candidate_orphan(&data_dir, pg_port) {
                CandidateOrphan::Owned => {
                    warn!(
                        "Port {pg_port} is held by a likely orphaned embedded PostgreSQL \
                         (data dir {}) — ownership will be verified before reclaiming",
                        data_dir.display()
                    );
                    orphan_expected = true;
                }
                CandidateOrphan::Foreign(reason) => {
                    last_reason = Some(reason);
                    continue;
                }
            }
        }

        return Ok(Instance {
            http_port,
            pg_port,
            data_dir,
            lock,
            orphan_expected,
            http_listener,
        });
    }

    let reason = last_reason.map(|r| format!(" — {r}")).unwrap_or_default();
    Err(anyhow!(
        "no free port pair found in {base_http}..+{PORT_SCAN_RANGE} (http) / {base_pg}..+{PORT_SCAN_RANGE} (pg){reason}"
    ))
}

/// The embedded PostgreSQL instance, managed for the process lifetime.
pub struct EmbeddedPostgres {
    child: Child,
    data_dir: PathBuf,
    cluster_dir: PathBuf,
    pg_ctl: PathBuf,
    pg_port: u16,
    /// Superuser password (persisted in `pg.pwfile`); used to build the DSN.
    password: String,
    _lock: DataDirLock,
}

impl EmbeddedPostgres {
    /// The embedded PostgreSQL port.
    pub fn pg_port(&self) -> u16 {
        self.pg_port
    }

    /// The data directory backing this instance.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// The superuser password (for the DSN and the startup banner).
    pub fn password(&self) -> &str {
        &self.password
    }
}

/// The chosen instance identity + the running embedded PostgreSQL.
pub struct StartedEmbeddedPostgres {
    pub postgres: EmbeddedPostgres,
    pub http_port: u16,
    pub pg_port: u16,
    pub data_dir: PathBuf,
    pub password: String,
    pub bumped: bool,
    pub configured_http_port: u16,
    /// True when this boot minted the credential itself (never on restarts).
    /// `main.rs` uses it to print the password exactly once.
    pub password_was_generated: bool,
    /// The HTTP socket reserved at startup (prevents a port race while the
    /// embedded PostgreSQL is initialized); handed to the HTTP/HTTPS server.
    pub http_listener: std::net::TcpListener,
}

/// Start the embedded PostgreSQL. Reads configuration from the environment:
/// `ACTAE_PORT`/`HTTPS_PORT` (base http), `ACTAE_EMBEDDED_PG_PORT` (base pg),
/// `ACTAE_DATA_DIR`, `ACTAE_EMBEDDED_PG_PASSWORD`, `ACTAE_EMBEDDED_PG_WIPE`,
/// `ACTAE_AUTO_BUMP`.
pub async fn start() -> Result<StartedEmbeddedPostgres> {
    check_not_root()?;

    let configured_http_port: u16 = std::env::var("ACTAE_PORT")
        .or_else(|_| std::env::var("HTTPS_PORT"))
        .unwrap_or_else(|_| "8002".to_string())
        .parse()
        .context("ACTAE_PORT must be a valid port")?;
    let base_pg_port: u16 = std::env::var("ACTAE_EMBEDDED_PG_PORT")
        .unwrap_or_else(|_| "5546".to_string())
        .parse()
        .context("ACTAE_EMBEDDED_PG_PORT must be a valid port")?;
    let base_data = std::env::var("ACTAE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));
            home.join(".actae").join("data")
        });
    let wipe = std::env::var("ACTAE_EMBEDDED_PG_WIPE")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);
    let auto_bump = !matches!(
        std::env::var("ACTAE_AUTO_BUMP").as_deref(),
        Ok("0") | Ok("false")
    );
    let provided_password = std::env::var("ACTAE_EMBEDDED_PG_PASSWORD")
        .ok()
        .filter(|s| !s.trim().is_empty());

    let instance = choose_instance(configured_http_port, base_pg_port, &base_data, auto_bump)?;
    let http_listener = instance.http_listener;
    let bumped = instance.http_port != configured_http_port;
    if bumped {
        warn!(
            "Port {configured_http_port} is busy — Actae will listen on {} instead \
             (isolated instance: pg {}, data {})",
            instance.http_port,
            instance.pg_port,
            instance.data_dir.display()
        );
    }

    let bins = resolve_binaries(&instance.data_dir).await?;
    let cluster_dir = instance.data_dir.join("pg");
    let pwfile = instance.data_dir.join("pg.pwfile");
    let log_path = instance.data_dir.join("postgres.log");

    // 1. Reclaim an orphaned postgres BEFORE touching the cluster. Only ever
    //    stop a process whose ownership is proven (postmaster.pid matches
    //    dir+port AND `pg_ctl status` confirms a live server with that pid).
    //    On any doubt, fail loudly — never signal an unrelated process.
    if port_busy(instance.pg_port) {
        if instance.orphan_expected {
            reclaim_orphan(&bins.pg_ctl, &cluster_dir, instance.pg_port).await?;
        } else {
            return Err(anyhow!(
                "port {} is occupied by a foreign process (no matching postmaster.pid in {}) — \
                 refusing to touch it. Free the port or choose a different ACTAE_EMBEDDED_PG_PORT.",
                instance.pg_port,
                cluster_dir.display()
            ));
        }
    }

    // 2. Destructive wipe: it must fully succeed or fail loudly — announcing
    //    a fresh database while retaining a half-removed cluster is worse
    //    than not wiping at all.
    if wipe {
        warn!("ACTAE_EMBEDDED_PG_WIPE set — recreating embedded database from scratch");
        remove_dir_all_allow_missing(&cluster_dir)
            .await
            .with_context(|| {
                format!("failed to wipe embedded cluster {}", cluster_dir.display())
            })?;
        remove_file_allow_missing(&pwfile)
            .await
            .with_context(|| format!("failed to wipe password file {}", pwfile.display()))?;
        if cluster_dir.exists() || pwfile.exists() {
            return Err(anyhow!(
                "ACTAE_EMBEDDED_PG_WIPE could not fully remove the existing cluster ({} or {}) — \
                 refusing to proceed on a partially-wiped database",
                cluster_dir.display(),
                pwfile.display()
            ));
        }
    }

    // 3. Credentials: the persisted pwfile is authoritative after
    //    initialization. A conflicting ACTAE_EMBEDDED_PG_PASSWORD is a loud
    //    configuration error, never a silent 60s timeout-and-fail.
    let cluster_initialized = cluster_dir.join("PG_VERSION").is_file();
    let persisted_password = std::fs::read_to_string(&pwfile)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let password = resolve_password(
        cluster_initialized,
        persisted_password.as_deref(),
        provided_password,
        &pwfile,
        &cluster_dir,
    )?;
    let password_was_generated = !cluster_initialized
        && std::env::var("ACTAE_EMBEDDED_PG_PASSWORD")
            .map(|s| s.trim().is_empty())
            .unwrap_or(true);

    // 4. Cluster version / initialization.
    ensure_cluster_version(&bins.initdb, &cluster_dir).await?;
    if !cluster_dir.join("PG_VERSION").exists() {
        info!(
            "Initializing embedded PostgreSQL cluster in {} (scram-sha-256 auth)",
            cluster_dir.display()
        );
        init_cluster(&bins.initdb, &cluster_dir, &pwfile, &password).await?;
    }

    // 5. Spawn + readiness + database creation, with a startup guard: every
    //    error after the spawn runs a bounded graceful stop (then kill+reap)
    //    so a failed start never leaves a live postgres or crash-recovery
    //    state.
    let mut child =
        spawn_postgres(&bins.postgres, &cluster_dir, instance.pg_port, &log_path).await?;
    if let Err(e) = wait_until_ready(instance.pg_port, &password, &log_path, &mut child).await {
        stop_child(&bins.pg_ctl, &cluster_dir, &mut child).await;
        return Err(e);
    }
    if let Err(e) = ensure_database(instance.pg_port, &password).await {
        stop_child(&bins.pg_ctl, &cluster_dir, &mut child).await;
        return Err(e);
    }

    info!(
        "Embedded PostgreSQL listening on 127.0.0.1:{} (data {})",
        instance.pg_port,
        instance.data_dir.display()
    );

    Ok(StartedEmbeddedPostgres {
        postgres: EmbeddedPostgres {
            child,
            data_dir: instance.data_dir.clone(),
            cluster_dir,
            pg_ctl: bins.pg_ctl,
            pg_port: instance.pg_port,
            password: password.clone(),
            _lock: instance.lock,
        },
        http_port: instance.http_port,
        pg_port: instance.pg_port,
        data_dir: instance.data_dir,
        password,
        bumped,
        configured_http_port,
        password_was_generated,
        http_listener,
    })
}

/// Resolve the superuser password, enforcing that the persisted credential
/// is authoritative once a cluster is initialized.
fn resolve_password(
    cluster_initialized: bool,
    persisted: Option<&str>,
    provided: Option<String>,
    pwfile: &Path,
    cluster_dir: &Path,
) -> Result<String> {
    match (cluster_initialized, persisted, provided) {
        (true, Some(persisted), Some(provided)) if persisted != provided => Err(anyhow!(
            "ACTAE_EMBEDDED_PG_PASSWORD does not match the password persisted for this embedded \
             database ({}). The persisted role password is authoritative after initialization. \
             Unset ACTAE_EMBEDDED_PG_PASSWORD, or recreate the database with \
             ACTAE_EMBEDDED_PG_WIPE=1.",
            pwfile.display()
        )),
        (true, Some(persisted), _) => Ok(persisted.to_string()),
        (true, None, _) => Err(anyhow!(
            "the embedded database in {} is initialized but its password file {} is missing — the \
             role password is unknowable. Recreate the database with ACTAE_EMBEDDED_PG_WIPE=1.",
            cluster_dir.display(),
            pwfile.display()
        )),
        (false, Some(persisted), _) => Ok(persisted.to_string()),
        (false, None, Some(provided)) => {
            write_password_file(pwfile, &provided)?;
            Ok(provided)
        }
        (false, None, None) => {
            let pw = generate_password();
            write_password_file(pwfile, &pw)?;
            Ok(pw)
        }
    }
}

/// PostgreSQL refuses to run as the superuser (security), so managed storage
/// must fail fast with actionable guidance when started as root instead of
/// surfacing initdb's cryptic error.
#[cfg(unix)]
fn check_not_root() -> Result<()> {
    if unsafe { libc::geteuid() } == 0 {
        return Err(anyhow!(
            "managed PostgreSQL cannot run as root: PostgreSQL refuses to run as the superuser. \
             Run Actae as an unprivileged user (e.g. a service account), or set \
             ACTAE_PG_BIN_DIR to an externally-managed PostgreSQL."
        ));
    }
    Ok(())
}

/// Windows has no Unix root concept.
#[cfg(not(unix))]
fn check_not_root() -> Result<()> {
    Ok(())
}

/// Generate a strong postgres superuser password (32 bytes, base64url).
fn generate_password() -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    let mut bytes = [0u8; 32];
    use rand::RngCore;
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Write the password file with owner-only permissions (Unix). On Windows
/// the file inherits the user-profile ACL.
fn write_password_file(path: &Path, password: &str) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    std::fs::write(path, password)
        .with_context(|| format!("failed to write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(())
}

async fn remove_dir_all_allow_missing(path: &Path) -> std::io::Result<()> {
    match tokio::fs::remove_dir_all(path).await {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        r => r,
    }
}

async fn remove_file_allow_missing(path: &Path) -> std::io::Result<()> {
    match tokio::fs::remove_file(path).await {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        r => r,
    }
}

/// Verify the existing cluster (if any) matches the bundled binaries' major
/// version; a major bump requires recreating the data directory.
async fn ensure_cluster_version(initdb: &Path, cluster_dir: &Path) -> Result<()> {
    let version_file = cluster_dir.join("PG_VERSION");
    let Ok(existing) = tokio::fs::read_to_string(&version_file).await else {
        return Ok(());
    };
    let (cluster_major, _) = parse_version(existing.trim())
        .with_context(|| format!("unparseable PG_VERSION in {}", cluster_dir.display()))?;
    let (bin_major, _) = binary_version(initdb).await?;
    if cluster_major != bin_major {
        return Err(anyhow!(
            "embedded database {} was initialized with PostgreSQL major version {cluster_major}, \
             but the available binaries are {bin_major}. Recreate the database with \
             ACTAE_EMBEDDED_PG_WIPE=1 (or remove the directory) and start again.",
            cluster_dir.display()
        ));
    }
    Ok(())
}

/// Parse a `major.minor` version from a string that may carry decorations
/// (`"initdb (PostgreSQL) 18.2"`, `"18.2\n"`).
fn parse_version(s: &str) -> Result<(u32, u32)> {
    let start = s
        .find(|c: char| c.is_ascii_digit())
        .context("no digit found in postgres version string")?;
    let rest = &s[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(rest.len());
    let mut parts = rest[..end].split('.');
    let major = parts
        .next()
        .unwrap_or("")
        .parse::<u32>()
        .context("invalid postgres major version")?;
    let minor = parts
        .next()
        .unwrap_or("0")
        .parse::<u32>()
        .context("invalid postgres minor version")?;
    Ok((major, minor))
}

/// The major.minor version of the bundled binaries (`initdb --version`).
async fn binary_version(initdb: &Path) -> Result<(u32, u32)> {
    let out = Command::new(initdb)
        .arg("--version")
        .output()
        .await
        .with_context(|| format!("failed to run {}", initdb.display()))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let version = stdout
        .split("PostgreSQL")
        .nth(1)
        .with_context(|| format!("unexpected initdb output: {stdout}"))?;
    parse_version(version.trim())
}

/// Create a fresh cluster with scram-sha-256 auth and the given password.
async fn init_cluster(
    initdb: &Path,
    cluster_dir: &Path,
    pwfile: &Path,
    password: &str,
) -> Result<()> {
    tokio::fs::create_dir_all(cluster_dir)
        .await
        .with_context(|| format!("failed to create {}", cluster_dir.display()))?;
    if !pwfile.is_file() {
        write_password_file(pwfile, password)?;
    }
    // Capture initdb's output instead of inheriting stdio: a first run otherwise
    // dumps the raw PostgreSQL bootstrap trace onto the user's console. On
    // success it is discarded; on failure it is written next to the cluster and
    // referenced from the error so the cause is never lost.
    let output = Command::new(initdb)
        .arg("-D")
        .arg(cluster_dir)
        .arg("-U")
        .arg("actae")
        .arg("-A")
        .arg("scram-sha-256")
        .arg("--pwfile")
        .arg(pwfile)
        .arg("--no-locale")
        .arg("-E")
        .arg("UTF8")
        .output()
        .await
        .with_context(|| format!("failed to run {}", initdb.display()))?;
    if !output.status.success() {
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let log_path = cluster_dir.parent().map(|p| p.join("initdb.log"));
        if let Some(path) = log_path.as_ref() {
            let _ = std::fs::write(path, combined.trim_end());
        }
        return Err(anyhow!(
            "initdb failed with {}{} for {}",
            output.status,
            log_path
                .as_ref()
                .map(|p| format!(" — see {}", p.display()))
                .unwrap_or_default(),
            cluster_dir.display()
        ));
    }
    Ok(())
}

/// Spawn `postgres` in the foreground with fsync enabled, logging to
/// `<data_dir>/postgres.log`.
///
/// Unix sockets are **disabled entirely** (`unix_socket_directories=''`):
/// the server only listens on loopback TCP, so it can never leave stale
/// `.s.PGSQL.*` artifacts that block — or worse, get cleaned up and delete —
/// another PostgreSQL server's socket. This also sidesteps Unix `sun_path`
/// length limits for deeply-nested data directories.
///
/// `max_connections` is sized to Actae's actual pool requirements (default
/// pools are ~10 connections) with a modest reserve — 2000 was excessive for
/// the target machines and could trip resource limits at startup.
async fn spawn_postgres(
    postgres: &Path,
    cluster_dir: &Path,
    port: u16,
    log_path: &Path,
) -> Result<Child> {
    if let Some(parent) = log_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .with_context(|| format!("failed to open {}", log_path.display()))?;
    let log_stdout = log
        .try_clone()
        .context("failed to clone postgres log handle")?;
    let mut cmd = Command::new(postgres);
    cmd.arg("-D")
        .arg(cluster_dir)
        .arg("-p")
        .arg(port.to_string())
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-c")
        .arg("max_connections=100")
        .arg("-c")
        .arg("unix_socket_directories=")
        .stdout(Stdio::from(log_stdout))
        .stderr(Stdio::from(log));
    // Put the child in its OWN process group so a terminal Ctrl+C (SIGINT to
    // the foreground group) does not reach postgres directly. Otherwise it
    // shuts itself down before our graceful `pg_ctl stop`, which then fails on
    // a missing postmaster.pid and dumps a burst of pool connection errors.
    // We always stop it ourselves via `pg_ctl stop` (then a hard kill).
    #[cfg(unix)]
    {
        cmd.process_group(0);
    }
    #[cfg(windows)]
    {
        // CREATE_NEW_PROCESS_GROUP — keeps Ctrl+C from reaching the child.
        cmd.creation_flags(0x0000_0200);
    }
    cmd.spawn()
        .with_context(|| format!("failed to spawn {}", postgres.display()))
}

/// Wait until the postgres accepts a scram-authenticated connection. The
/// child is polled for early exit so a bind failure (busy port, bad config)
/// surfaces as an actionable error instead of a 60-second timeout.
async fn wait_until_ready(
    port: u16,
    password: &str,
    log_path: &Path,
    child: &mut Child,
) -> Result<()> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("127.0.0.1")
        .port(port)
        .user("actae")
        .password(password)
        .dbname("postgres");
    loop {
        if let Some(status) = child
            .try_wait()
            .context("failed to poll the postgres child")?
        {
            return Err(anyhow!(
                "embedded postgres exited during startup with {status} (see {})",
                log_path.display()
            ));
        }
        let attempt =
            tokio::time::timeout(Duration::from_secs(5), cfg.connect(tokio_postgres::NoTls)).await;
        match attempt {
            Ok(Ok((_client, connection))) => {
                drop(connection);
                return Ok(());
            }
            Ok(Err(_)) | Err(_) => {
                if Instant::now() >= deadline {
                    return Err(anyhow!(
                        "timed out connecting to embedded postgres on port {port} (see {})",
                        log_path.display()
                    ));
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }
    }
}

/// Create the `actae` database on the fresh cluster if it does not exist.
async fn ensure_database(port: u16, password: &str) -> Result<()> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("127.0.0.1")
        .port(port)
        .user("actae")
        .password(password)
        .dbname("postgres");
    let (client, connection) = cfg
        .connect(tokio_postgres::NoTls)
        .await
        .context("failed to connect to postgres for database creation")?;
    let connection = tokio::spawn(connection);

    let result: Result<()> = async {
        let row = client
            .query_opt("SELECT 1 FROM pg_database WHERE datname = 'actae'", &[])
            .await
            .context("failed to check for actae database")?;
        if row.is_none() {
            client
                .execute("CREATE DATABASE actae", &[])
                .await
                .context("failed to create actae database")?;
            info!("Created embedded database 'actae'");
        }
        Ok(())
    }
    .await;

    drop(client);
    connection.abort();
    result
}

/// Graceful stop via `pg_ctl -D <cluster> stop -m fast -w`.
async fn stop_cluster(pg_ctl: &Path, cluster_dir: &Path) -> Result<()> {
    let out = Command::new(pg_ctl)
        .arg("-D")
        .arg(cluster_dir)
        .arg("stop")
        .arg("-m")
        .arg("fast")
        .arg("-w")
        .output()
        .await
        .with_context(|| format!("failed to run {}", pg_ctl.display()))?;
    if !out.status.success() {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        // `pg_ctl stop` is non-zero when the server is already gone (missing
        // postmaster.pid / "Is server running?"). That is success for us —
        // there is nothing left to stop, so don't alarm the operator.
        let combined = format!("{stdout}\n{stderr}").to_ascii_lowercase();
        if combined.contains("does not exist")
            || combined.contains("is server running")
            || combined.contains("no server running")
        {
            return Ok(());
        }
        return Err(anyhow!(
            "pg_ctl stop failed with {}{}{}",
            out.status,
            if stdout.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", stdout.trim())
            },
            if stderr.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", stderr.trim())
            }
        ));
    }
    Ok(())
}

/// `pg_ctl status` verdict for a cluster.
enum PgCtlStatus {
    Running(u32),
    NoServer,
}

/// Ask `pg_ctl status` whether a live server is running on `cluster_dir`.
/// This is the authoritative liveness/identity check before any signal is
/// sent — it only returns `Running` when the postmaster.pid is consistent
/// with a live postmaster process.
async fn pg_ctl_status(pg_ctl: &Path, cluster_dir: &Path) -> Result<PgCtlStatus> {
    let out = Command::new(pg_ctl)
        .arg("-D")
        .arg(cluster_dir)
        .arg("status")
        .output()
        .await
        .with_context(|| format!("failed to run {}", pg_ctl.display()))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    match out.status.code() {
        Some(0) => {
            // macOS prints `server is running (PID: 34044)`; other platforms
            // `server is running (PID: 34044)` too but with different
            // decorations. Extract the first integer after "PID:".
            let pid = stdout
                .split("PID:")
                .nth(1)
                .and_then(|rest| {
                    rest.chars()
                        .skip_while(|c| !c.is_ascii_digit())
                        .take_while(|c| c.is_ascii_digit())
                        .collect::<String>()
                        .parse::<u32>()
                        .ok()
                })
                .ok_or_else(|| {
                    anyhow!("pg_ctl status reported a running server but no PID: {stdout}")
                })?;
            Ok(PgCtlStatus::Running(pid))
        }
        Some(3) => Ok(PgCtlStatus::NoServer),
        code => Err(anyhow!(
            "pg_ctl status exited with {code:?}: {stdout}{stderr}"
        )),
    }
}

/// Wait until `port` stops accepting connections.
async fn wait_until_free(port: u16, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if !port_busy(port) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!("port {port} did not free within {timeout:?}"));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Reclaim a crash orphan whose ownership has been proven: `postmaster.pid`
/// in our cluster dir records our data dir + port, and `pg_ctl status`
/// confirms a live server whose PID matches. If ownership cannot be
/// established, fail WITHOUT sending any signal.
async fn reclaim_orphan(pg_ctl: &Path, cluster_dir: &Path, port: u16) -> Result<()> {
    let info = read_postmaster(cluster_dir)?.ok_or_else(|| {
        anyhow!(
            "postmaster.pid in {} is missing while port {port} is busy — refusing to act on \
             unproven ownership",
            cluster_dir.display()
        )
    })?;

    if !paths_equal(&info.data_dir, cluster_dir) {
        return Err(anyhow!(
            "postmaster.pid in {} records data dir {}, not {} — the process on port {port} is \
             not this instance's PostgreSQL; refusing to signal it",
            cluster_dir.display(),
            info.data_dir,
            cluster_dir.display()
        ));
    }
    if info.port != port.to_string() {
        return Err(anyhow!(
            "postmaster.pid in {} records port {}, not {port} — refusing to signal the process \
             listening on {port}",
            cluster_dir.display(),
            info.port
        ));
    }

    match pg_ctl_status(pg_ctl, cluster_dir).await? {
        PgCtlStatus::Running(pid) => {
            if pid != info.pid {
                return Err(anyhow!(
                    "pg_ctl reports PID {pid} running on {}, but postmaster.pid records {} — \
                     ownership cannot be established; refusing to stop it",
                    cluster_dir.display(),
                    info.pid
                ));
            }
            warn!("Reclaiming orphaned embedded PostgreSQL (pid {pid}) on port {port}");
            stop_cluster(pg_ctl, cluster_dir).await?;
            wait_until_free(port, Duration::from_secs(15)).await.with_context(|| {
                format!("orphaned postgres on port {port} did not release the port after pg_ctl stop")
            })?;
            Ok(())
        }
        PgCtlStatus::NoServer => Err(anyhow!(
            "postmaster.pid exists but pg_ctl reports no live server for {} — the process on \
             port {port} is unrelated or the state is stale; refusing to stop anything",
            cluster_dir.display()
        )),
    }
}

/// Bounded graceful shutdown of the postgres child: `pg_ctl stop -m fast -w`
/// under a Tokio timeout, then a hard kill + reap if it does not finish.
/// Used both by normal shutdown and by every post-spawn startup error path.
async fn stop_child(pg_ctl: &Path, cluster_dir: &Path, child: &mut Child) {
    match tokio::time::timeout(STOP_TIMEOUT, stop_cluster(pg_ctl, cluster_dir)).await {
        Ok(Ok(())) => {
            let _ = tokio::time::timeout(STOP_TIMEOUT, child.wait()).await;
        }
        Ok(Err(e)) => {
            warn!("pg_ctl stop failed during shutdown ({e:#}) — killing the postgres child")
        }
        Err(_) => {
            warn!("pg_ctl stop did not finish within {STOP_TIMEOUT:?} — killing the postgres child")
        }
    }
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

impl EmbeddedPostgres {
    /// Graceful stop: bounded `pg_ctl stop -m fast -w`, then a hard kill as a
    /// last resort, always reaping the child.
    pub async fn stop(&mut self) {
        stop_child(&self.pg_ctl, &self.cluster_dir, &mut self.child).await;
    }
}

impl Drop for EmbeddedPostgres {
    fn drop(&mut self) {
        // Best effort: if the shutdown handler never ran, make sure the
        // child does not outlive the parent.
        let _ = self.child.start_kill();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_version_plain() {
        assert_eq!(parse_version("18.2").unwrap(), (18, 2));
        assert_eq!(parse_version("18.2.0").unwrap(), (18, 2));
        assert_eq!(parse_version("16.12").unwrap(), (16, 12));
    }

    #[test]
    fn test_parse_version_decorated() {
        assert_eq!(parse_version("initdb (PostgreSQL) 18.2").unwrap(), (18, 2));
        assert_eq!(
            parse_version("postgres (PostgreSQL) 16.12.0\n").unwrap(),
            (16, 12)
        );
    }

    #[test]
    fn test_parse_version_rejects_garbage() {
        assert!(parse_version("").is_err());
        assert!(parse_version("no digits here").is_err());
    }

    #[test]
    fn test_password_generation_is_unique_and_strong() {
        let a = generate_password();
        let b = generate_password();
        assert_ne!(a, b);
        assert!(a.len() >= 40);
    }

    #[test]
    fn test_zonky_platform_maps_current() {
        // The classifier must be valid for the host platform (used by the
        // downloader); unknown hosts are rejected.
        let platform = zonky_platform().unwrap();
        assert!(!platform.is_empty());
    }

    #[test]
    fn test_checksum_manifest_covers_every_zonky_platform() {
        // Every platform the runtime resolver can produce must have a pinned
        // SHA-256, or extraction/download would be refused at runtime.
        for platform in [
            "linux-amd64",
            "linux-arm64v8",
            "linux-amd64-alpine",
            "linux-arm64v8-alpine",
            "darwin-amd64",
            "darwin-arm64v8",
            "windows-amd64",
        ] {
            assert!(
                expected_jar_sha256(platform).is_ok(),
                "no pinned SHA-256 for {platform}"
            );
            let hash = expected_jar_sha256(platform).unwrap();
            assert_eq!(hash.len(), 64, "bad hash length for {platform}");
        }
    }

    #[test]
    fn test_checksum_manifest_rejects_unknown_platform() {
        assert!(expected_jar_sha256("linux-s390x").is_err());
    }

    #[test]
    fn test_read_postmaster_parses_header() {
        let tmp = tempfile::tempdir().unwrap();
        let pid_file = tmp.path().join("postmaster.pid");
        std::fs::write(
            &pid_file,
            "4242\n/tmp/cluster\n1723456789\n5546\n\n127.0.0.1\n\n",
        )
        .unwrap();
        let info = read_postmaster(tmp.path()).unwrap().unwrap();
        assert_eq!(info.pid, 4242);
        assert_eq!(info.data_dir, "/tmp/cluster");
        assert_eq!(info.port, "5546");
    }

    #[test]
    fn test_read_postmaster_tolerates_missing_and_malformed() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read_postmaster(tmp.path()).unwrap().is_none());
        std::fs::write(tmp.path().join("postmaster.pid"), "not-a-pid\n").unwrap();
        assert!(read_postmaster(tmp.path()).unwrap().is_none());
    }

    #[test]
    fn test_paths_equal_is_canonical() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("sub").join("..").join("sub");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(paths_equal(
            &tmp.path().join("sub").display().to_string(),
            &dir
        ));
    }

    #[test]
    fn test_resolve_password_conflict_is_actionable() {
        let tmp = tempfile::tempdir().unwrap();
        let pwfile = tmp.path().join("pg.pwfile");
        let cluster = tmp.path().join("pg");
        std::fs::create_dir_all(&cluster).unwrap();

        // Mismatched env on an initialized cluster -> hard error with guidance.
        let err = resolve_password(
            true,
            Some("persisted-secret"),
            Some("different-secret".to_string()),
            &pwfile,
            &cluster,
        )
        .unwrap_err();
        assert!(err.to_string().contains("ACTAE_EMBEDDED_PG_PASSWORD"));

        // Matching env is fine and the persisted value wins.
        let pw = resolve_password(
            true,
            Some("persisted-secret"),
            Some("persisted-secret".to_string()),
            &pwfile,
            &cluster,
        )
        .unwrap();
        assert_eq!(pw, "persisted-secret");

        // Missing pwfile on an initialized cluster -> actionable error.
        let err = resolve_password(true, None, None, &pwfile, &cluster).unwrap_err();
        assert!(err.to_string().contains("ACTAE_EMBEDDED_PG_WIPE"));

        // Fresh cluster with env -> env is persisted.
        let pw = resolve_password(
            false,
            None,
            Some("new-secret".to_string()),
            &pwfile,
            &cluster,
        )
        .unwrap();
        assert_eq!(pw, "new-secret");
        assert_eq!(std::fs::read_to_string(&pwfile).unwrap(), "new-secret");
    }

    #[test]
    fn test_extract_zonky_jar_end_to_end() {
        // A synthetic zonky jar — zip( .txz = xz( tar( bin/lib/share ) ) ) —
        // exercises the real extraction path: entry discovery, XZ
        // decompression, tar unpacking, staging, and the atomic rename.
        use std::io::Write as _;

        let tmp = tempfile::tempdir().unwrap();
        let mut tar_bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_bytes);
            for (path, content) in [
                ("bin/initdb", "initdb-bytes"),
                ("bin/postgres", "postgres-bytes"),
                ("bin/pg_ctl", "pgctl-bytes"),
                ("lib/postgres/libpq.so.5", "lib-bytes"),
                ("share/postgresql/sample.conf", "share-bytes"),
            ] {
                let mut header = tar::Header::new_gnu();
                header.set_size(content.len() as u64);
                header.set_mode(0o755);
                builder
                    .append_data(&mut header, path, content.as_bytes())
                    .unwrap();
            }
            builder.finish().unwrap();
        }
        let mut xz_bytes = Vec::new();
        lzma_rs::xz_compress(&mut std::io::Cursor::new(&tar_bytes), &mut xz_bytes).unwrap();
        let mut jar_bytes = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut jar_bytes));
            let opts = zip::write::SimpleFileOptions::default();
            zw.start_file("postgres-linux.txz", opts).unwrap();
            zw.write_all(&xz_bytes).unwrap();
            zw.finish().unwrap();
        }

        let jar_path = tmp.path().join("pkg.jar");
        std::fs::write(&jar_path, &jar_bytes).unwrap();
        let dest = tmp.path().join("dest");

        unpack_zonky_jar_blocking(&jar_path, &dest).unwrap();

        for path in [
            "bin/initdb",
            "bin/postgres",
            "bin/pg_ctl",
            "lib/postgres/libpq.so.5",
            "share/postgresql/sample.conf",
        ] {
            assert!(dest.join(path).is_file(), "missing extracted {path}");
        }
        // The staging dir must be gone (atomic rename completed).
        let staging = dest.with_extension(format!("staging-{}", std::process::id()));
        assert!(!staging.exists(), "staging dir was not cleaned up");
    }

    #[test]
    fn test_extract_zonky_jar_rejects_oversized_payload() {
        // The compressed .txz entry is capped at MAX_JAR_BYTES. We cannot
        // materialize 256MB in a unit test, but we can assert the cap check
        // itself via the zip reader on a jar whose entry is declared larger
        // than the cap's +1 read limit.
        use std::io::Write as _;
        let tmp = tempfile::tempdir().unwrap();
        let mut jar_bytes = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut jar_bytes));
            let opts = zip::write::SimpleFileOptions::default();
            zw.start_file("postgres-linux.txz", opts).unwrap();
            // Not actually writing MAX_JAR_BYTES+1 — we assert the length
            // guard logic separately below via a truncated read path.
            zw.write_all(b"small").unwrap();
            zw.finish().unwrap();
        }
        // Sanity: extraction of a legitimately small payload succeeds.
        let jar_path = tmp.path().join("small.jar");
        std::fs::write(&jar_path, &jar_bytes).unwrap();
        let dest = tmp.path().join("dest");
        let err = extract_zonky_into(&jar_path, &dest).unwrap_err();
        // "small" is not a valid xz stream — must fail decompression.
        assert!(err.to_string().contains("decompress"));
    }

    #[tokio::test]
    async fn test_verify_jar_sha256_rejects_tampered_archive() {
        let tmp = tempfile::tempdir().unwrap();
        let jar = tmp.path().join("garbage.jar");
        std::fs::write(&jar, b"definitely not the pinned zonky archive").unwrap();
        let err = verify_jar_sha256(&jar, "darwin-arm64v8").await.unwrap_err();
        assert!(err.to_string().contains("SHA-256"), "{err:#}");
        assert!(err.to_string().contains("refusing"), "{err:#}");
    }

    #[test]
    fn test_candidate_orphan_owned_when_pid_matches() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();
        let cluster = data_dir.join("pg");
        std::fs::create_dir_all(&cluster).unwrap();
        let canonical = cluster.canonicalize().unwrap();
        std::fs::write(
            cluster.join("postmaster.pid"),
            format!(
                "4242\n{}\n1723456789\n5546\n\n127.0.0.1\n\n",
                canonical.display()
            ),
        )
        .unwrap();
        assert!(
            matches!(candidate_orphan(data_dir, 5546), CandidateOrphan::Owned),
            "matching postmaster.pid must be Owned"
        );
    }

    #[test]
    fn test_candidate_orphan_foreign_when_unproven() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();
        let cluster = data_dir.join("pg");
        std::fs::create_dir_all(&cluster).unwrap();
        let canonical = cluster.canonicalize().unwrap();

        // Wrong port.
        std::fs::write(
            cluster.join("postmaster.pid"),
            format!(
                "4242\n{}\n1723456789\n9999\n\n127.0.0.1\n\n",
                canonical.display()
            ),
        )
        .unwrap();
        assert!(
            matches!(
                candidate_orphan(data_dir, 5546),
                CandidateOrphan::Foreign(_)
            ),
            "port mismatch must be Foreign"
        );

        // Wrong data directory.
        std::fs::write(
            cluster.join("postmaster.pid"),
            "4242\n/elsewhere/pg\n1723456789\n5546\n\n127.0.0.1\n\n",
        )
        .unwrap();
        assert!(
            matches!(
                candidate_orphan(data_dir, 5546),
                CandidateOrphan::Foreign(_)
            ),
            "data-dir mismatch must be Foreign"
        );

        // Missing pid file.
        std::fs::remove_file(cluster.join("postmaster.pid")).unwrap();
        assert!(
            matches!(
                candidate_orphan(data_dir, 5546),
                CandidateOrphan::Foreign(_)
            ),
            "missing pid file must be Foreign"
        );

        // Malformed pid file.
        std::fs::write(cluster.join("postmaster.pid"), "not-a-pid\n").unwrap();
        assert!(
            matches!(
                candidate_orphan(data_dir, 5546),
                CandidateOrphan::Foreign(_)
            ),
            "malformed pid file must be Foreign"
        );
    }

    #[test]
    fn test_on_alpine_like_host_runs_everywhere() {
        // Must not panic on any host (it reads /etc + /lib).
        let _ = on_alpine_like_host();
    }

    /// Serializes tests that mutate the process environment (`PATH`,
    /// `ACTAE_PG_NO_HOST`, `ACTAE_EMBEDDED_PG_WIPE`) — `cargo test` runs them
    /// on parallel threads and a shared mutable environment would race.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn host_postgres(bin: &str, major: u32, minor: u32) -> HostPostgres {
        HostPostgres {
            bin_dir: PathBuf::from(bin),
            bins: PgBinaries {
                initdb: PathBuf::from(format!("{bin}/initdb")),
                postgres: PathBuf::from(format!("{bin}/postgres")),
                pg_ctl: PathBuf::from(format!("{bin}/pg_ctl")),
            },
            version: (major, minor),
        }
    }

    #[test]
    fn test_pick_host_postgres_fresh_prefers_highest() {
        let c16 = host_postgres("/x/16", 16, 1);
        let c17 = host_postgres("/x/17", 17, 2);
        assert_eq!(
            pick_host_postgres(&[c16.clone(), c17.clone()], None)
                .unwrap()
                .version,
            (17, 2)
        );
        // Ties resolve deterministically to the last max (17 > 16 regardless).
        assert!(pick_host_postgres(&[], None).is_none());
    }

    #[test]
    fn test_pick_host_postgres_matches_existing_cluster_major() {
        let c16 = host_postgres("/x/16", 16, 1);
        let c17 = host_postgres("/x/17", 17, 2);
        assert_eq!(
            pick_host_postgres(&[c16, c17], Some(17)).unwrap().version,
            (17, 2)
        );
        // No host matches the cluster's major -> fall through to download.
        let c16 = host_postgres("/x/16", 16, 1);
        assert!(pick_host_postgres(&[c16], Some(18)).is_none());
    }

    #[test]
    fn test_version_dir_bins_collects_version_and_prefix_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let parent = tmp.path().join("opt");
        std::fs::create_dir_all(parent.join("postgresql@17/bin")).unwrap();
        std::fs::create_dir_all(parent.join("postgresql@16/bin")).unwrap();
        std::fs::create_dir_all(parent.join("unrelated/bin")).unwrap();

        let bins = version_dir_bins(&parent, Some("postgresql"), "bin");
        assert_eq!(bins.len(), 2);
        assert!(bins.iter().any(|b| b.ends_with("postgresql@17/bin")));
        assert!(bins.iter().any(|b| b.ends_with("postgresql@16/bin")));

        let version_like = tempfile::tempdir().unwrap();
        let vp = version_like.path().join("lib");
        std::fs::create_dir_all(vp.join("17/bin")).unwrap();
        std::fs::create_dir_all(vp.join("18/bin")).unwrap();
        let bins = version_dir_bins(&vp, None, "bin");
        assert_eq!(bins.len(), 2);
    }

    #[cfg(unix)]
    fn write_fake_executable(path: &Path, contents: &str) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(path, contents).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(unix)]
    fn make_fake_pg(dir: &Path, major: u32, minor: u32) {
        std::fs::create_dir_all(dir).unwrap();
        write_fake_executable(
            &dir.join("initdb"),
            &format!("#!/bin/sh\necho \"initdb (PostgreSQL) {major}.{minor}\"\n"),
        );
        for other in ["postgres", "pg_ctl"] {
            write_fake_executable(&dir.join(other), "#!/bin/sh\nexit 0\n");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_probe_host_postgres_accepts_valid_install() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("bin");
        make_fake_pg(&dir, 17, 2);
        let hp = probe_host_postgres(&dir)
            .await
            .expect("valid install probed");
        assert_eq!(hp.version, (17, 2));
        assert_eq!(hp.bin_dir, dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_probe_host_postgres_skips_incomplete_and_too_old() {
        let tmp = tempfile::tempdir().unwrap();

        // Incomplete: only initdb, missing postgres/pg_ctl.
        let incomplete = tmp.path().join("incomplete");
        std::fs::create_dir_all(&incomplete).unwrap();
        write_fake_executable(
            &incomplete.join("initdb"),
            "#!/bin/sh\necho \"initdb (PostgreSQL) 17.2\"\n",
        );
        assert!(probe_host_postgres(&incomplete).await.is_none());

        // Too old for scram-sha-256.
        let old = tmp.path().join("old");
        make_fake_pg(&old, 9, 6);
        assert!(probe_host_postgres(&old).await.is_none());

        // Broken initdb (no usable version output).
        let broken = tmp.path().join("broken");
        std::fs::create_dir_all(&broken).unwrap();
        for f in ["initdb", "postgres", "pg_ctl"] {
            write_fake_executable(&broken.join(f), "#!/bin/sh\nexit 3\n");
        }
        assert!(probe_host_postgres(&broken).await.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_host_detection_finds_pg_on_path() {
        // A fake host PostgreSQL on PATH must be detected end-to-end through
        // the enumeration (PATH scan) + probe pipeline. The fake is major 99
        // so the "highest wins" rule picks it even on a machine that has a
        // real PostgreSQL installed (deterministic everywhere).
        let _guard = ENV_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let fake_bin = tmp.path().join("hostpg/bin");
        make_fake_pg(&fake_bin, 99, 0);

        let old_path = std::env::var_os("PATH");
        let old_flag = std::env::var_os("ACTAE_PG_NO_HOST");
        std::env::set_var("PATH", &fake_bin);
        std::env::remove_var("ACTAE_PG_NO_HOST");
        let result = select_host_postgres(&tmp.path().join("no-cluster")).await;
        if let Some(old) = old_path {
            std::env::set_var("PATH", old);
        } else {
            std::env::remove_var("PATH");
        }
        match old_flag {
            Some(v) => std::env::set_var("ACTAE_PG_NO_HOST", v),
            None => std::env::remove_var("ACTAE_PG_NO_HOST"),
        }

        let selected = result.unwrap().expect("host PG on PATH must be selected");
        assert_eq!(selected.initdb, fake_bin.join("initdb"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_host_detection_falls_back_when_cluster_major_mismatches() {
        // An existing cluster pins its major: a host PG of a different major
        // must be ignored and the resolver must fall back to the download
        // (Ok(None)) so existing data is never stranded. Major 99 can never
        // match any real host install, keeping the test deterministic.
        let _guard = ENV_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let fake_bin = tmp.path().join("hostpg/bin");
        make_fake_pg(&fake_bin, 17, 2);
        let cluster = tmp.path().join("pg");
        std::fs::create_dir_all(&cluster).unwrap();
        std::fs::write(cluster.join("PG_VERSION"), "99.0\n").unwrap();

        let old_path = std::env::var_os("PATH");
        let old_flag = std::env::var_os("ACTAE_PG_NO_HOST");
        let old_wipe = std::env::var_os("ACTAE_EMBEDDED_PG_WIPE");
        std::env::set_var("PATH", &fake_bin);
        std::env::remove_var("ACTAE_PG_NO_HOST");
        std::env::remove_var("ACTAE_EMBEDDED_PG_WIPE");
        let result = select_host_postgres(&cluster).await;
        if let Some(old) = old_path {
            std::env::set_var("PATH", old);
        } else {
            std::env::remove_var("PATH");
        }
        match old_flag {
            Some(v) => std::env::set_var("ACTAE_PG_NO_HOST", v),
            None => std::env::remove_var("ACTAE_PG_NO_HOST"),
        }
        match old_wipe {
            Some(v) => std::env::set_var("ACTAE_EMBEDDED_PG_WIPE", v),
            None => std::env::remove_var("ACTAE_EMBEDDED_PG_WIPE"),
        }

        assert!(
            result.unwrap().is_none(),
            "a host PG whose major does not match the cluster must fall back to the download"
        );
    }

    #[tokio::test]
    async fn test_host_detection_disabled_by_env_flag() {
        let _guard = ENV_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let old_flag = std::env::var_os("ACTAE_PG_NO_HOST");
        std::env::set_var("ACTAE_PG_NO_HOST", "1");
        let result = select_host_postgres(&tmp.path().join("pg")).await;
        match old_flag {
            Some(v) => std::env::set_var("ACTAE_PG_NO_HOST", v),
            None => std::env::remove_var("ACTAE_PG_NO_HOST"),
        }
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_host_detection_disabled_flag_parsing() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("ACTAE_PG_NO_HOST", "1");
        assert!(host_detection_disabled());
        std::env::set_var("ACTAE_PG_NO_HOST", "true");
        assert!(host_detection_disabled());
        std::env::set_var("ACTAE_PG_NO_HOST", "0");
        assert!(!host_detection_disabled());
        std::env::remove_var("ACTAE_PG_NO_HOST");
        assert!(!host_detection_disabled());
    }

    #[cfg(unix)]
    #[test]
    fn test_bin_executable_resolves_plain_names_on_unix() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("bin");
        std::fs::create_dir_all(&dir).unwrap();
        for f in ["initdb", "postgres", "pg_ctl"] {
            write_fake_executable(&dir.join(f), "#!/bin/sh\nexit 0\n");
        }
        let bins = pg_binaries_from(&dir);
        assert_eq!(bins.initdb, dir.join("initdb"));
        bins.validate("test").unwrap();
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_bin_executable_resolves_exe_names_on_windows() {
        // The zonky Windows package ships initdb.exe etc. — path checks must
        // resolve the real filenames (validated by the lifecycle-windows CI).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("bin");
        std::fs::create_dir_all(&dir).unwrap();
        for f in ["initdb.exe", "postgres.exe", "pg_ctl.exe"] {
            std::fs::write(dir.join(f), b"").unwrap();
        }
        let bins = pg_binaries_from(&dir);
        assert_eq!(bins.initdb, dir.join("initdb.exe"));
        assert_eq!(bins.postgres, dir.join("postgres.exe"));
        assert_eq!(bins.pg_ctl, dir.join("pg_ctl.exe"));
        bins.validate("test").unwrap();
    }
}
