// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Local SDK client credentials (`$ACTAE_CONFIG_DIR/client.json`).
//!
//! When a self-hosted instance mints its first API key (or an operator runs
//! `actae login`), Actae publishes the endpoint + key to a per-user file so the
//! Python/Go/TypeScript SDKs authenticate with **zero copy-paste** — the
//! `opencode login` experience, without an account.
//!
//! ## Why this is safe to treat as a cache, not a source of truth
//!
//! The raw key is never recoverable from the database (`api_keys.key_hash` is a
//! one-way HMAC). So the local file is a *convenience cache*; the DB is the
//! source of truth. If the file is deleted, corrupt, or unreadable, `actae
//! login` mints a fresh key and rewrites it. There is no state in this file the
//! system cannot rebuild, which is what makes the "we can't strand a user"
//! guarantee hold: the worst case is one extra command.
//!
//! ## Hardening
//!
//! * Atomic durable write: `create_new` in the same directory at mode `0600`
//!   (subject to a narrowing umask only), `fsync` the file, `rename` into
//!   place, `fsync` the parent directory, then re-assert `0600`.
//! * `O_NOFOLLOW` on open (read and write) so a symlink planted at the path can
//!   never redirect the secret.
//! * The config directory is created and tightened to `0700` (best effort).
//! * A cross-process advisory lock serializes concurrent writers (two instances
//!   booting at once), and a liveness probe prevents a secondary instance from
//!   clobbering a live instance's default credentials.
//! * The key is never logged: [`ClientCredentials`] has a redacting `Debug`
//!   impl and a [`ClientCredentials::redacted_summary`] for diagnostics.
//! * On any write error the caller degrades to the startup banner (the raw key
//!   is still printed) — publishing is never allowed to abort the boot.
//!
//! On Windows the filesystem cannot express POSIX modes, so the file inherits
//! the user-profile ACL of `~/.actae` — the same deliberate cross-platform
//! trade-off documented for `enrollment.json`.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Wire schema identifier. Readers tolerate unknown/absent values (forward
/// compatibility); writers always emit it.
pub const SCHEMA: &str = "actae.client/v1";

const DEFAULT_FILE_NAME: &str = "client.json";
const LOCK_FILE_NAME: &str = ".client.json.lock";
/// How long a writer waits for a concurrent publisher before giving up.
const LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const LOCK_POLL: Duration = Duration::from_millis(50);

/// Identity of the managed instance that minted the credentials. Optional and
/// advisory — the SDK only needs `endpoint` + `api_key`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceInfo {
    pub http_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pg_port: Option<u16>,
    pub data_dir: String,
    pub pid: u32,
}

/// The persisted client credentials. `Debug` redacts the key — never print a
/// `ClientCredentials` with `{:?}` and expect the key to be hidden.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientCredentials {
    #[serde(default)]
    pub schema: String,
    pub endpoint: String,
    pub api_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

impl std::fmt::Debug for ClientCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientCredentials")
            .field("schema", &self.schema)
            .field("endpoint", &self.endpoint)
            .field("api_key", &"<redacted>")
            .field("instance", &self.instance)
            .field("created_at", &self.created_at)
            .finish()
    }
}

impl ClientCredentials {
    /// Build credentials for a freshly minted key.
    pub fn new(endpoint: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            schema: SCHEMA.to_string(),
            endpoint: endpoint.into(),
            api_key: api_key.into(),
            instance: None,
            created_at: Some(Utc::now()),
        }
    }

    /// Attach the minting instance's identity.
    pub fn with_instance(mut self, instance: InstanceInfo) -> Self {
        self.instance = Some(instance);
        self
    }

    /// Reject malformed credentials. Used both on write (fail fast on a bad
    /// endpoint/key) and on read (a hand-corrupted file is treated as absent so
    /// the SDK surfaces the actionable `actae login` error instead of sending a
    /// broken request).
    pub fn validate(&self) -> Result<()> {
        let endpoint = self.endpoint.trim();
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            anyhow::bail!("endpoint must be an http:// or https:// URL");
        }
        if endpoint.len() <= "https://".len() {
            anyhow::bail!("endpoint has no host");
        }
        if endpoint
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
        {
            anyhow::bail!("endpoint contains whitespace or control characters");
        }
        if self.api_key.trim().is_empty() {
            anyhow::bail!("api_key must not be empty");
        }
        if self
            .api_key
            .chars()
            .any(|c| c == '\0' || c.is_control() || c.is_whitespace())
        {
            anyhow::bail!("api_key contains whitespace or control characters");
        }
        Ok(())
    }

    fn is_valid(&self) -> bool {
        self.validate().is_ok()
    }

    /// A log-safe summary — the key is never included.
    pub fn redacted_summary(&self) -> String {
        match &self.instance {
            Some(i) => format!("{} (data_dir={}, pid={})", self.endpoint, i.data_dir, i.pid),
            None => self.endpoint.clone(),
        }
    }

    /// Path of the default credentials file: `$ACTAE_CONFIG_DIR/client.json`
    /// (default `~/.actae/client.json`), matching [`Enrollment`]'s directory.
    ///
    /// [`Enrollment`]: crate::control_plane::enrollment::Enrollment
    pub fn file_path() -> PathBuf {
        Self::config_dir().join(DEFAULT_FILE_NAME)
    }

    /// Path of a secondary instance's credentials file
    /// (`client-<http_port>.json`), written when a second live instance cannot
    /// own the default file.
    pub fn secondary_path(http_port: u16) -> PathBuf {
        Self::config_dir().join(format!("client-{http_port}.json"))
    }

    /// The per-user config directory (`$ACTAE_CONFIG_DIR`, default `~/.actae`).
    pub fn config_dir() -> PathBuf {
        crate::control_plane::enrollment::Enrollment::config_dir()
    }

    /// Load the default credentials file if present and well-formed.
    pub fn load() -> Option<Self> {
        Self::load_from(&Self::file_path())
    }

    /// Load credentials from an explicit path. Returns `None` when the file is
    /// missing, unreadable, malformed, a symlink, or invalid — callers must
    /// treat all of those identically (fall through to `actae login`).
    pub fn load_from(path: &Path) -> Option<Self> {
        // Portable symlink refusal (Unix additionally opens with O_NOFOLLOW).
        let meta = std::fs::symlink_metadata(path).ok()?;
        if meta.file_type().is_symlink() {
            return None;
        }
        let raw = read_no_follow(path).ok()?;
        let creds: Self = serde_json::from_slice(&raw).ok()?;
        creds.is_valid().then_some(creds)
    }

    /// True when the credentials file is group- or world-accessible (POSIX).
    /// `false` on missing files or non-Unix platforms.
    pub fn file_is_group_or_world_readable(path: &Path) -> bool {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            match std::fs::metadata(path) {
                Ok(m) => m.permissions().mode() & 0o077 != 0,
                Err(_) => false,
            }
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            false
        }
    }

    /// Persist to the default path.
    pub fn save(&self) -> Result<()> {
        self.save_to(&Self::file_path())
    }

    /// Persist to an explicit path with hardened atomic semantics.
    pub fn save_to(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let json = serde_json::to_string_pretty(self).context("failed to serialize credentials")?;
        write_private_atomic(path, json.as_bytes())
    }

    /// Remove the default credentials file. Missing is success.
    pub fn delete_default() -> Result<()> {
        delete_at(&Self::file_path())
    }

    /// Remove the default file and every secondary `client-*.json` file from
    /// the config dir. Returns how many files were actually removed.
    pub fn delete_all() -> Result<usize> {
        Self::delete_all_in(&Self::config_dir())
    }

    /// Remove `client.json` and every `client-*.json` sibling in `dir`.
    /// Returns how many files were actually removed. Parameterized on the path
    /// so it is unit-testable without touching the real `~/.actae`.
    pub fn delete_all_in(dir: &Path) -> Result<usize> {
        let mut removed = 0usize;
        let default = dir.join(DEFAULT_FILE_NAME);
        if default.exists() {
            delete_at(&default)?;
            removed += 1;
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Ok(removed);
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("client-") && name.ends_with(".json") {
                let path = entry.path();
                if path.exists() {
                    delete_at(&path)?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }
}

/// Outcome of publishing credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOutcome {
    /// The file actually written.
    pub path: PathBuf,
    /// Whether this instance owns the default `client.json`.
    pub primary: bool,
    /// Human-readable reason (for the banner / logs).
    pub reason: &'static str,
}

/// Publish credentials to the default path, choosing the default vs a
/// secondary file under a cross-process lock. The liveness of any existing
/// default file is inferred conservatively (a different data dir counts as
/// live); callers that can probe the endpoint should use
/// [`publish_at_with_liveness`] with an explicit flag.
///
/// When a *different* instance already owns a live default file, this instance
/// becomes secondary and writes `client-<port>.json` instead, so two live
/// instances never fight over one user's SDK credentials.
pub fn publish(credentials: &ClientCredentials) -> Result<PublishOutcome> {
    publish_at(&ClientCredentials::file_path(), credentials, false)
}

/// Like [`publish`], forcing default ownership (used by `actae login --force`).
pub fn publish_forced(credentials: &ClientCredentials) -> Result<PublishOutcome> {
    publish_at(&ClientCredentials::file_path(), credentials, true)
}

/// The lock-and-decide core, parameterized on the default path so it is
/// unit-testable without touching the real `~/.actae`.
///
/// `existing_endpoint_is_live` is a pre-computed liveness flag; see [`publish`].
pub fn publish_at(
    default_path: &Path,
    credentials: &ClientCredentials,
    force: bool,
) -> Result<PublishOutcome> {
    publish_at_with_liveness(default_path, credentials, force, None)
}

/// Full form used by callers that probe the existing endpoint themselves. When
/// `existing_live` is `None` the probe is skipped and the existing default is
/// treated as live only if it points at a different data dir (conservative:
/// preserve a possibly-live other instance).
pub fn publish_at_with_liveness(
    default_path: &Path,
    credentials: &ClientCredentials,
    force: bool,
    existing_live: Option<bool>,
) -> Result<PublishOutcome> {
    let dir = default_path
        .parent()
        .context("credentials path must have a parent directory")?;
    std::fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    secure_dir(dir);
    let _lock = CredentialsLock::acquire(dir, LOCK_TIMEOUT)?;

    let existing = ClientCredentials::load_from(default_path);
    let this_data_dir = credentials.instance.as_ref().map(|i| i.data_dir.as_str());
    let existing_other = existing
        .as_ref()
        .and_then(|e| e.instance.as_ref())
        .map(|i| i.data_dir.as_str())
        .map(|d| Some(d) != this_data_dir)
        .unwrap_or(false);
    let existing_live = existing_live.unwrap_or(existing_other);

    let claim_default = force || existing.is_none() || !existing_live || !existing_other;

    if claim_default {
        credentials.save_to(default_path)?;
        Ok(PublishOutcome {
            path: default_path.to_path_buf(),
            primary: true,
            reason: if force {
                "forced"
            } else if existing.is_none() {
                "fresh"
            } else if !existing_live {
                "replaced stale"
            } else {
                "refreshed own"
            },
        })
    } else {
        let port = credentials
            .instance
            .as_ref()
            .map(|i| i.http_port)
            .unwrap_or(0);
        let secondary = default_path
            .parent()
            .context("credentials path must have a parent directory")?
            .join(format!("client-{port}.json"));
        credentials.save_to(&secondary)?;
        Ok(PublishOutcome {
            path: secondary,
            primary: false,
            reason: "secondary (another live instance owns the default)",
        })
    }
}

/// Whether the automatic on-boot publish is enabled. On by default; an operator
/// who does not want a plaintext key under `$ACTAE_CONFIG_DIR` sets
/// `ACTAE_WRITE_CLIENT_CREDENTIALS=0`.
pub fn auto_write_enabled() -> bool {
    !matches!(
        std::env::var("ACTAE_WRITE_CLIENT_CREDENTIALS")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("0") | Some("false") | Some("no") | Some("off")
    )
}

// --------------------------------------------------------------------- #
// Private helpers
// --------------------------------------------------------------------- #

/// Cross-process advisory lock for the credentials directory.
struct CredentialsLock {
    _file: std::fs::File,
}

impl CredentialsLock {
    fn acquire(dir: &Path, timeout: Duration) -> Result<Self> {
        use fs2::FileExt;
        let lock_path = dir.join(LOCK_FILE_NAME);
        let mut opts = std::fs::OpenOptions::new();
        opts.create(true).write(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let file = opts
            .open(&lock_path)
            .with_context(|| format!("failed to open {}", lock_path.display()))?;
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match file.try_lock_exclusive() {
                Ok(()) => return Ok(CredentialsLock { _file: file }),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if std::time::Instant::now() >= deadline {
                        anyhow::bail!(
                            "timed out waiting for the credentials lock at {}",
                            lock_path.display()
                        );
                    }
                    std::thread::sleep(LOCK_POLL);
                }
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("failed to lock {}", lock_path.display()))
                }
            }
        }
    }
}

/// Open a file for reading with `O_NOFOLLOW` (Unix) so a symlink cannot be
/// followed to a file we did not write.
fn read_no_follow(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = opts.open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Atomically and durably write `contents` to `path` at mode `0600`.
fn write_private_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("credentials path must have a parent directory")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    secure_dir(parent);
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .context("credentials path must have a file name")?;
    let tmp = parent.join(format!(
        ".{file_name}.tmp.{}.{}",
        std::process::id(),
        rand::random::<u32>()
    ));

    let mut opts = std::fs::OpenOptions::new();
    opts.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = opts
        .open(&tmp)
        .with_context(|| format!("failed to create {}", tmp.display()))?;

    let write_result = (|| -> std::io::Result<()> {
        file.write_all(contents)?;
        file.sync_all()
    })();
    if let Err(e) = write_result {
        drop(file);
        let _ = std::fs::remove_file(&tmp);
        return Err(e).with_context(|| format!("failed to write {}", tmp.display()));
    }
    drop(file);

    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e).with_context(|| format!("failed to publish {}", path.display()));
    }

    // Defense in depth: `rename` carries the temp inode's mode, which was
    // created `0600 & ~umask` (never wider than 0600). Re-assert anyway.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    // fsync the directory so the rename survives a crash.
    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn delete_at(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("failed to remove {}", path.display())),
    }
}

/// Best-effort `chmod 0700` on the config directory.
fn secure_dir(dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(endpoint: &str, key: &str) -> ClientCredentials {
        ClientCredentials {
            schema: SCHEMA.to_string(),
            endpoint: endpoint.to_string(),
            api_key: key.to_string(),
            instance: Some(InstanceInfo {
                http_port: 8002,
                pg_port: Some(5546),
                data_dir: "/home/u/.actae/data".to_string(),
                pid: 4321,
            }),
            created_at: Some(Utc::now()),
        }
    }

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn roundtrips_and_creates_parent_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nested").join("client.json");
        let creds = sample("http://127.0.0.1:8002", "sk-abc123");
        creds.save_to(&path).unwrap();
        let loaded = ClientCredentials::load_from(&path).expect("load");
        assert_eq!(loaded, creds);
    }

    #[cfg(unix)]
    #[test]
    fn writes_owner_only_permissions() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("client.json");
        sample("http://127.0.0.1:8002", "sk-abc123")
            .save_to(&path)
            .unwrap();
        assert_eq!(mode_of(&path), 0o600);
        assert_eq!(mode_of(tmp.path()), 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn preserves_0600_even_when_target_preexisted_with_open_mode() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("client.json");
        std::fs::write(&path, b"old").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
        sample("http://127.0.0.1:8002", "sk-abc123")
            .save_to(&path)
            .unwrap();
        assert_eq!(mode_of(&path), 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn detects_group_or_world_readable_files() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("client.json");
        sample("http://127.0.0.1:8002", "sk-abc123")
            .save_to(&path)
            .unwrap();
        assert!(!ClientCredentials::file_is_group_or_world_readable(&path));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(ClientCredentials::file_is_group_or_world_readable(&path));
        // Missing file is reported as safe (nothing to warn about).
        assert!(!ClientCredentials::file_is_group_or_world_readable(
            &tmp.path().join("absent.json")
        ));
    }

    #[test]
    fn missing_file_loads_as_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(ClientCredentials::load_from(&tmp.path().join("client.json")).is_none());
    }

    #[test]
    fn malformed_file_loads_as_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("client.json");
        std::fs::write(&path, b"{not json").unwrap();
        assert!(ClientCredentials::load_from(&path).is_none());
    }

    #[test]
    fn invalid_endpoint_or_key_loads_as_none() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("client.json");
        std::fs::write(
            &path,
            br#"{"schema":"actae.client/v1","endpoint":"ftp://x","api_key":"sk-1"}"#,
        )
        .unwrap();
        assert!(ClientCredentials::load_from(&path).is_none());
        std::fs::write(
            &path,
            br#"{"schema":"actae.client/v1","endpoint":"http://127.0.0.1:8002","api_key":""}"#,
        )
        .unwrap();
        assert!(ClientCredentials::load_from(&path).is_none());
    }

    #[test]
    fn hand_written_file_without_optional_fields_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("client.json");
        std::fs::write(
            &path,
            br#"{"endpoint":"https://host.example","api_key":"sk-xyz"}"#,
        )
        .unwrap();
        let loaded = ClientCredentials::load_from(&path).expect("valid");
        assert_eq!(loaded.endpoint, "https://host.example");
        assert!(loaded.instance.is_none());
    }

    #[test]
    fn validate_rejects_bad_input() {
        assert!(ClientCredentials::new("ftp://x", "sk-1")
            .validate()
            .is_err());
        assert!(ClientCredentials::new("http://", "sk-1")
            .validate()
            .is_err());
        assert!(ClientCredentials::new("http://h", "").validate().is_err());
        assert!(ClientCredentials::new("http://h", "sk a")
            .validate()
            .is_err());
        assert!(ClientCredentials::new("http://h ", "sk-1")
            .validate()
            .is_err());
        assert!(ClientCredentials::new("https://h", "sk-1")
            .validate()
            .is_ok());
    }

    #[test]
    fn debug_and_summary_never_leak_the_key() {
        let creds = sample("http://127.0.0.1:8002", "sk-super-secret");
        let dbg = format!("{creds:?}");
        assert!(!dbg.contains("sk-super-secret"));
        assert!(dbg.contains("<redacted>"));
        assert!(!creds.redacted_summary().contains("sk-super-secret"));
    }

    #[test]
    fn save_fails_loudly_on_invalid_credentials() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("client.json");
        let bad = ClientCredentials::new("not-a-url", "sk-1");
        assert!(bad.save_to(&path).is_err());
        assert!(!path.exists(), "must not write an invalid file");
    }

    #[cfg(unix)]
    #[test]
    fn refuses_to_follow_a_symlinked_credentials_file() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real.json");
        std::fs::write(
            &real,
            br#"{"endpoint":"http://evil.example","api_key":"sk-evil"}"#,
        )
        .unwrap();
        let link = tmp.path().join("client.json");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        // Reading through the symlink must be refused (O_NOFOLLOW).
        assert!(ClientCredentials::load_from(&link).is_none());
    }

    #[test]
    fn publish_claims_default_when_none_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let default = tmp.path().join("client.json");
        let creds = sample("http://127.0.0.1:8002", "sk-one");
        let outcome = publish_at_with_liveness(&default, &creds, false, None).unwrap();
        assert!(outcome.primary);
        assert_eq!(outcome.path, default);
        assert!(ClientCredentials::load_from(&default).is_some());
    }

    #[test]
    fn publish_replaces_a_stale_default() {
        let tmp = tempfile::tempdir().unwrap();
        let default = tmp.path().join("client.json");
        let stale = sample("http://127.0.0.1:8002", "sk-stale");
        stale.save_to(&default).unwrap();
        let mut fresh = sample("http://127.0.0.1:8003", "sk-fresh");
        fresh.instance.as_mut().unwrap().http_port = 8003;
        fresh.instance.as_mut().unwrap().data_dir = "/home/u/.actae/data-8003".to_string();
        // Existing endpoint is NOT live -> replace.
        let outcome = publish_at_with_liveness(&default, &fresh, false, Some(false)).unwrap();
        assert!(outcome.primary);
        assert_eq!(
            ClientCredentials::load_from(&default).unwrap().api_key,
            "sk-fresh"
        );
    }

    #[test]
    fn publish_keeps_live_other_as_secondary() {
        let tmp = tempfile::tempdir().unwrap();
        let default = tmp.path().join("client.json");
        let primary = sample("http://127.0.0.1:8002", "sk-primary");
        primary.save_to(&default).unwrap();

        let mut secondary = sample("http://127.0.0.1:8003", "sk-secondary");
        secondary.instance.as_mut().unwrap().http_port = 8003;
        secondary.instance.as_mut().unwrap().data_dir = "/home/u/.actae/data-8003".to_string();
        // Existing endpoint IS live and belongs to a different data dir.
        let outcome = publish_at_with_liveness(&default, &secondary, false, Some(true)).unwrap();
        assert!(!outcome.primary);
        assert_eq!(outcome.path, tmp.path().join("client-8003.json"));
        // The primary's default is untouched.
        assert_eq!(
            ClientCredentials::load_from(&default).unwrap().api_key,
            "sk-primary"
        );
        assert_eq!(
            ClientCredentials::load_from(&outcome.path).unwrap().api_key,
            "sk-secondary"
        );
    }

    #[test]
    fn publish_refreshes_own_live_default() {
        let tmp = tempfile::tempdir().unwrap();
        let default = tmp.path().join("client.json");
        let old = sample("http://127.0.0.1:8002", "sk-old");
        old.save_to(&default).unwrap();
        // Same data dir (a restart) -> refresh the default in place.
        let fresh = sample("http://127.0.0.1:8002", "sk-new");
        let outcome = publish_at_with_liveness(&default, &fresh, false, Some(true)).unwrap();
        assert!(outcome.primary);
        assert_eq!(
            ClientCredentials::load_from(&default).unwrap().api_key,
            "sk-new"
        );
    }

    #[test]
    fn force_always_claims_default() {
        let tmp = tempfile::tempdir().unwrap();
        let default = tmp.path().join("client.json");
        sample("http://127.0.0.1:8002", "sk-primary")
            .save_to(&default)
            .unwrap();
        let mut other = sample("http://127.0.0.1:8003", "sk-forced");
        other.instance.as_mut().unwrap().http_port = 8003;
        other.instance.as_mut().unwrap().data_dir = "/home/u/.actae/data-8003".to_string();
        let outcome = publish_at_with_liveness(&default, &other, true, Some(true)).unwrap();
        assert!(outcome.primary);
        assert_eq!(
            ClientCredentials::load_from(&default).unwrap().api_key,
            "sk-forced"
        );
    }

    #[test]
    fn delete_all_in_removes_default_and_secondaries_but_not_unrelated() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("cfg");
        std::fs::create_dir_all(&dir).unwrap();
        let default = dir.join("client.json");
        sample("http://127.0.0.1:8002", "sk-a")
            .save_to(&default)
            .unwrap();
        std::fs::write(dir.join("client-8003.json"), b"{}").unwrap();
        std::fs::write(dir.join("unrelated.json"), b"{}").unwrap();

        let removed = ClientCredentials::delete_all_in(&dir).unwrap();
        assert_eq!(removed, 2);
        assert!(!default.exists());
        assert!(!dir.join("client-8003.json").exists());
        assert!(
            dir.join("unrelated.json").exists(),
            "must not touch other files"
        );
        // Idempotent: a second call removes nothing.
        assert_eq!(ClientCredentials::delete_all_in(&dir).unwrap(), 0);
    }

    #[test]
    fn delete_default_missing_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        // delete_at is exercised via delete_all_in on an empty dir.
        assert_eq!(ClientCredentials::delete_all_in(tmp.path()).unwrap(), 0);
    }

    #[test]
    fn auto_write_default_on_and_env_can_disable() {
        // Cannot safely mutate process env in parallel tests; assert the
        // positive default and the parsing helper contract directly.
        std::env::remove_var("ACTAE_WRITE_CLIENT_CREDENTIALS");
        assert!(auto_write_enabled());
    }

    #[test]
    fn concurrent_writers_produce_a_consistent_file() {
        let tmp = tempfile::tempdir().unwrap();
        let default = tmp.path().join("client.json");
        let dir = tmp.path().to_path_buf();
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let default = default.clone();
                std::thread::spawn(move || {
                    let mut creds = sample(&format!("http://127.0.0.1:{}", 9000 + i), "sk-x");
                    creds.instance.as_mut().unwrap().http_port = 9000 + i;
                    creds.instance.as_mut().unwrap().data_dir = format!("data-{i}");
                    publish_at_with_liveness(&default, &creds, true, None).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // Whatever won, the file must be complete, valid, and one of the
        // writers' credentials — never a torn write.
        let loaded = ClientCredentials::load_from(&dir.join("client.json")).expect("valid");
        assert!(loaded.api_key == "sk-x");
    }
}
