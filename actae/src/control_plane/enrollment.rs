// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Enrollment persistence for the control plane.
//!
//! The enrollment record (control plane URL, install id, and enrollment
//! secret) is the long-lived phone-home credential for one Actae instance.
//! It is stored in a 0600 file (`$ACTAE_CONFIG_DIR/enrollment.json`,
//! default `~/.actae/enrollment.json`) written by `actae auth login`, or
//! supplied via environment variables for container/IaC deployments.
//! The secret is a credential — it is never logged and never stored in
//! PostgreSQL.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

/// A linked Actae instance. `enrollment_secret` must never be logged or
/// serialized into any response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Enrollment {
    pub control_plane_url: String,
    pub install_id: String,
    pub enrollment_secret: String,
    pub enrolled_at: DateTime<Utc>,
    /// Ed25519 license token returned by the control plane at enrollment
    /// time (portal paste-to-enroll and device flow). Absent for the
    /// env-var / air-gapped flow, which never talks to the control plane
    /// during enrollment. Not a secret — it is a signed public claim.
    #[serde(default)]
    pub license: Option<String>,
}

impl Enrollment {
    /// Directory holding Actae's local state. Overridable for containers.
    pub fn config_dir() -> PathBuf {
        env::var("ACTAE_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                dirs_home()
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join(".actae")
            })
    }

    /// Path of the enrollment file.
    pub fn file_path() -> PathBuf {
        Self::config_dir().join("enrollment.json")
    }

    /// Load the enrollment file if present and parseable.
    pub fn load() -> Option<Enrollment> {
        Self::load_from(&Self::file_path())
    }

    /// Persist the enrollment to disk with 0600 permissions (owner-only).
    pub fn save(&self) -> Result<()> {
        self.save_to(&Self::file_path())
    }

    /// Load from an explicit path (used by tests and diagnostics).
    pub fn load_from(path: &PathBuf) -> Option<Enrollment> {
        let raw = fs::read(path).ok()?;
        let enrollment: Enrollment = serde_json::from_slice(&raw).ok()?;
        if enrollment.control_plane_url.trim().is_empty()
            || enrollment.install_id.trim().is_empty()
            || enrollment.enrollment_secret.trim().is_empty()
        {
            return None;
        }
        Some(enrollment)
    }

    /// Save to an explicit path (used by tests and diagnostics).
    pub fn save_to(&self, path: &PathBuf) -> Result<()> {
        let dir = path
            .parent()
            .context("enrollment file must have a parent directory")?;
        fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
        let json = serde_json::to_string_pretty(self).context("failed to serialize enrollment")?;
        let tmp_path = path.with_extension("json.tmp");
        fs::write(&tmp_path, json)
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
        restrict_permissions(&tmp_path)?;
        fs::rename(&tmp_path, path).with_context(|| {
            format!("failed to move enrollment into place at {}", path.display())
        })?;
        Ok(())
    }

    /// Remove the enrollment file.
    pub fn delete() -> Result<()> {
        Self::delete_at(&Self::file_path())
    }

    /// Remove the enrollment file at an explicit path.
    pub fn delete_at(path: &PathBuf) -> Result<()> {
        match fs::remove_file(path) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("failed to remove {}", path.display())),
        }
    }

    /// Display form with the secret redacted.
    pub fn redacted_display(&self) -> String {
        format!(
            "control_plane_url={}, install_id={}, enrollment_secret=<redacted>, enrolled_at={}",
            self.control_plane_url,
            self.install_id,
            self.enrolled_at.to_rfc3339()
        )
    }
}

/// Best-effort home directory lookup without the `dirs` crate.
fn dirs_home() -> Option<PathBuf> {
    env::var_os("HOME").map(PathBuf::from)
}

/// Restrict a file to owner-only access.
///
/// On Unix this is `chmod 0600` — the enrollment secret must never be
/// world/group readable. On Windows the standard library cannot express
/// POSIX modes; the file inherits the user-profile ACL of `~/.actae` (the
/// enrollment lives under the user's home directory, which is
/// owner-restricted by default). This is a documented, deliberate
/// cross-platform trade-off.
fn restrict_permissions(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Enrollment {
        Enrollment {
            control_plane_url: "https://saas.example.com".to_string(),
            install_id: "ins_123".to_string(),
            enrollment_secret: "es_super_secret".to_string(),
            enrolled_at: Utc::now(),
            license: None,
        }
    }

    #[test]
    fn test_save_load_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("enrollment.json");
        let e = sample();
        e.save_to(&path).unwrap();
        let loaded = Enrollment::load_from(&path).expect("must load after save");
        assert_eq!(loaded.install_id, e.install_id);
        assert_eq!(loaded.enrollment_secret, e.enrollment_secret);
        assert_eq!(loaded.control_plane_url, e.control_plane_url);
    }

    #[cfg(unix)]
    #[test]
    fn test_save_sets_0600_permissions() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("enrollment.json");
        sample().save_to(&path).unwrap();
        let meta = fs::metadata(&path).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
    }

    #[test]
    fn test_load_none_when_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("enrollment.json");
        assert!(Enrollment::load_from(&path).is_none());
    }

    #[test]
    fn test_load_rejects_empty_fields() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("enrollment.json");
        fs::write(
            &path,
            r#"{"control_plane_url":"","install_id":"","enrollment_secret":"","enrolled_at":"2026-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        assert!(Enrollment::load_from(&path).is_none());
    }

    #[test]
    fn test_load_rejects_invalid_json() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("enrollment.json");
        fs::write(&path, "not json").unwrap();
        assert!(Enrollment::load_from(&path).is_none());
    }

    #[test]
    fn test_delete_removes_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("enrollment.json");
        sample().save_to(&path).unwrap();
        assert!(path.exists());
        Enrollment::delete_at(&path).unwrap();
        assert!(!path.exists());
        Enrollment::delete_at(&path).unwrap();
    }

    #[test]
    fn test_redacted_display_hides_secret() {
        let e = sample();
        let display = e.redacted_display();
        assert!(!display.contains("es_super_secret"));
        assert!(display.contains("<redacted>"));
        assert!(display.contains("ins_123"));
    }
}
