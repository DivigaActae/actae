// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Standalone manager for the community build.
//!
//! The managed control plane is a separate product. This module keeps the same
//! public surface that the rest of the runtime consumes (`ControlPlaneManager`
//! and its status methods) but implements it locally: no enrollment, no HTTP
//! client, no sync loop, no subscription enforcement.
//!
//! Local dashboard tokens and license-token verification remain in
//! [`license`]; the enrollment file format remains in [`enrollment`] so an
//! operator can still keep `~/.actae` state if they move to the managed build.

pub mod client;
pub mod enrollment;
pub mod license;

#[cfg(test)]
pub(crate) mod test_util;

use crate::config::AppConfig;
use crate::control_plane::enrollment::Enrollment;
use anyhow::Result;
use deadpool_postgres::Pool;
use std::sync::Arc;

/// Kept for source compatibility with call sites that format status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnforcementStatus {
    Standalone,
    Active,
    LockedInactive,
    Stale,
}

/// Why a request was rejected. The community build never locks, so this is
/// only constructed by call sites that match on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockReason {
    pub http_status: u16,
    pub message: String,
}

/// Local-only manager. The name is retained so the rest of the runtime does not
/// need to change; there is no remote counterpart in this build.
pub struct ControlPlaneManager {
    config: Arc<AppConfig>,
    #[allow(dead_code)]
    client_pool: Pool,
    revoked_signal: Arc<tokio::sync::Notify>,
}

impl ControlPlaneManager {
    /// Always standalone. Configuration pointing at a control plane is ignored
    /// with a warning rather than failing the boot.
    pub fn new(config: Arc<AppConfig>, client_pool: Pool) -> Result<Self> {
        if config.control_plane_url.is_some()
            || config.install_id.is_some()
            || config.enroll_secret.is_some()
        {
            tracing::warn!(
                "control-plane configuration ignored: this build is standalone and has no \
                 enrollment, sync, or subscription enforcement"
            );
        } else {
            tracing::info!("running standalone (no control plane, no subscription enforcement)");
        }

        Ok(Self {
            config,
            client_pool,
            revoked_signal: Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// Test helper kept for parity with the previous API.
    #[cfg(test)]
    pub fn new_standalone(config: Arc<AppConfig>, client_pool: Pool) -> Result<Self> {
        Self::new(config, client_pool)
    }

    pub fn is_enrolled(&self) -> bool {
        false
    }

    /// No enrollment exists in this build.
    pub fn enrollment(&self) -> Option<&Enrollment> {
        None
    }

    /// Local federation trust anchors only. Nothing is distributed.
    pub fn federation_config(&self) -> crate::auth::federation::FederationConfig {
        let local = &self.config.federation;
        crate::auth::federation::FederationConfig {
            enabled: local.enabled,
            issuer: local.issuer.clone(),
            audience: local.audience.clone(),
            verification_keys: local.verification_keys.clone(),
            revoked_key_ids: local.revoked_key_ids.clone(),
            clock_skew_secs: local.clock_skew_secs,
        }
    }

    /// The operator's policy applies as-is; there is no central policy.
    pub fn effective_source_policy(&self) -> crate::auth::source_policy::FederationSourcePolicy {
        self.config.federation_source_policy.clone()
    }

    /// Never fires: nothing can be revoked remotely in this build.
    pub fn revoked_notifier(&self) -> Arc<tokio::sync::Notify> {
        self.revoked_signal.clone()
    }

    /// No background loop to start.
    pub fn start(&self) {}

    /// Nothing persisted to load.
    pub async fn load_persisted_state(&self) -> Result<()> {
        Ok(())
    }

    /// Dashboard SSO handoff is a managed-build feature. Local dashboard
    /// password auth is handled elsewhere and does not need a control plane.
    pub async fn authenticate_dashboard(
        &self,
        _email: &str,
        _password: &str,
    ) -> Result<crate::control_plane::license::DashboardClaims> {
        Err(anyhow::anyhow!(
            "dashboard SSO is not available in the community build"
        ))
    }

    pub fn enforcement_status(&self) -> EnforcementStatus {
        EnforcementStatus::Standalone
    }

    pub fn blocks_key(&self) -> Option<LockReason> {
        None
    }

    pub fn blocks_anonymous(&self) -> Option<LockReason> {
        None
    }

    pub fn remaining_grace_secs(&self) -> Option<i64> {
        None
    }

    pub fn lock_imminent(&self) -> bool {
        false
    }

    /// Shape kept stable for the dashboard and `/metrics` consumers.
    pub fn status_json(&self) -> sonic_rs::Value {
        sonic_rs::json!({
            "mode": "standalone",
            "status": "standalone",
            "subscription": "unknown",
            "last_sync_at": sonic_rs::Value::default(),
            "last_attempt_at": sonic_rs::Value::default(),
            "last_error": sonic_rs::Value::default(),
            "rev": 0,
            "remaining_grace_secs": sonic_rs::Value::default(),
            "lock_imminent": false,
            "docs_url": "",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_never_blocks() {
        let config = Arc::new(AppConfig::default());
        let manager = ControlPlaneManager {
            config,
            client_pool: crate::control_plane::test_util::dead_pool(),
            revoked_signal: Arc::new(tokio::sync::Notify::new()),
        };
        assert!(!manager.is_enrolled());
        assert!(manager.blocks_key().is_none());
        assert!(manager.blocks_anonymous().is_none());
        assert!(!manager.lock_imminent());
        assert_eq!(manager.enforcement_status(), EnforcementStatus::Standalone);
    }
}
