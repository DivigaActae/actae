// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use crate::auth::password_hash::SecurePasswordHasher;
use crate::auth::AuthManager;
use crate::config::AppConfig;
use crate::control_plane::ControlPlaneManager;
use crate::database::{
    repositories::{ApiKeyRepository, SessionRepository, UserRepository},
    DatabaseManager,
};
use crate::websocket::batch_broadcaster::{BatchBroadcastConfig, BatchBroadcaster};
use crate::websocket::connection_manager::ConnectionManager;
use anyhow::Result;
use std::sync::Arc;

/// Application state containing all shared resources
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub db_manager: Arc<DatabaseManager>,
    pub user_repo: Arc<UserRepository>,
    pub api_key_repo: Arc<ApiKeyRepository>,
    pub session_repo: Arc<SessionRepository>,
    pub batch_broadcaster: Arc<BatchBroadcaster>,
    pub password_hasher: Arc<SecurePasswordHasher>,
    pub auth_manager: Arc<AuthManager>,
    pub control_plane: Arc<ControlPlaneManager>,
}

impl AppState {
    /// Creates a new application state with all resources initialized.
    /// The ConnectionManager must be passed to enable message delivery via BatchBroadcaster.
    /// Accepts an already-loaded config to avoid redundant I/O (the caller should load it once).
    pub async fn new(
        connection_manager: Option<Arc<ConnectionManager>>,
        config: Arc<AppConfig>,
    ) -> Result<Self> {
        let db_manager = Arc::new(DatabaseManager::new(&config).await?);

        // Create batch broadcaster with ConnectionManager to enable message delivery
        let batch_broadcaster = Arc::new(
            BatchBroadcaster::new_with_connection_manager(
                BatchBroadcastConfig::default(),
                connection_manager,
            )
            .await?,
        );

        // Control plane manager is constructed before client migrations run;
        // it touches the database lazily via the sync loop (started by main
        // after migrations), so creation here is safe.
        let control_plane = Arc::new(ControlPlaneManager::new(
            config.clone(),
            db_manager.client_pool.clone(),
        )?);

        Ok(Self {
            config: config.clone(),
            db_manager: db_manager.clone(),
            user_repo: Arc::new(UserRepository::new(db_manager.clone())),
            api_key_repo: Arc::new(ApiKeyRepository::new(db_manager.clone())),
            session_repo: Arc::new(SessionRepository::new(db_manager.clone())),
            batch_broadcaster,
            password_hasher: Arc::new(SecurePasswordHasher::with_secure_defaults()),
            auth_manager: Arc::new(AuthManager::new(
                db_manager.clone(),
                config.api_key_hash_secret.as_bytes().to_vec(),
            )),
            control_plane,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_state_is_clone() {
        fn _assert_clone<T: Clone>() {}
        _assert_clone::<AppState>();
    }

    #[test]
    fn test_app_state_is_send() {
        fn _assert_send<T: Send>() {}
        _assert_send::<AppState>();
    }

    #[test]
    fn test_app_state_is_sync() {
        fn _assert_sync<T: Sync>() {}
        _assert_sync::<AppState>();
    }

    #[test]
    fn test_app_state_config_field_type() {
        // Verify AppState struct is accessible and has expected fields
        let _ = std::any::type_name::<AppState>();
    }
}
