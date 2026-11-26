// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use std::sync::Arc;
use tracing::info;

pub mod backpressure;
pub mod batch_broadcaster;
pub mod batching;
pub mod connection;
pub mod connection_manager;
pub mod delivery;
pub mod handler;
pub mod message;
pub mod security;
pub mod server;

pub use connection::Connection;
pub use connection_manager::ConnectionManager;
pub use message::Message;
pub use server::{ServerStats, WebSocketServer};

use crate::config::AppConfig;
use crate::state::AppState;

/// WebSocket manager that coordinates all WebSocket functionality
/// Uses sonic-rs for high-performance JSON serialization/deserialization
#[derive(Clone)]
pub struct WebSocketManager {
    connection_manager: Arc<ConnectionManager>,
    server: Arc<WebSocketServer>,
    #[allow(dead_code)]
    config: Arc<AppConfig>,
    #[allow(dead_code)]
    app_state: AppState,
}

impl WebSocketManager {
    /// Create a new WebSocket manager
    pub async fn new(config: &AppConfig, app_state: AppState) -> Result<Self> {
        let connection_manager = Arc::new(ConnectionManager::new(
            config.ws_max_connections as usize,
            config.broadcaster_queue_size as usize,
        ));

        let server = Arc::new(
            WebSocketServer::new(config, connection_manager.clone(), app_state.clone()).await?,
        );

        info!(
            "WebSocket manager initialized - Max connections: {}, Buffer size: {}",
            config.ws_max_connections, config.ws_buffer_size
        );

        Ok(Self {
            connection_manager,
            server,
            config: Arc::new(config.clone()),
            app_state,
        })
    }

    /// Start the WebSocket manager background tasks
    pub async fn start(&self) -> Result<()> {
        info!("Starting WebSocket server background tasks...");

        // Start back-pressure monitoring
        let _monitoring_handle = self
            .connection_manager
            .start_back_pressure_monitoring()
            .await;
        info!("Back-pressure monitoring started");

        self.server.start_background().await?;
        Ok(())
    }

    /// Stop the WebSocket manager
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping WebSocket server...");
        self.server.stop().await?;

        // Clean up all connections
        info!("Cleaning up connections...");
        let cleaned = self.connection_manager.cleanup_all_connections().await;
        info!("Cleaned up {} connections", cleaned);

        Ok(())
    }

    /// Get connection manager
    pub fn get_connection_manager(&self) -> Arc<ConnectionManager> {
        self.connection_manager.clone()
    }

    /// Get server statistics
    pub async fn get_stats(&self) -> Result<ServerStats> {
        Ok(self.server.get_stats().await)
    }

    /// Send a message to a specific connection
    pub async fn send_to_connection(
        &self,
        connection_id: &uuid::Uuid,
        message: Message,
    ) -> Result<()> {
        self.connection_manager
            .send_to_connection(connection_id, message)
            .await
    }

    /// Broadcast a message to all subscribers of a topic
    pub async fn broadcast_to_topic(
        &self,
        topic: &str,
        message: Message,
        exclude_connection: Option<&uuid::Uuid>,
    ) -> Result<usize> {
        self.connection_manager
            .broadcast_to_topic(topic, message, exclude_connection)
            .await
    }

    /// Subscribe a connection to a topic
    pub async fn subscribe_to_topic(
        &self,
        connection_id: &uuid::Uuid,
        topic: &str,
    ) -> Result<bool> {
        self.connection_manager
            .subscribe_to_topic(connection_id, topic)
            .await
    }

    /// Unsubscribe a connection from a topic
    pub async fn unsubscribe_from_topic(
        &self,
        connection_id: &uuid::Uuid,
        topic: &str,
    ) -> Result<bool> {
        self.connection_manager
            .unsubscribe_from_topic(connection_id, topic)
            .await
    }

    /// Get connection count
    pub async fn connection_count(&self) -> usize {
        self.connection_manager.connection_count().await
    }

    /// Get topic subscriber count
    pub async fn topic_subscriber_count(&self, topic: &str) -> usize {
        self.connection_manager.topic_subscriber_count(topic).await
    }

    /// Authenticate a connection
    pub async fn authenticate_connection(
        &self,
        connection_id: &uuid::Uuid,
        user_id: String,
        auth_method: Option<String>,
        auth_context: crate::auth::types::AuthContext,
    ) -> Result<()> {
        self.connection_manager
            .authenticate_connection(connection_id, user_id, auth_method, auth_context)
            .await
    }

    /// Check if connection is authenticated
    pub async fn is_connection_authenticated(&self, connection_id: &uuid::Uuid) -> bool {
        self.connection_manager
            .is_connection_authenticated(connection_id)
            .await
    }

    /// Get connection information
    pub async fn get_connection(&self, connection_id: &uuid::Uuid) -> Option<Connection> {
        self.connection_manager.get_connection(connection_id).await
    }

    /// Broadcast a message to a channel (alias for broadcast_to_topic)
    pub async fn broadcast_to_channel(
        &self,
        channel: &str,
        message: sonic_rs::Value,
    ) -> Result<usize> {
        let msg = Message::Broadcast {
            topic: channel.to_string(),
            payload: message,
            timestamp: Some(chrono::Utc::now().timestamp()),
            operation_id: None,
            request_id: None,
        };

        self.broadcast_to_topic(channel, msg, None).await
    }
}

/// Helper functions for message creation with sonic-rs JSON values
pub mod helpers {
    use super::message::Message;
    use sonic_rs::{JsonValueMutTrait, Value as JsonValue};

    /// Create a success message with JSON payload
    pub fn success_message(event: &str, payload: JsonValue) -> Message {
        Message::Connection {
            event: event.to_string(),
            payload,
        }
    }

    /// Create an error message with details
    pub fn error_message(message: &str, code: Option<&str>) -> Message {
        let mut payload = sonic_rs::json!({
            "message": message,
            "timestamp": chrono::Utc::now().timestamp_millis()
        });

        if let Some(error_code) = code {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("code", JsonValue::from(error_code));
            }
        }

        Message::Error {
            message: message.to_string(),
            code: code.map(|c| c.to_string()),
            details: None,
        }
    }

    /// Create a subscription event message
    pub fn subscription_message(topic: &str, event: &str, payload: JsonValue) -> Message {
        Message::Subscription {
            topic: topic.to_string(),
            event: event.to_string(),
            payload,
        }
    }

    /// Create a broadcast message with metadata
    pub fn broadcast_message(topic: &str, data: JsonValue, sender_info: JsonValue) -> Message {
        Message::DirectMessage {
            topic: topic.to_string(),
            payload: sonic_rs::json!({
                "data": data,
                "sender": sender_info,
                "timestamp": chrono::Utc::now().timestamp_millis()
            }),
            timestamp: Some(chrono::Utc::now().timestamp_millis()),
        }
    }
}

pub use crate::auth::types::AuthContext;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::database::repositories::{ApiKeyRepository, SessionRepository, UserRepository};
    use crate::database::DatabaseManager;
    use deadpool_postgres::{Config, Runtime};
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use tokio_postgres::NoTls;

    // Mock implementation for testing
    async fn create_mock_app_state() -> AppState {
        // This is a simplified mock for testing purposes.
        // It doesn't initialize real database connections.
        let app_config = AppConfig::default();
        let mut main_cfg = Config::new();
        main_cfg.host = Some("localhost".to_string());
        main_cfg.port = Some(5432);
        main_cfg.user = Some("postgres".to_string());
        main_cfg.password = Some("postgres".to_string());
        main_cfg.dbname = Some("actae".to_string());
        let main_pool = main_cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

        let mut client_cfg = Config::new();
        client_cfg.host = Some("localhost".to_string());
        client_cfg.port = Some(5433);
        client_cfg.user = Some("postgres".to_string());
        client_cfg.password = Some("postgres".to_string());
        client_cfg.dbname = Some("actae".to_string());
        let client_pool = client_cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .unwrap();

        let db_manager = Arc::new(DatabaseManager {
            main_pool,
            client_pool,
            pool_config: Arc::new(crate::database::PoolConfig {
                max_connections: 10,
                min_connections: 2,
                connection_timeout: Duration::from_secs(5),
                idle_timeout: Duration::from_secs(300),
                enable_connection_warmup: false,
            }),
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            db_semaphore: Arc::new(Semaphore::new(200)),
        });

        AppState {
            config: Arc::new(app_config),
            db_manager: db_manager.clone(),
            user_repo: Arc::new(UserRepository::new(db_manager.clone())),
            api_key_repo: Arc::new(ApiKeyRepository::new(db_manager.clone())),
            session_repo: Arc::new(SessionRepository::new(db_manager.clone())),
            batch_broadcaster: Arc::new(
                crate::websocket::batch_broadcaster::BatchBroadcaster::new(
                    crate::websocket::batch_broadcaster::BatchBroadcastConfig::default(),
                )
                .await
                .unwrap(),
            ),
            password_hasher: Arc::new(
                crate::auth::password_hash::SecurePasswordHasher::with_secure_defaults(),
            ),
            control_plane: Arc::new(
                crate::control_plane::ControlPlaneManager::new(
                    Arc::new(crate::config::AppConfig::default()),
                    db_manager.client_pool.clone(),
                )
                .unwrap(),
            ),
            auth_manager: Arc::new(crate::auth::AuthManager::new(db_manager, vec![])),
        }
    }

    #[tokio::test]
    #[ignore] // Skip this test — requires live database
    async fn test_websocket_manager_creation() {
        let config = AppConfig::default();
        let app_state = create_mock_app_state().await;
        let manager = WebSocketManager::new(&config, app_state)
            .await
            .expect("Should create WebSocket manager");

        let stats = manager.get_stats().await.expect("Should get stats");
        assert_eq!(stats.active_connections, 0);
    }

    #[tokio::test]
    async fn test_message_helpers() {
        use helpers::*;

        // Test success message creation
        let success_msg = success_message("test", sonic_rs::json!({"key": "value"}));
        match success_msg {
            Message::Connection { event, .. } => assert_eq!(event, "test"),
            _ => {
                tracing::error!(
                    "Unexpected message type in test: expected Connection, got {:?}",
                    success_msg
                );
                assert!(false, "Wrong message type: expected Connection");
            }
        }

        // Test error message creation
        let error_msg = error_message("Test error", Some("TEST_ERROR"));
        match error_msg {
            Message::Error { message, code, .. } => {
                assert_eq!(message, "Test error");
                assert_eq!(code, Some("TEST_ERROR".to_string()));
            }
            _ => {
                tracing::error!(
                    "Unexpected message type in test: expected Error, got {:?}",
                    error_msg
                );
                assert!(false, "Wrong message type: expected Error");
            }
        }
    }

    #[tokio::test]
    async fn test_json_performance() {
        // Test sonic-rs performance vs serde_json (if available)
        use sonic_rs::{from_str, to_string, JsonValueTrait};

        let test_data = sonic_rs::json!({
            "type": "test",
            "data": {
                "array": [1, 2, 3, 4, 5],
                "nested": {
                    "key": "value",
                    "number": 42,
                    "boolean": true
                }
            },
            "timestamp": chrono::Utc::now().timestamp_millis()
        });

        // Test serialization
        let serialized = to_string(&test_data).expect("Should serialize");
        assert!(serialized.contains("test"));

        // Test deserialization
        let deserialized: sonic_rs::Value = from_str(&serialized).expect("Should deserialize");
        assert!(deserialized.is_object());
    }
}
