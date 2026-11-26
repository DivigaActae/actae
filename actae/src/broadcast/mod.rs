// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

// Message broadcasting with WebSocket integration

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub mod dead_letter_queue;
pub mod delivery_tracker;

/// Manages broadcasting messages to WebSocket clients with real integration.
#[derive(Clone)]
pub struct BroadcastManager {
    /// Optional WebSocket connection manager for real message delivery
    websocket_manager: Option<Arc<dyn WebSocketConnectionManager>>,
    /// Broadcast statistics for monitoring
    stats: Arc<tokio::sync::RwLock<BroadcastStats>>,
}

/// Statistics for broadcast operations
#[derive(Debug, Clone, Default)]
pub struct BroadcastStats {
    pub total_broadcasts: u64,
    pub successful_deliveries: u64,
    pub failed_deliveries: u64,
    pub unique_topics: u64,
    pub last_broadcast: Option<chrono::DateTime<chrono::Utc>>,
}

/// Trait for WebSocket connection management integration
#[async_trait]
pub trait WebSocketConnectionManager: Send + Sync {
    /// Broadcasts a message to all subscribers of a topic
    async fn broadcast_to_topic(&self, topic: &str, message: &str) -> Result<usize>;

    /// Broadcasts a message to a specific client
    async fn broadcast_to_client(&self, client_id: &str, message: &str) -> Result<()>;

    /// Gets the number of subscribers for a topic
    async fn get_topic_subscriber_count(&self, topic: &str) -> usize;

    /// Gets the total number of connected clients
    async fn get_connection_count(&self) -> usize;

    /// Forcefully disconnect a client by its ID
    async fn disconnect_client(&self, client_id: &str) -> Result<()>;
}

impl BroadcastManager {
    /// Creates a new BroadcastManager without WebSocket integration.
    pub fn new() -> Self {
        Self {
            websocket_manager: None,
            stats: Arc::new(tokio::sync::RwLock::new(BroadcastStats::default())),
        }
    }

    /// Creates a new BroadcastManager with WebSocket integration.
    pub fn with_websocket_manager(websocket_manager: Arc<dyn WebSocketConnectionManager>) -> Self {
        Self {
            websocket_manager: Some(websocket_manager),
            stats: Arc::new(tokio::sync::RwLock::new(BroadcastStats::default())),
        }
    }

    /// Sets the WebSocket connection manager for real message delivery.
    pub fn set_websocket_manager(&mut self, manager: Arc<dyn WebSocketConnectionManager>) {
        self.websocket_manager = Some(manager);
        info!(" WebSocket connection manager integrated with BroadcastManager");
    }

    /// Broadcasts a message to all clients subscribed to a topic.
    pub async fn broadcast<T>(&self, topic: &str, message: &T) -> Result<usize>
    where
        T: serde::Serialize + std::fmt::Debug,
    {
        let start_time = std::time::Instant::now();

        // Serialize message to JSON
        let message_json = sonic_rs::to_string(message)
            .map_err(|e| anyhow::anyhow!("Failed to serialize message: {e}"))?;

        info!("Broadcasting message to topic '{}': {:?}", topic, message);
        debug!("Message JSON: {}", message_json);

        let delivered_count = if let Some(ref ws_manager) = self.websocket_manager {
            // Real WebSocket delivery
            match ws_manager.broadcast_to_topic(topic, &message_json).await {
                Ok(count) => {
                    info!(
                        " Delivered message to {} subscribers on topic '{}'",
                        count, topic
                    );

                    // Update success stats
                    let mut stats = self.stats.write().await;
                    stats.successful_deliveries += count as u64;
                    stats.total_broadcasts += 1;
                    stats.last_broadcast = Some(chrono::Utc::now());

                    count
                }
                Err(e) => {
                    error!(" Failed to broadcast to topic '{}': {}", topic, e);

                    // Update failure stats
                    let mut stats = self.stats.write().await;
                    stats.failed_deliveries += 1;
                    stats.total_broadcasts += 1;
                    stats.last_broadcast = Some(chrono::Utc::now());

                    return Err(e);
                }
            }
        } else {
            // Fallback: log-only mode for development/testing
            warn!("No WebSocket manager configured - message logged only");
            debug!("Would broadcast to topic '{}': {}", topic, message_json);

            // Update stats for log-only mode
            let mut stats = self.stats.write().await;
            stats.total_broadcasts += 1;
            stats.last_broadcast = Some(chrono::Utc::now());

            0 // No actual delivery
        };

        let duration = start_time.elapsed();
        debug!("Broadcast completed in {:?}", duration);

        Ok(delivered_count)
    }

    /// Broadcasts a message to a specific client.
    pub async fn broadcast_to_client<T>(&self, client_id: &str, message: &T) -> Result<()>
    where
        T: serde::Serialize + std::fmt::Debug,
    {
        // Serialize message to JSON
        let message_json = sonic_rs::to_string(message)
            .map_err(|e| anyhow::anyhow!("Failed to serialize message: {e}"))?;

        info!(
            "Broadcasting message to client '{}': {:?}",
            client_id, message
        );

        if let Some(ref ws_manager) = self.websocket_manager {
            // Real WebSocket delivery
            match ws_manager
                .broadcast_to_client(client_id, &message_json)
                .await
            {
                Ok(()) => {
                    info!(" Delivered message to client '{}'", client_id);

                    // Update success stats
                    let mut stats = self.stats.write().await;
                    stats.successful_deliveries += 1;
                    stats.total_broadcasts += 1;
                    stats.last_broadcast = Some(chrono::Utc::now());
                }
                Err(e) => {
                    error!(" Failed to broadcast to client '{}': {}", client_id, e);

                    // Update failure stats
                    let mut stats = self.stats.write().await;
                    stats.failed_deliveries += 1;
                    stats.total_broadcasts += 1;
                    stats.last_broadcast = Some(chrono::Utc::now());

                    return Err(e);
                }
            }
        } else {
            // Fallback: log-only mode
            warn!("No WebSocket manager configured - message logged only");
            debug!(
                "Would broadcast to client '{}': {}",
                client_id, message_json
            );

            // Update stats for log-only mode
            let mut stats = self.stats.write().await;
            stats.total_broadcasts += 1;
            stats.last_broadcast = Some(chrono::Utc::now());
        }

        Ok(())
    }

    /// Gets the number of clients subscribed to a topic.
    pub async fn get_subscriber_count(&self, topic: &str) -> usize {
        if let Some(ref ws_manager) = self.websocket_manager {
            ws_manager.get_topic_subscriber_count(topic).await
        } else {
            debug!(
                "No WebSocket manager configured - returning 0 subscribers for topic '{}'",
                topic
            );
            0
        }
    }

    /// Gets the total number of connected clients.
    pub async fn get_connection_count(&self) -> usize {
        if let Some(ref ws_manager) = self.websocket_manager {
            ws_manager.get_connection_count().await
        } else {
            debug!("No WebSocket manager configured - returning 0 connections");
            0
        }
    }

    /// Lists all active topics.
    pub async fn list_topics(&self) -> Vec<String> {
        if let Some(ref _ws_manager) = self.websocket_manager {
            // In a real implementation, this would query the WebSocket manager for active topics
            // For now, return empty list as this requires additional WebSocket manager methods
            debug!("Topic listing not yet implemented - returning empty list");
            vec![]
        } else {
            debug!("No WebSocket manager configured - returning empty topic list");
            vec![]
        }
    }

    /// Gets broadcast statistics for monitoring.
    pub async fn get_stats(&self) -> BroadcastStats {
        self.stats.read().await.clone()
    }

    /// Resets broadcast statistics.
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = BroadcastStats::default();
        info!("Broadcast statistics reset");
    }

    /// Gets success rate as a percentage.
    pub async fn get_success_rate(&self) -> f64 {
        let stats = self.stats.read().await;
        let total_deliveries = stats.successful_deliveries + stats.failed_deliveries;

        if total_deliveries == 0 {
            100.0 // No failures if no attempts
        } else {
            (stats.successful_deliveries as f64 / total_deliveries as f64) * 100.0
        }
    }

    /// Checks if WebSocket integration is available.
    pub fn has_websocket_integration(&self) -> bool {
        self.websocket_manager.is_some()
    }
}

impl Default for BroadcastManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;

    // Mock WebSocket connection manager for testing
    struct MockWebSocketManager {
        subscriber_counts: HashMap<String, usize>,
        connection_count: usize,
    }

    #[async_trait]
    impl WebSocketConnectionManager for MockWebSocketManager {
        async fn broadcast_to_topic(&self, topic: &str, _message: &str) -> Result<usize> {
            Ok(self.subscriber_counts.get(topic).copied().unwrap_or(0))
        }

        async fn broadcast_to_client(&self, _client_id: &str, _message: &str) -> Result<()> {
            Ok(())
        }

        async fn get_topic_subscriber_count(&self, topic: &str) -> usize {
            self.subscriber_counts.get(topic).copied().unwrap_or(0)
        }

        async fn get_connection_count(&self) -> usize {
            self.connection_count
        }

        async fn disconnect_client(&self, _client_id: &str) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_broadcast_manager_creation() {
        let manager = BroadcastManager::new();
        assert!(!manager.has_websocket_integration());

        let stats = manager.get_stats().await;
        assert_eq!(stats.total_broadcasts, 0);
    }

    #[tokio::test]
    async fn test_broadcast_with_mock_websocket() {
        let mut mock_counts = HashMap::new();
        mock_counts.insert("test-topic".to_string(), 5);

        let mock_manager = Arc::new(MockWebSocketManager {
            subscriber_counts: mock_counts,
            connection_count: 10,
        });

        let broadcast_manager = BroadcastManager::with_websocket_manager(mock_manager);

        let test_message = sonic_rs::json!({
            "type": "test",
            "data": "hello world"
        });

        let delivered = broadcast_manager
            .broadcast("test-topic", &test_message)
            .await
            .unwrap();

        assert_eq!(delivered, 5);

        let stats = broadcast_manager.get_stats().await;
        assert_eq!(stats.total_broadcasts, 1);
        assert_eq!(stats.successful_deliveries, 5);
    }

    #[tokio::test]
    async fn test_subscriber_count() {
        let mut mock_counts = HashMap::new();
        mock_counts.insert("test-topic".to_string(), 3);

        let mock_manager = Arc::new(MockWebSocketManager {
            subscriber_counts: mock_counts,
            connection_count: 10,
        });

        let broadcast_manager = BroadcastManager::with_websocket_manager(mock_manager);

        let count = broadcast_manager.get_subscriber_count("test-topic").await;
        assert_eq!(count, 3);

        let total_connections = broadcast_manager.get_connection_count().await;
        assert_eq!(total_connections, 10);
    }

    #[tokio::test]
    async fn test_broadcast_no_ws() {
        let bm = BroadcastManager::new();
        assert!(!bm.has_websocket_integration());
        let count = bm
            .broadcast("no-topic", &sonic_rs::json!({"t": 1}))
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_broadcast_stats() {
        let mut counts = HashMap::new();
        counts.insert("s".into(), 2usize);
        let mock = Arc::new(MockWebSocketManager {
            subscriber_counts: counts,
            connection_count: 5,
        });
        let bm = BroadcastManager::with_websocket_manager(mock);
        bm.broadcast("s", &sonic_rs::json!({"m": 1})).await.unwrap();
        let stats = bm.get_stats().await;
        assert_eq!(stats.total_broadcasts, 1);
        assert_eq!(stats.successful_deliveries, 2);
    }

    #[tokio::test]
    async fn test_get_success_rate() {
        let bm = BroadcastManager::new();
        let rate = bm.get_success_rate().await;
        assert_eq!(rate, 100.0);
    }
}
