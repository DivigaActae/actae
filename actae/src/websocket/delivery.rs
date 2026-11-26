// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use super::backpressure::QueueResult;
use crate::broadcast::WebSocketConnectionManager;
use async_trait::async_trait;
use flume::TrySendError;
use sonic_rs::{from_str as json_from_str, Value as JsonValue};
use std::sync::Arc;
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use super::connection::OutgoingMessage;
use super::connection_manager::ConnectionManager;
use super::message::Message;

impl ConnectionManager {
    /// Send message to specific connection with back-pressure handling
    #[instrument(level = "debug", skip(self, message), fields(conn_id = %connection_id, msg_type = %message.message_type()))]
    pub async fn send_to_connection(
        &self,
        connection_id: &Uuid,
        message: Message,
    ) -> anyhow::Result<()> {
        let consumer_id = connection_id.to_string();

        // Calculate message size for back-pressure estimation
        let message_size = message.to_json()?.len();

        // Apply back-pressure management
        match self
            .back_pressure_manager
            .try_queue_message(&consumer_id, message_size)
            .await?
        {
            QueueResult::Queued => {
                // Message was accepted, proceed with sending
                if let Some(sender) = self.message_senders.get(connection_id) {
                    match sender.try_send(OutgoingMessage::Message(Arc::new(message))) {
                        Ok(()) => {
                            // Successfully enqueued into the per-connection channel. This
                            // does not guarantee socket delivery — the websocket task
                            // performs the actual write.
                            debug!("Enqueued message to connection {}", connection_id);

                            // Try and fetch consumer back-pressure stats for additional
                            // diagnostic context (non-fatal on error).
                            if let Some(stats) = self
                                .back_pressure_manager
                                .get_consumer_stats(&consumer_id)
                                .await
                            {
                                debug!(
                                    "ConsumerStats for {}: pending={}, processed={}, dropped={}, rate={:.2} msg/s, slow={}",
                                    connection_id,
                                    stats.pending_messages,
                                    stats.messages_processed,
                                    stats.messages_dropped,
                                    stats.processing_rate,
                                    stats.is_slow
                                );
                            }

                            // Mark message as processed for back-pressure tracking
                            self.back_pressure_manager
                                .mark_message_processed(&consumer_id)
                                .await?;
                            Ok(())
                        }
                        Err(TrySendError::Full(_)) => {
                            // Queue is full - this should have been caught by back-pressure manager
                            warn!(
                                "Message queue FULL for connection {}, dropping message (consumer may be slow)",
                                connection_id
                            );

                            // Include current consumer stats to help triage saturation
                            if let Some(stats) = self
                                .back_pressure_manager
                                .get_consumer_stats(&consumer_id)
                                .await
                            {
                                warn!(
                                    "Backpressure stats for {}: pending={}, processed={}, dropped={}, rate={:.2} msg/s",
                                    connection_id,
                                    stats.pending_messages,
                                    stats.messages_processed,
                                    stats.messages_dropped,
                                    stats.processing_rate
                                );
                            }

                            Err(anyhow::anyhow!("Connection queue full"))
                        }
                        Err(e) => {
                            // Could be Disconnected or other send errors
                            warn!(
                                "Failed to enqueue message to connection {}: {}",
                                connection_id, e
                            );

                            if let Some(stats) = self
                                .back_pressure_manager
                                .get_consumer_stats(&consumer_id)
                                .await
                            {
                                debug!(
                                    "ConsumerStats (post-fail) for {}: pending={}, processed={}, dropped={}, rate={:.2}",
                                    connection_id,
                                    stats.pending_messages,
                                    stats.messages_processed,
                                    stats.messages_dropped,
                                    stats.processing_rate
                                );
                            }

                            Err(anyhow::anyhow!("Failed to send message: {e}"))
                        }
                    }
                } else {
                    Err(anyhow::anyhow!("Connection not found: {connection_id}"))
                }
            }
            QueueResult::DroppedConsumerLimit => {
                debug!(
                    "Message dropped for connection {} due to consumer limit",
                    connection_id
                );
                Ok(()) // Don't treat as error - message was intentionally dropped
            }
            QueueResult::DroppedConsumerLag => {
                debug!(
                    "Message dropped for connection {} due to consumer lag",
                    connection_id
                );
                Ok(()) // Don't treat as error - message was intentionally dropped
            }
            QueueResult::DroppedGlobalLimit => {
                debug!(
                    "Message dropped for connection {} due to global limit",
                    connection_id
                );
                Ok(()) // Don't treat as error - message was intentionally dropped
            }
            QueueResult::ConsumerBackPressure => {
                warn!("Back-pressure applied for connection {}", connection_id);
                Err(anyhow::anyhow!("Back-pressure applied - consumer too slow"))
            }
        }
    }

    /// Send an Arc<Message> to a specific connection without additional cloning.
    /// This is the zero-copy fast-path used by the batch broadcaster and fast-path
    /// delivery code. It mirrors the back-pressure and queue logic in
    /// `send_to_connection` but accepts an already-Arc'd message.
    pub async fn send_arc_to_connection(
        &self,
        connection_id: &Uuid,
        message: Arc<Message>,
    ) -> anyhow::Result<()> {
        let consumer_id = connection_id.to_string();

        // Calculate message size for back-pressure estimation
        let message_size = match message.as_ref().to_json() {
            Ok(s) => s.len(),
            Err(_) => 1024, // Fallback estimate
        };

        // Apply back-pressure management
        match self
            .back_pressure_manager
            .try_queue_message(&consumer_id, message_size)
            .await?
        {
            QueueResult::Queued => {
                if let Some(sender) = self.message_senders.get(connection_id) {
                    match sender.try_send(OutgoingMessage::Message(message)) {
                        Ok(()) => {
                            debug!("Enqueued Arc<Message> to connection {}", connection_id);

                            if let Some(stats) = self
                                .back_pressure_manager
                                .get_consumer_stats(&consumer_id)
                                .await
                            {
                                debug!(
                                    "ConsumerStats for {}: pending={}, processed={}, dropped={}, rate={:.2} msg/s, slow={}",
                                    connection_id,
                                    stats.pending_messages,
                                    stats.messages_processed,
                                    stats.messages_dropped,
                                    stats.processing_rate,
                                    stats.is_slow
                                );
                            }

                            self.back_pressure_manager
                                .mark_message_processed(&consumer_id)
                                .await?;
                            Ok(())
                        }
                        Err(TrySendError::Full(_)) => {
                            warn!(
                                "Arc<Message> queue FULL for connection {}, dropping message (consumer may be slow)",
                                connection_id
                            );
                            if let Some(stats) = self
                                .back_pressure_manager
                                .get_consumer_stats(&consumer_id)
                                .await
                            {
                                warn!(
                                    "Backpressure stats for {}: pending={}, processed={}, dropped={}, rate={:.2} msg/s",
                                    connection_id,
                                    stats.pending_messages,
                                    stats.messages_processed,
                                    stats.messages_dropped,
                                    stats.processing_rate
                                );
                            }
                            Err(anyhow::anyhow!("Connection queue full"))
                        }
                        Err(e) => {
                            warn!(
                                "Failed to enqueue Arc<Message> to connection {}: {}",
                                connection_id, e
                            );
                            if let Some(stats) = self
                                .back_pressure_manager
                                .get_consumer_stats(&consumer_id)
                                .await
                            {
                                debug!(
                                    "ConsumerStats (post-fail) for {}: pending={}, processed={}, dropped={}, rate={:.2}",
                                    connection_id,
                                    stats.pending_messages,
                                    stats.messages_processed,
                                    stats.messages_dropped,
                                    stats.processing_rate
                                );
                            }
                            Err(anyhow::anyhow!("Failed to send Arc<Message>: {e}"))
                        }
                    }
                } else {
                    Err(anyhow::anyhow!("Connection not found: {connection_id}"))
                }
            }
            QueueResult::DroppedConsumerLimit => {
                debug!(
                    "Arc<Message> dropped for connection {} due to consumer limit",
                    connection_id
                );
                Ok(())
            }
            QueueResult::DroppedConsumerLag => {
                debug!(
                    "Arc<Message> dropped for connection {} due to consumer lag",
                    connection_id
                );
                Ok(())
            }
            QueueResult::DroppedGlobalLimit => {
                debug!(
                    "Arc<Message> dropped for connection {} due to global limit",
                    connection_id
                );
                Ok(())
            }
            QueueResult::ConsumerBackPressure => {
                warn!("Back-pressure applied for connection {}", connection_id);
                Err(anyhow::anyhow!("Back-pressure applied - consumer too slow"))
            }
        }
    }

    /// ULTRA-FAST PATH: Direct channel send bypassing backpressure.
    /// Backpressure is handled naturally by the bounded flume channel capacity.
    /// No async overhead, no RwLock acquisition, no serialization.
    /// This is the hot path for batch broadcaster delivery.
    pub fn try_send_to_connection(
        &self,
        connection_id: &Uuid,
        message: OutgoingMessage,
    ) -> anyhow::Result<()> {
        match self.message_senders.get(connection_id) {
            Some(sender) => sender
                .try_send(message)
                .map_err(|e| anyhow::anyhow!("Send failed to {connection_id}: {e}")),
            None => Err(anyhow::anyhow!("Connection not found: {connection_id}")),
        }
    }

    /// BATCH RESOLVE: Look up all sender channels for the given subscriber UUIDs
    /// in a single pass. Returns a Vec aligned 1:1 with the input (same order),
    /// containing only senders that were found. Used by the batch broadcaster to
    /// avoid per-subscriber DashMap lookups on the hot delivery path.
    /// The returned senders are cloned from the DashMap (cheap refcount bump),
    /// so the caller can iterate them with zero hashing overhead.
    pub fn resolve_senders(&self, ids: &[Uuid]) -> Vec<flume::Sender<OutgoingMessage>> {
        let mut senders = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(sender) = self.message_senders.get(id) {
                senders.push(sender.value().clone());
            }
        }
        senders
    }

    /// Send a pre-serialized JSON payload to a specific connection.
    /// This avoids re-serializing Broadcast messages per-subscriber.
    pub async fn send_serialized_to_connection(
        &self,
        connection_id: &Uuid,
        serialized: Arc<String>,
    ) -> anyhow::Result<()> {
        let consumer_id = connection_id.to_string();

        let message_size = serialized.len();

        match self
            .back_pressure_manager
            .try_queue_message(&consumer_id, message_size)
            .await?
        {
            QueueResult::Queued => {
                if let Some(sender) = self.message_senders.get(connection_id) {
                    match sender.try_send(OutgoingMessage::Serialized(serialized)) {
                        Ok(()) => {
                            debug!(
                                "Enqueued serialized payload to connection {}",
                                connection_id
                            );
                            if let Some(stats) = self
                                .back_pressure_manager
                                .get_consumer_stats(&consumer_id)
                                .await
                            {
                                debug!(
                                    "ConsumerStats for {}: pending={}, processed={}, dropped={}, rate={:.2} msg/s, slow={}",
                                    connection_id,
                                    stats.pending_messages,
                                    stats.messages_processed,
                                    stats.messages_dropped,
                                    stats.processing_rate,
                                    stats.is_slow
                                );
                            }
                            self.back_pressure_manager
                                .mark_message_processed(&consumer_id)
                                .await?;
                            Ok(())
                        }
                        Err(TrySendError::Full(_)) => {
                            warn!(
                                "Serialized payload queue FULL for connection {}, dropping",
                                connection_id
                            );
                            Err(anyhow::anyhow!("Connection queue full"))
                        }
                        Err(e) => {
                            warn!(
                                "Failed to enqueue serialized payload to connection {}: {}",
                                connection_id, e
                            );
                            Err(anyhow::anyhow!("Failed to send serialized payload: {e}"))
                        }
                    }
                } else {
                    Err(anyhow::anyhow!("Connection not found: {connection_id}"))
                }
            }
            QueueResult::DroppedConsumerLimit => {
                debug!(
                    "Serialized payload dropped for connection {} due to consumer limit",
                    connection_id
                );
                Ok(())
            }
            QueueResult::DroppedConsumerLag => {
                debug!(
                    "Serialized payload dropped for connection {} due to consumer lag",
                    connection_id
                );
                Ok(())
            }
            QueueResult::DroppedGlobalLimit => {
                debug!(
                    "Serialized payload dropped for connection {} due to global limit",
                    connection_id
                );
                Ok(())
            }
            QueueResult::ConsumerBackPressure => {
                warn!("Back-pressure applied for connection {}", connection_id);
                Err(anyhow::anyhow!("Back-pressure applied - consumer too slow"))
            }
        }
    }

    /// Get message receiver for connection (used by server)
    pub async fn get_connection_receiver(
        &self,
        _connection_id: &Uuid,
    ) -> flume::Receiver<OutgoingMessage> {
        // This is a placeholder - in real implementation, this would be handled differently
        // as the receiver is created and returned when connection is added
        let (_, receiver) = flume::bounded(1);
        receiver
    }

    /// Broadcast message to all subscribers of a topic.
    ///
    /// Return value semantics vary by delivery path:
    /// - Multi-node: returns subscriber count from external PUBLISH, not actual
    ///   deliveries. Per-subscriber exclusion is not supported in this mode.
    /// - Local batching: returns subscriber count (estimate), delivery is asynchronous.
    /// - Local per-subscriber: returns actual number of successful deliveries.
    #[instrument(level = "debug", skip(self, message), fields(topic, msg_type = %message.message_type()))]
    pub async fn broadcast_to_topic(
        &self,
        topic: &str,
        message: Message,
        exclude_connection: Option<&Uuid>,
    ) -> anyhow::Result<usize> {
        message.to_json()?;

        // Deliver to local subscribers, honoring exclusion
        self.broadcast_to_local_topic(topic, message, exclude_connection)
            .await
    }

    /// Broadcast message to all local subscribers of a topic
    pub async fn broadcast_to_local_topic(
        &self,
        topic: &str,
        message: Message,
        exclude_connection: Option<&Uuid>,
    ) -> anyhow::Result<usize> {
        const MAX_CONSECUTIVE_SEND_ERRORS: u32 = 5;
        let message_arc = Arc::new(message);

        // Only batch when there is no exclusion — the batched path
        // delivers to all subscribers without per-connection filtering.
        if exclude_connection.is_none() {
            if let Ok(batched) = self.try_batch_message(topic, message_arc.clone()).await {
                if batched {
                    let subscribers_count =
                        self.topics.get(topic).map(|subs| subs.len()).unwrap_or(0);
                    return Ok(subscribers_count);
                }
            }
        }

        let subscribers: Vec<Uuid> = self
            .topics
            .get(topic)
            .map(|subs| {
                subs.iter()
                    .filter(|&id| exclude_connection != Some(id))
                    .copied()
                    .collect()
            })
            .unwrap_or_default();

        let mut delivered = 0;

        for connection_id in subscribers {
            if let Some(sender) = self.message_senders.get(&connection_id) {
                match sender.try_send(OutgoingMessage::Message(message_arc.clone())) {
                    Ok(()) => {
                        delivered += 1;
                        // reset failure counter on success
                        self.send_failures.remove(&connection_id);
                    }
                    Err(TrySendError::Full(_)) => {
                        warn!("Back-pressure: queue full for connection {}", connection_id);
                        let mut counter = self.send_failures.entry(connection_id).or_insert(0);
                        *counter += 1;
                        let count_value = *counter;
                        if count_value >= MAX_CONSECUTIVE_SEND_ERRORS {
                            warn!(
                                "Dropping slow connection {} after {} consecutive send failures",
                                connection_id, count_value
                            );
                            let _ = self.remove_connection(&connection_id).await;
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Failed to send message to connection {}: {}",
                            connection_id, e
                        );
                    }
                }
            }
        }
        Ok(delivered)
    }

    // ── Ring-buffer fanout API ──

    /// Returns true if this topic has an active ring buffer (>= threshold subscribers).
    pub fn has_topic_ring(&self, topic: &str) -> bool {
        self.topic_rings.contains_key(topic)
    }

    /// Publish a pre-serialized message via the topic's ring buffer.
    /// Returns Ok(number of active receivers) on success.
    /// If the topic has no ring, returns `None`.
    pub fn send_via_ring(&self, topic: &str, msg: Arc<String>) -> Option<usize> {
        let sender = self.topic_rings.get(topic)?;
        match sender.send(msg) {
            Ok(n) => Some(n),
            Err(_) => Some(0), // No active receivers — still "successful" for the sender
        }
    }

    /// Set the EventStore for cursor-based replay on subscribe/resume.
    /// Can be called after construction even when ConnectionManager is shared via Arc.
    pub async fn set_event_store(&self, event_store: Arc<crate::events::EventStore>) {
        *self.event_store.write().await = Some(event_store);
    }
}

// Integrate ConnectionManager with the broadcasting layer
#[async_trait]
impl WebSocketConnectionManager for ConnectionManager {
    async fn broadcast_to_topic(&self, topic: &str, message: &str) -> anyhow::Result<usize> {
        // Attempt to parse the raw JSON string into a Message. If parsing fails we
        // encapsulate the payload inside a generic Broadcast variant so the
        // consumer still receives something useful.

        let parsed: Result<super::message::Message, _> =
            super::message::Message::from_json(message);

        let msg = match parsed {
            Ok(m) => m,
            Err(_) => super::message::Message::Broadcast {
                topic: topic.to_string(),
                payload: if crate::database::json_compat::json_nesting_depth(message)
                    > crate::database::json_compat::MAX_JSON_DEPTH
                {
                    // Too deep to parse safely — keep a plain string.
                    sonic_rs::Value::from(message)
                } else {
                    json_from_str::<JsonValue>(message).unwrap_or_else(|_| {
                        // Fallback to plain string value
                        sonic_rs::Value::from(message)
                    })
                },
                timestamp: Some(chrono::Utc::now().timestamp()),
                operation_id: None,
                request_id: None,
            },
        };

        self.broadcast_to_local_topic(topic, msg, None).await
    }

    async fn broadcast_to_client(&self, client_id: &str, message: &str) -> anyhow::Result<()> {
        let uuid = Uuid::parse_str(client_id)?;

        let parsed: Result<super::message::Message, _> =
            super::message::Message::from_json(message);

        let msg = match parsed {
            Ok(m) => m,
            Err(_) => super::message::Message::DirectMessage {
                topic: "direct".to_string(),
                payload: json_from_str::<JsonValue>(message).unwrap_or_else(|_| {
                    // Fallback to plain string value
                    sonic_rs::Value::from(message)
                }),
                timestamp: Some(chrono::Utc::now().timestamp()),
            },
        };

        self.send_to_connection(&uuid, msg).await
    }

    async fn get_topic_subscriber_count(&self, topic: &str) -> usize {
        self.topic_subscriber_count(topic).await
    }

    async fn get_connection_count(&self) -> usize {
        self.connection_count().await
    }

    async fn disconnect_client(&self, client_id: &str) -> anyhow::Result<()> {
        let uuid = Uuid::parse_str(client_id)?;
        self.remove_connection(&uuid).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_send_to_nonexistent_connection() {
        let cm = ConnectionManager::new(10, 256);
        let id = Uuid::new_v4();
        let msg = Message::Connection {
            event: "ping".into(),
            payload: sonic_rs::json!({}),
        };
        // Sending to a nonexistent connection should error
        assert!(cm.send_to_connection(&id, msg).await.is_err());
    }

    #[tokio::test]
    async fn test_send_arc_to_nonexistent() {
        let cm = ConnectionManager::new(10, 256);
        let id = Uuid::new_v4();
        let msg = Arc::new(Message::Connection {
            event: "ping".into(),
            payload: sonic_rs::json!({}),
        });
        assert!(cm.send_arc_to_connection(&id, msg).await.is_err());
    }

    #[tokio::test]
    async fn test_send_to_nonexistent_via_try_send() {
        let cm = ConnectionManager::new(10, 256);
        let id = Uuid::new_v4();
        assert!(cm
            .send_to_connection(
                &id,
                Message::Connection {
                    event: "ping".into(),
                    payload: sonic_rs::json!({}),
                }
            )
            .await
            .is_err());
    }

    #[test]
    fn test_resolve_senders_empty() {
        let cm = ConnectionManager::new(10, 256);
        let ids = vec![Uuid::new_v4(), Uuid::new_v4()];
        let senders = cm.resolve_senders(&ids);
        assert!(senders.is_empty());
    }

    #[tokio::test]
    async fn test_send_serialized_to_nonexistent() {
        let cm = ConnectionManager::new(10, 256);
        let id = Uuid::new_v4();
        assert!(cm
            .send_serialized_to_connection(&id, Arc::new("{}".to_string()))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_get_connection_receiver_returns_receiver() {
        let cm = ConnectionManager::new(10, 256);
        let id = Uuid::new_v4();
        let _rx = cm.get_connection_receiver(&id).await;
        // Just verify it returns a Receiver without panicking
    }

    #[tokio::test]
    async fn test_has_topic_ring_false() {
        let cm = ConnectionManager::new(10, 256);
        assert!(!cm.has_topic_ring("nonexistent-topic"));
    }

    #[test]
    fn test_send_via_ring_empty() {
        let cm = ConnectionManager::new(10, 256);
        let result = cm.send_via_ring("nonexistent", Arc::new("test".to_string()));
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_broadcast_to_local_topic_empty() {
        let cm = ConnectionManager::new(10, 256);
        let msg = Message::Connection {
            event: "test".into(),
            payload: sonic_rs::json!({}),
        };
        let count = cm
            .broadcast_to_local_topic("empty-topic", msg, None)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_broadcast_to_client_nonexistent() {
        let cm = ConnectionManager::new(10, 256);
        let result = cm
            .broadcast_to_client("00000000-0000-0000-0000-000000000000", "{}")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_event_store_accessible() {
        let cm = ConnectionManager::new(10, 256);
        let store_opt = cm.event_store.read().await.clone();
        assert!(store_opt.is_none());
    }

    // ── WebSocketConnectionManager trait integration tests ────

    #[tokio::test]
    async fn test_trait_get_topic_subscriber_count() {
        let cm = ConnectionManager::new(10, 256);
        let count = cm.get_topic_subscriber_count("test").await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_trait_get_connection_count() {
        let cm = ConnectionManager::new(10, 256);
        let count = cm.get_connection_count().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_trait_disconnect_client_nonexistent() {
        let cm = ConnectionManager::new(10, 256);
        // Disconnecting a nonexistent client may succeed (no-op) or fail
        let _ = cm
            .disconnect_client("00000000-0000-0000-0000-000000000000")
            .await;
    }

    #[tokio::test]
    async fn test_trait_broadcast_to_client_invalid_uuid() {
        let cm = ConnectionManager::new(10, 256);
        let result = cm.broadcast_to_client("not-a-uuid", "{}").await;
        assert!(result.is_err());
    }
}
