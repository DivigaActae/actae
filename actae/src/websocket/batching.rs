// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use flume::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::connection_manager::ConnectionManager;
use super::message::Message;

/// Configuration for message batching
#[derive(Debug, Clone)]
pub struct BatchingConfig {
    /// Maximum number of messages in a batch
    pub max_batch_size: usize,
    /// Maximum time to wait before sending a batch
    pub max_batch_delay: Duration,
    /// Maximum memory usage for batching in bytes
    pub max_memory_usage: usize,
    /// Enable batching for specific message types
    pub enable_broadcast_batching: bool,
    pub enable_database_batching: bool,
    pub enable_direct_batching: bool,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_batch_delay: Duration::from_millis(10),
            max_memory_usage: 1024 * 1024, // 1MB
            enable_broadcast_batching: true,
            enable_database_batching: true,
            enable_direct_batching: true,
        }
    }
}

/// A batch of messages to be processed together
#[derive(Debug, Clone)]
pub struct MessageBatch {
    pub messages: Vec<Arc<Message>>,
    pub created_at: Instant,
    pub topic: String,
    pub batch_id: Uuid,
    pub estimated_size: usize,
}

impl MessageBatch {
    pub fn new(topic: String) -> Self {
        Self {
            messages: Vec::new(),
            created_at: Instant::now(),
            topic,
            batch_id: Uuid::new_v4(),
            estimated_size: 0,
        }
    }

    pub fn add_message(&mut self, message: Arc<Message>) -> Result<(), String> {
        // Estimate message size for memory management
        let estimated_msg_size = match message.to_json_bytes() {
            Ok(bytes) => bytes.len(),
            Err(_) => 1024, // Default estimate
        };

        self.estimated_size += estimated_msg_size;
        self.messages.push(message);
        Ok(())
    }

    pub fn is_ready(&self, config: &BatchingConfig) -> bool {
        self.messages.len() >= config.max_batch_size
            || self.created_at.elapsed() >= config.max_batch_delay
            || self.estimated_size >= config.max_memory_usage
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

/// Message batching manager for high-throughput scenarios
#[derive(Debug)]
pub struct BatchingManager {
    config: BatchingConfig,
    /// Batches organized by topic
    batches: Arc<DashMap<String, MessageBatch>>,
    /// Sender for completed batches
    batch_sender: Sender<MessageBatch>,
    /// Receiver for completed batches
    batch_receiver: Receiver<MessageBatch>,
    /// Connection manager for sending batches
    connection_manager: Option<Arc<ConnectionManager>>,
    /// Batching statistics
    stats: Arc<RwLock<BatchingStats>>,
}

/// Batching performance statistics
#[derive(Debug, Clone, Default)]
pub struct BatchingStats {
    pub batches_created: u64,
    pub batches_processed: u64,
    pub messages_batched: u64,
    pub messages_sent: u64,
    pub avg_batch_size: f64,
    pub avg_batch_latency_ms: f64,
    pub memory_usage_bytes: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl BatchingManager {
    pub fn new(config: BatchingConfig, connection_manager: Arc<ConnectionManager>) -> Self {
        let (batch_sender, batch_receiver) = flume::bounded(10000);

        Self {
            config,
            batches: Arc::new(DashMap::new()),
            batch_sender,
            batch_receiver,
            connection_manager: Some(connection_manager),
            stats: Arc::new(RwLock::new(BatchingStats::default())),
        }
    }

    /// Create a new batching manager without connection manager dependency
    /// This is a placeholder implementation - in practice, use new() with a real ConnectionManager
    pub fn new_simple(config: BatchingConfig) -> Self {
        let (batch_sender, batch_receiver) = flume::bounded(10000);

        // Create a placeholder connection manager for simple initialization
        // This should be replaced with a real ConnectionManager via set_connection_manager
        let placeholder_cm = None;

        Self {
            config,
            batches: Arc::new(DashMap::new()),
            batch_sender,
            batch_receiver,
            connection_manager: placeholder_cm,
            stats: Arc::new(RwLock::new(BatchingStats::default())),
        }
    }

    /// Add a message to the appropriate batch
    pub async fn add_message(&self, topic: &str, message: Arc<Message>) -> Result<(), String> {
        if !self.should_batch_message(&message) {
            return Err("Message type not suitable for batching".to_string());
        }

        let topic_key = topic.to_string();

        // Get or create batch for this topic
        let mut batch = self.batches.entry(topic_key.clone()).or_insert_with(|| {
            let mut stats = self.stats.try_write().unwrap();
            stats.batches_created += 1;
            MessageBatch::new(topic_key.clone())
        });

        // Add message to batch
        batch.add_message(message)?;

        // Update statistics
        {
            let mut stats = self.stats.try_write().unwrap();
            stats.messages_batched += 1;
            stats.memory_usage_bytes += batch.estimated_size;
        }

        // Check if batch is ready to be sent
        if batch.is_ready(&self.config) {
            let completed_batch = batch.clone();
            self.batches.remove(&topic_key);

            // Send batch for processing
            if let Err(e) = self.batch_sender.try_send(completed_batch) {
                error!("Failed to send batch for processing: {}", e);
                return Err(format!("Failed to queue batch: {e}"));
            }
        }

        Ok(())
    }

    /// Check if a message should be batched
    fn should_batch_message(&self, message: &Message) -> bool {
        match message {
            Message::Broadcast { .. } => self.config.enable_broadcast_batching,
            Message::DatabaseChange { .. } => self.config.enable_database_batching,
            Message::DirectMessage { .. } => self.config.enable_direct_batching,
            _ => false,
        }
    }

    /// Start the batch processing loop
    pub async fn start_batch_processor(&self) -> tokio::task::JoinHandle<()> {
        let receiver = self.batch_receiver.clone();
        let connection_manager = self.connection_manager.clone();
        let stats = self.stats.clone();

        tokio::spawn(async move {
            info!("Starting batch processor...");

            while let Ok(batch) = receiver.recv_async().await {
                let start_time = Instant::now();

                debug!(
                    "Processing batch {} with {} messages for topic '{}'",
                    batch.batch_id,
                    batch.len(),
                    batch.topic
                );

                // Process the batch
                let mut successful_sends = 0;
                let result = if let Some(ref cm) = connection_manager {
                    Self::process_batch(&batch, cm).await
                } else {
                    warn!("No connection manager available, skipping batch processing");
                    Ok(0)
                };
                match result {
                    Ok(sent_count) => {
                        successful_sends = sent_count;
                        debug!(
                            "Batch {} processed successfully: {} messages sent",
                            batch.batch_id, sent_count
                        );
                    }
                    Err(e) => {
                        error!("Failed to process batch {}: {}", batch.batch_id, e);
                    }
                }

                // Update statistics
                {
                    let mut stats = stats.write().await;
                    stats.batches_processed += 1;
                    stats.messages_sent += successful_sends;

                    // Update averages using exponential moving average
                    let batch_size = batch.len() as f64;
                    let batch_latency = start_time.elapsed().as_secs_f64() * 1000.0;

                    if stats.avg_batch_size == 0.0 {
                        stats.avg_batch_size = batch_size;
                        stats.avg_batch_latency_ms = batch_latency;
                    } else {
                        stats.avg_batch_size = 0.1 * batch_size + 0.9 * stats.avg_batch_size;
                        stats.avg_batch_latency_ms =
                            0.1 * batch_latency + 0.9 * stats.avg_batch_latency_ms;
                    }

                    stats.memory_usage_bytes = stats
                        .memory_usage_bytes
                        .saturating_sub(batch.estimated_size);
                }
            }

            warn!("Batch processor stopped");
        })
    }

    /// Process a batch of messages
    async fn process_batch(
        batch: &MessageBatch,
        connection_manager: &ConnectionManager,
    ) -> Result<u64, String> {
        let mut sent_count = 0;

        // Group messages by type for optimized processing
        let mut broadcast_messages = Vec::new();
        let mut database_messages = Vec::new();
        let mut direct_messages = Vec::new();

        for message in &batch.messages {
            match message.as_ref() {
                Message::Broadcast { .. } => broadcast_messages.push(message.clone()),
                Message::DatabaseChange { .. } => database_messages.push(message.clone()),
                Message::DirectMessage { .. } => direct_messages.push(message.clone()),
                _ => {}
            }
        }

        // Process broadcast messages in batch
        if !broadcast_messages.is_empty() {
            match Self::process_broadcast_batch(
                &batch.topic,
                broadcast_messages,
                connection_manager,
            )
            .await
            {
                Ok(count) => sent_count += count,
                Err(e) => error!("Failed to process broadcast batch: {}", e),
            }
        }

        // Process database messages in batch
        if !database_messages.is_empty() {
            match Self::process_database_batch(&batch.topic, database_messages, connection_manager)
                .await
            {
                Ok(count) => sent_count += count,
                Err(e) => error!("Failed to process database batch: {}", e),
            }
        }

        // Process direct messages in batch
        if !direct_messages.is_empty() {
            match Self::process_direct_batch(&batch.topic, direct_messages, connection_manager)
                .await
            {
                Ok(count) => sent_count += count,
                Err(e) => error!("Failed to process direct batch: {}", e),
            }
        }

        Ok(sent_count)
    }

    /// Process a batch of broadcast messages
    async fn process_broadcast_batch(
        topic: &str,
        messages: Vec<Arc<Message>>,
        connection_manager: &ConnectionManager,
    ) -> Result<u64, String> {
        let mut sent_count = 0;

        // Create a combined message for batch broadcast
        let combined_payloads = messages
            .iter()
            .filter_map(|msg| match msg.as_ref() {
                Message::Broadcast { payload, .. } => Some(payload.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        if !combined_payloads.is_empty() {
            let batch_message = Message::Broadcast {
                topic: topic.to_string(),
                payload: sonic_rs::json!({
                    "batch": true,
                    "messages": combined_payloads
                }),
                timestamp: Some(chrono::Utc::now().timestamp()),
                operation_id: None,
                request_id: None,
            };

            match connection_manager
                .broadcast_to_local_topic(topic, batch_message, None)
                .await
            {
                Ok(count) => sent_count += count as u64,
                Err(e) => return Err(format!("Batch broadcast failed: {e}")),
            }
        }

        Ok(sent_count)
    }

    /// Process a batch of database change messages
    async fn process_database_batch(
        topic: &str,
        messages: Vec<Arc<Message>>,
        connection_manager: &ConnectionManager,
    ) -> Result<u64, String> {
        let mut sent_count = 0;

        // Group database messages by event type
        let mut grouped_messages: HashMap<String, Vec<&Message>> = HashMap::new();

        for message in &messages {
            if let Message::DatabaseChange { event, .. } = message.as_ref() {
                grouped_messages
                    .entry(event.clone())
                    .or_default()
                    .push(message);
            }
        }

        // Send grouped messages
        for (event_type, event_messages) in grouped_messages {
            let combined_payloads = event_messages
                .iter()
                .filter_map(|msg| match msg {
                    Message::DatabaseChange { payload, .. } => Some(payload.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();

            if !combined_payloads.is_empty() {
                let batch_message = Message::DatabaseChange {
                    topic: topic.to_string(),
                    event: event_type,
                    payload: sonic_rs::json!({
                        "batch": true,
                        "changes": combined_payloads
                    }),
                    timestamp: Some(chrono::Utc::now().timestamp()),
                };

                match connection_manager
                    .broadcast_to_local_topic(topic, batch_message, None)
                    .await
                {
                    Ok(count) => sent_count += count as u64,
                    Err(e) => error!("Database batch broadcast failed: {}", e),
                }
            }
        }

        Ok(sent_count)
    }

    /// Process a batch of direct messages
    async fn process_direct_batch(
        _topic: &str,
        messages: Vec<Arc<Message>>,
        _connection_manager: &ConnectionManager,
    ) -> Result<u64, String> {
        // Direct messages are typically sent individually
        // This is a placeholder for future batch optimization
        Ok(messages.len() as u64)
    }

    /// Start the batch timeout processor
    pub async fn start_timeout_processor(&self) -> tokio::task::JoinHandle<()> {
        let batches = self.batches.clone();
        let batch_sender = self.batch_sender.clone();
        let config = self.config.clone();
        let stats = self.stats.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.max_batch_delay / 2);

            loop {
                interval.tick().await;

                // Check for expired batches
                let mut expired_batches = Vec::new();

                for entry in batches.iter() {
                    let batch = entry.value();
                    if batch.created_at.elapsed() >= config.max_batch_delay && !batch.is_empty() {
                        expired_batches.push(entry.key().clone());
                    }
                }

                // Send expired batches
                for topic_key in expired_batches {
                    if let Some((_, batch)) = batches.remove(&topic_key) {
                        debug!(
                            "Sending expired batch {} for topic '{}' with {} messages",
                            batch.batch_id,
                            batch.topic,
                            batch.len()
                        );

                        if let Err(e) = batch_sender.try_send(batch) {
                            error!("Failed to send expired batch: {}", e);
                        } else {
                            let mut stats = stats.write().await;
                            stats.batches_processed += 1;
                        }
                    }
                }
            }
        })
    }

    /// Get current batching statistics
    pub async fn get_stats(&self) -> BatchingStats {
        self.stats.read().await.clone()
    }

    /// Clear all pending batches (for shutdown)
    pub async fn clear_pending_batches(&self) -> usize {
        let mut cleared_count = 0;
        let batches_to_clear: Vec<String> = self
            .batches
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for topic_key in batches_to_clear {
            if let Some((_, batch)) = self.batches.remove(&topic_key) {
                cleared_count += batch.len();

                // Try to send the batch before clearing
                if let Err(e) = self.batch_sender.try_send(batch) {
                    warn!("Failed to send batch during shutdown: {}", e);
                }
            }
        }

        info!("Cleared {} pending batched messages", cleared_count);
        cleared_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batching_config_default() {
        let config = BatchingConfig::default();
        assert_eq!(config.max_batch_size, 100);
        assert!(config.enable_broadcast_batching);
        assert!(config.max_batch_delay <= Duration::from_millis(100));
    }

    #[test]
    fn test_batching_config_custom() {
        let config = BatchingConfig {
            max_batch_size: 50,
            max_batch_delay: Duration::from_secs(1),
            max_memory_usage: 1_000_000,
            enable_broadcast_batching: true,
            enable_database_batching: false,
            enable_direct_batching: false,
        };
        assert_eq!(config.max_batch_size, 50);
        assert_eq!(config.max_batch_delay, Duration::from_secs(1));
    }

    #[test]
    fn test_message_batch_creation() {
        let batch = MessageBatch::new("topic".into());
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }

    fn sample_message() -> Arc<Message> {
        Arc::new(Message::Broadcast {
            topic: "ch".into(),
            payload: sonic_rs::json!({"k": "v"}),
            timestamp: None,
            operation_id: None,
            request_id: None,
        })
    }

    #[test]
    fn message_batch_adds_and_estimates_size() {
        let mut batch = MessageBatch::new("topic".into());
        batch.add_message(sample_message()).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
        assert!(batch.estimated_size > 0, "size estimate must accumulate");
        batch.add_message(sample_message()).unwrap();
        assert_eq!(batch.len(), 2);
        let first_size = batch.estimated_size;
        assert!(first_size >= 2);
    }

    #[test]
    fn message_batch_ready_by_count() {
        let config = BatchingConfig {
            max_batch_size: 2,
            max_batch_delay: Duration::from_secs(60),
            max_memory_usage: 1_000_000,
            ..Default::default()
        };
        let mut batch = MessageBatch::new("topic".into());
        assert!(!batch.is_ready(&config));
        batch.add_message(sample_message()).unwrap();
        assert!(!batch.is_ready(&config));
        batch.add_message(sample_message()).unwrap();
        assert!(
            batch.is_ready(&config),
            "count threshold must trigger ready"
        );
    }

    #[test]
    fn message_batch_ready_by_memory() {
        let config = BatchingConfig {
            max_batch_size: 100,
            max_batch_delay: Duration::from_secs(60),
            max_memory_usage: 1,
            ..Default::default()
        };
        let mut batch = MessageBatch::new("topic".into());
        batch.add_message(sample_message()).unwrap();
        assert!(
            batch.is_ready(&config),
            "memory threshold must trigger ready"
        );
    }

    #[test]
    fn message_batch_ready_by_delay() {
        let config = BatchingConfig {
            max_batch_size: 100,
            max_batch_delay: Duration::from_millis(1),
            max_memory_usage: 1_000_000,
            ..Default::default()
        };
        let batch = MessageBatch::new("topic".into());
        std::thread::sleep(Duration::from_millis(10));
        assert!(batch.is_ready(&config), "age threshold must trigger ready");
    }
}
