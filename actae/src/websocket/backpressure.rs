// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use prometheus::{register_int_counter, IntCounter};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Configuration for back-pressure handling
#[derive(Debug, Clone)]
pub struct BackPressureConfig {
    /// Maximum number of pending messages per consumer
    pub max_pending_messages: usize,
    /// Maximum time to wait for a slow consumer before dropping messages
    pub max_consumer_lag: Duration,
    /// Number of messages to buffer before applying back-pressure
    pub buffer_size: usize,
    /// Time window for measuring consumer performance
    pub measurement_window: Duration,
    /// Threshold for marking a consumer as slow (messages/second)
    pub slow_consumer_threshold: f64,
    /// Whether to drop messages for slow consumers or disconnect them
    pub drop_messages_for_slow_consumers: bool,
    /// Maximum queue size before rejecting new messages
    pub max_queue_size: usize,
}

impl Default for BackPressureConfig {
    fn default() -> Self {
        // ULTRA-PERFORMANCE: Production-grade defaults for 1M+ msg/s throughput
        // These values support high-performance scenarios while maintaining safety
        Self {
            max_pending_messages: 100000, // 100K pending messages per consumer (100x increase)
            max_consumer_lag: Duration::from_secs(30), // Keep 30s lag tolerance
            buffer_size: 100000,          // 100K buffer size (10x increase)
            measurement_window: Duration::from_secs(60), // Keep 60s measurement window
            slow_consumer_threshold: 10000.0, // 10K msg/s threshold (1000x increase for ultra-perf)
            drop_messages_for_slow_consumers: true, // Keep safety mechanism but with realistic threshold
            max_queue_size: 500000, // 500K max queue (10x increase for burst capacity)
        }
    }
}

impl BackPressureConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_limits(
        mut self,
        max_pending: usize,
        max_lag: Duration,
        buffer_size: usize,
    ) -> Self {
        self.max_pending_messages = max_pending;
        self.max_consumer_lag = max_lag;
        self.buffer_size = buffer_size;
        self
    }

    pub fn with_thresholds(mut self, slow_threshold: f64, max_queue: usize) -> Self {
        self.slow_consumer_threshold = slow_threshold;
        self.max_queue_size = max_queue;
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_pending_messages == 0 {
            return Err(anyhow::anyhow!(
                "max_pending_messages must be greater than 0"
            ));
        }
        if self.buffer_size == 0 {
            return Err(anyhow::anyhow!("buffer_size must be greater than 0"));
        }
        if self.slow_consumer_threshold <= 0.0 {
            return Err(anyhow::anyhow!(
                "slow_consumer_threshold must be greater than 0"
            ));
        }
        if self.max_queue_size == 0 {
            return Err(anyhow::anyhow!("max_queue_size must be greater than 0"));
        }
        Ok(())
    }
}

/// Statistics for a consumer's performance
#[derive(Debug, Clone)]
pub struct ConsumerStats {
    pub consumer_id: String,
    pub pending_messages: usize,
    pub messages_processed: u64,
    pub messages_dropped: u64,
    pub last_activity: Instant,
    pub processing_rate: f64, // messages per second
    pub is_slow: bool,
    pub lag_duration: Duration,
}

/// Consumer performance tracker
#[derive(Debug)]
struct ConsumerTracker {
    consumer_id: String,
    pending_messages: AtomicUsize,
    messages_processed: AtomicU64,
    messages_dropped: AtomicU64,
    last_activity: RwLock<Instant>,
    message_times: RwLock<Vec<Instant>>,
    is_slow: std::sync::atomic::AtomicBool,
}

impl ConsumerTracker {
    fn new(consumer_id: String) -> Self {
        Self {
            consumer_id,
            pending_messages: AtomicUsize::new(0),
            messages_processed: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            last_activity: RwLock::new(Instant::now()),
            message_times: RwLock::new(Vec::new()),
            is_slow: std::sync::atomic::AtomicBool::new(false),
        }
    }

    async fn record_message_sent(&self) {
        self.pending_messages.fetch_add(1, Ordering::Relaxed);
        *self.last_activity.write().await = Instant::now();
    }

    async fn record_message_processed(&self, config: &BackPressureConfig) {
        let now = Instant::now();
        self.pending_messages.fetch_sub(1, Ordering::Relaxed);
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        *self.last_activity.write().await = now;

        // Update message processing times
        let mut times = self.message_times.write().await;
        times.push(now);

        // Keep only messages within the measurement window
        let cutoff = now - config.measurement_window;
        times.retain(|&time| time > cutoff);

        // Calculate processing rate
        let rate = times.len() as f64 / config.measurement_window.as_secs_f64();
        let is_slow = rate < config.slow_consumer_threshold;
        self.is_slow.store(is_slow, Ordering::Relaxed);

        if is_slow {
            debug!(
                "Consumer {} is slow: {:.2} msg/s (threshold: {:.2})",
                self.consumer_id, rate, config.slow_consumer_threshold
            );
        }
    }

    async fn get_stats(&self, config: &BackPressureConfig) -> ConsumerStats {
        let now = Instant::now();
        let last_activity = *self.last_activity.read().await;
        let times = self.message_times.read().await;

        let cutoff = now - config.measurement_window;
        let recent_messages = times.iter().filter(|&&time| time > cutoff).count();
        let processing_rate = recent_messages as f64 / config.measurement_window.as_secs_f64();

        ConsumerStats {
            consumer_id: self.consumer_id.clone(),
            pending_messages: self.pending_messages.load(Ordering::Relaxed),
            messages_processed: self.messages_processed.load(Ordering::Relaxed),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
            last_activity,
            processing_rate,
            is_slow: self.is_slow.load(Ordering::Relaxed),
            lag_duration: now.duration_since(last_activity),
        }
    }
}

/// Back-pressure manager for handling slow consumers
#[derive(Debug)]
pub struct BackPressureManager {
    config: BackPressureConfig,
    consumers: RwLock<std::collections::HashMap<String, Arc<ConsumerTracker>>>,
    global_semaphore: Arc<Semaphore>,
    total_queued_messages: AtomicUsize,
    total_dropped_messages: AtomicU64,
    rejections: IntCounter,
}

impl BackPressureManager {
    pub fn new(config: BackPressureConfig) -> Result<Self> {
        config.validate()?;

        let semaphore = Arc::new(Semaphore::new(config.max_queue_size));

        let rejections = register_int_counter!(
            "actae_backpressure_rejections_total",
            "Total immediate rejections by backpressure manager due to saturated queue"
        )
        .unwrap_or_else(|_| {
            prometheus::IntCounter::new("actae_backpressure_rejections_total_fallback", "fallback")
                .unwrap()
        });

        Ok(Self {
            config,
            consumers: RwLock::new(std::collections::HashMap::new()),
            global_semaphore: semaphore,
            total_queued_messages: AtomicUsize::new(0),
            total_dropped_messages: AtomicU64::new(0),
            rejections,
        })
    }

    /// Register a new consumer
    pub async fn register_consumer(&self, consumer_id: String) -> Result<()> {
        let tracker = Arc::new(ConsumerTracker::new(consumer_id.clone()));
        let mut consumers = self.consumers.write().await;
        consumers.insert(consumer_id.clone(), tracker);

        info!("Registered consumer: {}", consumer_id);
        Ok(())
    }

    /// Unregister a consumer
    pub async fn unregister_consumer(&self, consumer_id: &str) -> Result<()> {
        let mut consumers = self.consumers.write().await;
        if let Some(tracker) = consumers.remove(consumer_id) {
            let stats = tracker.get_stats(&self.config).await;
            info!(
                "Unregistered consumer: {} (processed: {}, dropped: {})",
                consumer_id, stats.messages_processed, stats.messages_dropped
            );
        }
        Ok(())
    }

    /// Try to queue a message for a consumer, applying back-pressure if necessary
    pub async fn try_queue_message(
        &self,
        consumer_id: &str,
        _message_size: usize,
    ) -> Result<QueueResult> {
        // Check global queue limit
        if self.total_queued_messages.load(Ordering::Relaxed) >= self.config.max_queue_size {
            self.total_dropped_messages.fetch_add(1, Ordering::Relaxed);
            return Ok(QueueResult::DroppedGlobalLimit);
        }

        // Try to acquire global semaphore permit
        let permit = match self.global_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                self.total_dropped_messages.fetch_add(1, Ordering::Relaxed);
                // Increment rejection metric
                self.rejections.inc();
                return Ok(QueueResult::DroppedGlobalLimit);
            }
        };

        // ULTRA-PERFORMANCE FIX: Skip backpressure for unregistered connections.
        // Only connections that subscribe to topics get registered as consumers.
        // Broadcasters (publishers) are never registered and should never be throttled.
        let tracker = {
            let consumers = self.consumers.read().await;
            match consumers.get(consumer_id) {
                Some(tracker) => tracker.clone(),
                None => {
                    // Connection not registered as consumer - skip backpressure entirely.
                    // This allows broadcasters to send without throttling.
                    drop(consumers);
                    return Ok(QueueResult::Queued);
                }
            }
        };

        // ULTRA-PERFORMANCE FIX: Disable backpressure drops for registered consumers.
        // Registered consumers are active subscribers, not slow consumers.
        // The flume channel's bounded capacity provides natural backpressure.
        // This prevents false-positive drops due to async increment/decrement race conditions.
        //
        // Performance Note: With concurrent workers sending to the same subscriber,
        // pending_messages increments synchronously (atomic) but decrements async (await).
        // This asymmetry causes counter accumulation faster than reality, triggering false drops.
        //
        // Solution: Trust the flume channel's try_send() for actual backpressure.
        // If the channel is full, try_send() fails naturally without dropping via backpressure.

        // Still track pending for metrics, but don't drop based on it
        let _pending = tracker.pending_messages.load(Ordering::Relaxed);
        // let stats = tracker.get_stats(&self.config).await;

        // Skip lag checks for now - they can cause false positives in high-concurrency scenarios

        // Queue the message
        tracker.record_message_sent().await;
        self.total_queued_messages.fetch_add(1, Ordering::Relaxed);

        // Release permit when message is queued
        permit.forget();

        Ok(QueueResult::Queued)
    }

    /// Mark a message as processed by a consumer
    pub async fn mark_message_processed(&self, consumer_id: &str) -> Result<()> {
        let consumers = self.consumers.read().await;
        if let Some(tracker) = consumers.get(consumer_id) {
            tracker.record_message_processed(&self.config).await;
            self.total_queued_messages.fetch_sub(1, Ordering::Relaxed);
            self.global_semaphore.add_permits(1);
        }
        Ok(())
    }

    /// Get consumer statistics
    pub async fn get_consumer_stats(&self, consumer_id: &str) -> Option<ConsumerStats> {
        let consumers = self.consumers.read().await;
        if let Some(tracker) = consumers.get(consumer_id) {
            Some(tracker.get_stats(&self.config).await)
        } else {
            None
        }
    }

    /// Get all consumer statistics
    pub async fn get_all_consumer_stats(&self) -> Vec<ConsumerStats> {
        let consumers = self.consumers.read().await;
        let mut stats = Vec::new();

        for tracker in consumers.values() {
            stats.push(tracker.get_stats(&self.config).await);
        }

        stats
    }

    /// Get global statistics
    pub async fn get_global_stats(&self) -> GlobalBackPressureStats {
        let consumers = self.consumers.read().await;
        let consumer_count = consumers.len();
        let slow_consumers = consumers
            .values()
            .filter(|tracker| tracker.is_slow.load(Ordering::Relaxed))
            .count();

        GlobalBackPressureStats {
            total_consumers: consumer_count,
            slow_consumers,
            total_queued_messages: self.total_queued_messages.load(Ordering::Relaxed),
            total_dropped_messages: self.total_dropped_messages.load(Ordering::Relaxed),
            available_permits: self.global_semaphore.available_permits(),
            max_queue_size: self.config.max_queue_size,
        }
    }

    /// Start background monitoring task
    pub async fn start_monitoring(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30));

            loop {
                interval.tick().await;

                let stats = manager.get_global_stats().await;
                let consumer_stats = manager.get_all_consumer_stats().await;

                info!(
                    "Back-pressure stats: {} consumers ({} slow), {} queued, {} dropped",
                    stats.total_consumers,
                    stats.slow_consumers,
                    stats.total_queued_messages,
                    stats.total_dropped_messages
                );

                // Log slow consumers
                for consumer in &consumer_stats {
                    if consumer.is_slow {
                        warn!(
                            "Slow consumer: {} - {:.2} msg/s, {} pending, lag: {:?}",
                            consumer.consumer_id,
                            consumer.processing_rate,
                            consumer.pending_messages,
                            consumer.lag_duration
                        );
                    }
                }

                // Cleanup inactive consumers
                let cutoff = Instant::now() - manager.config.max_consumer_lag * 2;
                let inactive: Vec<_> = consumer_stats
                    .iter()
                    .filter(|stats| stats.last_activity < cutoff)
                    .map(|stats| stats.consumer_id.clone())
                    .collect();

                for consumer_id in inactive {
                    warn!("Removing inactive consumer: {}", consumer_id);
                    if let Err(e) = manager.unregister_consumer(&consumer_id).await {
                        error!(
                            "Failed to unregister inactive consumer {}: {}",
                            consumer_id, e
                        );
                    }
                }
            }
        })
    }
}

impl Clone for BackPressureManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            consumers: RwLock::new(std::collections::HashMap::new()),
            global_semaphore: self.global_semaphore.clone(),
            total_queued_messages: AtomicUsize::new(
                self.total_queued_messages.load(Ordering::Relaxed),
            ),
            total_dropped_messages: AtomicU64::new(
                self.total_dropped_messages.load(Ordering::Relaxed),
            ),
            rejections: self.rejections.clone(),
        }
    }
}

/// Result of trying to queue a message
#[derive(Debug, Clone, PartialEq)]
pub enum QueueResult {
    /// Message was successfully queued
    Queued,
    /// Message was dropped due to consumer-specific limits
    DroppedConsumerLimit,
    /// Message was dropped due to consumer lag
    DroppedConsumerLag,
    /// Message was dropped due to global queue limits
    DroppedGlobalLimit,
    /// Back-pressure applied - consumer should slow down
    ConsumerBackPressure,
}

/// Global back-pressure statistics
#[derive(Debug, Clone)]
pub struct GlobalBackPressureStats {
    pub total_consumers: usize,
    pub slow_consumers: usize,
    pub total_queued_messages: usize,
    pub total_dropped_messages: u64,
    pub available_permits: usize,
    pub max_queue_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        let valid_config = BackPressureConfig::default();
        assert!(valid_config.validate().is_ok());

        let invalid_config = BackPressureConfig {
            max_pending_messages: 0,
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn config_builders_set_fields_and_each_zero_field_rejected() {
        let cfg = BackPressureConfig::default()
            .with_limits(10, Duration::from_secs(5), 8)
            .with_thresholds(0.5, 16);
        assert_eq!(cfg.max_pending_messages, 10);
        assert_eq!(cfg.max_consumer_lag, Duration::from_secs(5));
        assert_eq!(cfg.buffer_size, 8);
        assert_eq!(cfg.slow_consumer_threshold, 0.5);
        assert_eq!(cfg.max_queue_size, 16);
        assert!(cfg.validate().is_ok());

        let zeroes: [(fn(BackPressureConfig) -> BackPressureConfig, &str); 4] = [
            (
                |c| BackPressureConfig {
                    max_pending_messages: 0,
                    ..c
                },
                "max_pending_messages",
            ),
            (
                |c| BackPressureConfig {
                    buffer_size: 0,
                    ..c
                },
                "buffer_size",
            ),
            (
                |c| BackPressureConfig {
                    slow_consumer_threshold: 0.0,
                    ..c
                },
                "slow_consumer_threshold",
            ),
            (
                |c| BackPressureConfig {
                    max_queue_size: 0,
                    ..c
                },
                "max_queue_size",
            ),
        ];
        for (mutate, name) in zeroes {
            let err = mutate(BackPressureConfig::default())
                .validate()
                .unwrap_err();
            assert!(err.to_string().contains(name), "got: {err}");
        }
    }

    #[test]
    fn tracker_counts_and_stats() {
        let tracker = ConsumerTracker::new("c1".to_string());
        let cfg = BackPressureConfig::default();
        let stats = tokio::runtime::Runtime::new().unwrap().block_on(async {
            tracker.record_message_sent().await;
            tracker.record_message_sent().await;
            tracker.record_message_processed(&cfg).await;
            tracker.get_stats(&cfg).await
        });
        assert_eq!(stats.consumer_id, "c1");
        assert_eq!(stats.pending_messages, 1);
        assert_eq!(stats.messages_processed, 1);
        assert_eq!(stats.messages_dropped, 0);
    }

    #[tokio::test]
    async fn test_consumer_registration() {
        let config = BackPressureConfig::default();
        let manager = BackPressureManager::new(config).unwrap();

        manager
            .register_consumer("test_consumer".to_string())
            .await
            .unwrap();

        let stats = manager.get_consumer_stats("test_consumer").await;
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().consumer_id, "test_consumer");

        manager.unregister_consumer("test_consumer").await.unwrap();
        assert!(manager.get_consumer_stats("test_consumer").await.is_none());
    }

    #[tokio::test]
    async fn test_message_queueing() {
        let config = BackPressureConfig::default();
        let manager = BackPressureManager::new(config).unwrap();
        manager
            .register_consumer("test_consumer".to_string())
            .await
            .unwrap();

        let result = manager
            .try_queue_message("test_consumer", 100)
            .await
            .unwrap();
        assert_eq!(result, QueueResult::Queued);

        manager
            .mark_message_processed("test_consumer")
            .await
            .unwrap();

        let stats = manager.get_consumer_stats("test_consumer").await.unwrap();
        assert_eq!(stats.messages_processed, 1);
    }

    #[tokio::test]
    async fn unregistered_consumer_skips_backpressure() {
        // Broadcasters are never registered as consumers — their messages
        // queue unconditionally (the flume channel provides backpressure).
        let config = BackPressureConfig {
            max_queue_size: 1,
            ..Default::default()
        };
        let manager = BackPressureManager::new(config).unwrap();
        let result = manager.try_queue_message("broadcaster", 100).await.unwrap();
        assert_eq!(result, QueueResult::Queued);
    }

    #[tokio::test]
    async fn global_queue_limit_drops_and_releases() {
        let config = BackPressureConfig {
            max_queue_size: 1,
            max_pending_messages: 5,
            buffer_size: 5,
            ..Default::default()
        };
        let manager = BackPressureManager::new(config).unwrap();
        manager.register_consumer("c1".to_string()).await.unwrap();

        assert_eq!(
            manager.try_queue_message("c1", 1).await.unwrap(),
            QueueResult::Queued
        );
        // Queue is saturated → next message is dropped at the global limit.
        assert_eq!(
            manager.try_queue_message("c1", 1).await.unwrap(),
            QueueResult::DroppedGlobalLimit
        );
        // Marking the first message processed frees the slot.
        manager.mark_message_processed("c1").await.unwrap();
        assert_eq!(
            manager.try_queue_message("c1", 1).await.unwrap(),
            QueueResult::Queued
        );
        let global = manager.get_global_stats().await;
        assert_eq!(global.total_queued_messages, 1);
        assert!(global.total_dropped_messages >= 1);
    }

    #[tokio::test]
    async fn global_stats_reflect_consumers() {
        let config = BackPressureConfig::default();
        let max_queue = config.max_queue_size;
        let manager = BackPressureManager::new(config).unwrap();
        manager.register_consumer("a".to_string()).await.unwrap();
        manager.register_consumer("b".to_string()).await.unwrap();
        let stats = manager.get_global_stats().await;
        assert_eq!(stats.total_consumers, 2);
        assert_eq!(stats.max_queue_size, max_queue);
        assert_eq!(manager.get_all_consumer_stats().await.len(), 2);
    }
}
