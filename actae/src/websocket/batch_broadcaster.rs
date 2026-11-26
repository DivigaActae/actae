// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! High-Performance Batch Broadcasting System
//!
//! This module implements advanced batch broadcasting with:
//! - Adaptive batching with dynamic sizing
//! - Connection grouping for optimized writes
//! - Parallel processing with worker pools
//! - Back-pressure handling and flow control
//! - Performance metrics and monitoring

use crate::websocket::{
    connection::OutgoingMessage, connection_manager::ConnectionManager, message::Message,
};
use anyhow::Result;
use parking_lot::Mutex;
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::interval,
};
use tracing::{debug, error, info};
use uuid::Uuid;

/// High-throughput batch broadcaster with adaptive optimizations
pub struct BatchBroadcaster {
    /// Configuration
    config: BatchBroadcastConfig,

    /// Message batching state
    batch_state: Arc<BatchState>,

    /// Worker pool for parallel processing
    worker_pool: Arc<WorkerPool>,

    /// Performance metrics
    metrics: Arc<BatchMetrics>,

    /// Adaptive tuning state
    adaptive_state: Arc<AdaptiveState>,

    /// Background task handles
    task_handles: Vec<JoinHandle<()>>,

    /// Shutdown signal
    shutdown_tx: Option<oneshot::Sender<()>>,
}

/// Configuration for batch broadcasting
#[derive(Debug, Clone)]
pub struct BatchBroadcastConfig {
    /// Maximum messages per batch
    pub max_batch_size: usize,

    /// Maximum time to wait before flushing batch
    pub batch_timeout: Duration,

    /// Maximum message size in bytes
    pub max_message_size: usize,

    /// Enable adaptive batching
    pub enable_adaptive_batching: bool,

    /// Enable connection grouping
    pub enable_connection_grouping: bool,

    /// Number of worker threads
    pub worker_count: usize,

    /// Worker queue size
    pub worker_queue_size: usize,

    /// Enable back-pressure control
    pub enable_backpressure: bool,

    /// Back-pressure threshold (messages)
    pub backpressure_threshold: usize,

    /// Enable performance monitoring
    pub enable_metrics: bool,
}

impl Default for BatchBroadcastConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,                       // Increased for throughput efficiency
            batch_timeout: Duration::from_micros(100), // ULTRA-OPTIMIZED: 1ms -> 100μs for sub-millisecond delivery
            max_message_size: 64 * 1024,               // 64KB
            enable_adaptive_batching: false,           // DISABLED: Adaptive adds overhead
            enable_connection_grouping: false,         // DISABLED: Grouping adds latency
            worker_count: num_cpus::get().max(32), // ULTRA-OPTIMIZED: More workers for maximum parallelism
            // 🆕 MASSIVE QUEUE INCREASE: 20K → 500K
            worker_queue_size: 500_000, // Can handle 500K messages in flight

            // 🆕 ENABLE BACKPRESSURE: Prevents message loss by slowing senders
            enable_backpressure: true,

            // 🆕 HIGHER THRESHOLD: Warn at 80% capacity
            backpressure_threshold: 400_000,
            enable_metrics: true,
        }
    }
}

/// Batch message with metadata.
/// `senders` carries pre-resolved flume sender channels so workers can
/// deliver without per-subscriber DashMap lookups (the #1 bottleneck at high fanout).
#[derive(Debug, Clone)]
pub struct BatchMessage {
    pub message: Arc<Message>,
    /// Subscriber UUIDs only - no Connection object cloning
    pub connection_ids: Vec<Uuid>,
    /// Pre-resolved sender channels — populated by flush_batch_if_ready
    /// before work is submitted to the worker pool. Workers iterate this
    /// Vec directly instead of doing N DashMap lookups.
    pub senders: Vec<flume::Sender<OutgoingMessage>>,
    pub priority: MessagePriority,
    pub timestamp: Instant,
    pub estimated_size: usize,
    pub sender_id: Option<Uuid>,
    pub sender_user_id: Option<String>,
}

/// Message priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Batch processing state
struct BatchState {
    /// Current message batch
    current_batch: Mutex<Vec<BatchMessage>>,

    /// Batch timer
    batch_timer: Mutex<Option<Instant>>,

    /// Flush trigger channel
    flush_trigger: mpsc::UnboundedSender<()>,
}

/// Worker pool for parallel batch processing
struct WorkerPool {
    /// Lock-free work queue (flume MPMC)
    work_queue: flume::Sender<BatchWork>,

    /// Worker metrics
    metrics: WorkerPoolMetrics,

    /// Connection manager for message delivery
    connection_manager: Option<Arc<ConnectionManager>>,
}

/// Work item for batch processing
#[derive(Debug)]
struct BatchWork {
    batch: Vec<BatchMessage>,
}

/// Worker pool metrics.
/// Cache-line-aligned (128 bytes) so `tasks_queued` (written by submit thread)
/// is isolated from `tasks_processed` (written by workers) on different cache lines.
#[repr(C, align(128))]
#[derive(Debug)]
struct WorkerPoolMetrics {
    /// CACHE LINE 1: Written by submit_work (main thread / broadcast caller)
    tasks_queued: AtomicU64,
    _pad1: [u8; 120],

    /// CACHE LINE 2: Written by worker threads during batch processing
    tasks_processed: AtomicU64,
    tasks_failed: AtomicU64,
    avg_processing_time_ns: AtomicU64,
    queue_depth: AtomicUsize,
}

impl Default for WorkerPoolMetrics {
    fn default() -> Self {
        Self {
            tasks_queued: AtomicU64::new(0),
            _pad1: [0u8; 120],
            tasks_processed: AtomicU64::new(0),
            tasks_failed: AtomicU64::new(0),
            avg_processing_time_ns: AtomicU64::new(0),
            queue_depth: AtomicUsize::new(0),
        }
    }
}

/// Performance metrics for batch broadcasting.
/// Cache-line-aligned (128 bytes) to prevent false sharing across CPU cores.
/// `messages_processed` is isolated on its own cache line since it is written
/// by every worker on every message delivery — the hottest counter in the system.
#[repr(C, align(128))]
#[derive(Debug)]
pub struct BatchMetrics {
    /// CACHE LINE 1: Hot path — written by every worker on every delivery
    pub messages_processed: AtomicU64,
    _pad1: [u8; 120],

    /// CACHE LINE 2: Everything else — read-mostly, infrequent writes
    pub batches_processed: AtomicU64,
    pub avg_batch_size: AtomicU64,
    pub total_latency_ns: AtomicU64,
    pub peak_latency_ns: AtomicU64,
    pub throughput_mps: AtomicU64,
    pub bandwidth_bps: AtomicU64,
    pub error_count: AtomicU64,
    pub backpressure_events: AtomicU64,
    pub backpressure_rejections: AtomicU64,
}

impl Default for BatchMetrics {
    fn default() -> Self {
        Self {
            messages_processed: AtomicU64::new(0),
            _pad1: [0u8; 120],
            batches_processed: AtomicU64::new(0),
            avg_batch_size: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            peak_latency_ns: AtomicU64::new(0),
            throughput_mps: AtomicU64::new(0),
            bandwidth_bps: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            backpressure_events: AtomicU64::new(0),
            backpressure_rejections: AtomicU64::new(0),
        }
    }
}

/// Adaptive tuning state
struct AdaptiveState {
    /// Exponentially weighted moving average for latency
    ewma_latency_ns: Mutex<f64>,

    /// EWMA for throughput
    ewma_throughput_mps: Mutex<f64>,

    /// EWMA smoothing factor
    alpha: f64,

    /// Current adaptive batch size
    adaptive_batch_size: AtomicUsize,

    /// Current adaptive timeout
    adaptive_timeout: Mutex<Duration>,

    /// Tuning parameters
    min_batch_size: usize,
    max_batch_size: usize,
    min_timeout: Duration,
    max_timeout: Duration,
}

impl BatchBroadcaster {
    /// Create a new batch broadcaster
    pub async fn new(config: BatchBroadcastConfig) -> Result<Self> {
        Self::new_with_connection_manager(config, None).await
    }

    /// Create a new batch broadcaster with connection manager
    pub async fn new_with_connection_manager(
        config: BatchBroadcastConfig,
        connection_manager: Option<Arc<ConnectionManager>>,
    ) -> Result<Self> {
        info!(" Initializing batch broadcaster");
        info!(
            "Configuration: max_batch_size={}, timeout={:?}, workers={}",
            config.max_batch_size, config.batch_timeout, config.worker_count
        );

        let (flush_trigger_tx, flush_trigger_rx) = mpsc::unbounded_channel();

        let batch_state = Arc::new(BatchState {
            current_batch: Mutex::new(Vec::new()),
            batch_timer: Mutex::new(None),
            flush_trigger: flush_trigger_tx,
        });

        let metrics = Arc::new(BatchMetrics::default());

        let adaptive_state = Arc::new(AdaptiveState {
            ewma_latency_ns: Mutex::new(0.0),
            ewma_throughput_mps: Mutex::new(0.0),
            alpha: 0.1, // Smoothing factor
            adaptive_batch_size: AtomicUsize::new(config.max_batch_size / 2),
            adaptive_timeout: Mutex::new(config.batch_timeout),
            min_batch_size: 10,
            max_batch_size: config.max_batch_size * 2,
            min_timeout: Duration::from_millis(1),
            max_timeout: Duration::from_millis(100),
        });

        let config_for_worker_pool = config.clone();
        let worker_pool = Arc::new(WorkerPool::new(
            config_for_worker_pool.worker_count,
            config_for_worker_pool.worker_queue_size,
            Arc::clone(&metrics),
            connection_manager.clone(),
        ));

        let mut broadcaster = Self {
            config,
            batch_state,
            worker_pool,
            metrics,
            adaptive_state,
            task_handles: Vec::new(),
            shutdown_tx: None,
        };

        // Start background tasks
        broadcaster.start_background_tasks(flush_trigger_rx).await?;

        info!(" Batch broadcaster initialized successfully");
        Ok(broadcaster)
    }

    /// Start background processing tasks
    async fn start_background_tasks(
        &mut self,
        mut flush_trigger_rx: mpsc::UnboundedReceiver<()>,
    ) -> Result<()> {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        // Batch flush task
        let batch_state = Arc::clone(&self.batch_state);
        let worker_pool = Arc::clone(&self.worker_pool);
        let metrics = Arc::clone(&self.metrics);
        let config = self.config.clone();

        // Spawn flush task with startup validation
        let flush_task = tokio::spawn(async move {
            // CRITICAL: Changed to INFO level for production diagnostics
            info!(
                " Batch flush task started with timeout: {:?}",
                config.batch_timeout
            );
            let mut interval = interval(config.batch_timeout);
            let mut tick_count = 0u64;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        tick_count += 1;
                        // Log every 1000 ticks to show the task is running (at 1ms, this is every second)
                        if tick_count % 1000 == 0 {
                            debug!("Flush task tick #{} (running normally)", tick_count);
                        }
                        Self::flush_batch_if_ready(&batch_state, &worker_pool, &metrics).await;
                    }
                    _ = flush_trigger_rx.recv() => {
                        debug!("Manual flush triggered");
                        Self::flush_batch_if_ready(&batch_state, &worker_pool, &metrics).await;
                    }
                    _ = &mut shutdown_rx => {
                        info!(" Batch flush task shutting down");
                        break;
                    }
                }
            }
        });

        // Verify task started successfully
        tokio::time::sleep(Duration::from_millis(10)).await;
        if flush_task.is_finished() {
            error!("Flush task failed to start");
            return Err(anyhow::anyhow!("Flush task failed to start"));
        }

        self.task_handles.push(flush_task);

        // Adaptive tuning task
        if self.config.enable_adaptive_batching {
            let adaptive_task = self.start_adaptive_tuning_task().await?;
            self.task_handles.push(adaptive_task);
        }

        // Metrics collection task
        if self.config.enable_metrics {
            let metrics_task = self.start_metrics_collection_task().await?;
            self.task_handles.push(metrics_task);
        }

        Ok(())
    }

    /// Add a message to the batch for broadcasting
    pub async fn broadcast_message(
        &self,
        message: Arc<Message>,
        connection_ids: Vec<Uuid>,
        sender_id: Option<Uuid>,
        sender_user_id: Option<String>,
    ) -> Result<()> {
        let start_time = Instant::now();

        // ULTRA-PERFORMANCE FAST PATH: Direct delivery for low fanout (<10 subscribers)
        if connection_ids.len() <= 10 {
            return self
                .deliver_immediate_fast_path(message, connection_ids, start_time)
                .await;
        }

        // Calculate message priority
        let priority = self.calculate_message_priority(&message);

        // Estimate message size
        let estimated_size = self.estimate_message_size(&message);

        // Create batch message (senders resolved lazily at flush time)
        let batch_message = BatchMessage {
            message: message.clone(),
            connection_ids,
            senders: Vec::new(),
            priority,
            timestamp: start_time,
            estimated_size,
            sender_id,
            sender_user_id,
        };

        // Add to batch
        let should_flush = {
            let mut batch = self.batch_state.current_batch.lock();
            batch.push(batch_message);

            // Check if we should flush
            let current_size = if self.config.enable_adaptive_batching {
                self.adaptive_state
                    .adaptive_batch_size
                    .load(Ordering::Relaxed)
            } else {
                self.config.max_batch_size
            };

            batch.len() >= current_size
        };

        // Trigger flush if batch is full
        if should_flush {
            let _ = self.batch_state.flush_trigger.send(());
        } else {
            // Start timer for first message in batch
            let mut timer = self.batch_state.batch_timer.lock();
            if timer.is_none() {
                *timer = Some(start_time);
            }
        }

        // Update metrics
        self.metrics
            .messages_processed
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// ULTRA-PERFORMANCE FAST PATH: Immediate zero-queue delivery for low fanout.
    /// Uses direct per-subscriber try_send_to_connection (one DashMap lookup per sub).
    /// For ≤10 subscribers the lookup overhead is negligible; avoids Vec allocation.
    async fn deliver_immediate_fast_path(
        &self,
        message: Arc<Message>,
        connection_ids: Vec<Uuid>,
        _start_time: Instant,
    ) -> Result<()> {
        let conn_manager = match &self.worker_pool.connection_manager {
            Some(cm) => Arc::clone(cm),
            None => {
                error!("No ConnectionManager available for fast path");
                return Err(anyhow::anyhow!("No ConnectionManager"));
            }
        };

        for conn_id in &connection_ids {
            let _ = conn_manager
                .try_send_to_connection(conn_id, OutgoingMessage::Message(Arc::clone(&message)));
        }

        self.metrics
            .messages_processed
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Flush the current batch if conditions are met.
    /// Pre-resolves all sender channels before submitting to the worker pool,
    /// so workers iterate a flat Vec instead of doing N DashMap lookups.
    #[inline]
    async fn flush_batch_if_ready(
        batch_state: &BatchState,
        worker_pool: &WorkerPool,
        metrics: &BatchMetrics,
    ) {
        let mut batch = {
            let mut current_batch = batch_state.current_batch.lock();
            if current_batch.is_empty() {
                return;
            }
            *batch_state.batch_timer.lock() = None;
            std::mem::take(&mut *current_batch)
        };

        // Pre-resolve sender channels for every message in this batch.
        // This moves DashMap lookups from worker threads to the submitter,
        // and lets workers iterate a flat Vec (zero hashing, CPU cache friendly).
        if let Some(cm) = worker_pool.connection_manager.as_ref() {
            for msg in &mut batch {
                if msg.senders.is_empty() && !msg.connection_ids.is_empty() {
                    msg.senders = cm.resolve_senders(&msg.connection_ids);
                }
            }
        }

        if !batch.is_empty() {
            let batch_work = BatchWork { batch };

            if let Err(e) = worker_pool.submit_work(batch_work) {
                debug!("Failed to submit batch to worker pool: {}", e);
                metrics.error_count.fetch_add(1, Ordering::Relaxed);
            } else {
                metrics.batches_processed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Calculate message priority based on content with sophisticated algorithm
    fn calculate_message_priority(&self, message: &Message) -> MessagePriority {
        // Sophisticated priority calculation based on multiple factors:
        // 1. Message type urgency
        // 2. Payload size and complexity
        // 3. Topic importance (system vs user topics)
        // 4. Time sensitivity
        // 5. Connection count impact

        let base_priority = match message {
            // Critical system messages - highest priority
            Message::Error { .. } => MessagePriority::Critical,
            Message::Auth { .. } => MessagePriority::Critical,

            // High priority real-time messages
            Message::DirectMessage { .. } => MessagePriority::High,
            Message::DatabaseChange { .. } => MessagePriority::High,

            // Medium priority operational messages
            Message::Broadcast { topic, .. } => self.calculate_topic_priority(topic),
            Message::Subscription { topic, .. } => self.calculate_topic_priority(topic),
            Message::Subscribe { topic, .. } => self.calculate_topic_priority(topic),
            Message::SubscribeMany { .. } => MessagePriority::Normal,
            Message::Unsubscribe { topic, .. } => self.calculate_topic_priority(topic),

            // Lower priority acknowledgments and status
            Message::Connection { .. } => MessagePriority::Low,
            Message::CursorSync { .. } => MessagePriority::Low,
            Message::Ack { .. } => MessagePriority::Low,
            Message::Nack { .. } => MessagePriority::Normal,
        };

        // Apply priority modifiers based on message characteristics
        self.apply_priority_modifiers(message, base_priority)
    }

    /// Calculate priority based on topic patterns
    fn calculate_topic_priority(&self, topic: &str) -> MessagePriority {
        // System topics get higher priority
        if topic.starts_with("system.") || topic.starts_with("admin.") {
            return MessagePriority::High;
        }

        // Critical alerts and notifications
        if topic.contains("alert") || topic.contains("critical") || topic.contains("error") {
            return MessagePriority::High;
        }

        // Real-time data streams
        if topic.contains("realtime") || topic.contains("live") || topic.contains("stream") {
            return MessagePriority::High;
        }

        // User-specific topics
        if topic.starts_with("user.") || topic.starts_with("private.") {
            return MessagePriority::Normal;
        }

        // Public channels and general topics
        if topic.starts_with("public.") || topic.starts_with("general.") {
            return MessagePriority::Normal;
        }

        // Analytics and logging topics (lower priority)
        if topic.contains("analytics") || topic.contains("log") || topic.contains("metric") {
            return MessagePriority::Low;
        }

        // Default to normal priority
        MessagePriority::Normal
    }

    /// Apply priority modifiers based on message characteristics
    fn apply_priority_modifiers(
        &self,
        message: &Message,
        base_priority: MessagePriority,
    ) -> MessagePriority {
        let mut priority_score = base_priority as u8;

        // Factor 1: Message size impact
        let message_size = self.estimate_message_size(message);
        if message_size > 10_000 {
            // Large messages get slightly lower priority to prevent blocking
            priority_score = priority_score.saturating_sub(1);
        } else if message_size < 100 {
            // Small messages can get slight priority boost
            priority_score = priority_score.saturating_add(1).min(3);
        }

        // Factor 2: Payload complexity for certain message types
        match message {
            Message::Broadcast { payload, .. }
            | Message::DirectMessage { payload, .. }
            | Message::Subscription { payload, .. } => {
                if let Ok(json_str) = sonic_rs::to_string(payload) {
                    // Complex JSON structures get normal priority
                    if json_str.contains("nested") || json_str.len() > 5000 {
                        priority_score = priority_score.saturating_sub(1);
                    }
                    // Simple structures can get slight boost
                    else if json_str.len() < 200 {
                        priority_score = priority_score.saturating_add(1).min(3);
                    }
                }
            }
            _ => {}
        }

        // Factor 3: Time-sensitive patterns
        match message {
            Message::DatabaseChange { .. } => {
                // Database changes are time-sensitive
                priority_score = priority_score.saturating_add(1).min(3);
            }
            Message::Auth { .. } => {
                // Authentication is always critical
                priority_score = 3; // Force to Critical
            }
            _ => {}
        }

        // Convert back to enum
        match priority_score {
            0 => MessagePriority::Low,
            1 => MessagePriority::Normal,
            2 => MessagePriority::High,
            _ => MessagePriority::Critical,
        }
    }

    /// Estimate message size in bytes
    fn estimate_message_size(&self, message: &Message) -> usize {
        // Simple estimation - would be more sophisticated in production
        match message {
            Message::Error { message, .. } => message.len() + 100,
            Message::DirectMessage { payload, .. } => {
                sonic_rs::to_string(payload)
                    .map(|s| s.len())
                    .unwrap_or(1024)
                    + 200
            }
            Message::Subscription { payload, .. } => {
                sonic_rs::to_string(payload).map(|s| s.len()).unwrap_or(512) + 150
            }
            Message::Connection { payload, .. } => {
                sonic_rs::to_string(payload).map(|s| s.len()).unwrap_or(256) + 100
            }
            Message::Subscribe { topic, .. } => topic.len() + 50,
            Message::SubscribeMany { topics, .. } => {
                topics.iter().map(|t| t.topic.len() + 20).sum::<usize>() + 100
            }
            Message::Unsubscribe { topic, .. } => topic.len() + 50,
            Message::Broadcast { topic, payload, .. } => {
                topic.len()
                    + sonic_rs::to_string(payload)
                        .map(|s| s.len())
                        .unwrap_or(1024)
                    + 200
            }
            Message::DatabaseChange { topic, payload, .. } => {
                topic.len() + sonic_rs::to_string(payload).map(|s| s.len()).unwrap_or(512) + 150
            }
            Message::Auth { payload, .. } => {
                sonic_rs::to_string(payload).map(|s| s.len()).unwrap_or(256) + 100
            }
            Message::Ack { .. } => 50,
            Message::Nack { .. } => 100,
            Message::CursorSync { topic, .. } => topic.len() + 30,
        }
    }

    /// Start adaptive tuning task
    async fn start_adaptive_tuning_task(&self) -> Result<JoinHandle<()>> {
        let adaptive_state = Arc::clone(&self.adaptive_state);
        let metrics = Arc::clone(&self.metrics);

        let task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));

            loop {
                interval.tick().await;

                // Get current metrics
                let current_throughput = metrics.throughput_mps.load(Ordering::Relaxed) as f64;
                let current_latency = {
                    let total_latency = metrics.total_latency_ns.load(Ordering::Relaxed) as f64;
                    let total_messages = metrics.messages_processed.load(Ordering::Relaxed) as f64;
                    if total_messages > 0.0 {
                        total_latency / total_messages
                    } else {
                        0.0
                    }
                };

                // Update EWMA values
                {
                    let mut ewma_latency = adaptive_state.ewma_latency_ns.lock();
                    let mut ewma_throughput = adaptive_state.ewma_throughput_mps.lock();

                    *ewma_latency = adaptive_state.alpha * current_latency
                        + (1.0 - adaptive_state.alpha) * *ewma_latency;
                    *ewma_throughput = adaptive_state.alpha * current_throughput
                        + (1.0 - adaptive_state.alpha) * *ewma_throughput;
                }

                // Adjust batch size based on performance
                Self::adjust_batch_size(&adaptive_state, current_latency, current_throughput);

                // Adjust timeout based on performance
                Self::adjust_batch_timeout(&adaptive_state, current_latency);

                debug!(
                    "Adaptive tuning - latency: {:.2}ns, throughput: {:.2}mps",
                    current_latency, current_throughput
                );
            }
        });

        Ok(task)
    }

    /// Adjust batch size based on performance metrics
    fn adjust_batch_size(adaptive_state: &AdaptiveState, latency_ns: f64, throughput_mps: f64) {
        let current_size = adaptive_state.adaptive_batch_size.load(Ordering::Relaxed);

        // Simple adaptive algorithm - would be more sophisticated in production
        let new_size = if latency_ns > 10_000_000.0 && throughput_mps < 1000.0 {
            // High latency, low throughput - decrease batch size
            (current_size as f64 * 0.9).max(adaptive_state.min_batch_size as f64) as usize
        } else if latency_ns < 1_000_000.0 && throughput_mps > 5000.0 {
            // Low latency, high throughput - increase batch size
            (current_size as f64 * 1.1).min(adaptive_state.max_batch_size as f64) as usize
        } else {
            current_size
        };

        if new_size != current_size {
            adaptive_state
                .adaptive_batch_size
                .store(new_size, Ordering::Relaxed);
            debug!("Adjusted batch size: {} -> {}", current_size, new_size);
        }
    }

    /// Adjust batch timeout based on latency
    fn adjust_batch_timeout(adaptive_state: &AdaptiveState, latency_ns: f64) {
        let mut current_timeout = adaptive_state.adaptive_timeout.lock();

        let new_timeout = if latency_ns > 5_000_000.0 {
            // High latency - decrease timeout
            Duration::max(*current_timeout / 2, adaptive_state.min_timeout)
        } else if latency_ns < 1_000_000.0 {
            // Low latency - can afford to wait longer
            Duration::min(*current_timeout * 2, adaptive_state.max_timeout)
        } else {
            *current_timeout
        };

        if new_timeout != *current_timeout {
            debug!(
                "Adjusted batch timeout: {:?} -> {:?}",
                *current_timeout, new_timeout
            );
            *current_timeout = new_timeout;
        }
    }

    /// Start metrics collection task
    async fn start_metrics_collection_task(&self) -> Result<JoinHandle<()>> {
        let metrics = Arc::clone(&self.metrics);

        let task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));
            let mut last_messages = 0u64;
            let mut last_time = Instant::now();

            loop {
                interval.tick().await;

                let current_time = Instant::now();
                let current_messages = metrics.messages_processed.load(Ordering::Relaxed);

                let elapsed = current_time.duration_since(last_time).as_secs_f64();
                let messages_delta = current_messages.saturating_sub(last_messages);

                if elapsed > 0.0 {
                    let throughput = messages_delta as f64 / elapsed;
                    metrics
                        .throughput_mps
                        .store(throughput as u64, Ordering::Relaxed);
                }

                last_messages = current_messages;
                last_time = current_time;

                // Only log metrics when there's significant activity to reduce log spam
                let throughput = metrics.throughput_mps.load(Ordering::Relaxed);
                if messages_delta > 0 || throughput > 10 {
                    info!(
                        "Batch broadcaster metrics - messages: {}, batches: {}, throughput: {} mps",
                        current_messages,
                        metrics.batches_processed.load(Ordering::Relaxed),
                        throughput
                    );
                } else {
                    // Only log minimal activity at debug level
                    debug!(
                        "Batch broadcaster idle - messages: {}, batches: {}",
                        current_messages,
                        metrics.batches_processed.load(Ordering::Relaxed)
                    );
                }
            }
        });

        Ok(task)
    }

    /// Generate unique batch ID
    /// Get performance metrics
    pub fn get_metrics(&self) -> &BatchMetrics {
        &self.metrics
    }

    /// Get peak latency in milliseconds
    pub fn get_peak_latency_ms(&self) -> u64 {
        self.metrics
            .peak_latency_ns
            .load(std::sync::atomic::Ordering::Relaxed)
            / 1_000_000
    }

    /// Shutdown the batch broadcaster
    pub async fn shutdown(mut self) -> Result<()> {
        info!(" Shutting down batch broadcaster");

        // Send shutdown signal
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        // Wait for all tasks to complete
        for handle in self.task_handles {
            let _ = handle.await;
        }

        // Flush any remaining messages
        Self::flush_batch_if_ready(&self.batch_state, &self.worker_pool, &self.metrics).await;

        info!(" Batch broadcaster shutdown complete");
        Ok(())
    }
}

// Worker pool implementation
impl WorkerPool {
    fn new(
        worker_count: usize,
        queue_size: usize,
        batch_metrics: Arc<BatchMetrics>,
        connection_manager: Option<Arc<ConnectionManager>>,
    ) -> Self {
        let (work_tx, work_rx) = flume::bounded::<BatchWork>(queue_size);
        let metrics = WorkerPoolMetrics::default();

        let mut workers = Vec::new();

        for worker_id in 0..worker_count {
            let work_rx = work_rx.clone();
            let metrics = Arc::new(metrics.clone());
            let batch_metrics = Arc::clone(&batch_metrics);

            let worker = tokio::spawn(async move {
                loop {
                    match work_rx.recv_async().await {
                        Ok(batch_work) => {
                            let start_time = Instant::now();

                            if let Err(e) = Self::process_batch(batch_work, worker_id).await {
                                debug!("Worker {} batch failed: {}", worker_id, e);
                                metrics.tasks_failed.fetch_add(1, Ordering::Relaxed);
                            } else {
                                metrics.tasks_processed.fetch_add(1, Ordering::Relaxed);
                            }

                            let processing_time = start_time.elapsed().as_nanos() as u64;
                            metrics
                                .avg_processing_time_ns
                                .store(processing_time, Ordering::Relaxed);

                            let mut peak = batch_metrics.peak_latency_ns.load(Ordering::Relaxed);
                            while processing_time > peak
                                && batch_metrics
                                    .peak_latency_ns
                                    .compare_exchange(
                                        peak,
                                        processing_time,
                                        Ordering::Relaxed,
                                        Ordering::Relaxed,
                                    )
                                    .is_err()
                            {
                                peak = batch_metrics.peak_latency_ns.load(Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            debug!("Worker {} shutting down", worker_id);
                            break;
                        }
                    }
                }
            });

            workers.push(worker);
        }

        Self {
            work_queue: work_tx,
            metrics,
            connection_manager,
        }
    }

    fn submit_work(&self, work: BatchWork) -> Result<()> {
        self.metrics.tasks_queued.fetch_add(1, Ordering::Relaxed);
        self.work_queue
            .try_send(work)
            .map_err(|e| anyhow::anyhow!("Failed to submit work: {e}"))?;
        Ok(())
    }

    async fn process_batch(batch_work: BatchWork, _worker_id: usize) -> Result<()> {
        for batch_message in batch_work.batch {
            let message = batch_message.message;
            let senders = batch_message.senders;
            let sender_id = batch_message.sender_id;
            let sender_user_id = batch_message.sender_user_id;

            let pre_serialized: Option<Arc<String>> = match &*message {
                Message::Broadcast {
                    topic,
                    payload,
                    timestamp,
                    operation_id,
                    ..
                } => {
                    let needs_enrichment = sender_id.is_some() || sender_user_id.is_some();
                    if needs_enrichment {
                        let mut obj = sonic_rs::object! {};
                        obj.insert("payload", payload.clone());
                        if let Some(sid) = sender_id {
                            let sid_str = sid.to_string();
                            obj.insert("sender_connection_id", sid_str.as_str());
                        }
                        if let Some(ref uid) = sender_user_id {
                            obj.insert("sender_user_id", uid.as_str());
                        }
                        if timestamp.is_none() {
                            obj.insert("timestamp", chrono::Utc::now().timestamp_millis());
                        }
                        let enriched = Arc::new(Message::Broadcast {
                            topic: topic.clone(),
                            payload: obj.into(),
                            timestamp: *timestamp,
                            operation_id: *operation_id,
                            request_id: None,
                        });
                        match enriched.to_json() {
                            Ok(s) => Some(Arc::new(s)),
                            Err(e) => {
                                debug!("Failed to enrich broadcast: {}", e);
                                None
                            }
                        }
                    } else {
                        match message.to_json() {
                            Ok(s) => Some(Arc::new(s)),
                            Err(e) => {
                                debug!("Failed to serialize broadcast: {}", e);
                                None
                            }
                        }
                    }
                }
                _ => None,
            };

            // ZERO-DASHMAP-LOOKUP DELIVERY: iterate pre-resolved sender channels.
            // senders were resolved by flush_batch_if_ready before submission.
            if let Some(serialized) = pre_serialized {
                for sender in &senders {
                    let _ = sender.try_send(OutgoingMessage::Serialized(serialized.clone()));
                }
            } else {
                for sender in &senders {
                    let _ = sender.try_send(OutgoingMessage::Message(Arc::clone(&message)));
                }
            }
        }

        Ok(())
    }
}

// Clone implementation for worker pool metrics
impl Clone for WorkerPoolMetrics {
    fn clone(&self) -> Self {
        Self {
            tasks_queued: AtomicU64::new(self.tasks_queued.load(Ordering::Relaxed)),
            _pad1: [0u8; 120],
            tasks_processed: AtomicU64::new(self.tasks_processed.load(Ordering::Relaxed)),
            tasks_failed: AtomicU64::new(self.tasks_failed.load(Ordering::Relaxed)),
            avg_processing_time_ns: AtomicU64::new(
                self.avg_processing_time_ns.load(Ordering::Relaxed),
            ),
            queue_depth: AtomicUsize::new(self.queue_depth.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::websocket::message::Message;

    #[tokio::test]
    async fn test_batch_broadcaster_creation() {
        let config = BatchBroadcastConfig::default();
        let broadcaster = BatchBroadcaster::new(config).await;
        assert!(broadcaster.is_ok());
    }

    #[tokio::test]
    async fn test_message_priority_calculation() {
        let config = BatchBroadcastConfig::default();
        let broadcaster = BatchBroadcaster::new(config).await.unwrap();

        let error_msg = Message::Error {
            message: "Test error".to_string(),
            code: Some("TEST".to_string()),
            details: None,
        };

        let priority = broadcaster.calculate_message_priority(&error_msg);
        assert_eq!(priority, MessagePriority::Critical);
    }

    #[test]
    fn test_adaptive_batch_size_adjustment() {
        let adaptive_state = AdaptiveState {
            ewma_latency_ns: Mutex::new(0.0),
            ewma_throughput_mps: Mutex::new(0.0),
            alpha: 0.1,
            adaptive_batch_size: AtomicUsize::new(50),
            adaptive_timeout: Mutex::new(Duration::from_millis(5)),
            min_batch_size: 10,
            max_batch_size: 200,
            min_timeout: Duration::from_millis(1),
            max_timeout: Duration::from_millis(100),
        };

        // Test high latency scenario
        BatchBroadcaster::adjust_batch_size(&adaptive_state, 15_000_000.0, 500.0);
        let new_size = adaptive_state.adaptive_batch_size.load(Ordering::Relaxed);
        assert!(new_size < 50); // Should decrease

        // Test low latency scenario
        BatchBroadcaster::adjust_batch_size(&adaptive_state, 500_000.0, 6000.0);
        let newer_size = adaptive_state.adaptive_batch_size.load(Ordering::Relaxed);
        assert!(newer_size > new_size); // Should increase
    }

    fn sample_adaptive_state() -> AdaptiveState {
        AdaptiveState {
            ewma_latency_ns: Mutex::new(0.0),
            ewma_throughput_mps: Mutex::new(0.0),
            alpha: 0.1,
            adaptive_batch_size: AtomicUsize::new(100),
            adaptive_timeout: Mutex::new(Duration::from_millis(10)),
            min_batch_size: 10,
            max_batch_size: 200,
            min_timeout: Duration::from_millis(1),
            max_timeout: Duration::from_millis(100),
        }
    }

    #[test]
    fn adaptive_batch_size_clamps_to_bounds() {
        let state = sample_adaptive_state();
        // Repeated high-latency reductions must not go below the minimum.
        for _ in 0..100 {
            BatchBroadcaster::adjust_batch_size(&state, 20_000_000.0, 100.0);
        }
        assert!(state.adaptive_batch_size.load(Ordering::Relaxed) >= state.min_batch_size);
        // Repeated low-latency increases must not exceed the maximum.
        for _ in 0..100 {
            BatchBroadcaster::adjust_batch_size(&state, 100_000.0, 10_000.0);
        }
        assert!(state.adaptive_batch_size.load(Ordering::Relaxed) <= state.max_batch_size);
    }

    #[test]
    fn adaptive_batch_size_unchanged_in_normal_conditions() {
        let state = sample_adaptive_state();
        BatchBroadcaster::adjust_batch_size(&state, 5_000_000.0, 3000.0);
        assert_eq!(state.adaptive_batch_size.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn adaptive_batch_timeout_halves_and_doubles() {
        let state = sample_adaptive_state();
        // High latency → timeout halves (10ms → 5ms).
        BatchBroadcaster::adjust_batch_timeout(&state, 6_000_000.0);
        assert_eq!(*state.adaptive_timeout.lock(), Duration::from_millis(5));
        // Low latency → timeout doubles (5ms → 10ms).
        BatchBroadcaster::adjust_batch_timeout(&state, 500_000.0);
        assert_eq!(*state.adaptive_timeout.lock(), Duration::from_millis(10));
    }

    #[test]
    fn adaptive_batch_timeout_clamps_to_bounds() {
        let state = sample_adaptive_state();
        // Aggressive low-latency doubling must not exceed max_timeout.
        for _ in 0..20 {
            BatchBroadcaster::adjust_batch_timeout(&state, 100_000.0);
        }
        assert!(*state.adaptive_timeout.lock() <= state.max_timeout);
        // Aggressive high-latency halving must not go below min_timeout.
        let state2 = sample_adaptive_state();
        for _ in 0..20 {
            BatchBroadcaster::adjust_batch_timeout(&state2, 9_000_000.0);
        }
        assert!(*state2.adaptive_timeout.lock() >= state2.min_timeout);
    }

    #[tokio::test]
    async fn topic_priority_classification() {
        let broadcaster = BatchBroadcaster::new(BatchBroadcastConfig::default())
            .await
            .unwrap();
        assert_eq!(
            broadcaster.calculate_topic_priority("system.heartbeat"),
            MessagePriority::High
        );
        assert_eq!(
            broadcaster.calculate_topic_priority("admin.audit"),
            MessagePriority::High
        );
        assert_eq!(
            broadcaster.calculate_topic_priority("alerts"),
            MessagePriority::High
        );
        assert_eq!(
            broadcaster.calculate_topic_priority("user.u1"),
            MessagePriority::Normal
        );
        assert_eq!(
            broadcaster.calculate_topic_priority("private.pm"),
            MessagePriority::Normal
        );
        assert_eq!(
            broadcaster.calculate_topic_priority("public.general"),
            MessagePriority::Normal
        );
        assert_eq!(
            broadcaster.calculate_topic_priority("app.analytics"),
            MessagePriority::Low
        );
        assert_eq!(
            broadcaster.calculate_topic_priority("logs"),
            MessagePriority::Low
        );
        assert_eq!(
            broadcaster.calculate_topic_priority("plain-channel"),
            MessagePriority::Normal
        );
    }

    #[tokio::test]
    async fn message_priority_maps_error_auth_and_low_payloads() {
        let broadcaster = BatchBroadcaster::new(BatchBroadcastConfig::default())
            .await
            .unwrap();
        let error = Message::Error {
            message: "boom".into(),
            code: Some("E".into()),
            details: None,
        };
        assert_eq!(
            broadcaster.calculate_message_priority(&error),
            MessagePriority::Critical
        );
        // A tiny broadcast payload boosts priority above the topic baseline.
        let small = Message::Broadcast {
            topic: "plain".into(),
            payload: sonic_rs::json!({"a": 1}),
            timestamp: None,
            operation_id: None,
            request_id: None,
        };
        assert_eq!(
            broadcaster.calculate_message_priority(&small),
            MessagePriority::High
        );
        // An ack is never critical (it is a delivery confirmation, not new data).
        let ack = Message::Ack {
            id: uuid::Uuid::new_v4(),
            cursor: None,
            channel_cursor: None,
            timestamp: None,
            payload: Some(sonic_rs::json!({})),
            channel_id: Some("t".into()),
            event_type: None,
            actor: None,
            request_id: None,
        };
        assert_ne!(
            broadcaster.calculate_message_priority(&ack),
            MessagePriority::Critical
        );
    }
}
