// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use flume::{bounded, Receiver, Sender};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{interval, timeout};
use tracing::{debug, warn};
use uuid::Uuid;

use super::SecurityMonitor;
use crate::auth::websocket_auth::AuthMethod;

/// High-performance batch processor for security events
/// Reduces overhead by batching operations and using lock-free queues
pub struct SecurityBatchProcessor {
    // Lock-free channels for high throughput
    auth_event_tx: Sender<AuthBatchEvent>,
    rate_limit_event_tx: Sender<RateLimitBatchEvent>,
    connection_event_tx: Sender<ConnectionBatchEvent>,

    // Batch configuration
    config: BatchProcessorConfig,

    // Monitoring
    monitor: Arc<SecurityMonitor>,

    // Metrics
    metrics: Arc<RwLock<BatchProcessorMetrics>>,
}

#[derive(Debug, Clone)]
pub struct BatchProcessorConfig {
    pub auth_batch_size: usize,
    pub rate_limit_batch_size: usize,
    pub connection_batch_size: usize,
    pub batch_timeout_ms: u64,
    pub worker_threads: usize,
    pub queue_capacity: usize,
}

#[derive(Debug, Clone)]
pub struct AuthBatchEvent {
    pub timestamp: u64,
    pub ip: Option<std::net::IpAddr>,
    pub user_id: Option<String>,
    pub auth_method: AuthMethod,
    pub success: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RateLimitBatchEvent {
    pub timestamp: u64,
    pub ip: Option<std::net::IpAddr>,
    pub user_id: Option<String>,
    pub limit_type: String,
    pub exceeded: bool,
    pub current_rate: u32,
    pub limit_threshold: u32,
}

#[derive(Debug, Clone)]
pub struct ConnectionBatchEvent {
    pub timestamp: u64,
    pub connection_id: Uuid,
    pub ip: Option<std::net::IpAddr>,
    pub event_type: String,
    pub details: String,
}

#[derive(Debug, Default, Clone)]
pub struct BatchProcessorMetrics {
    pub auth_events_processed: u64,
    pub rate_limit_events_processed: u64,
    pub connection_events_processed: u64,
    pub batches_processed: u64,
    pub avg_batch_size: f64,
    pub processing_latency_ms: f64,
    pub queue_depth: usize,
}

impl Default for BatchProcessorConfig {
    fn default() -> Self {
        Self {
            auth_batch_size: 100,
            rate_limit_batch_size: 50,
            connection_batch_size: 75,
            batch_timeout_ms: 100, // 100ms batching window
            worker_threads: num_cpus::get(),
            queue_capacity: 10000,
        }
    }
}

impl SecurityBatchProcessor {
    pub fn new(config: BatchProcessorConfig, monitor: Arc<SecurityMonitor>) -> Result<Self> {
        let (auth_event_tx, auth_event_rx) = bounded(config.queue_capacity);
        let (rate_limit_event_tx, rate_limit_event_rx) = bounded(config.queue_capacity);
        let (connection_event_tx, connection_event_rx) = bounded(config.queue_capacity);

        let metrics = Arc::new(RwLock::new(BatchProcessorMetrics::default()));

        let processor = Self {
            auth_event_tx,
            rate_limit_event_tx,
            connection_event_tx,
            config: config.clone(),
            monitor: monitor.clone(),
            metrics: metrics.clone(),
        };

        // Start worker threads
        processor.start_workers(auth_event_rx, rate_limit_event_rx, connection_event_rx)?;

        Ok(processor)
    }

    fn start_workers(
        &self,
        auth_rx: Receiver<AuthBatchEvent>,
        rate_limit_rx: Receiver<RateLimitBatchEvent>,
        connection_rx: Receiver<ConnectionBatchEvent>,
    ) -> Result<()> {
        let config = Arc::new(self.config.clone());
        let monitor = Arc::clone(&self.monitor);
        let metrics = Arc::clone(&self.metrics);

        // Auth event processor
        let config_auth = Arc::clone(&config);
        tokio::spawn(async move {
            Self::process_auth_events(
                auth_rx,
                (*config_auth).clone(),
                monitor.clone(),
                metrics.clone(),
            )
            .await;
        });

        // Rate limit event processor
        let config_rl = Arc::clone(&config);
        let monitor_rl = Arc::clone(&self.monitor);
        let metrics_rl = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            Self::process_rate_limit_events(
                rate_limit_rx,
                (*config_rl).clone(),
                monitor_rl,
                metrics_rl,
            )
            .await;
        });

        // Connection event processor
        let config_conn = Arc::clone(&config);
        let monitor_conn = Arc::clone(&self.monitor);
        let metrics_conn = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            Self::process_connection_events(
                connection_rx,
                (*config_conn).clone(),
                monitor_conn,
                metrics_conn,
            )
            .await;
        });

        Ok(())
    }

    async fn process_auth_events(
        rx: Receiver<AuthBatchEvent>,
        config: BatchProcessorConfig,
        monitor: Arc<SecurityMonitor>,
        metrics: Arc<RwLock<BatchProcessorMetrics>>,
    ) {
        let mut batch = Vec::with_capacity(config.auth_batch_size);
        let mut last_process = Instant::now();
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);

        let mut ticker = interval(Duration::from_millis(config.batch_timeout_ms / 2));

        loop {
            tokio::select! {
                // Receive events
                event_result = rx.recv_async() => {
                    match event_result {
                        Ok(event) => {
                            batch.push(event);

                            // Process batch if full or timeout reached
                            if batch.len() >= config.auth_batch_size ||
                               last_process.elapsed() >= batch_timeout {
                                Self::flush_auth_batch(&mut batch, &monitor, &metrics).await;
                                last_process = Instant::now();
                            }
                        }
                        Err(_) => {
                            // Channel closed, flush remaining and exit
                            if !batch.is_empty() {
                                Self::flush_auth_batch(&mut batch, &monitor, &metrics).await;
                            }
                            break;
                        }
                    }
                }

                // Timeout ticker
                _ = ticker.tick() => {
                    if !batch.is_empty() && last_process.elapsed() >= batch_timeout {
                        Self::flush_auth_batch(&mut batch, &monitor, &metrics).await;
                        last_process = Instant::now();
                    }
                }
            }
        }
    }

    async fn flush_auth_batch(
        batch: &mut Vec<AuthBatchEvent>,
        monitor: &SecurityMonitor,
        metrics: &Arc<RwLock<BatchProcessorMetrics>>,
    ) {
        if batch.is_empty() {
            return;
        }

        let start_time = Instant::now();
        let batch_size = batch.len();

        // Process events in batch
        for event in batch.drain(..) {
            if event.success {
                let success_event = super::AuthSuccessEvent {
                    timestamp: event.timestamp,
                    ip: event.ip,
                    user_id: event.user_id.unwrap_or_default(),
                    connection_id: None,
                    auth_method: event.auth_method,
                    user_agent: None,
                    session_duration: None,
                };
                monitor.log_auth_success(success_event);
            } else {
                let failure_event = super::AuthFailureEvent {
                    timestamp: event.timestamp,
                    ip: event.ip,
                    user_id: event.user_id,
                    connection_id: None,
                    auth_method: event.auth_method,
                    reason: event.reason.unwrap_or_default(),
                    user_agent: None,
                };
                monitor.log_auth_failure(failure_event);
            }
        }

        // Update metrics
        let processing_time = start_time.elapsed().as_millis() as f64;
        let mut metrics_guard = metrics.write();
        metrics_guard.auth_events_processed += batch_size as u64;
        metrics_guard.batches_processed += 1;
        metrics_guard.avg_batch_size = (metrics_guard.avg_batch_size
            * (metrics_guard.batches_processed - 1) as f64
            + batch_size as f64)
            / metrics_guard.batches_processed as f64;
        metrics_guard.processing_latency_ms =
            (metrics_guard.processing_latency_ms * 0.9) + (processing_time * 0.1); // Exponential moving average

        debug!(
            "Processed auth batch of {} events in {:.2}ms",
            batch_size, processing_time
        );
    }

    async fn process_rate_limit_events(
        rx: Receiver<RateLimitBatchEvent>,
        config: BatchProcessorConfig,
        monitor: Arc<SecurityMonitor>,
        metrics: Arc<RwLock<BatchProcessorMetrics>>,
    ) {
        let mut batch = Vec::with_capacity(config.rate_limit_batch_size);
        let mut last_process = Instant::now();
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);

        let mut ticker = interval(Duration::from_millis(config.batch_timeout_ms / 2));

        loop {
            tokio::select! {
                event_result = rx.recv_async() => {
                    match event_result {
                        Ok(event) => {
                            batch.push(event);

                            if batch.len() >= config.rate_limit_batch_size ||
                               last_process.elapsed() >= batch_timeout {
                                Self::flush_rate_limit_batch(&mut batch, &monitor, &metrics).await;
                                last_process = Instant::now();
                            }
                        }
                        Err(_) => {
                            if !batch.is_empty() {
                                Self::flush_rate_limit_batch(&mut batch, &monitor, &metrics).await;
                            }
                            break;
                        }
                    }
                }

                _ = ticker.tick() => {
                    if !batch.is_empty() && last_process.elapsed() >= batch_timeout {
                        Self::flush_rate_limit_batch(&mut batch, &monitor, &metrics).await;
                        last_process = Instant::now();
                    }
                }
            }
        }
    }

    async fn flush_rate_limit_batch(
        batch: &mut Vec<RateLimitBatchEvent>,
        monitor: &SecurityMonitor,
        metrics: &Arc<RwLock<BatchProcessorMetrics>>,
    ) {
        if batch.is_empty() {
            return;
        }

        let start_time = Instant::now();
        let batch_size = batch.len();

        for event in batch.drain(..) {
            if event.exceeded {
                let rate_limit_event = super::RateLimitEvent {
                    timestamp: event.timestamp,
                    ip: event.ip,
                    user_id: event.user_id,
                    connection_id: None,
                    limit_type: super::RateLimitType::MessageRate, // Default
                    current_rate: event.current_rate,
                    limit_threshold: event.limit_threshold,
                    action_taken: super::RateLimitAction::ConnectionThrottled,
                };
                monitor.log_rate_limit_violation(rate_limit_event);
            }
        }

        let processing_time = start_time.elapsed().as_millis() as f64;
        let mut metrics_guard = metrics.write();
        metrics_guard.rate_limit_events_processed += batch_size as u64;

        debug!(
            "Processed rate limit batch of {} events in {:.2}ms",
            batch_size, processing_time
        );
    }

    async fn process_connection_events(
        rx: Receiver<ConnectionBatchEvent>,
        config: BatchProcessorConfig,
        monitor: Arc<SecurityMonitor>,
        metrics: Arc<RwLock<BatchProcessorMetrics>>,
    ) {
        let mut batch = Vec::with_capacity(config.connection_batch_size);
        let mut last_process = Instant::now();
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);

        let mut ticker = interval(Duration::from_millis(config.batch_timeout_ms / 2));

        loop {
            tokio::select! {
                event_result = rx.recv_async() => {
                    match event_result {
                        Ok(event) => {
                            batch.push(event);

                            if batch.len() >= config.connection_batch_size ||
                               last_process.elapsed() >= batch_timeout {
                                Self::flush_connection_batch(&mut batch, &monitor, &metrics).await;
                                last_process = Instant::now();
                            }
                        }
                        Err(_) => {
                            if !batch.is_empty() {
                                Self::flush_connection_batch(&mut batch, &monitor, &metrics).await;
                            }
                            break;
                        }
                    }
                }

                _ = ticker.tick() => {
                    if !batch.is_empty() && last_process.elapsed() >= batch_timeout {
                        Self::flush_connection_batch(&mut batch, &monitor, &metrics).await;
                        last_process = Instant::now();
                    }
                }
            }
        }
    }

    async fn flush_connection_batch(
        batch: &mut Vec<ConnectionBatchEvent>,
        monitor: &SecurityMonitor,
        metrics: &Arc<RwLock<BatchProcessorMetrics>>,
    ) {
        if batch.is_empty() {
            return;
        }

        let start_time = Instant::now();
        let batch_size = batch.len();

        for event in batch.drain(..) {
            let connection_event = super::ConnectionSecurityEvent {
                timestamp: event.timestamp,
                connection_id: event.connection_id,
                ip: event.ip,
                event_type: super::ConnectionSecurityType::SecurityValidationFailed, // Default
                details: event.details,
            };
            monitor.log_connection_security(connection_event);
        }

        let processing_time = start_time.elapsed().as_millis() as f64;
        let mut metrics_guard = metrics.write();
        metrics_guard.connection_events_processed += batch_size as u64;

        debug!(
            "Processed connection batch of {} events in {:.2}ms",
            batch_size, processing_time
        );
    }

    /// Submit auth event for batch processing
    pub async fn submit_auth_event(&self, event: AuthBatchEvent) -> Result<()> {
        match timeout(
            Duration::from_millis(10),
            self.auth_event_tx.send_async(event),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => {
                warn!("Auth event queue full, dropping event");
                Ok(())
            }
            Err(_) => {
                warn!("Auth event submission timeout");
                Ok(())
            }
        }
    }

    /// Submit rate limit event for batch processing
    pub async fn submit_rate_limit_event(&self, event: RateLimitBatchEvent) -> Result<()> {
        match timeout(
            Duration::from_millis(10),
            self.rate_limit_event_tx.send_async(event),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => {
                warn!("Rate limit event queue full, dropping event");
                Ok(())
            }
            Err(_) => {
                warn!("Rate limit event submission timeout");
                Ok(())
            }
        }
    }

    /// Submit connection event for batch processing
    pub async fn submit_connection_event(&self, event: ConnectionBatchEvent) -> Result<()> {
        match timeout(
            Duration::from_millis(10),
            self.connection_event_tx.send_async(event),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => {
                warn!("Connection event queue full, dropping event");
                Ok(())
            }
            Err(_) => {
                warn!("Connection event submission timeout");
                Ok(())
            }
        }
    }

    /// Get processing metrics
    pub fn get_metrics(&self) -> BatchProcessorMetrics {
        self.metrics.read().clone()
    }

    /// Get queue depths for monitoring
    pub fn get_queue_depths(&self) -> (usize, usize, usize) {
        (
            self.auth_event_tx.len(),
            self.rate_limit_event_tx.len(),
            self.connection_event_tx.len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_processor_config_default() {
        let config = BatchProcessorConfig::default();
        assert_eq!(config.auth_batch_size, 100);
        assert_eq!(config.rate_limit_batch_size, 50);
        assert_eq!(config.connection_batch_size, 75);
        assert_eq!(config.batch_timeout_ms, 100);
        assert!(config.worker_threads > 0);
        assert_eq!(config.queue_capacity, 10000);
    }

    #[test]
    fn test_batch_processor_config_custom() {
        let config = BatchProcessorConfig {
            auth_batch_size: 200,
            rate_limit_batch_size: 100,
            connection_batch_size: 150,
            batch_timeout_ms: 50,
            worker_threads: 4,
            queue_capacity: 5000,
        };
        assert_eq!(config.auth_batch_size, 200);
        assert_eq!(config.batch_timeout_ms, 50);
        assert_eq!(config.worker_threads, 4);
    }

    #[test]
    fn test_auth_batch_event_construction() {
        let event = AuthBatchEvent {
            timestamp: 12345,
            ip: Some("127.0.0.1".parse().unwrap()),
            user_id: Some("user-1".into()),
            auth_method: AuthMethod::ApiKey,
            success: true,
            reason: None,
        };
        assert_eq!(event.timestamp, 12345);
        assert!(event.success);
        assert_eq!(event.user_id, Some("user-1".into()));
    }

    #[test]
    fn test_rate_limit_batch_event_construction() {
        let event = RateLimitBatchEvent {
            timestamp: 67890,
            ip: Some("10.0.0.1".parse().unwrap()),
            user_id: None,
            limit_type: "connection".into(),
            exceeded: true,
            current_rate: 100,
            limit_threshold: 50,
        };
        assert!(event.exceeded);
        assert_eq!(event.current_rate, 100);
        assert_eq!(event.limit_threshold, 50);
    }

    #[test]
    fn test_rate_limit_batch_event_not_exceeded() {
        let event = RateLimitBatchEvent {
            timestamp: 11111,
            ip: None,
            user_id: Some("user-2".into()),
            limit_type: "message".into(),
            exceeded: false,
            current_rate: 30,
            limit_threshold: 100,
        };
        assert!(!event.exceeded);
        assert_eq!(event.limit_type, "message");
    }

    #[test]
    fn test_connection_batch_event_construction() {
        let event = ConnectionBatchEvent {
            timestamp: 99999,
            connection_id: Uuid::new_v4(),
            ip: Some("192.168.1.1".parse().unwrap()),
            event_type: "connect".into(),
            details: "new connection from unknown IP".into(),
        };
        assert_eq!(event.event_type, "connect");
        assert!(!event.details.is_empty());
    }

    #[test]
    fn test_batch_processor_metrics_default() {
        let metrics = BatchProcessorMetrics::default();
        assert_eq!(metrics.auth_events_processed, 0);
        assert_eq!(metrics.rate_limit_events_processed, 0);
        assert_eq!(metrics.connection_events_processed, 0);
        assert_eq!(metrics.batches_processed, 0);
        assert_eq!(metrics.avg_batch_size, 0.0);
        assert_eq!(metrics.processing_latency_ms, 0.0);
        assert_eq!(metrics.queue_depth, 0);
    }

    #[test]
    fn test_batch_processor_metrics_custom() {
        let metrics = BatchProcessorMetrics {
            auth_events_processed: 1000,
            rate_limit_events_processed: 500,
            connection_events_processed: 750,
            batches_processed: 25,
            avg_batch_size: 90.0,
            processing_latency_ms: 15.5,
            queue_depth: 3,
        };
        assert_eq!(metrics.auth_events_processed, 1000);
        assert_eq!(metrics.batches_processed, 25);
        assert!((metrics.avg_batch_size - 90.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_batch_processor_metrics_clone() {
        let metrics = BatchProcessorMetrics {
            auth_events_processed: 42,
            rate_limit_events_processed: 10,
            connection_events_processed: 5,
            batches_processed: 3,
            avg_batch_size: 19.0,
            processing_latency_ms: 2.5,
            queue_depth: 1,
        };
        let cloned = metrics.clone();
        assert_eq!(cloned.auth_events_processed, 42);
        assert_eq!(cloned.avg_batch_size, 19.0);
    }

    #[test]
    fn test_auth_batch_event_debug() {
        let event = AuthBatchEvent {
            timestamp: 0,
            ip: None,
            user_id: None,
            auth_method: AuthMethod::ApiKey,
            success: false,
            reason: Some("bad key".into()),
        };
        let _ = format!("{event:?}");
    }

    #[test]
    fn test_rate_limit_batch_event_debug() {
        let event = RateLimitBatchEvent {
            timestamp: 1,
            ip: None,
            user_id: None,
            limit_type: "auth".into(),
            exceeded: false,
            current_rate: 0,
            limit_threshold: 10,
        };
        let _ = format!("{event:?}");
    }

    #[test]
    fn test_connection_batch_event_debug() {
        let event = ConnectionBatchEvent {
            timestamp: 2,
            connection_id: Uuid::new_v4(),
            ip: None,
            event_type: "disconnect".into(),
            details: String::new(),
        };
        let _ = format!("{event:?}");
    }
}
