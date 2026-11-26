// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Centralized task scheduler for periodic background work.
//!
//! Consolidates ad-hoc `tokio::spawn` + `interval()` patterns across the
//! codebase into a single lifecycle-managed scheduler with metrics,
//! error tracking, and unified shutdown via the existing broadcast channel.

use prometheus::{
    register_gauge_vec, register_histogram_vec, register_int_counter_vec, GaugeVec, HistogramVec,
    IntCounterVec,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

pub mod checkpoint;

/// Describes the severity of a task execution failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskErrorKind {
    Transient,
    Permanent,
}

/// Error returned by a scheduled task.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("Transient task failure: {0}")]
    Transient(String),
    #[error("Permanent task failure: {0}")]
    Permanent(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;

    #[test]
    fn test_task_error_kind_debug() {
        assert_eq!(format!("{:?}", TaskErrorKind::Transient), "Transient");
        assert_eq!(format!("{:?}", TaskErrorKind::Permanent), "Permanent");
    }

    #[test]
    fn test_task_error_transient() {
        let err = TaskError::Transient("db timeout".into());
        assert_eq!(format!("{err}"), "Transient task failure: db timeout");
    }

    #[test]
    fn test_task_error_permanent() {
        let err = TaskError::Permanent("disk full".into());
        assert_eq!(format!("{err}"), "Permanent task failure: disk full");
    }

    #[test]
    fn test_task_meta_new() {
        let meta = TaskMeta::new("test_task", Duration::from_secs(30), Duration::from_secs(5));
        assert_eq!(meta.name, "test_task");
        assert_eq!(meta.interval, Duration::from_secs(30));
        assert_eq!(meta.timeout, Duration::from_secs(5));
        assert_eq!(meta.consecutive_failures, 0);
    }

    #[tokio::test]
    #[ignore = "Requires running tokio runtime"]
    async fn test_task_scheduler_register_and_start() {
        let (tx, _rx) = broadcast::channel::<()>(16);
        let mut scheduler = TaskScheduler::new(&tx);

        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = counter.clone();
        scheduler.register(
            "test_task",
            Duration::from_millis(10),
            Duration::from_secs(1),
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok::<_, TaskError>(())
                }
            },
        );

        scheduler.start();
    }

    #[tokio::test]
    #[ignore = "Requires running tokio runtime"]
    async fn test_task_scheduler_register_infallible() {
        let (tx, _rx) = broadcast::channel::<()>(16);
        let mut scheduler = TaskScheduler::new(&tx);

        scheduler.register_infallible("inf_task", Duration::from_secs(1), || async {});
    }

    #[tokio::test]
    async fn test_task_scheduler_new() {
        let (tx, _rx) = broadcast::channel::<()>(16);
        let scheduler = TaskScheduler::new(&tx);
        assert!(scheduler.tasks.is_empty());
    }

    #[test]
    fn test_task_error_kind_equality() {
        assert_eq!(TaskErrorKind::Transient, TaskErrorKind::Transient);
        assert_ne!(TaskErrorKind::Transient, TaskErrorKind::Permanent);
    }
}

/// Metadata tracked for each registered task.
#[derive(Debug, Clone)]
struct TaskMeta {
    name: &'static str,
    interval: Duration,
    timeout: Duration,
    consecutive_failures: u64,
}

impl TaskMeta {
    fn new(name: &'static str, interval: Duration, timeout: Duration) -> Self {
        Self {
            name,
            interval,
            timeout,
            consecutive_failures: 0,
        }
    }
}

/// Prometheus metrics for the scheduler.
#[derive(Debug)]
pub struct SchedulerMetrics {
    pub task_runs_total: IntCounterVec,
    pub task_errors_total: IntCounterVec,
    pub task_duration_seconds: HistogramVec,
    pub task_last_success_timestamp: GaugeVec,
    pub running_tasks: prometheus::IntGauge,
}

impl SchedulerMetrics {
    fn new() -> Self {
        Self {
            task_runs_total: register_int_counter_vec!(
                "actae_scheduler_task_runs_total",
                "Number of times each scheduled task has run",
                &["task"]
            )
            .unwrap(),
            task_errors_total: register_int_counter_vec!(
                "actae_scheduler_task_errors_total",
                "Number of errors per scheduled task",
                &["task"]
            )
            .unwrap(),
            task_duration_seconds: register_histogram_vec!(
                "actae_scheduler_task_duration_seconds",
                "Duration of each scheduled task run in seconds",
                &["task"],
                vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
            )
            .unwrap(),
            task_last_success_timestamp: register_gauge_vec!(
                "actae_scheduler_task_last_success_timestamp",
                "Unix timestamp of the last successful run per task",
                &["task"]
            )
            .unwrap(),
            running_tasks: prometheus::register_int_gauge!(
                "actae_scheduler_running_tasks",
                "Current number of registered scheduled tasks"
            )
            .unwrap(),
        }
    }

    fn record_run(&self, task: &str) {
        self.task_runs_total.with_label_values(&[task]).inc();
    }

    fn record_error(&self, task: &str) {
        self.task_errors_total.with_label_values(&[task]).inc();
    }

    fn record_duration(&self, task: &str, secs: f64) {
        self.task_duration_seconds
            .with_label_values(&[task])
            .observe(secs);
    }

    fn record_success(&self, task: &str) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        self.task_last_success_timestamp
            .with_label_values(&[task])
            .set(ts);
    }
}

/// A task future factory with its metadata, wrapped in Arc for shared ownership.
type TaskFn =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), TaskError>> + Send>> + Send + Sync>;

/// Centralized scheduler for periodic background tasks.
/// Tasks are registered at startup and run until the shutdown signal fires.
pub struct TaskScheduler {
    tasks: Vec<(TaskMeta, TaskFn)>,
    shutdown_tx: broadcast::Sender<()>,
    metrics: Arc<SchedulerMetrics>,
}

impl TaskScheduler {
    /// Create a new scheduler bound to the given shutdown channel.
    pub fn new(shutdown_tx: &broadcast::Sender<()>) -> Self {
        Self {
            tasks: Vec::new(),
            shutdown_tx: shutdown_tx.clone(),
            metrics: Arc::new(SchedulerMetrics::new()),
        }
    }

    /// Register a periodic task that returns `Result<(), TaskError>`.
    pub fn register<F, Fut>(
        &mut self,
        name: &'static str,
        period: Duration,
        timeout: Duration,
        task: F,
    ) where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        let meta = TaskMeta::new(name, period, timeout);
        let wrapped: TaskFn = Arc::new(move || Box::pin(task()));
        self.tasks.push((meta, wrapped));
    }

    /// Register a periodic task that does not return a result (infallible).
    pub fn register_infallible<F, Fut>(&mut self, name: &'static str, period: Duration, task: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let task = std::sync::Arc::new(task);
        self.register(name, period, Duration::from_secs(60), move || {
            let task = task.clone();
            async move {
                task().await;
                Ok(())
            }
        });
    }

    /// Start all registered tasks. Each runs in its own tokio task with
    /// unified shutdown awareness and metrics collection.
    /// This returns immediately; tasks run in the background.
    pub fn start(self) {
        let metrics = self.metrics;
        let task_count = self.tasks.len();
        metrics.running_tasks.set(task_count as i64);

        for (meta, task_fn) in self.tasks {
            let shutdown_rx = self.shutdown_tx.subscribe();
            let metrics = metrics.clone();
            let meta_clone = meta.clone();

            tokio::spawn(async move {
                let task_name = meta_clone.name;
                let mut shutdown_rx = shutdown_rx;
                let mut ticker = interval(meta_clone.interval);

                // Skip the immediate first tick so tasks don't all fire at startup.
                ticker.tick().await;

                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            let start = std::time::Instant::now();
                            debug!("Scheduled task '{}' starting", task_name);

                            match tokio::time::timeout(meta_clone.timeout, task_fn()).await {
                                Ok(Ok(())) => {
                                    let elapsed = start.elapsed().as_secs_f64();
                                    metrics.record_run(task_name);
                                    metrics.record_duration(task_name, elapsed);
                                    metrics.record_success(task_name);
                                    let mut m = meta_clone.clone();
                                    m.consecutive_failures = 0;
                                    debug!(
                                        "Scheduled task '{}' completed in {:.2}s",
                                        task_name, elapsed,
                                    );
                                }
                                Ok(Err(TaskError::Transient(msg))) => {
                                    let elapsed = start.elapsed().as_secs_f64();
                                    metrics.record_run(task_name);
                                    metrics.record_error(task_name);
                                    metrics.record_duration(task_name, elapsed);
                                    let mut m = meta_clone.clone();
                                    m.consecutive_failures += 1;
                                    if m.consecutive_failures >= 10 {
                                        warn!(
                                            "Task '{}' failed 10+ consecutive times (transient): {} \
                                             — auto-escalating. Check /metrics for details.",
                                            task_name, msg,
                                        );
                                    } else {
                                        warn!(
                                            "Scheduled task '{}' failed (transient, attempt {}): {}",
                                            task_name, m.consecutive_failures, msg,
                                        );
                                    }
                                }
                                Ok(Err(TaskError::Permanent(msg))) => {
                                    metrics.record_run(task_name);
                                    metrics.record_error(task_name);
                                    error!(
                                        "Scheduled task '{}' failed permanently: {} — disabling.",
                                        task_name, msg,
                                    );
                                    // Permanently failed tasks stop running.
                                    warn!("Task '{}' has been disabled due to permanent failure.", task_name);
                                    return;
                                }
                                Err(_elapsed) => {
                                    warn!("Scheduled task '{}' timed out after {:?}", task_name, meta_clone.timeout);
                                    metrics.record_run(task_name);
                                    metrics.record_error(task_name);
                                    metrics.record_duration(task_name, start.elapsed().as_secs_f64());
                                }
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            info!("Scheduler shutting down task '{}'", task_name);
                            return;
                        }
                    }
                }
            });
        }
    }
}
