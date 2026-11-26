// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{
    register_gauge, register_gauge_vec, register_histogram, register_histogram_vec,
    register_int_counter, register_int_counter_vec, register_int_gauge, register_int_gauge_vec,
    Encoder, Gauge, GaugeVec, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, TextEncoder,
};

// =============================================================================
// Connection Metrics
// =============================================================================

pub static CONNECTED_CLIENTS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "actae_connected_clients",
        "Number of currently connected clients"
    )
    .unwrap()
});

pub static CONNECTION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "actae_connection_duration_seconds",
        "Duration of WebSocket connections in seconds",
        &["status"], // disconnected, timeout, error
        vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0]
    )
    .unwrap()
});

pub static CONNECTIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "actae_connections_total",
        "Total number of connection attempts",
        &["status"] // accepted, rejected, rate_limited
    )
    .unwrap()
});

pub static ACTIVE_SUBSCRIPTIONS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "actae_active_subscriptions",
        "Number of active real-time subscriptions"
    )
    .unwrap()
});

// =============================================================================
// Message Metrics
// =============================================================================

pub static MESSAGES_SENT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("actae_messages_sent_total", "Total number of messages sent").unwrap()
});

pub static MESSAGES_RECEIVED: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "actae_messages_received_total",
        "Total number of messages received"
    )
    .unwrap()
});

pub static MESSAGE_SIZE_BYTES: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "actae_message_size_bytes",
        "Size of messages in bytes",
        &["direction"], // sent, received
        vec![64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0]
    )
    .unwrap()
});

pub static MESSAGE_PROCESSING_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "actae_message_processing_duration_seconds",
        "Time spent processing messages",
        &["message_type"], // subscription, unsubscription, data, heartbeat
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
    )
    .unwrap()
});

pub static MESSAGES_QUEUED: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "actae_messages_queued",
        "Number of messages waiting to be sent"
    )
    .unwrap()
});

pub static MESSAGE_QUEUE_DURATION: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "actae_message_queue_duration_seconds",
        "Time messages spend in the send queue",
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
    )
    .unwrap()
});

// =============================================================================
// Performance Metrics
// =============================================================================

pub static REQUEST_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "actae_request_duration_seconds",
        "HTTP request duration in seconds",
        &["method", "endpoint", "status"],
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
    )
    .unwrap()
});

pub static REQUESTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "actae_requests_total",
        "Total number of HTTP requests",
        &["method", "endpoint", "status"]
    )
    .unwrap()
});

pub static CONCURRENT_REQUESTS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "actae_concurrent_requests",
        "Number of concurrent HTTP requests being processed"
    )
    .unwrap()
});

pub static MEMORY_USAGE_BYTES: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!("actae_memory_usage_bytes", "Current memory usage in bytes").unwrap()
});

pub static CPU_USAGE_PERCENT: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!("actae_cpu_usage_percent", "Current CPU usage percentage").unwrap()
});

// =============================================================================
// Error and Rate Limiting Metrics
// =============================================================================

pub static ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "actae_errors_total",
        "Total number of errors",
        &["error_type", "component"] // websocket, database, auth
    )
    .unwrap()
});

pub static RATE_LIMIT_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "actae_rate_limit_hits_total",
        "Number of rate limit violations",
        &["limit_type"] // connection, message, api
    )
    .unwrap()
});

pub static AUTHENTICATION_ATTEMPTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "actae_authentication_attempts_total",
        "Total authentication attempts",
        &["method", "status"] // jwt, api_key / success, failure, expired
    )
    .unwrap()
});

// =============================================================================
// Database Metrics
// =============================================================================

pub static DATABASE_CONNECTIONS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "actae_database_connections",
        "Number of active database connections"
    )
    .unwrap()
});

pub static DATABASE_QUERY_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "actae_database_query_duration_seconds",
        "Database query execution time",
        &["query_type"], // select, insert, update, delete, subscription
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
    )
    .unwrap()
});

pub static CACHE_OPERATIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "actae_cache_operations_total",
        "Cache operations",
        &["operation", "status"] // get, set, delete / hit, miss, error
    )
    .unwrap()
});

// =============================================================================
// Resource Metrics
// =============================================================================

pub static RESOURCE_QUOTA_USAGE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "actae_resource_quota_usage_percent",
        "Resource quota usage percentage",
        &["resource"] // connections, messages, storage
    )
    .unwrap()
});

// =============================================================================
// Health and Status Metrics
// =============================================================================

pub static SERVICE_STATUS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "actae_service_status",
        "Service health status (1=healthy, 0=unhealthy)",
        &["service"] // websocket, database
    )
    .unwrap()
});

pub static LAST_SUCCESSFUL_HEALTH_CHECK: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "actae_last_successful_health_check_timestamp",
        "Timestamp of last successful health check"
    )
    .unwrap()
});

// =============================================================================
// Feature Flag and Circuit Breaker Metrics
// =============================================================================

pub static FEATURE_FLAG_EVALUATIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "actae_feature_flag_evaluations_total",
        "Number of feature flag evaluations",
        &["flag_name", "result"] // enabled, disabled
    )
    .unwrap()
});

pub static CIRCUIT_BREAKER_STATE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "actae_circuit_breaker_state",
        "Circuit breaker state (0=closed, 1=open, 2=half-open)",
        &["service"] // database, external_api
    )
    .unwrap()
});

// =============================================================================
// Broadcast Delivery Metrics
// =============================================================================

/// Total broadcast messages delivered to subscribers (by delivery outcome).
/// Labels: result = "first_try" | "retry" | "failed_first" | "ok" | "error"
pub static BROADCAST_DELIVERED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "actae_broadcast_delivered_total",
        "Total number of broadcast messages delivered to subscribers",
        &["result"]
    )
    .unwrap()
});

/// Subscriber acknowledgements received.
pub static BROADCAST_ACKED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "actae_broadcast_acked_total",
        "Total number of broadcast messages acknowledged by subscribers"
    )
    .unwrap()
});

/// Subscriber negative acknowledgements received.
pub static BROADCAST_NACKED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "actae_broadcast_nacked_total",
        "Total number of broadcast messages negatively acknowledged by subscribers"
    )
    .unwrap()
});

/// Retry attempts (per delivery, not per event).
pub static BROADCAST_RETRY_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "actae_broadcast_retry_total",
        "Total number of broadcast delivery retry attempts"
    )
    .unwrap()
});

/// Events moved to dead letter queue after exhausting retries.
pub static BROADCAST_DEAD_LETTER_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "actae_broadcast_dead_letter_total",
        "Total number of events moved to dead letter queue"
    )
    .unwrap()
});

/// Current number of pending deliveries (not yet acked).
pub static BROADCAST_PENDING_COUNT: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "actae_broadcast_pending_count",
        "Number of broadcast deliveries pending acknowledgment"
    )
    .unwrap()
});

/// Current dead letter queue size.
pub static BROADCAST_DLQ_SIZE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "actae_broadcast_dlq_size",
        "Number of events currently in the dead letter queue"
    )
    .unwrap()
});

// =============================================================================
// Idempotent Tool Execution Metrics
// =============================================================================

/// Total tool executions by outcome label: claimed, reclaimed, replayed,
/// completed, failed.
pub static EXECUTIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "actae_executions_total",
        "Total number of idempotent tool execution transitions",
        &["outcome"] // claimed, reclaimed, replayed, completed, failed
    )
    .unwrap()
});

/// Completed executions whose persisted result was served without re-running.
pub static EXECUTION_REPLAYS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "actae_execution_replays_total",
        "Total number of replayed idempotent tool executions"
    )
    .unwrap()
});

/// Wall-clock duration of completed tool executions (seconds).
pub static EXECUTION_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "actae_execution_duration_seconds",
        "Duration of completed tool executions in seconds",
        vec![0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0, 1800.0]
    )
    .unwrap()
});

// =============================================================================
// Fork / recovery metrics
// =============================================================================

/// Fork operations created on this instance.
///
/// This is the *local* view of Actae's differentiated primitive — forking an
/// execution from a saved state boundary (which also covers
/// `resume(fork_at_step=)`). It is always available on the instance via
/// `GET /metrics` with no phone-home. When the instance is explicitly linked
/// to a control plane, the same event is metered transactionally to the
/// `usage_delivery_outbox` as `fork_operations` and delivered on sync; a
/// standalone, unlinked instance never sends it anywhere.
///
/// Recorded **server-side only** — never by the MIT SDKs.
pub static FORKS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "actae_forks_total",
        "Total fork operations created on this instance (fork / resume-fork)"
    )
    .unwrap()
});

/// Crash-recovery resumes completed on this instance.
///
/// Incremented server-side when a channel's session metadata moves from
/// `crashed` back to `started` (a resume on the same channel — not a fork).
/// Local view only; the same event is metered to `usage_delivery_outbox` as
/// `recovery_operations` and delivered only from a linked instance. Recorded
/// server-side only — never in the MIT SDKs.
pub static RECOVERIES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "actae_recoveries_total",
        "Total same-channel crash-recovery resumes completed on this instance"
    )
    .unwrap()
});

pub fn gather_metrics() -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Force the Lazy metric statics so the global prometheus registry is
    /// populated before gathering — `gather()` returns an empty registry
    /// otherwise when this test runs before any other metric-touching test.
    /// (`&*STATIC` dereferences the `Lazy`, which runs its init closure.)
    fn force_registration() {
        let _ = &*CONNECTED_CLIENTS;
        let _ = &*CONNECTION_DURATION;
        let _ = &*CONNECTIONS_TOTAL;
        let _ = &*ACTIVE_SUBSCRIPTIONS;
        let _ = &*MESSAGES_SENT;
        let _ = &*MESSAGES_RECEIVED;
        let _ = &*REQUEST_DURATION;
        let _ = &*REQUESTS_TOTAL;
        let _ = &*ERRORS_TOTAL;
        let _ = &*RATE_LIMIT_HITS;
        let _ = &*AUTHENTICATION_ATTEMPTS;
        let _ = &*DATABASE_QUERY_DURATION;
        let _ = &*EXECUTIONS_TOTAL;
        let _ = &*BROADCAST_DELIVERED_TOTAL;
        let _ = &*SERVICE_STATUS;
        let _ = &*FORKS_TOTAL;
        let _ = &*RECOVERIES_TOTAL;
    }

    #[test]
    fn test_gather_metrics_returns_string() {
        force_registration();
        let metrics = gather_metrics();
        assert!(!metrics.is_empty());
    }

    #[test]
    fn test_metric_names_contain_actae_prefix() {
        force_registration();
        let metrics = gather_metrics();
        assert!(metrics.contains("actae_"));
    }

    #[test]
    fn test_gather_metrics_is_utf8() {
        force_registration();
        let metrics = gather_metrics();
        assert!(std::str::from_utf8(metrics.as_bytes()).is_ok());
    }

    #[test]
    fn test_metric_statics_accessible() {
        let _ = &CONNECTED_CLIENTS;
        let _ = &ACTIVE_SUBSCRIPTIONS;
        let _ = &MESSAGES_SENT;
        let _ = &MESSAGES_RECEIVED;
        let _ = &ERRORS_TOTAL;
        let _ = &REQUESTS_TOTAL;
        let _ = &REQUEST_DURATION;
        let _ = &DATABASE_QUERY_DURATION;
    }

    #[test]
    fn test_metric_output_uses_prometheus_text_format() {
        force_registration();
        let metrics = gather_metrics();
        for line in metrics.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // Metric lines are `name{labels} value`.
            assert!(
                line.split_whitespace().count() == 2 || line.contains('{'),
                "non-conforming metric line: {line}"
            );
        }
        assert!(metrics.contains("# TYPE"), "expected TYPE metadata");
        assert!(metrics.contains("# HELP"), "expected HELP metadata");
    }

    #[test]
    fn test_metric_output_includes_connection_and_message_families() {
        force_registration();
        let metrics = gather_metrics();
        // Simple gauge/counter families always emit a line. Labeled vector
        // families (actae_errors_total, actae_request_duration_seconds, …)
        // only appear once a labeled sample exists, so they are not asserted.
        for family in [
            "actae_connected_clients",
            "actae_active_subscriptions",
            "actae_messages_sent_total",
            "actae_messages_received_total",
        ] {
            assert!(metrics.contains(family), "missing metric family {family}");
        }
    }

    #[test]
    fn test_metric_output_lines_have_values() {
        force_registration();
        let metrics = gather_metrics();
        // A concrete gauge line must carry a numeric value after the name.
        let connected_line = metrics
            .lines()
            .find(|l| l.starts_with("actae_connected_clients"))
            .expect("connected-clients gauge present");
        let value = connected_line.split_whitespace().last().unwrap();
        assert!(
            value.parse::<f64>().is_ok(),
            "gauge value not numeric: {value}"
        );
    }
}
