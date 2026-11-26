// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Fleet gateway metrics (Phase 8). Registered into the shared prometheus
//! default registry so `actae::metrics::gather_metrics()` includes them.

use once_cell::sync::Lazy;
use prometheus::{HistogramVec, IntCounter, IntCounterVec, IntGauge};

pub static GATEWAY_REQUESTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus::register_int_counter_vec!(
        "actae_gateway_requests_total",
        "Fleet gateway requests by route and status class",
        &["route", "status_class"]
    )
    .expect("metric")
});

pub static GATEWAY_REQUEST_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus::register_histogram_vec!(
        "actae_gateway_request_duration_seconds",
        "Fleet gateway request latency by route",
        &["route"],
        vec![0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    .expect("metric")
});

pub static GATEWAY_FANOUT_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    prometheus::register_int_counter!(
        "actae_gateway_fanout_total",
        "Instances called by multi-instance queries"
    )
    .expect("metric")
});

pub static GATEWAY_FANOUT_ERRORS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    prometheus::register_int_counter!(
        "actae_gateway_fanout_errors_total",
        "Per-instance query calls that failed (typed errors / partial)"
    )
    .expect("metric")
});

pub static GATEWAY_RELAY_SESSIONS: Lazy<IntGauge> = Lazy::new(|| {
    prometheus::register_int_gauge!(
        "actae_gateway_relay_sessions",
        "Live outbound relay sessions from self-hosted instances"
    )
    .expect("metric")
});

pub static GATEWAY_RELAY_COMMANDS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    prometheus::register_int_counter!(
        "actae_gateway_relay_commands_total",
        "Canonical commands dispatched through outbound relay sessions"
    )
    .expect("metric")
});

pub static GATEWAY_CIRCUIT_BREAKER_OPEN: Lazy<IntGauge> = Lazy::new(|| {
    prometheus::register_int_gauge!(
        "actae_gateway_circuit_breaker_open",
        "Number of instances currently behind an open circuit breaker"
    )
    .expect("metric")
});

pub static GATEWAY_QUOTA_REJECTED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    prometheus::register_int_counter!(
        "actae_gateway_quota_rejected_total",
        "Fleet requests rejected by the per-org quota"
    )
    .expect("metric")
});

pub static GATEWAY_STREAM_CONNECTIONS: Lazy<IntGauge> = Lazy::new(|| {
    prometheus::register_int_gauge!(
        "actae_gateway_stream_connections",
        "Active fleet stream connections"
    )
    .expect("metric")
});
