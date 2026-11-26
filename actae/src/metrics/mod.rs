// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Metrics and monitoring system
//!
//! This module provides metrics collection and observability features for the Actae system.

pub mod basic_metrics;

pub use basic_metrics::{
    gather_metrics, ACTIVE_SUBSCRIPTIONS, AUTHENTICATION_ATTEMPTS, CONNECTED_CLIENTS,
    DATABASE_QUERY_DURATION, ERRORS_TOTAL, FORKS_TOTAL, MESSAGES_RECEIVED, MESSAGES_SENT,
    RATE_LIMIT_HITS, RECOVERIES_TOTAL, REQUESTS_TOTAL, REQUEST_DURATION, SERVICE_STATUS,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gather_metrics_callable() {
        let _: fn() -> String = gather_metrics;
    }
}
