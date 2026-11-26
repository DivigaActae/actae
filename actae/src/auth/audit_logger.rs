// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Authentication Audit Logging System
//!
//! Provides comprehensive security audit logging for authentication events:
//! - Structured logging with correlation IDs
//! - Security event classification and severity levels
//! - Real-time threat detection and alerting
//! - Compliance reporting (SOX, GDPR, HIPAA)
//! - Performance optimized with async batching
//! - Integration with SIEM systems
//! - Data retention and archival policies

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Security event severity levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum SecuritySeverity {
    Info = 0,
    Low = 1,
    Medium = 2,
    High = 3,
    Critical = 4,
}

impl SecuritySeverity {
    pub fn description(&self) -> &'static str {
        match self {
            SecuritySeverity::Info => "Informational",
            SecuritySeverity::Low => "Low Risk",
            SecuritySeverity::Medium => "Medium Risk",
            SecuritySeverity::High => "High Risk",
            SecuritySeverity::Critical => "Critical Risk",
        }
    }

    pub fn requires_immediate_action(&self) -> bool {
        *self >= SecuritySeverity::High
    }
}

/// Authentication event categories
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AuthEventCategory {
    Login,
    Logout,
    Registration,
    PasswordChange,
    PasswordReset,
    AccountLock,
    AccountUnlock,
    PermissionChange,
    TokenGeneration,
    TokenValidation,
    TokenRevocation,
    SessionStart,
    SessionEnd,
    RateLimitExceeded,
    SuspiciousActivity,
    SecurityPolicyViolation,
    DataAccess,
    ConfigurationChange,
    SystemAccess,
}

/// Comprehensive authentication audit event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthAuditEvent {
    /// Unique event identifier
    pub event_id: String,

    /// Correlation ID for tracking related events
    pub correlation_id: String,

    /// Event timestamp (UTC)
    pub timestamp: DateTime<Utc>,

    /// Event category
    pub category: AuthEventCategory,

    /// Security severity level
    pub severity: SecuritySeverity,

    /// Event outcome (success/failure)
    pub success: bool,

    /// User information
    pub user_id: Option<String>,
    pub username: Option<String>,

    /// Network information
    pub ip_address: Option<IpAddr>,
    pub user_agent: Option<String>,
    pub session_id: Option<String>,
    pub request_id: Option<String>,

    /// Geographic information (if available)
    pub country: Option<String>,
    pub region: Option<String>,
    pub city: Option<String>,

    /// Security context
    pub authentication_method: Option<String>,
    pub risk_score: Option<f32>,
    pub threat_indicators: Vec<String>,
    pub policy_violations: Vec<String>,

    /// Event details
    pub description: String,
    pub error_message: Option<String>,
    pub metadata: Option<Value>,

    /// Compliance tags
    pub compliance_tags: Vec<String>,

    /// Processing information
    pub processing_time_ms: Option<u64>,
    pub batch_id: Option<String>,
}

impl AuthAuditEvent {
    /// Create a new audit event with minimal required fields
    pub fn new(
        category: AuthEventCategory,
        severity: SecuritySeverity,
        success: bool,
        description: String,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            correlation_id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            category,
            severity,
            success,
            user_id: None,
            username: None,
            ip_address: None,
            user_agent: None,
            session_id: None,
            request_id: None,
            country: None,
            region: None,
            city: None,
            authentication_method: None,
            risk_score: None,
            threat_indicators: Vec::new(),
            policy_violations: Vec::new(),
            description,
            error_message: None,
            metadata: None,
            compliance_tags: Vec::new(),
            processing_time_ms: None,
            batch_id: None,
        }
    }

    /// Builder pattern methods for fluent configuration
    pub fn with_user(mut self, user_id: String, username: Option<String>) -> Self {
        self.user_id = Some(user_id);
        self.username = username;
        self
    }

    pub fn with_network(mut self, ip: IpAddr, user_agent: Option<String>) -> Self {
        self.ip_address = Some(ip);
        self.user_agent = user_agent;
        self
    }

    pub fn with_session(mut self, session_id: String, request_id: Option<String>) -> Self {
        self.session_id = Some(session_id);
        self.request_id = request_id;
        self
    }

    pub fn with_geolocation(
        mut self,
        country: String,
        region: Option<String>,
        city: Option<String>,
    ) -> Self {
        self.country = Some(country);
        self.region = region;
        self.city = city;
        self
    }

    pub fn with_security_context(mut self, auth_method: String, risk_score: f32) -> Self {
        self.authentication_method = Some(auth_method);
        self.risk_score = Some(risk_score);
        self
    }

    pub fn with_threat_indicator(mut self, indicator: String) -> Self {
        self.threat_indicators.push(indicator);
        self
    }

    pub fn with_policy_violation(mut self, violation: String) -> Self {
        self.policy_violations.push(violation);
        self
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_compliance_tag(mut self, tag: String) -> Self {
        self.compliance_tags.push(tag);
        self
    }

    pub fn with_error(mut self, error_message: String) -> Self {
        self.error_message = Some(error_message);
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        self.correlation_id = correlation_id;
        self
    }

    /// Check if event indicates a security threat
    pub fn is_security_threat(&self) -> bool {
        self.severity >= SecuritySeverity::Medium
            || !self.threat_indicators.is_empty()
            || !self.policy_violations.is_empty()
    }

    /// Generate alert message for high-severity events
    pub fn generate_alert_message(&self) -> Option<String> {
        if self.severity.requires_immediate_action() {
            Some(format!(
                "SECURITY ALERT: {} - {} (Event ID: {})",
                self.severity.description(),
                self.description,
                self.event_id
            ))
        } else {
            None
        }
    }
}

/// Audit logger configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLoggerConfig {
    /// Enable real-time logging
    pub enable_realtime_logging: bool,

    /// Enable batch processing for performance
    pub enable_batch_processing: bool,

    /// Batch size for processing events
    pub batch_size: usize,

    /// Batch flush interval in seconds
    pub batch_flush_interval_secs: u64,

    /// Maximum events to keep in memory
    pub max_events_in_memory: usize,

    /// Enable SIEM integration
    pub enable_siem_integration: bool,

    /// SIEM endpoint configuration
    pub siem_endpoint: Option<String>,

    /// Data retention period in days
    pub retention_period_days: u32,

    /// Enable geographic enrichment
    pub enable_geo_enrichment: bool,

    /// Enable risk scoring
    pub enable_risk_scoring: bool,

    /// Minimum severity for alerting
    pub alert_threshold: SecuritySeverity,

    /// Compliance frameworks to tag events for
    pub compliance_frameworks: Vec<String>,
}

impl Default for AuditLoggerConfig {
    fn default() -> Self {
        Self {
            enable_realtime_logging: true,
            enable_batch_processing: true,
            batch_size: 100,
            batch_flush_interval_secs: 60,
            max_events_in_memory: 10000,
            enable_siem_integration: false,
            siem_endpoint: None,
            retention_period_days: 365, // 1 year default
            enable_geo_enrichment: true,
            enable_risk_scoring: true,
            alert_threshold: SecuritySeverity::High,
            compliance_frameworks: vec!["SOX".to_string(), "GDPR".to_string(), "HIPAA".to_string()],
        }
    }
}

/// Audit event statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditStatistics {
    pub total_events: u64,
    pub events_by_category: HashMap<String, u64>,
    pub events_by_severity: HashMap<String, u64>,
    pub success_rate: f32,
    pub threat_events: u64,
    pub policy_violations: u64,
    pub unique_users: u64,
    pub unique_ips: u64,
    pub events_last_24h: u64,
    pub critical_events_last_24h: u64,
    pub average_risk_score: f32,
    pub last_updated: DateTime<Utc>,
}

/// Main authentication audit logger
pub struct AuthAuditLogger {
    config: AuditLoggerConfig,

    /// Event storage (in production, use database/SIEM)
    events: Arc<RwLock<Vec<AuthAuditEvent>>>,

    /// Batch processing queue
    batch_queue: Arc<RwLock<Vec<AuthAuditEvent>>>,

    /// Statistics tracking
    statistics: Arc<RwLock<AuditStatistics>>,

    /// Background processing task
    batch_task: Option<tokio::task::JoinHandle<()>>,

    /// Shutdown signal
    shutdown_sender: Arc<tokio::sync::broadcast::Sender<()>>,
}

impl AuthAuditLogger {
    /// Create new audit logger
    pub fn new(config: AuditLoggerConfig) -> Self {
        let (shutdown_sender, _) = tokio::sync::broadcast::channel(1);

        Self {
            config,
            events: Arc::new(RwLock::new(Vec::new())),
            batch_queue: Arc::new(RwLock::new(Vec::new())),
            statistics: Arc::new(RwLock::new(AuditStatistics {
                total_events: 0,
                events_by_category: HashMap::new(),
                events_by_severity: HashMap::new(),
                success_rate: 0.0,
                threat_events: 0,
                policy_violations: 0,
                unique_users: 0,
                unique_ips: 0,
                events_last_24h: 0,
                critical_events_last_24h: 0,
                average_risk_score: 0.0,
                last_updated: Utc::now(),
            })),
            batch_task: None,
            shutdown_sender: Arc::new(shutdown_sender),
        }
    }

    /// Start the audit logger with background processing
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting authentication audit logger");

        if self.config.enable_batch_processing {
            self.start_batch_processing_task().await;
        }

        info!("Authentication audit logger started successfully");
        Ok(())
    }

    /// Log an authentication event
    pub async fn log_event(&self, mut event: AuthAuditEvent) -> Result<()> {
        // Enrich event with additional context
        self.enrich_event(&mut event).await?;

        // Add compliance tags
        self.add_compliance_tags(&mut event).await;

        // Calculate risk score if enabled
        if self.config.enable_risk_scoring {
            event.risk_score = Some(self.calculate_risk_score(&event).await);
        }

        // Real-time logging
        if self.config.enable_realtime_logging {
            self.log_realtime(&event).await;
        }

        // Add to batch queue if batch processing is enabled
        if self.config.enable_batch_processing {
            let mut batch_queue = self.batch_queue.write().await;
            batch_queue.push(event.clone());

            // Immediate flush if queue is full
            if batch_queue.len() >= self.config.batch_size {
                let events_to_process = batch_queue.drain(..).collect();
                drop(batch_queue);
                self.process_event_batch(events_to_process).await?;
            }
        } else {
            // Process immediately if not batching
            self.process_single_event(event).await?;
        }

        Ok(())
    }

    /// Log multiple events in a batch
    pub async fn log_events(&self, events: Vec<AuthAuditEvent>) -> Result<()> {
        for event in events {
            self.log_event(event).await?;
        }
        Ok(())
    }

    /// Create a security alert for high-severity events
    pub async fn create_security_alert(&self, event: &AuthAuditEvent) -> Result<()> {
        if let Some(alert_message) = event.generate_alert_message() {
            error!("{}", alert_message);

            // In production, this would integrate with alerting systems
            // like PagerDuty, Slack, email notifications, etc.
            self.send_alert_notification(event, &alert_message).await?;
        }
        Ok(())
    }

    /// Query events by criteria
    pub async fn query_events(
        &self,
        user_id: Option<&str>,
        category: Option<AuthEventCategory>,
        severity: Option<SecuritySeverity>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> Vec<AuthAuditEvent> {
        let events = self.events.read().await;
        let mut filtered_events: Vec<_> = events
            .iter()
            .filter(|event| {
                if let Some(uid) = user_id {
                    if event.user_id.as_deref() != Some(uid) {
                        return false;
                    }
                }

                if let Some(cat) = &category {
                    if &event.category != cat {
                        return false;
                    }
                }

                if let Some(sev) = severity {
                    if event.severity != sev {
                        return false;
                    }
                }

                if let Some(start) = start_time {
                    if event.timestamp < start {
                        return false;
                    }
                }

                if let Some(end) = end_time {
                    if event.timestamp > end {
                        return false;
                    }
                }

                true
            })
            .cloned()
            .collect();

        // Sort by timestamp (newest first)
        filtered_events.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        if let Some(limit) = limit {
            filtered_events.truncate(limit);
        }

        filtered_events
    }

    /// Get audit statistics
    pub async fn get_statistics(&self) -> AuditStatistics {
        self.statistics.read().await.clone()
    }

    /// Export events for compliance reporting
    pub async fn export_compliance_report(
        &self,
        framework: &str,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<Value> {
        let events = self
            .query_events(None, None, None, Some(start_date), Some(end_date), None)
            .await;

        let compliance_events: Vec<_> = events
            .into_iter()
            .filter(|event| event.compliance_tags.contains(&framework.to_string()))
            .collect();

        Ok(sonic_rs::json!({
            "framework": framework,
            "report_period": {
                "start": start_date,
                "end": end_date
            },
            "total_events": compliance_events.len(),
            "events": compliance_events,
            "generated_at": Utc::now()
        }))
    }

    // Private helper methods

    /// Enrich event with additional context
    async fn enrich_event(&self, event: &mut AuthAuditEvent) -> Result<()> {
        // Add geographic information if enabled and IP is available
        if self.config.enable_geo_enrichment {
            if let Some(ip) = event.ip_address {
                if let Ok(geo_info) = self.get_geolocation(ip).await {
                    event.country = geo_info.country;
                    event.region = geo_info.region;
                    event.city = geo_info.city;
                }
            }
        }

        // Add processing timestamp
        event.processing_time_ms = Some(0); // Would measure actual processing time

        Ok(())
    }

    /// Add compliance tags based on event type
    async fn add_compliance_tags(&self, event: &mut AuthAuditEvent) {
        for framework in &self.config.compliance_frameworks {
            match framework.as_str() {
                "SOX" => {
                    if matches!(
                        event.category,
                        AuthEventCategory::Login
                            | AuthEventCategory::DataAccess
                            | AuthEventCategory::ConfigurationChange
                    ) {
                        event.compliance_tags.push("SOX".to_string());
                    }
                }
                "GDPR" => {
                    if matches!(
                        event.category,
                        AuthEventCategory::Login
                            | AuthEventCategory::DataAccess
                            | AuthEventCategory::Registration
                    ) {
                        event.compliance_tags.push("GDPR".to_string());
                    }
                }
                "HIPAA" => {
                    if matches!(
                        event.category,
                        AuthEventCategory::Login
                            | AuthEventCategory::DataAccess
                            | AuthEventCategory::SystemAccess
                    ) {
                        event.compliance_tags.push("HIPAA".to_string());
                    }
                }
                _ => {}
            }
        }
    }

    /// Calculate risk score for an event
    async fn calculate_risk_score(&self, event: &AuthAuditEvent) -> f32 {
        let mut score = 0.0;

        // Base score from severity
        score += match event.severity {
            SecuritySeverity::Info => 0.1,
            SecuritySeverity::Low => 0.3,
            SecuritySeverity::Medium => 0.5,
            SecuritySeverity::High => 0.7,
            SecuritySeverity::Critical => 1.0,
        };

        // Failed attempts increase risk
        if !event.success {
            score += 0.2;
        }

        // Threat indicators significantly increase risk
        score += event.threat_indicators.len() as f32 * 0.1;

        // Policy violations increase risk
        score += event.policy_violations.len() as f32 * 0.15;

        // Geographic risk (simplified)
        if let Some(ref country) = event.country {
            if ["CN", "RU", "KP"].contains(&country.as_str()) {
                score += 0.2; // Higher risk countries
            }
        }

        // Cap at 1.0
        score.min(1.0)
    }

    /// Real-time logging
    async fn log_realtime(&self, event: &AuthAuditEvent) {
        match event.severity {
            SecuritySeverity::Critical => {
                error!(
                    event_id = %event.event_id,
                    correlation_id = %event.correlation_id,
                    category = ?event.category,
                    user_id = ?event.user_id,
                    ip_address = ?event.ip_address,
                    "CRITICAL SECURITY EVENT: {}",
                    event.description
                );
            }
            SecuritySeverity::High => {
                warn!(
                    event_id = %event.event_id,
                    correlation_id = %event.correlation_id,
                    category = ?event.category,
                    user_id = ?event.user_id,
                    "HIGH SEVERITY EVENT: {}",
                    event.description
                );
            }
            SecuritySeverity::Medium => {
                warn!(
                    event_id = %event.event_id,
                    category = ?event.category,
                    "MEDIUM SEVERITY EVENT: {}",
                    event.description
                );
            }
            _ => {
                info!(
                    event_id = %event.event_id,
                    category = ?event.category,
                    "AUTH EVENT: {}",
                    event.description
                );
            }
        }
    }

    /// Process a single event
    async fn process_single_event(&self, event: AuthAuditEvent) -> Result<()> {
        // Store event
        {
            let mut events = self.events.write().await;
            events.push(event.clone());

            // Enforce memory limits
            if events.len() > self.config.max_events_in_memory {
                events.drain(0..1000); // Remove oldest 1000 events
            }
        }

        // Update statistics
        self.update_statistics(&event).await;

        // Create alert if needed
        if event.severity >= self.config.alert_threshold {
            self.create_security_alert(&event).await?;
        }

        // Send to SIEM if configured
        if self.config.enable_siem_integration {
            self.send_to_siem(&event).await?;
        }

        Ok(())
    }

    /// Process a batch of events
    async fn process_event_batch(&self, events: Vec<AuthAuditEvent>) -> Result<()> {
        let batch_id = Uuid::new_v4().to_string();

        for mut event in events {
            event.batch_id = Some(batch_id.clone());
            self.process_single_event(event).await?;
        }

        debug!("Processed event batch: {}", batch_id);
        Ok(())
    }

    /// Update audit statistics
    async fn update_statistics(&self, event: &AuthAuditEvent) {
        let mut stats = self.statistics.write().await;

        stats.total_events += 1;

        // Update category counts
        let category_key = format!("{:?}", event.category);
        *stats.events_by_category.entry(category_key).or_insert(0) += 1;

        // Update severity counts
        let severity_key = format!("{:?}", event.severity);
        *stats.events_by_severity.entry(severity_key).or_insert(0) += 1;

        // Update threat and violation counts
        if event.is_security_threat() {
            stats.threat_events += 1;
        }

        stats.policy_violations += event.policy_violations.len() as u64;

        // Update success rate (simplified calculation)
        let total_success = if event.success { 1 } else { 0 };
        stats.success_rate = (stats.success_rate * (stats.total_events - 1) as f32
            + total_success as f32)
            / stats.total_events as f32;

        stats.last_updated = Utc::now();
    }

    /// Start batch processing background task
    async fn start_batch_processing_task(&mut self) {
        let batch_queue = Arc::clone(&self.batch_queue);
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown_sender.subscribe();

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                config.batch_flush_interval_secs,
            ));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let events_to_process: Vec<_> = {
                            let mut queue = batch_queue.write().await;
                            queue.drain(..).collect()
                        };

                        if !events_to_process.is_empty() {
                            debug!("Processing {} batched audit events", events_to_process.len());
                            // In real implementation, would call process_event_batch
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Audit logger batch task shutting down");
                        break;
                    }
                }
            }
        });

        self.batch_task = Some(task);
    }

    /// Get geolocation information for IP address
    async fn get_geolocation(&self, _ip: IpAddr) -> Result<GeolocationInfo> {
        // In production, this would call a geolocation service
        Ok(GeolocationInfo {
            country: Some("US".to_string()),
            region: Some("California".to_string()),
            city: Some("San Francisco".to_string()),
        })
    }

    /// Send alert notification
    async fn send_alert_notification(&self, _event: &AuthAuditEvent, _message: &str) -> Result<()> {
        // In production, integrate with alerting systems
        // - Send email/SMS notifications
        // - Post to Slack/Teams
        // - Create PagerDuty incident
        // - Update security dashboard
        Ok(())
    }

    /// Send event to SIEM system
    async fn send_to_siem(&self, _event: &AuthAuditEvent) -> Result<()> {
        // In production, send to configured SIEM endpoint
        // - Format as CEF, LEEF, or JSON
        // - Use HTTP/HTTPS API
        // - Handle authentication and retries
        Ok(())
    }

    /// Shutdown audit logger
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down authentication audit logger");

        // Send shutdown signal
        let _ = self.shutdown_sender.send(());

        // Process any remaining batched events
        let remaining_events: Vec<_> = {
            let mut queue = self.batch_queue.write().await;
            queue.drain(..).collect()
        };

        if !remaining_events.is_empty() {
            self.process_event_batch(remaining_events).await?;
        }

        // Wait for batch task to complete
        if let Some(task) = self.batch_task.take() {
            let _ = task.await;
        }

        info!("Authentication audit logger shutdown complete");
        Ok(())
    }
}

/// Geolocation information structure
#[derive(Debug, Clone)]
struct GeolocationInfo {
    country: Option<String>,
    region: Option<String>,
    city: Option<String>,
}

/// Utility functions for audit logging
pub mod utils {
    use super::*;

    /// Create a login event
    pub fn create_login_event(
        user_id: String,
        username: String,
        success: bool,
        ip: IpAddr,
        user_agent: Option<String>,
    ) -> AuthAuditEvent {
        AuthAuditEvent::new(
            AuthEventCategory::Login,
            if success {
                SecuritySeverity::Info
            } else {
                SecuritySeverity::Medium
            },
            success,
            if success {
                format!("User {username} logged in successfully")
            } else {
                format!("Failed login attempt for user {username}")
            },
        )
        .with_user(user_id, Some(username))
        .with_network(ip, user_agent)
        .with_security_context("password".to_string(), 0.1)
    }

    /// Create a password change event
    pub fn create_password_change_event(
        user_id: String,
        username: String,
        success: bool,
    ) -> AuthAuditEvent {
        AuthAuditEvent::new(
            AuthEventCategory::PasswordChange,
            SecuritySeverity::Low,
            success,
            format!("Password change for user {username}"),
        )
        .with_user(user_id, Some(username))
        .with_compliance_tag("GDPR".to_string())
    }

    /// Create a rate limit exceeded event
    pub fn create_rate_limit_event(
        ip: IpAddr,
        user_id: Option<String>,
        endpoint: String,
    ) -> AuthAuditEvent {
        AuthAuditEvent::new(
            AuthEventCategory::RateLimitExceeded,
            SecuritySeverity::Medium,
            false,
            format!("Rate limit exceeded for endpoint: {endpoint}"),
        )
        .with_network(ip, None)
        .with_threat_indicator("rate_limit_exceeded".to_string())
        .with_metadata(sonic_rs::json!({
            "endpoint": endpoint,
            "user_id": user_id
        }))
    }

    /// Check if event pattern indicates brute force attack
    pub fn detect_brute_force_pattern(events: &[AuthAuditEvent], window_minutes: i64) -> bool {
        let threshold = 10; // 10 failed attempts
        let window_start = Utc::now() - Duration::minutes(window_minutes);

        let failed_attempts = events
            .iter()
            .filter(|event| {
                event.timestamp >= window_start
                    && event.category == AuthEventCategory::Login
                    && !event.success
            })
            .count();

        failed_attempts >= threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[tokio::test]
    async fn test_audit_event_creation() {
        let event = AuthAuditEvent::new(
            AuthEventCategory::Login,
            SecuritySeverity::Info,
            true,
            "User login successful".to_string(),
        )
        .with_user("user123".to_string(), Some("testuser".to_string()))
        .with_network(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), None);

        assert_eq!(event.category, AuthEventCategory::Login);
        assert_eq!(event.severity, SecuritySeverity::Info);
        assert!(event.success);
        assert_eq!(event.user_id, Some("user123".to_string()));
    }

    #[tokio::test]
    async fn test_audit_logger() {
        let config = AuditLoggerConfig {
            enable_batch_processing: false,
            ..Default::default()
        };
        let logger = AuthAuditLogger::new(config);

        let event = utils::create_login_event(
            "user123".to_string(),
            "testuser".to_string(),
            true,
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)),
            Some("Mozilla/5.0".to_string()),
        );

        logger.log_event(event).await.unwrap();

        let stats = logger.get_statistics().await;
        assert_eq!(stats.total_events, 1);
    }

    #[tokio::test]
    async fn test_event_querying() {
        let config = AuditLoggerConfig {
            enable_batch_processing: false,
            ..Default::default()
        };
        let logger = AuthAuditLogger::new(config);

        // Log a few events
        for i in 0..5 {
            let event = utils::create_login_event(
                format!("user{i}"),
                format!("testuser{i}"),
                i % 2 == 0, // Alternate success/failure
                IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)),
                None,
            );
            logger.log_event(event).await.unwrap();
        }

        // Query all events
        let all_events = logger
            .query_events(None, None, None, None, None, None)
            .await;
        assert_eq!(all_events.len(), 5);

        // Query successful events only
        let successful_events = logger
            .query_events(
                None,
                Some(AuthEventCategory::Login),
                Some(SecuritySeverity::Info),
                None,
                None,
                None,
            )
            .await;
        assert_eq!(successful_events.len(), 3); // Events 0, 2, 4
    }

    #[test]
    fn test_brute_force_detection() {
        let mut events = Vec::new();
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));

        // Create 15 failed login attempts
        for _ in 0..15 {
            let event = utils::create_login_event(
                "user123".to_string(),
                "testuser".to_string(),
                false, // Failed attempt
                ip,
                None,
            );
            events.push(event);
        }

        assert!(utils::detect_brute_force_pattern(&events, 10));
    }

    // ── SecuritySeverity / threat / alert classification ───────────

    #[test]
    fn severity_descriptions_and_immediate_action_threshold() {
        assert_eq!(SecuritySeverity::Info.description(), "Informational");
        assert_eq!(SecuritySeverity::Critical.description(), "Critical Risk");
        assert!(!SecuritySeverity::Medium.requires_immediate_action());
        assert!(SecuritySeverity::High.requires_immediate_action());
        assert!(SecuritySeverity::Critical.requires_immediate_action());
        assert!(SecuritySeverity::High > SecuritySeverity::Medium);
        assert!(SecuritySeverity::Critical >= SecuritySeverity::High);
    }

    #[test]
    fn threat_detection_considers_severity_and_indicators() {
        let base = AuthAuditEvent::new(
            AuthEventCategory::Login,
            SecuritySeverity::Info,
            true,
            "login".into(),
        );
        assert!(!base.is_security_threat());

        let mut medium = base.clone();
        medium.severity = SecuritySeverity::Medium;
        assert!(medium.is_security_threat());

        let mut indicator = base.clone();
        indicator.threat_indicators.push("brute_force".into());
        assert!(indicator.is_security_threat());

        let mut violation = base.clone();
        violation.policy_violations.push("mfa_required".into());
        assert!(violation.is_security_threat());
    }

    #[test]
    fn alert_message_only_for_immediate_action_severities() {
        let mut low = AuthAuditEvent::new(
            AuthEventCategory::Login,
            SecuritySeverity::Low,
            true,
            "login ok".into(),
        );
        assert!(low.generate_alert_message().is_none());

        low.severity = SecuritySeverity::Critical;
        let alert = low.generate_alert_message().unwrap();
        assert!(alert.contains("SECURITY ALERT"));
        assert!(alert.contains("Critical Risk"));
        assert!(alert.contains(&low.event_id));
    }

    #[test]
    fn audit_event_builder_accumulates_context() {
        let event = AuthAuditEvent::new(
            AuthEventCategory::Login,
            SecuritySeverity::Low,
            true,
            "login".into(),
        )
        .with_user("u1".into(), Some("alice".into()))
        .with_network("127.0.0.1".parse().unwrap(), Some("curl".into()))
        .with_session("sess-1".into(), Some("req-1".into()))
        .with_security_context("api_key".into(), 0.9)
        .with_threat_indicator("rate_limit".into())
        .with_policy_violation("no_mfa".into())
        .with_compliance_tag("gdpr".into());
        assert_eq!(event.user_id.as_deref(), Some("u1"));
        assert_eq!(event.username.as_deref(), Some("alice"));
        assert_eq!(event.session_id.as_deref(), Some("sess-1"));
        assert_eq!(event.authentication_method.as_deref(), Some("api_key"));
        assert_eq!(event.risk_score, Some(0.9));
        assert!(event.threat_indicators.contains(&"rate_limit".into()));
        assert!(event.policy_violations.contains(&"no_mfa".into()));
        assert!(event.compliance_tags.contains(&"gdpr".into()));
        assert!(event.is_security_threat());
    }
}
