// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::auth::websocket_auth::AuthMethod;

/// Non-blocking security event monitoring system
pub struct SecurityMonitor {
    event_sender: mpsc::UnboundedSender<SecurityEvent>,
    _event_processor: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecurityEvent {
    AuthFailure(AuthFailureEvent),
    AuthSuccess(AuthSuccessEvent),
    SuspiciousActivity(SuspiciousActivityEvent),
    ConnectionSecurity(ConnectionSecurityEvent),
    RateLimitViolation(RateLimitEvent),
    IpBlocked(IpBlockedEvent),
    PermissionDenied(PermissionDeniedEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthFailureEvent {
    pub timestamp: u64,
    pub ip: Option<IpAddr>,
    pub user_id: Option<String>,
    pub connection_id: Option<Uuid>,
    pub auth_method: AuthMethod,
    pub reason: String,
    pub user_agent: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSuccessEvent {
    pub timestamp: u64,
    pub ip: Option<IpAddr>,
    pub user_id: String,
    pub connection_id: Option<Uuid>,
    pub auth_method: AuthMethod,
    pub user_agent: Option<String>,
    pub session_duration: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuspiciousActivityEvent {
    pub timestamp: u64,
    pub connection_id: Uuid,
    pub ip: Option<IpAddr>,
    pub user_id: Option<String>,
    pub activity_type: SuspiciousActivityType,
    pub details: String,
    pub severity: SecuritySeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SuspiciousActivityType {
    MultipleFailedAuths,
    UnusualConnectionPattern,
    ExcessiveMessageRate,
    InvalidTokenFormat,
    PrivilegeEscalation,
    UnauthorizedChannelAccess,
    AnomalousUserAgent,
    GeolocationAnomaly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionSecurityEvent {
    pub timestamp: u64,
    pub connection_id: Uuid,
    pub ip: Option<IpAddr>,
    pub event_type: ConnectionSecurityType,
    pub details: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectionSecurityType {
    TlsHandshakeFailed,
    CertificateValidationFailed,
    SecurityValidationFailed,
    ConnectionFromBlockedIp,
    ExcessiveConnections,
    ConnectionTimeoutExceeded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitEvent {
    pub timestamp: u64,
    pub ip: Option<IpAddr>,
    pub user_id: Option<String>,
    pub connection_id: Option<Uuid>,
    pub limit_type: RateLimitType,
    pub current_rate: u32,
    pub limit_threshold: u32,
    pub action_taken: RateLimitAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RateLimitType {
    MessageRate,
    ConnectionRate,
    AuthAttemptRate,
    SubscriptionRate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RateLimitAction {
    Warning,
    ConnectionThrottled,
    ConnectionDropped,
    IpTemporarilyBlocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpBlockedEvent {
    pub timestamp: u64,
    pub ip: IpAddr,
    pub reason: String,
    pub duration_seconds: Option<u64>,
    pub trigger_event: Option<String>,
    pub blocked_connections: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionDeniedEvent {
    pub timestamp: u64,
    pub user_id: String,
    pub connection_id: Option<Uuid>,
    pub resource: String,
    pub action: String,
    pub ip: Option<IpAddr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecuritySeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl SecurityMonitor {
    pub fn new() -> Result<Self> {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        let event_processor = tokio::spawn(Self::process_events(event_receiver));

        Ok(Self {
            event_sender,
            _event_processor: event_processor,
        })
    }

    /// Non-blocking event logging
    pub fn log_auth_failure(&self, event: AuthFailureEvent) {
        let _ = self.event_sender.send(SecurityEvent::AuthFailure(event));
    }

    pub fn log_auth_success(&self, event: AuthSuccessEvent) {
        let _ = self.event_sender.send(SecurityEvent::AuthSuccess(event));
    }

    pub fn log_suspicious_activity(&self, event: SuspiciousActivityEvent) {
        let _ = self
            .event_sender
            .send(SecurityEvent::SuspiciousActivity(event));
    }

    pub fn log_connection_security(&self, event: ConnectionSecurityEvent) {
        let _ = self
            .event_sender
            .send(SecurityEvent::ConnectionSecurity(event));
    }

    pub fn log_rate_limit_violation(&self, event: RateLimitEvent) {
        let _ = self
            .event_sender
            .send(SecurityEvent::RateLimitViolation(event));
    }

    pub fn log_ip_blocked(&self, event: IpBlockedEvent) {
        let _ = self.event_sender.send(SecurityEvent::IpBlocked(event));
    }

    pub fn log_permission_denied(&self, event: PermissionDeniedEvent) {
        let _ = self
            .event_sender
            .send(SecurityEvent::PermissionDenied(event));
    }

    /// Process security events asynchronously
    async fn process_events(mut receiver: mpsc::UnboundedReceiver<SecurityEvent>) {
        while let Some(event) = receiver.recv().await {
            Self::handle_security_event(event).await;
        }
    }

    async fn handle_security_event(event: SecurityEvent) {
        match event {
            SecurityEvent::AuthFailure(ref event) => {
                warn!(
                    "AUTH_FAILURE: ip={:?}, user={:?}, method={:?}, reason={}, timestamp={}",
                    event.ip, event.user_id, event.auth_method, event.reason, event.timestamp
                );

                // Detect brute force attacks
                if event.reason.contains("invalid") || event.reason.contains("wrong") {
                    // This could trigger additional security measures
                    Self::analyze_brute_force_pattern(event).await;
                }
            }

            SecurityEvent::AuthSuccess(ref event) => {
                debug!(
                    "AUTH_SUCCESS: user={}, ip={:?}, method={:?}, timestamp={}",
                    event.user_id, event.ip, event.auth_method, event.timestamp
                );
            }

            SecurityEvent::SuspiciousActivity(ref event) => {
                let log_level = match event.severity {
                    SecuritySeverity::Critical => "CRITICAL",
                    SecuritySeverity::High => "HIGH",
                    SecuritySeverity::Medium => "MEDIUM",
                    SecuritySeverity::Low => "LOW",
                };

                error!(
                    "SUSPICIOUS_ACTIVITY[{}]: connection={}, ip={:?}, user={:?}, type={:?}, details={}",
                    log_level, event.connection_id, event.ip, event.user_id,
                    event.activity_type, event.details
                );

                // Critical events might trigger immediate action
                if matches!(event.severity, SecuritySeverity::Critical) {
                    Self::handle_critical_security_event(event).await;
                }
            }

            SecurityEvent::ConnectionSecurity(ref event) => {
                warn!(
                    "CONNECTION_SECURITY: connection={}, ip={:?}, type={:?}, details={}",
                    event.connection_id, event.ip, event.event_type, event.details
                );
            }

            SecurityEvent::RateLimitViolation(ref event) => {
                warn!(
                    "RATE_LIMIT_VIOLATION: ip={:?}, user={:?}, type={:?}, rate={}/{}, action={:?}",
                    event.ip,
                    event.user_id,
                    event.limit_type,
                    event.current_rate,
                    event.limit_threshold,
                    event.action_taken
                );
            }

            SecurityEvent::IpBlocked(ref event) => {
                error!(
                    "IP_BLOCKED: ip={}, reason={}, duration={:?}s, connections_blocked={}",
                    event.ip, event.reason, event.duration_seconds, event.blocked_connections
                );
            }

            SecurityEvent::PermissionDenied(ref event) => {
                warn!(
                    "PERMISSION_DENIED: user={}, resource={}, action={}, ip={:?}",
                    event.user_id, event.resource, event.action, event.ip
                );
            }
        }

        // Additional processing like storing to database, sending alerts, etc.
        Self::store_security_event(event).await;
    }

    async fn analyze_brute_force_pattern(event: &AuthFailureEvent) {
        // This could implement sophisticated pattern analysis
        // For now, just log potential brute force attempts
        if let Some(ip) = event.ip {
            warn!(
                "POTENTIAL_BRUTE_FORCE: ip={}, user={:?}, timestamp={}",
                ip, event.user_id, event.timestamp
            );
        }
    }

    async fn handle_critical_security_event(event: &SuspiciousActivityEvent) {
        error!(
            "CRITICAL_SECURITY_EVENT: Immediate attention required - connection={}, type={:?}",
            event.connection_id, event.activity_type
        );

        // In a production system, this might:
        // - Send alerts to security team
        // - Automatically block IPs
        // - Trigger incident response procedures
        // - Update threat intelligence feeds
    }

    async fn store_security_event(event: SecurityEvent) {
        // In production, this would store events to a security database
        // or send to a SIEM system like Elasticsearch, Splunk, etc.

        // For now, we'll just ensure the event is properly serialized
        if let Ok(serialized) = sonic_rs::to_string(&event) {
            // Could send to Kafka, a message bus, or a database
            tracing::debug!("Security event serialized: {}", serialized);
        }
    }

    /// Create helper methods for common security events
    pub fn create_auth_failure_event(
        ip: Option<IpAddr>,
        user_id: Option<String>,
        connection_id: Option<Uuid>,
        auth_method: AuthMethod,
        reason: String,
    ) -> AuthFailureEvent {
        AuthFailureEvent {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ip,
            user_id,
            connection_id,
            auth_method,
            reason,
            user_agent: None, // Could be populated from request headers
        }
    }

    pub fn create_suspicious_activity_event(
        connection_id: Uuid,
        ip: Option<IpAddr>,
        user_id: Option<String>,
        activity_type: SuspiciousActivityType,
        details: String,
        severity: SecuritySeverity,
    ) -> SuspiciousActivityEvent {
        SuspiciousActivityEvent {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            connection_id,
            ip,
            user_id,
            activity_type,
            details,
            severity,
        }
    }

    pub fn create_rate_limit_event(
        ip: Option<IpAddr>,
        user_id: Option<String>,
        connection_id: Option<Uuid>,
        limit_type: RateLimitType,
        current_rate: u32,
        limit_threshold: u32,
        action_taken: RateLimitAction,
    ) -> RateLimitEvent {
        RateLimitEvent {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ip,
            user_id,
            connection_id,
            limit_type,
            current_rate,
            limit_threshold,
            action_taken,
        }
    }
}

impl Default for SecurityMonitor {
    fn default() -> Self {
        Self::new().expect("Failed to create SecurityMonitor")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::websocket_auth::AuthMethod;
    use std::net::{IpAddr, Ipv4Addr};
    use uuid::Uuid;

    // --- SecurityEvent tests ---

    #[test]
    fn test_all_enum_variants_constructable() {
        let timestamp = 1000_u64;
        let ip = Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let conn_id = Uuid::new_v4();

        let _auth_failure = SecurityEvent::AuthFailure(AuthFailureEvent {
            timestamp,
            ip,
            user_id: None,
            connection_id: None,
            auth_method: AuthMethod::JWT,
            reason: "bad credentials".into(),
            user_agent: None,
        });

        let _auth_success = SecurityEvent::AuthSuccess(AuthSuccessEvent {
            timestamp,
            ip,
            user_id: "user1".into(),
            connection_id: None,
            auth_method: AuthMethod::JWT,
            user_agent: None,
            session_duration: None,
        });

        let _suspicious = SecurityEvent::SuspiciousActivity(SuspiciousActivityEvent {
            timestamp,
            connection_id: conn_id,
            ip,
            user_id: None,
            activity_type: SuspiciousActivityType::MultipleFailedAuths,
            details: "suspicious".into(),
            severity: SecuritySeverity::High,
        });

        let _conn_security = SecurityEvent::ConnectionSecurity(ConnectionSecurityEvent {
            timestamp,
            connection_id: conn_id,
            ip,
            event_type: ConnectionSecurityType::TlsHandshakeFailed,
            details: "TLS error".into(),
        });

        let _rate_limit = SecurityEvent::RateLimitViolation(RateLimitEvent {
            timestamp,
            ip,
            user_id: None,
            connection_id: None,
            limit_type: RateLimitType::ConnectionRate,
            current_rate: 100,
            limit_threshold: 50,
            action_taken: RateLimitAction::ConnectionDropped,
        });

        let _ip_blocked = SecurityEvent::IpBlocked(IpBlockedEvent {
            timestamp,
            ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            reason: "DoS".into(),
            duration_seconds: Some(3600),
            trigger_event: Some("rate_limit".into()),
            blocked_connections: 5,
        });

        let _perm_denied = SecurityEvent::PermissionDenied(PermissionDeniedEvent {
            timestamp,
            user_id: "user1".into(),
            connection_id: None,
            resource: "db.messages".into(),
            action: "write".into(),
            ip,
        });
    }

    #[test]
    fn test_security_event_debug_clone() {
        let event = SecurityEvent::AuthFailure(AuthFailureEvent {
            timestamp: 0,
            ip: None,
            user_id: None,
            connection_id: None,
            auth_method: AuthMethod::JWT,
            reason: "test".into(),
            user_agent: None,
        });
        let _ = format!("{:?}", &event);
        let _ = event.clone();
    }

    #[test]
    fn test_serialize_deserialize_roundtrip_auth_failure() {
        let event = SecurityEvent::AuthFailure(AuthFailureEvent {
            timestamp: 12345,
            ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            user_id: Some("user1".to_string()),
            connection_id: Some(Uuid::new_v4()),
            auth_method: AuthMethod::JWT,
            reason: "invalid token".to_string(),
            user_agent: Some("curl/7.0".to_string()),
        });

        let serialized = sonic_rs::to_string(&event).unwrap();
        let deserialized: SecurityEvent = sonic_rs::from_str(&serialized).unwrap();

        match (event, deserialized) {
            (SecurityEvent::AuthFailure(a), SecurityEvent::AuthFailure(b)) => {
                assert_eq!(a.timestamp, b.timestamp);
                assert_eq!(a.reason, b.reason);
                assert_eq!(a.ip, b.ip);
                assert_eq!(a.user_id, b.user_id);
                assert_eq!(a.auth_method as u8, b.auth_method as u8);
            }
            _ => panic!("Wrong variant after deserialization"),
        }
    }

    #[test]
    fn test_serialize_deserialize_roundtrip_auth_success() {
        let event = SecurityEvent::AuthSuccess(AuthSuccessEvent {
            timestamp: 67890,
            ip: Some(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))),
            user_id: "alice".to_string(),
            connection_id: Some(Uuid::new_v4()),
            auth_method: AuthMethod::ApiKey,
            user_agent: Some("Mozilla/5.0".to_string()),
            session_duration: Some(3600),
        });

        let serialized = sonic_rs::to_string(&event).unwrap();
        let deserialized: SecurityEvent = sonic_rs::from_str(&serialized).unwrap();

        match (event, deserialized) {
            (SecurityEvent::AuthSuccess(a), SecurityEvent::AuthSuccess(b)) => {
                assert_eq!(a.timestamp, b.timestamp);
                assert_eq!(a.user_id, b.user_id);
                assert_eq!(a.session_duration, b.session_duration);
            }
            _ => panic!("Wrong variant after deserialization"),
        }
    }

    #[test]
    fn test_serialize_deserialize_roundtrip_suspicious_activity() {
        let conn_id = Uuid::new_v4();
        let event = SecurityEvent::SuspiciousActivity(SuspiciousActivityEvent {
            timestamp: 11111,
            connection_id: conn_id,
            ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 99))),
            user_id: Some("mallory".to_string()),
            activity_type: SuspiciousActivityType::PrivilegeEscalation,
            details: "Privilege escalation detected".to_string(),
            severity: SecuritySeverity::Critical,
        });

        let serialized = sonic_rs::to_string(&event).unwrap();
        let deserialized: SecurityEvent = sonic_rs::from_str(&serialized).unwrap();

        match (event, deserialized) {
            (SecurityEvent::SuspiciousActivity(a), SecurityEvent::SuspiciousActivity(b)) => {
                assert_eq!(a.timestamp, b.timestamp);
                assert_eq!(a.details, b.details);
                assert_eq!(a.severity as u8, b.severity as u8);
                assert_eq!(a.activity_type as u8, b.activity_type as u8);
            }
            _ => panic!("Wrong variant after deserialization"),
        }
    }

    #[test]
    fn test_serialize_deserialize_roundtrip_connection_security() {
        let conn_id = Uuid::new_v4();
        let event = SecurityEvent::ConnectionSecurity(ConnectionSecurityEvent {
            timestamp: 22222,
            connection_id: conn_id,
            ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            event_type: ConnectionSecurityType::CertificateValidationFailed,
            details: "Invalid certificate chain".to_string(),
        });

        let serialized = sonic_rs::to_string(&event).unwrap();
        let deserialized: SecurityEvent = sonic_rs::from_str(&serialized).unwrap();

        match (event, deserialized) {
            (SecurityEvent::ConnectionSecurity(a), SecurityEvent::ConnectionSecurity(b)) => {
                assert_eq!(a.timestamp, b.timestamp);
                assert_eq!(a.event_type as u8, b.event_type as u8);
                assert_eq!(a.details, b.details);
            }
            _ => panic!("Wrong variant after deserialization"),
        }
    }

    #[test]
    fn test_serialize_deserialize_roundtrip_rate_limit() {
        let event = SecurityEvent::RateLimitViolation(RateLimitEvent {
            timestamp: 33333,
            ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            user_id: Some("heavy_user".to_string()),
            connection_id: Some(Uuid::new_v4()),
            limit_type: RateLimitType::MessageRate,
            current_rate: 150,
            limit_threshold: 100,
            action_taken: RateLimitAction::ConnectionThrottled,
        });

        let serialized = sonic_rs::to_string(&event).unwrap();
        let deserialized: SecurityEvent = sonic_rs::from_str(&serialized).unwrap();

        match (event, deserialized) {
            (SecurityEvent::RateLimitViolation(a), SecurityEvent::RateLimitViolation(b)) => {
                assert_eq!(a.timestamp, b.timestamp);
                assert_eq!(a.limit_type as u8, b.limit_type as u8);
                assert_eq!(a.action_taken as u8, b.action_taken as u8);
                assert_eq!(a.current_rate, b.current_rate);
            }
            _ => panic!("Wrong variant after deserialization"),
        }
    }

    #[test]
    fn test_serialize_deserialize_roundtrip_ip_blocked() {
        let event = SecurityEvent::IpBlocked(IpBlockedEvent {
            timestamp: 44444,
            ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
            reason: "Brute force attack".to_string(),
            duration_seconds: Some(3600),
            trigger_event: Some("auth_failure".to_string()),
            blocked_connections: 10,
        });

        let serialized = sonic_rs::to_string(&event).unwrap();
        let deserialized: SecurityEvent = sonic_rs::from_str(&serialized).unwrap();

        match (event, deserialized) {
            (SecurityEvent::IpBlocked(a), SecurityEvent::IpBlocked(b)) => {
                assert_eq!(a.timestamp, b.timestamp);
                assert_eq!(a.reason, b.reason);
                assert_eq!(a.ip, b.ip);
                assert_eq!(a.blocked_connections, b.blocked_connections);
            }
            _ => panic!("Wrong variant after deserialization"),
        }
    }

    #[test]
    fn test_serialize_deserialize_roundtrip_permission_denied() {
        let event = SecurityEvent::PermissionDenied(PermissionDeniedEvent {
            timestamp: 55555,
            user_id: "bob".to_string(),
            connection_id: Some(Uuid::new_v4()),
            resource: "admin.panel".to_string(),
            action: "delete".to_string(),
            ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10))),
        });

        let serialized = sonic_rs::to_string(&event).unwrap();
        let deserialized: SecurityEvent = sonic_rs::from_str(&serialized).unwrap();

        match (event, deserialized) {
            (SecurityEvent::PermissionDenied(a), SecurityEvent::PermissionDenied(b)) => {
                assert_eq!(a.timestamp, b.timestamp);
                assert_eq!(a.user_id, b.user_id);
                assert_eq!(a.resource, b.resource);
                assert_eq!(a.action, b.action);
            }
            _ => panic!("Wrong variant after deserialization"),
        }
    }

    // --- AuthFailureEvent tests ---

    #[test]
    fn test_auth_failure_event_all_fields() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let event = AuthFailureEvent {
            timestamp: now,
            ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            user_id: Some("user1".to_string()),
            connection_id: Some(Uuid::new_v4()),
            auth_method: AuthMethod::JWT,
            reason: "wrong password".to_string(),
            user_agent: Some("Mozilla/5.0".to_string()),
        };
        assert_eq!(event.reason, "wrong password");
        assert_eq!(event.timestamp, now);
        assert!(event.ip.is_some());
        assert!(event.user_id.is_some());
        assert!(event.connection_id.is_some());
    }

    #[test]
    fn test_auth_failure_event_debug_clone() {
        let event = AuthFailureEvent {
            timestamp: 0,
            ip: None,
            user_id: None,
            connection_id: None,
            auth_method: AuthMethod::Anonymous,
            reason: "".into(),
            user_agent: None,
        };
        let _ = format!("{:?}", &event);
        let _ = event.clone();
    }

    // --- AuthSuccessEvent tests ---

    #[test]
    fn test_auth_success_event_all_fields() {
        let conn_id = Uuid::new_v4();
        let event = AuthSuccessEvent {
            timestamp: 100,
            ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            user_id: "alice".to_string(),
            connection_id: Some(conn_id),
            auth_method: AuthMethod::Session,
            user_agent: Some("Mozilla/5.0".to_string()),
            session_duration: Some(7200),
        };
        assert_eq!(event.user_id, "alice");
        assert_eq!(event.session_duration, Some(7200));
    }

    // --- SuspiciousActivityEvent tests ---

    #[test]
    fn test_suspicious_activity_all_activity_types() {
        let conn_id = Uuid::new_v4();
        let types = vec![
            SuspiciousActivityType::MultipleFailedAuths,
            SuspiciousActivityType::UnusualConnectionPattern,
            SuspiciousActivityType::ExcessiveMessageRate,
            SuspiciousActivityType::InvalidTokenFormat,
            SuspiciousActivityType::PrivilegeEscalation,
            SuspiciousActivityType::PrivilegeEscalation,
            SuspiciousActivityType::UnauthorizedChannelAccess,
            SuspiciousActivityType::AnomalousUserAgent,
            SuspiciousActivityType::GeolocationAnomaly,
        ];
        for t in types {
            let event = SuspiciousActivityEvent {
                timestamp: 0,
                connection_id: conn_id,
                ip: None,
                user_id: None,
                activity_type: t,
                details: "test".into(),
                severity: SecuritySeverity::Low,
            };
            let serialized = sonic_rs::to_string(&event).unwrap();
            let deserialized: SuspiciousActivityEvent = sonic_rs::from_str(&serialized).unwrap();
            assert_eq!(event.activity_type as u8, deserialized.activity_type as u8);
        }
    }

    #[test]
    fn test_suspicious_activity_all_severity_levels() {
        let conn_id = Uuid::new_v4();
        let severities = vec![
            SecuritySeverity::Low,
            SecuritySeverity::Medium,
            SecuritySeverity::High,
            SecuritySeverity::Critical,
        ];
        for severity in severities {
            let event = SuspiciousActivityEvent {
                timestamp: 0,
                connection_id: conn_id,
                ip: None,
                user_id: None,
                activity_type: SuspiciousActivityType::AnomalousUserAgent,
                details: "test".into(),
                severity,
            };
            let serialized = sonic_rs::to_string(&event).unwrap();
            let deserialized: SuspiciousActivityEvent = sonic_rs::from_str(&serialized).unwrap();
            assert_eq!(event.severity as u8, deserialized.severity as u8);
        }
    }

    #[test]
    fn test_suspicious_activity_debug_clone() {
        let event = SuspiciousActivityEvent {
            timestamp: 0,
            connection_id: Uuid::new_v4(),
            ip: None,
            user_id: None,
            activity_type: SuspiciousActivityType::MultipleFailedAuths,
            details: "".into(),
            severity: SecuritySeverity::Low,
        };
        let _ = format!("{:?}", &event);
        let _ = event.clone();
    }

    // --- SecuritySeverity enum tests ---

    #[test]
    fn test_security_severity_order() {
        assert_ne!(SecuritySeverity::Low as u8, SecuritySeverity::High as u8);
        assert_eq!(SecuritySeverity::Critical as u8, 3);
    }

    // --- RateLimitEvent / RateLimitType / RateLimitAction tests ---

    #[test]
    fn test_rate_limit_types_all_variants() {
        let types = vec![
            RateLimitType::MessageRate,
            RateLimitType::ConnectionRate,
            RateLimitType::AuthAttemptRate,
            RateLimitType::SubscriptionRate,
        ];
        for t in &types {
            let _ = format!("{t:?}");
        }
    }

    #[test]
    fn test_rate_limit_actions_all_variants() {
        let actions = vec![
            RateLimitAction::Warning,
            RateLimitAction::ConnectionThrottled,
            RateLimitAction::ConnectionDropped,
            RateLimitAction::IpTemporarilyBlocked,
        ];
        for a in &actions {
            let _ = format!("{a:?}");
        }
    }

    // --- ConnectionSecurityEvent / ConnectionSecurityType tests ---

    #[test]
    fn test_connection_security_types_all_variants() {
        let types = vec![
            ConnectionSecurityType::TlsHandshakeFailed,
            ConnectionSecurityType::CertificateValidationFailed,
            ConnectionSecurityType::SecurityValidationFailed,
            ConnectionSecurityType::ConnectionFromBlockedIp,
            ConnectionSecurityType::ExcessiveConnections,
            ConnectionSecurityType::ConnectionTimeoutExceeded,
        ];
        for t in &types {
            let _ = format!("{t:?}");
        }
    }

    // --- IpBlockedEvent tests ---

    #[test]
    fn test_ip_blocked_event_all_fields() {
        let event = IpBlockedEvent {
            timestamp: 100,
            ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            reason: "DoS".to_string(),
            duration_seconds: Some(3600),
            trigger_event: Some("rate_limit".to_string()),
            blocked_connections: 42,
        };
        assert_eq!(event.ip.to_string(), "10.0.0.1");
        assert_eq!(event.blocked_connections, 42);
        assert_eq!(event.duration_seconds, Some(3600));
    }

    // --- PermissionDeniedEvent tests ---

    #[test]
    fn test_permission_denied_event_all_fields() {
        let event = PermissionDeniedEvent {
            timestamp: 200,
            user_id: "bob".to_string(),
            connection_id: Some(Uuid::new_v4()),
            resource: "admin.users".to_string(),
            action: "delete".to_string(),
            ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
        };
        assert_eq!(event.action, "delete");
    }

    // --- SecurityMonitor tests ---

    #[tokio::test]
    async fn test_event_sender_non_blocking() {
        let monitor = SecurityMonitor::new().unwrap();
        let auth_failure = SecurityMonitor::create_auth_failure_event(
            Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            Some("user1".to_string()),
            Some(Uuid::new_v4()),
            AuthMethod::JWT,
            "invalid password".to_string(),
        );
        monitor.log_auth_failure(auth_failure);
    }

    #[tokio::test]
    async fn test_log_all_event_types() {
        let monitor = SecurityMonitor::new().unwrap();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let conn_id = Uuid::new_v4();

        monitor.log_auth_failure(AuthFailureEvent {
            timestamp: 0,
            ip: Some(ip),
            user_id: None,
            connection_id: None,
            auth_method: AuthMethod::JWT,
            reason: "test".into(),
            user_agent: None,
        });

        monitor.log_auth_success(AuthSuccessEvent {
            timestamp: 0,
            ip: Some(ip),
            user_id: "user".into(),
            connection_id: None,
            auth_method: AuthMethod::JWT,
            user_agent: None,
            session_duration: None,
        });

        monitor.log_suspicious_activity(SuspiciousActivityEvent {
            timestamp: 0,
            connection_id: conn_id,
            ip: Some(ip),
            user_id: None,
            activity_type: SuspiciousActivityType::MultipleFailedAuths,
            details: "test".into(),
            severity: SecuritySeverity::Low,
        });

        monitor.log_connection_security(ConnectionSecurityEvent {
            timestamp: 0,
            connection_id: conn_id,
            ip: Some(ip),
            event_type: ConnectionSecurityType::TlsHandshakeFailed,
            details: "test".into(),
        });

        monitor.log_rate_limit_violation(RateLimitEvent {
            timestamp: 0,
            ip: Some(ip),
            user_id: None,
            connection_id: None,
            limit_type: RateLimitType::ConnectionRate,
            current_rate: 0,
            limit_threshold: 0,
            action_taken: RateLimitAction::Warning,
        });

        monitor.log_ip_blocked(IpBlockedEvent {
            timestamp: 0,
            ip,
            reason: "test".into(),
            duration_seconds: None,
            trigger_event: None,
            blocked_connections: 0,
        });

        monitor.log_permission_denied(PermissionDeniedEvent {
            timestamp: 0,
            user_id: "user".into(),
            connection_id: None,
            resource: "r".into(),
            action: "a".into(),
            ip: Some(ip),
        });
    }

    #[test]
    fn test_create_auth_failure_event_helper() {
        let ip = Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let user_id = Some("user1".to_string());
        let conn_id = Some(Uuid::new_v4());
        let event = SecurityMonitor::create_auth_failure_event(
            ip,
            user_id.clone(),
            conn_id,
            AuthMethod::JWT,
            "rate limited".to_string(),
        );
        assert_eq!(event.reason, "rate limited");
        assert_eq!(event.ip, ip);
        assert_eq!(event.user_id, user_id);
    }

    #[test]
    fn test_create_suspicious_activity_event_helper() {
        let conn_id = Uuid::new_v4();
        let event = SecurityMonitor::create_suspicious_activity_event(
            conn_id,
            None,
            None,
            SuspiciousActivityType::PrivilegeEscalation,
            "Attempted role change".to_string(),
            SecuritySeverity::High,
        );
        assert_eq!(
            event.activity_type as u8,
            SuspiciousActivityType::PrivilegeEscalation as u8
        );
        assert_eq!(event.severity as u8, SecuritySeverity::High as u8);
        assert_eq!(event.details, "Attempted role change");
    }

    #[test]
    fn test_create_rate_limit_event_helper() {
        let event = SecurityMonitor::create_rate_limit_event(
            Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            Some("user1".to_string()),
            Some(Uuid::new_v4()),
            RateLimitType::SubscriptionRate,
            10,
            5,
            RateLimitAction::ConnectionDropped,
        );
        assert_eq!(
            event.limit_type as u8,
            RateLimitType::SubscriptionRate as u8
        );
        assert_eq!(event.current_rate, 10);
        assert_eq!(
            event.action_taken as u8,
            RateLimitAction::ConnectionDropped as u8
        );
    }

    #[tokio::test]
    async fn test_security_monitor_default() {
        let _monitor = SecurityMonitor::default();
    }
}
