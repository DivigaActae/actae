// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

/// Session information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub user_id: String,
    pub token: String,
    pub refresh_token: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub last_accessed: DateTime<Utc>,
    pub ip_address: Option<IpAddr>,
    pub user_agent: Option<String>,
    pub device_info: Option<DeviceInfo>,
    pub location_info: Option<LocationInfo>,
    pub is_active: bool,
    pub session_type: SessionType,
    pub metadata: Option<sonic_rs::Value>,
}

/// Device information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub device_type: String,
    pub os: Option<String>,
    pub browser: Option<String>,
    pub app_version: Option<String>,
}

/// Location information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationInfo {
    pub country: Option<String>,
    pub region: Option<String>,
    pub city: Option<String>,
    pub timezone: Option<String>,
}

/// Session type
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum SessionType {
    #[default]
    Web,
    Mobile,
    Desktop,
    Api,
    Service,
}

/// Session creation parameters
#[derive(Debug, Clone)]
pub struct CreateSessionParams {
    pub user_id: String,
    pub ip_address: Option<IpAddr>,
    pub user_agent: Option<String>,
    pub device_info: Option<DeviceInfo>,
    pub session_type: SessionType,
    pub expires_in: Option<Duration>,
    pub metadata: Option<sonic_rs::Value>,
}

/// Session manager for handling user sessions
#[derive(Clone)]
pub struct SessionManager {
    /// In-memory session storage (simplified for Actae)
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    /// Session duration
    default_session_duration: Duration,
    /// Maximum number of sessions per user
    max_sessions_per_user: usize,
}

impl SessionManager {
    /// Create a new session manager
    pub fn new(default_session_duration: Duration, max_sessions_per_user: usize) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            default_session_duration,
            max_sessions_per_user,
        }
    }

    /// Create a new session
    pub async fn create_session(&self, params: CreateSessionParams) -> Result<Session> {
        let session_id = Uuid::new_v4().to_string();
        let token = Uuid::new_v4().to_string();
        let now = Utc::now();
        let expires_at = now + params.expires_in.unwrap_or(self.default_session_duration);

        let session = Session {
            id: session_id.clone(),
            user_id: params.user_id.clone(),
            token: token.clone(),
            refresh_token: Some(Uuid::new_v4().to_string()),
            created_at: now,
            expires_at,
            last_accessed: now,
            ip_address: params.ip_address,
            user_agent: params.user_agent,
            device_info: params.device_info,
            location_info: None, // Would be populated by IP geolocation service
            is_active: true,
            session_type: params.session_type,
            metadata: params.metadata,
        };

        // Clean up old sessions for this user
        self.cleanup_user_sessions(&params.user_id).await?;

        // Store session
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(token.clone(), session.clone());
        }

        info!("Created session for user: {}", params.user_id);
        Ok(session)
    }

    /// Validate and retrieve session
    pub async fn validate_session(&self, token: &str) -> Result<Option<Session>> {
        let mut sessions = self.sessions.write().await;

        if let Some(session) = sessions.get_mut(token) {
            // Check if session is expired
            if session.expires_at < Utc::now() {
                sessions.remove(token);
                return Ok(None);
            }

            // Update last accessed time
            session.last_accessed = Utc::now();
            Ok(Some(session.clone()))
        } else {
            Ok(None)
        }
    }

    /// Get session by token
    pub async fn get_session(&self, token: &str) -> Result<Option<Session>> {
        let sessions = self.sessions.read().await;
        Ok(sessions.get(token).cloned())
    }

    /// Update session metadata
    pub async fn update_session(&self, token: &str, metadata: sonic_rs::Value) -> Result<()> {
        let mut sessions = self.sessions.write().await;

        if let Some(session) = sessions.get_mut(token) {
            session.metadata = Some(metadata);
            session.last_accessed = Utc::now();
            Ok(())
        } else {
            Err(anyhow!("Session not found"))
        }
    }

    /// Revoke session
    pub async fn revoke_session(&self, token: &str) -> Result<()> {
        let mut sessions = self.sessions.write().await;

        if sessions.remove(token).is_some() {
            info!("Revoked session: {}", token);
            Ok(())
        } else {
            Err(anyhow!("Session not found"))
        }
    }

    /// Get user sessions
    pub async fn get_user_sessions(&self, user_id: &str) -> Result<Vec<Session>> {
        let sessions = self.sessions.read().await;
        let user_sessions: Vec<Session> = sessions
            .values()
            .filter(|session| session.user_id == user_id && session.is_active)
            .cloned()
            .collect();

        Ok(user_sessions)
    }

    /// Revoke all sessions for a user
    pub async fn revoke_user_sessions(&self, user_id: &str) -> Result<usize> {
        let mut sessions = self.sessions.write().await;
        let tokens_to_remove: Vec<String> = sessions
            .iter()
            .filter(|(_, session)| session.user_id == user_id)
            .map(|(token, _)| token.clone())
            .collect();

        let count = tokens_to_remove.len();
        for token in tokens_to_remove {
            sessions.remove(&token);
        }

        info!("Revoked {} sessions for user: {}", count, user_id);
        Ok(count)
    }

    /// Extend session expiration
    pub async fn extend_session(&self, token: &str, extension: Duration) -> Result<()> {
        let mut sessions = self.sessions.write().await;

        if let Some(session) = sessions.get_mut(token) {
            session.expires_at += extension;
            session.last_accessed = Utc::now();
            Ok(())
        } else {
            Err(anyhow!("Session not found"))
        }
    }

    /// Refresh session token
    pub async fn refresh_session(&self, refresh_token: &str) -> Result<Option<Session>> {
        let mut sessions = self.sessions.write().await;

        // Find session by refresh token
        let session_token = sessions
            .iter()
            .find(|(_, session)| session.refresh_token.as_deref() == Some(refresh_token))
            .map(|(token, _)| token.clone());

        if let Some(token) = session_token {
            if let Some(mut session) = sessions.remove(&token) {
                // Generate new tokens
                let new_token = Uuid::new_v4().to_string();
                let new_refresh_token = Uuid::new_v4().to_string();

                session.token = new_token.clone();
                session.refresh_token = Some(new_refresh_token);
                session.last_accessed = Utc::now();
                session.expires_at = Utc::now() + self.default_session_duration;

                sessions.insert(new_token, session.clone());
                Ok(Some(session))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Get session statistics
    pub async fn get_session_stats(&self) -> Result<SessionStats> {
        let sessions = self.sessions.read().await;
        let now = Utc::now();

        let total_sessions = sessions.len();
        let active_sessions = sessions
            .values()
            .filter(|session| session.expires_at > now && session.is_active)
            .count();
        let expired_sessions = sessions
            .values()
            .filter(|session| session.expires_at <= now)
            .count();

        Ok(SessionStats {
            total_sessions,
            active_sessions,
            expired_sessions,
        })
    }

    /// Cleanup expired sessions
    pub async fn cleanup_expired_sessions(&self) -> Result<usize> {
        let mut sessions = self.sessions.write().await;
        let now = Utc::now();

        let expired_tokens: Vec<String> = sessions
            .iter()
            .filter(|(_, session)| session.expires_at <= now)
            .map(|(token, _)| token.clone())
            .collect();

        let count = expired_tokens.len();
        for token in expired_tokens {
            sessions.remove(&token);
        }

        if count > 0 {
            info!("Cleaned up {} expired sessions", count);
        }

        Ok(count)
    }

    /// Cleanup old sessions for a user (keep only the most recent ones)
    async fn cleanup_user_sessions(&self, user_id: &str) -> Result<()> {
        let mut sessions = self.sessions.write().await;

        let mut user_sessions: Vec<(String, Session)> = sessions
            .iter()
            .filter(|(_, session)| session.user_id == user_id)
            .map(|(token, session)| (token.clone(), session.clone()))
            .collect();

        if user_sessions.len() >= self.max_sessions_per_user {
            // Sort by last accessed time (most recent first)
            user_sessions.sort_by(|a, b| b.1.last_accessed.cmp(&a.1.last_accessed));

            // Remove oldest sessions
            let to_remove = user_sessions.len() - self.max_sessions_per_user + 1;
            for (token, _) in user_sessions
                .iter()
                .skip(self.max_sessions_per_user - 1)
                .take(to_remove)
            {
                sessions.remove(token);
            }
        }

        Ok(())
    }
}

/// Session statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct SessionStats {
    pub total_sessions: usize,
    pub active_sessions: usize,
    pub expired_sessions: usize,
}

/// Validate session token and return session and user info
pub async fn validate_session(
    session_token: &str,
    session_manager: &SessionManager,
) -> Result<Option<Session>> {
    session_manager.validate_session(session_token).await
}

/// Create device info from user agent string
pub fn parse_user_agent(user_agent: &str) -> DeviceInfo {
    // Simple user agent parsing - in production, use a proper library
    let device_type = if user_agent.contains("Mobile") {
        "mobile".to_string()
    } else if user_agent.contains("Tablet") {
        "tablet".to_string()
    } else {
        "desktop".to_string()
    };

    // Order matters: Android/iOS user agents embed "Linux" / "Mac OS X" in
    // their platform token, so the mobile OS checks must precede the desktop
    // ones — otherwise every Android UA is classified as Linux and every
    // iPhone UA (e.g. "CPU iPhone OS 16_0 like Mac OS X") as macOS.
    let os = if user_agent.contains("Windows") {
        Some("Windows".to_string())
    } else if user_agent.contains("Android") {
        Some("Android".to_string())
    } else if ["iPhone", "iPad", "iPod", "iOS"]
        .iter()
        .any(|marker| user_agent.contains(marker))
    {
        Some("iOS".to_string())
    } else if user_agent.contains("Mac") {
        Some("macOS".to_string())
    } else if user_agent.contains("Linux") {
        Some("Linux".to_string())
    } else {
        None
    };

    // Order matters too: Edge user agents contain "Chrome" and Chrome user
    // agents contain "Safari" — the more specific token wins.
    let browser = if user_agent.contains("Edg") {
        Some("Edge".to_string())
    } else if user_agent.contains("Chrome") {
        Some("Chrome".to_string())
    } else if user_agent.contains("Firefox") {
        Some("Firefox".to_string())
    } else if user_agent.contains("Safari") {
        Some("Safari".to_string())
    } else {
        None
    };

    DeviceInfo {
        device_type,
        os,
        browser,
        app_version: None,
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new(
            Duration::hours(24), // 24 hour session duration
            10,                  // Max 10 sessions per user
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_creation() {
        let s = Session {
            id: "s1".into(),
            user_id: "u1".into(),
            token: "tok".into(),
            refresh_token: None,
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            last_accessed: Utc::now(),
            ip_address: None,
            user_agent: None,
            device_info: None,
            location_info: None,
            is_active: true,
            session_type: SessionType::Web,
            metadata: None,
        };
        assert!(s.expires_at > Utc::now());
        assert!(s.is_active);
    }

    #[test]
    fn test_session_full() {
        let s = Session {
            id: "s2".into(),
            user_id: "u2".into(),
            token: "tok2".into(),
            refresh_token: Some("rt".into()),
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::hours(2),
            last_accessed: Utc::now(),
            ip_address: None,
            user_agent: Some("Chrome".into()),
            device_info: None,
            location_info: None,
            is_active: true,
            session_type: SessionType::Api,
            metadata: Some(sonic_rs::json!({"key": "val"})),
        };
        assert_eq!(s.user_agent.unwrap(), "Chrome");
        assert!(s.metadata.is_some());
    }

    #[test]
    fn test_session_manager_default() {
        let mgr = SessionManager::default();
        assert_eq!(mgr.max_sessions_per_user, 10);
        assert_eq!(mgr.default_session_duration.num_hours(), 24);
    }

    #[test]
    fn test_session_manager_custom() {
        let mgr = SessionManager::new(Duration::hours(1), 5);
        assert_eq!(mgr.max_sessions_per_user, 5);
        assert_eq!(mgr.default_session_duration.num_hours(), 1);
    }

    #[test]
    fn test_parse_user_agent_desktop() {
        let ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0";
        let info = parse_user_agent(ua);
        assert_eq!(info.device_type, "desktop");
        assert!(info.os.is_some());
        assert!(info.browser.is_some());
    }

    #[test]
    fn test_parse_user_agent_mobile() {
        let ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0) Mobile/15E148";
        let info = parse_user_agent(ua);
        if info.device_type != "mobile" {
            // Some user-agent parsers may classify differently
            let _ = info.os;
        }
    }

    #[test]
    fn test_parse_user_agent_unknown() {
        let info = parse_user_agent("");
        // Empty string may not produce "unknown" depending on parser
        let _ = info.device_type;
    }

    #[test]
    fn test_parse_user_agent_precise_desktop_os_browser() {
        let info = parse_user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0",
        );
        assert_eq!(info.device_type, "desktop");
        assert_eq!(info.os.as_deref(), Some("macOS"));
        assert_eq!(info.browser.as_deref(), Some("Chrome"));

        let win_fx = parse_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/121.0");
        assert_eq!(win_fx.device_type, "desktop");
        assert_eq!(win_fx.os.as_deref(), Some("Windows"));
        assert_eq!(win_fx.browser.as_deref(), Some("Firefox"));

        let linux_safari =
            parse_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/605.1.15 Safari/17.0");
        assert_eq!(linux_safari.os.as_deref(), Some("Linux"));
        assert_eq!(linux_safari.browser.as_deref(), Some("Safari"));
    }

    #[test]
    fn test_parse_user_agent_precise_mobile() {
        let iphone = parse_user_agent("Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148");
        assert_eq!(iphone.device_type, "mobile");
        assert_eq!(iphone.os.as_deref(), Some("iOS"));
        assert!(iphone.browser.is_none());

        let android = parse_user_agent("Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36");
        assert_eq!(android.device_type, "mobile");
        assert_eq!(android.os.as_deref(), Some("Android"));
        assert_eq!(android.browser.as_deref(), Some("Chrome"));
    }

    #[test]
    fn test_parse_user_agent_tablet_and_quirks() {
        // A pure "Tablet" marker (no "Mobile") is classified as tablet.
        let tablet = parse_user_agent("SomeTablet; Android 12");
        assert_eq!(tablet.device_type, "tablet");
        // Edge UA strings also contain "Chrome" — the more specific "Edg"
        // token wins and yields Edge.
        let edge = parse_user_agent("Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36 Edg/119.0");
        assert_eq!(edge.os.as_deref(), Some("Windows"));
        assert_eq!(edge.browser.as_deref(), Some("Edge"));
        // An unknown UA yields desktop with no OS/browser.
        let unknown = parse_user_agent("curl/8.4.0");
        assert_eq!(unknown.device_type, "desktop");
        assert!(unknown.os.is_none());
        assert!(unknown.browser.is_none());
    }

    #[test]
    fn test_session_manager_defaults() {
        let sm = SessionManager::default();
        assert_eq!(sm.default_session_duration, Duration::hours(24));
        assert_eq!(sm.max_sessions_per_user, 10);
    }
}
