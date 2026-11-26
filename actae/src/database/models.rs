// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sonic_rs::{Deserialize, Serialize};
use tokio_postgres::Row;

/// Main database models - for replication database (actae)

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealtimeMessage {
    pub id: i64,
    pub channel: String,
    pub payload: sonic_rs::Value,
    pub metadata: sonic_rs::Value,
    pub created_at: DateTime<Utc>,
}

impl From<Row> for RealtimeMessage {
    fn from(row: Row) -> Self {
        Self {
            id: row.try_get::<_, i64>("id").unwrap_or(0),
            channel: row.try_get::<_, String>("channel").unwrap_or_default(),
            payload: {
                match row.try_get::<_, crate::database::json_compat::JsonValue>("payload") {
                    Ok(payload_wrapper) => {
                        crate::database::json_compat::from_sql_json(payload_wrapper)
                    }
                    Err(e) => {
                        tracing::error!(
                            "Error retrieving payload column: {}, using empty object",
                            e
                        );
                        sonic_rs::json!({})
                    }
                }
            },
            metadata: {
                match row.try_get::<_, Option<crate::database::json_compat::JsonValue>>("metadata")
                {
                    Ok(Some(metadata_wrapper)) => {
                        crate::database::json_compat::from_sql_json(metadata_wrapper)
                    }
                    Ok(None) => sonic_rs::json!({}),
                    Err(e) => {
                        tracing::error!(
                            "Error retrieving metadata column: {}, using empty object",
                            e
                        );
                        sonic_rs::json!({})
                    }
                }
            },
            created_at: row
                .try_get::<_, DateTime<Utc>>("created_at")
                .unwrap_or(Utc::now()),
        }
    }
}

impl RealtimeMessage {
    /// Get payload as sonic-rs Value (already in correct format)
    pub fn payload_as_sonic(&self) -> &sonic_rs::Value {
        &self.payload
    }

    /// Get metadata as sonic-rs Value (already in correct format)
    pub fn metadata_as_sonic(&self) -> &sonic_rs::Value {
        &self.metadata
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestData {
    pub id: i32,
    pub data: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<Row> for TestData {
    fn from(row: Row) -> Self {
        Self {
            id: row.try_get::<_, i32>("id").unwrap_or(0),
            data: row.try_get::<_, Option<String>>("data").unwrap_or(None),
            created_at: row
                .try_get::<_, DateTime<Utc>>("created_at")
                .unwrap_or(Utc::now()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Drawing {
    pub id: i32,
    pub data: sonic_rs::Value,
    pub created_at: DateTime<Utc>,
}

impl From<Row> for Drawing {
    fn from(row: Row) -> Self {
        Self {
            id: row.try_get::<_, i32>("id").unwrap_or(0),
            data: {
                match row.try_get::<_, crate::database::json_compat::JsonValue>("data") {
                    Ok(data_wrapper) => crate::database::json_compat::from_sql_json(data_wrapper),
                    Err(e) => {
                        tracing::error!("Error retrieving drawing data: {}, using empty object", e);
                        sonic_rs::json!({})
                    }
                }
            },
            created_at: row
                .try_get::<_, DateTime<Utc>>("created_at")
                .unwrap_or(Utc::now()),
        }
    }
}

impl Drawing {
    /// Get data as sonic-rs Value (already in correct format)
    pub fn data_as_sonic(&self) -> &sonic_rs::Value {
        &self.data
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    pub id: i32,
    pub x: i32,
    pub y: i32,
    pub user_id: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl From<Row> for Cursor {
    fn from(row: Row) -> Self {
        Self {
            id: row.try_get::<_, i32>("id").unwrap_or(0),
            x: row.try_get::<_, i32>("x").unwrap_or(0),
            y: row.try_get::<_, i32>("y").unwrap_or(0),
            user_id: row.try_get::<_, Option<String>>("user_id").unwrap_or(None),
            updated_at: row
                .try_get::<_, DateTime<Utc>>("updated_at")
                .unwrap_or(Utc::now()),
        }
    }
}

/// Client database models - for client database (actae)

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub email: String,
    pub email_verified: bool,
    pub name: String,
    pub image: Option<String>,
    pub password_hash: Option<String>,
    /// Dashboard role: `admin` (manage users) or `member`.
    pub role: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<Row> for User {
    fn from(row: Row) -> Self {
        Self {
            id: row.try_get::<_, String>("id").unwrap_or_default(),
            email: row.try_get::<_, String>("email").unwrap_or_default(),
            email_verified: row.try_get::<_, bool>("emailVerified").unwrap_or(false),
            name: row.try_get::<_, String>("name").unwrap_or_default(),
            image: row.try_get::<_, Option<String>>("image").unwrap_or(None),
            password_hash: row
                .try_get::<_, Option<String>>("password_hash")
                .unwrap_or(None),
            role: row
                .try_get::<_, String>("role")
                .unwrap_or_else(|_| "member".to_string()),
            created_at: row
                .try_get::<_, DateTime<Utc>>("createdAt")
                .unwrap_or(Utc::now()),
            updated_at: row
                .try_get::<_, DateTime<Utc>>("updatedAt")
                .unwrap_or(Utc::now()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub expires_at: DateTime<Utc>,
    pub token: String,
    pub user_id: String,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<Row> for Session {
    fn from(row: Row) -> Self {
        Self {
            id: row.try_get::<_, String>("id").unwrap_or_default(),
            expires_at: row
                .try_get::<_, DateTime<Utc>>("expiresAt")
                .unwrap_or(Utc::now()),
            token: row.try_get::<_, String>("token").unwrap_or_default(),
            user_id: row.try_get::<_, String>("userId").unwrap_or_default(),
            ip_address: row
                .try_get::<_, Option<String>>("ipAddress")
                .unwrap_or(None),
            user_agent: row
                .try_get::<_, Option<String>>("userAgent")
                .unwrap_or(None),
            created_at: row
                .try_get::<_, DateTime<Utc>>("createdAt")
                .unwrap_or(Utc::now()),
            updated_at: row
                .try_get::<_, DateTime<Utc>>("updatedAt")
                .unwrap_or(Utc::now()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub key_hash: String,
    pub key_prefix: String,
    pub permissions: sonic_rs::Value,
    pub last_used_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: String,
    /// Key origin: `control_plane` (synced from the SaaS control plane) or
    /// `local` (created via `actae keys create` / first-boot provisioning).
    /// Both are subject to subscription enforcement.
    pub source: String,
}

impl From<Row> for ApiKey {
    fn from(row: Row) -> Self {
        Self {
            id: row.try_get::<_, String>("id").unwrap_or_default(),
            user_id: row.try_get::<_, String>("user_id").unwrap_or_default(),
            name: row.try_get::<_, String>("name").unwrap_or_default(),
            key_hash: row.try_get::<_, String>("key_hash").unwrap_or_default(),
            key_prefix: row.try_get::<_, String>("key_prefix").unwrap_or_default(),
            permissions: {
                match row
                    .try_get::<_, Option<crate::database::json_compat::JsonValue>>("permissions")
                {
                    Ok(Some(json_value)) => crate::database::json_compat::from_sql_json(json_value),
                    Ok(None) => sonic_rs::json!({}),
                    Err(e) => {
                        tracing::error!(
                            "Error retrieving permissions column: {}, using empty object",
                            e
                        );
                        sonic_rs::json!({})
                    }
                }
            },
            last_used_at: row
                .try_get::<_, Option<DateTime<Utc>>>("last_used_at")
                .unwrap_or(None),
            expires_at: row
                .try_get::<_, Option<DateTime<Utc>>>("expires_at")
                .unwrap_or(None),
            created_at: row
                .try_get::<_, DateTime<Utc>>("created_at")
                .unwrap_or(Utc::now()),
            updated_at: row
                .try_get::<_, DateTime<Utc>>("updated_at")
                .unwrap_or(Utc::now()),
            status: row.try_get::<_, String>("status").unwrap_or_default(),
            source: row
                .try_get::<_, String>("source")
                .unwrap_or_else(|_| "local".to_string()),
        }
    }
}

impl ApiKey {
    /// Convert permissions to sonic-rs Value for API responses
    /// Get permissions as sonic-rs Value (already in correct format)
    pub fn permissions_as_sonic(&self) -> &sonic_rs::Value {
        &self.permissions
    }
}

/// Repository patterns for database operations
pub struct MainDatabaseRepository {
    pool: deadpool_postgres::Pool,
}

impl MainDatabaseRepository {
    pub fn new(pool: deadpool_postgres::Pool) -> Self {
        Self { pool }
    }

    /// Insert realtime message
    pub async fn insert_realtime_message(
        &self,
        channel: &str,
        payload: &sonic_rs::Value,
        metadata: Option<&sonic_rs::Value>,
    ) -> Result<i64> {
        let client = self
            .pool
            .get()
            .await
            .context("Failed to get database connection")?;

        let metadata_val = metadata
            .cloned()
            .map(|v| crate::database::json_compat::to_sql_json(&v))
            .unwrap_or_else(crate::database::json_compat::empty_json_object);

        let row = client
            .query_one(
                "INSERT INTO public.realtime_messages (channel, payload, metadata) 
                 VALUES ($1, $2, $3) RETURNING id",
                &[
                    &channel,
                    &crate::database::json_compat::to_sql_json(payload),
                    &metadata_val,
                ],
            )
            .await
            .context("Failed to insert realtime message")?;

        Ok(row.try_get::<_, i64>("id").unwrap_or(0))
    }

    /// Get recent messages for a channel
    pub async fn get_recent_messages(
        &self,
        channel: &str,
        limit: i64,
    ) -> Result<Vec<RealtimeMessage>> {
        let client = self
            .pool
            .get()
            .await
            .context("Failed to get database connection")?;

        let rows = client
            .query(
                "SELECT * FROM public.realtime_messages 
                 WHERE channel = $1
                 ORDER BY created_at DESC LIMIT $2",
                &[&channel, &limit],
            )
            .await
            .context("Failed to query realtime messages")?;

        Ok(rows.into_iter().map(RealtimeMessage::from).collect())
    }

    /// Health check - simple query
    pub async fn health_check(&self) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .context("Failed to get database connection")?;

        client
            .query_one("SELECT 1", &[])
            .await
            .context("Health check query failed")?;

        Ok(())
    }
}

pub struct ClientDatabaseRepository {
    pool: deadpool_postgres::Pool,
}

impl ClientDatabaseRepository {
    pub fn new(pool: deadpool_postgres::Pool) -> Self {
        Self { pool }
    }

    /// Get user by ID
    pub async fn get_user(&self, id: &str) -> Result<Option<User>> {
        let client = self
            .pool
            .get()
            .await
            .context("Failed to get database connection")?;

        let rows = client
            .query("SELECT * FROM public.user WHERE id = $1", &[&id])
            .await
            .context("Failed to query user")?;

        if let Some(row) = rows.into_iter().next() {
            Ok(Some(User::from(row)))
        } else {
            Ok(None)
        }
    }

    /// Get user by email
    pub async fn get_user_by_email(&self, email: &str) -> Result<Option<User>> {
        let client = self
            .pool
            .get()
            .await
            .context("Failed to get database connection")?;

        let rows = client
            .query("SELECT * FROM public.user WHERE email = $1", &[&email])
            .await
            .context("Failed to query user by email")?;

        if let Some(row) = rows.into_iter().next() {
            Ok(Some(User::from(row)))
        } else {
            Ok(None)
        }
    }

    /// Validate session token
    pub async fn validate_session(&self, token: &str) -> Result<Option<(User, Session)>> {
        let client = self
            .pool
            .get()
            .await
            .context("Failed to get database connection")?;

        let rows = client
            .query(
                "SELECT u.*, s.* FROM public.user u 
                 JOIN public.session s ON u.id = s.userId 
                 WHERE s.token = $1 AND s.expiresAt > NOW()",
                &[&token],
            )
            .await
            .context("Failed to validate session")?;

        if let Some(row) = rows.into_iter().next() {
            let user = User::from(row.clone());
            let session = Session::from(row);
            Ok(Some((user, session)))
        } else {
            Ok(None)
        }
    }

    /// Health check - simple query
    pub async fn health_check(&self) -> Result<()> {
        let client = self
            .pool
            .get()
            .await
            .context("Failed to get database connection")?;

        client
            .query_one("SELECT 1", &[])
            .await
            .context("Health check query failed")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_realtime_message_creation() {
        let m = RealtimeMessage {
            id: 1,
            channel: "ch".into(),
            payload: sonic_rs::json!({"k": "v"}),
            metadata: sonic_rs::json!({"s": "t"}),
            created_at: Utc::now(),
        };
        assert_eq!(m.channel, "ch");
    }

    #[test]
    fn test_user_creation() {
        let u = User {
            id: "user-1".into(),
            email: "test@example.com".into(),
            email_verified: true,
            name: "Test User".into(),
            image: Some("https://example.com/avatar.png".into()),
            password_hash: Some("hash".into()),
            role: "admin".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert!(u.email_verified);
        assert_eq!(u.email, "test@example.com");
    }

    #[test]
    fn test_user_no_image() {
        let u = User {
            id: "u2".into(),
            email: "anon@example.com".into(),
            email_verified: false,
            name: "Anon".into(),
            image: None,
            password_hash: None,
            role: "member".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert!(u.image.is_none());
        assert!(!u.email_verified);
    }

    #[test]
    fn test_session_creation() {
        let s = Session {
            id: "sess-1".into(),
            expires_at: Utc::now() + chrono::Duration::hours(24),
            token: "token-abc".into(),
            user_id: "user-1".into(),
            ip_address: Some("127.0.0.1".into()),
            user_agent: Some("Chrome".into()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert_eq!(s.token, "token-abc");
        assert!(s.expires_at > Utc::now());
    }

    #[test]
    fn test_session_no_ip() {
        let s = Session {
            id: "sess-2".into(),
            expires_at: Utc::now() - chrono::Duration::hours(1),
            token: "expired".into(),
            user_id: "u1".into(),
            ip_address: None,
            user_agent: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert!(s.expires_at < Utc::now());
        assert!(s.ip_address.is_none());
    }

    #[test]
    fn test_api_key_construction() {
        let key = ApiKey {
            id: "key-1".into(),
            user_id: "user-1".into(),
            name: "My Key".into(),
            key_hash: "hash".into(),
            key_prefix: "diviga_12".into(),
            permissions: sonic_rs::json!({"channels": ["read"]}),
            last_used_at: None,
            expires_at: None,
            status: "active".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            source: "control_plane".into(),
        };
        assert_eq!(key.name, "My Key");
        assert_eq!(key.status, "active");
        assert_eq!(key.source, "control_plane");
    }

    #[test]
    fn test_cursor_construction() {
        let c = Cursor {
            id: 1,
            x: 100,
            y: 200,
            user_id: Some("u1".into()),
            updated_at: Utc::now(),
        };
        assert_eq!(c.x, 100);
        assert_eq!(c.user_id, Some("u1".into()));
    }
}
