// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, Utc};
use dashmap::DashSet;
use sonic_rs::{JsonValueMutTrait, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

use super::message::Message;
use crate::auth::types::AuthContext;

/// Memory usage statistics for connection management
#[derive(Debug, Clone)]
pub struct ConnectionMemoryStats {
    pub connection_count: usize,
    pub topic_count: usize,
    pub sender_count: usize,
    pub failure_count: usize,
    pub total_subscriptions: usize,
    pub avg_subscriptions: f64,
    pub max_connections: usize,
    pub utilization_percent: f64,
}

/// WebSocket connection state
#[derive(Debug, Clone)]
pub struct Connection {
    pub id: Uuid,
    pub user_id: Option<String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub connected_at: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    pub subscriptions: Arc<RwLock<HashSet<String>>>,
    pub is_authenticated: bool,
    pub auth_method: Option<String>,
    /// The full authorization principal established by the `auth` handshake.
    ///
    /// `Some(AuthContext::Anonymous)` means the handshake completed under
    /// anonymous mode; `None` means no handshake has completed and every
    /// non-auth message must be rejected (audit item 1). This replaces the
    /// synthesized read/write permission lists that previously granted every
    /// authenticated client full access (audit item 3).
    pub auth_context: Option<AuthContext>,
    pub metadata: Arc<RwLock<JsonValue>>,
    // NEW FIELDS FOR PERFORMANCE CACHING
    /// Topics that this connection has been validated to write to
    /// Validated at subscription time, no per-message checks needed
    pub validated_write_topics: Arc<DashSet<String>>,

    /// Topics that this connection has been validated to read from
    /// Validated at subscription time
    pub validated_read_topics: Arc<DashSet<String>>,

    /// Expiry time for permission cache (optional, for dynamic permissions)
    pub permission_cache_expiry: Option<DateTime<Utc>>,

    /// Per-topic cursor tracking for resume on reconnect.
    /// Maps topic_name -> last_seen_cursor for this connection.
    pub topic_cursors: Arc<RwLock<HashMap<String, i64>>>,
}

impl Connection {
    pub fn new(
        user_id: Option<String>,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Self {
        let is_authenticated = user_id.is_some();
        Self {
            id: Uuid::new_v4(),
            user_id,
            ip_address,
            user_agent,
            connected_at: Utc::now(),
            last_activity: Utc::now(),
            subscriptions: Arc::new(RwLock::new(HashSet::new())),
            is_authenticated,
            auth_method: None,
            auth_context: None,
            metadata: Arc::new(RwLock::new(sonic_rs::json!({}))),
            validated_write_topics: Arc::new(DashSet::new()),
            validated_read_topics: Arc::new(DashSet::new()),
            permission_cache_expiry: None,
            topic_cursors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create an anonymous connection
    pub fn anonymous(ip_address: Option<String>, user_agent: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            user_id: None,
            ip_address,
            user_agent,
            connected_at: now,
            last_activity: now,
            subscriptions: Arc::new(RwLock::new(HashSet::new())),
            is_authenticated: false,
            auth_method: None,
            auth_context: None,
            metadata: Arc::new(RwLock::new(sonic_rs::json!({}))),
            validated_write_topics: Arc::new(DashSet::new()),
            validated_read_topics: Arc::new(DashSet::new()),
            permission_cache_expiry: None,
            topic_cursors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Authenticate the connection with a full authorization principal.
    pub fn authenticate_with(
        &mut self,
        user_id: String,
        auth_method: Option<String>,
        auth_context: AuthContext,
    ) {
        self.user_id = Some(user_id);
        self.auth_method = auth_method;
        self.auth_context = Some(auth_context);
        self.update_activity();
    }

    /// Authenticate the connection
    pub fn authenticate(&mut self, user_id: String, auth_method: Option<String>) {
        self.user_id = Some(user_id);
        self.auth_method = auth_method;
        self.update_activity();
    }

    /// Check if connection is authenticated
    pub fn is_authenticated(&self) -> bool {
        self.user_id.is_some()
    }

    /// Update last activity timestamp
    pub fn update_activity(&mut self) {
        self.last_activity = Utc::now();
    }

    /// Subscribe to a topic
    pub async fn subscribe(&self, topic: &str) -> bool {
        self.subscriptions.write().await.insert(topic.to_string())
    }

    /// Unsubscribe from a topic
    pub async fn unsubscribe(&self, topic: &str) -> bool {
        self.subscriptions.write().await.remove(topic)
    }

    /// Check if subscribed to a topic
    pub async fn is_subscribed(&self, topic: &str) -> bool {
        self.subscriptions.read().await.contains(topic)
    }

    /// Get all subscriptions
    pub async fn get_subscriptions(&self) -> HashSet<String> {
        self.subscriptions.read().await.clone()
    }

    /// Set metadata using sonic-rs for better performance
    pub async fn set_metadata(&self, metadata: JsonValue) {
        *self.metadata.write().await = metadata;
    }

    /// Update a specific metadata field using sonic-rs
    pub async fn update_metadata(&self, key: &str, value: JsonValue) {
        let mut metadata = self.metadata.write().await;
        if let Some(obj) = metadata.as_object_mut() {
            obj.insert(key, value);
        } else {
            *metadata = sonic_rs::json!({key: value});
        }
    }

    /// Get connection info for logging
    pub fn info(&self) -> String {
        format!(
            "Connection(id={}, user_id={:?}, ip={:?})",
            self.id, self.user_id, self.ip_address
        )
    }
}

/// Outgoing envelope for per-connection channels.
#[derive(Debug, Clone)]
pub enum OutgoingMessage {
    Message(Arc<Message>),
    Serialized(Arc<String>),
}

/// Pre-allocated connection pool for faster connection creation
#[derive(Debug)]
pub(crate) struct ConnectionPool {
    available_connections: Arc<RwLock<Vec<Connection>>>,
    pool_size: usize,
}

impl ConnectionPool {
    pub(crate) fn new(pool_size: usize) -> Self {
        Self {
            available_connections: Arc::new(RwLock::new(Vec::with_capacity(pool_size))),
            pool_size,
        }
    }

    pub(crate) async fn prewarm(&self) {
        let mut connections = self.available_connections.write().await;
        for _ in 0..self.pool_size {
            let connection = Connection::anonymous(None, None);
            connections.push(connection);
        }
        info!(
            "Pre-warmed connection pool with {} connections",
            self.pool_size
        );
    }

    pub(crate) async fn get_connection(&self) -> Option<Connection> {
        let mut connections = self.available_connections.write().await;
        connections.pop()
    }

    pub(crate) async fn return_connection(&self, mut connection: Connection) {
        // Reset connection state for reuse — EVERY principal field must be
        // cleared, otherwise a pooled connection reused for a new session
        // inherits a stale authenticated auth_context and bypasses the
        // unauthenticated-message gate (audit item 1).
        connection.user_id = None;
        connection.is_authenticated = false;
        connection.auth_method = None;
        connection.auth_context = None;
        connection.connected_at = Utc::now();
        connection.last_activity = Utc::now();

        // Clear subscriptions + validated topic caches
        connection.subscriptions.write().await.clear();
        connection.validated_read_topics.clear();
        connection.validated_write_topics.clear();
        connection.permission_cache_expiry = None;

        let mut connections = self.available_connections.write().await;
        if connections.len() < self.pool_size {
            connections.push(connection);
        }
    }

    pub(crate) async fn get_stats(&self) -> (usize, usize) {
        let connections = self.available_connections.read().await;
        (connections.len(), self.pool_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_new_authenticated() {
        let conn = Connection::new(
            Some("user-1".into()),
            Some("127.0.0.1".into()),
            Some("test-agent".into()),
        );
        assert!(conn.is_authenticated);
        assert_eq!(conn.user_id, Some("user-1".into()));
    }

    #[test]
    fn test_connection_new_unauthenticated() {
        let conn = Connection::new(None, None, None);
        assert!(!conn.is_authenticated);
        assert!(conn.user_id.is_none());
    }

    #[test]
    fn test_connection_anonymous() {
        let conn = Connection::anonymous(None, None);
        assert!(!conn.is_authenticated);
        assert!(conn.user_id.is_none());
    }

    #[test]
    fn test_connection_anonymous_with_ip() {
        let conn = Connection::anonymous(Some("10.0.0.1".into()), Some("curl".into()));
        assert_eq!(conn.ip_address, Some("10.0.0.1".into()));
        assert_eq!(conn.user_agent, Some("curl".into()));
    }

    #[test]
    fn test_connection_authenticate() {
        let mut conn = Connection::new(None, None, None);
        assert!(!conn.is_authenticated());
        conn.authenticate("user-1".into(), Some("jwt".into()));
        assert!(conn.is_authenticated());
        assert_eq!(conn.auth_method, Some("jwt".into()));
    }

    #[test]
    fn test_connection_is_authenticated_with_user() {
        let mut conn = Connection::new(Some("user".into()), None, None);
        conn.authenticate("user".into(), None);
        assert!(conn.is_authenticated());
    }

    #[test]
    fn test_connection_update_activity() {
        let mut conn = Connection::new(None, None, None);
        let before = conn.last_activity;
        std::thread::sleep(std::time::Duration::from_millis(5));
        conn.update_activity();
        assert!(conn.last_activity > before);
    }

    #[tokio::test]
    async fn test_connection_subscribe_unsubscribe() {
        let conn = Connection::new(None, None, None);
        assert!(conn.subscribe("topic-1").await);
        assert!(!conn.subscribe("topic-1").await); // duplicate
        assert!(conn.subscribe("topic-2").await);
        assert!(conn.is_subscribed("topic-1").await);
        assert!(conn.is_subscribed("topic-2").await);
        assert!(!conn.is_subscribed("topic-3").await);
        assert!(conn.unsubscribe("topic-1").await);
        assert!(!conn.is_subscribed("topic-1").await);
        assert_eq!(conn.get_subscriptions().await.len(), 1);
    }

    #[tokio::test]
    async fn test_connection_subscriptions_empty() {
        let conn = Connection::new(None, None, None);
        assert!(conn.get_subscriptions().await.is_empty());
    }

    #[tokio::test]
    async fn test_connection_metadata() {
        let conn = Connection::new(None, None, None);
        let meta = sonic_rs::json!({"key": "value", "num": 42});
        conn.set_metadata(meta).await;
        let stored_json = sonic_rs::to_string(&*conn.metadata.read().await).unwrap();
        assert!(stored_json.contains("\"key\""));
        assert!(stored_json.contains("\"value\""));
    }

    #[tokio::test]
    async fn test_connection_update_metadata() {
        let conn = Connection::new(None, None, None);
        conn.update_metadata("color", sonic_rs::json!("red")).await;
        let stored_json = sonic_rs::to_string(&*conn.metadata.read().await).unwrap();
        assert!(stored_json.contains("\"color\""));
        assert!(stored_json.contains("\"red\""));
    }

    #[test]
    fn test_connection_info() {
        let conn = Connection::new(Some("user-1".into()), Some("1.2.3.4".into()), None);
        let info = conn.info();
        assert!(info.contains("user-1"));
        assert!(info.contains("1.2.3.4"));
    }

    #[test]
    fn test_connection_info_anonymous() {
        let conn = Connection::anonymous(None, None);
        let info = conn.info();
        assert!(info.contains("Connection(id="));
    }

    #[test]
    fn test_connection_id_unique() {
        let conn1 = Connection::new(None, None, None);
        let conn2 = Connection::new(None, None, None);
        assert_ne!(conn1.id, conn2.id);
    }

    #[test]
    fn test_connection_generates_uuid() {
        let conn = Connection::new(None, None, None);
        let uuid_str = conn.id.to_string();
        assert_eq!(uuid_str.len(), 36);
    }

    #[test]
    fn test_connection_memory_stats_clone() {
        let stats = ConnectionMemoryStats {
            connection_count: 100,
            topic_count: 50,
            sender_count: 75,
            failure_count: 3,
            total_subscriptions: 200,
            avg_subscriptions: 2.5,
            max_connections: 500,
            utilization_percent: 20.0,
        };
        let cloned = stats.clone();
        assert_eq!(cloned.connection_count, 100);
    }

    #[test]
    fn test_connection_memory_stats_debug() {
        let stats = ConnectionMemoryStats {
            connection_count: 10,
            topic_count: 5,
            sender_count: 8,
            failure_count: 0,
            total_subscriptions: 15,
            avg_subscriptions: 1.5,
            max_connections: 100,
            utilization_percent: 10.0,
        };
        let _ = format!("{stats:?}");
    }

    #[test]
    fn test_outgoing_message_message_variant() {
        let msg = Arc::new(Message::Connection {
            event: "connected".into(),
            payload: sonic_rs::json!({}),
        });
        let outgoing = OutgoingMessage::Message(msg);
        match &outgoing {
            OutgoingMessage::Message(m) => {
                assert!(matches!(m.as_ref(), &Message::Connection { .. }))
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_outgoing_message_serialized_variant() {
        let s = Arc::new("serialized".into());
        let outgoing = OutgoingMessage::Serialized(s);
        match &outgoing {
            OutgoingMessage::Serialized(s) => assert_eq!(s.as_ref(), "serialized"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_outgoing_message_debug() {
        let msg = OutgoingMessage::Serialized(Arc::new("test".into()));
        let _ = format!("{msg:?}");
    }

    #[tokio::test]
    async fn test_connection_pool_new() {
        let pool = ConnectionPool::new(5);
        assert_eq!(pool.pool_size, 5);
        let (available, _) = pool.get_stats().await;
        assert_eq!(available, 0);
    }

    #[tokio::test]
    async fn test_connection_pool_prewarm() {
        let pool = ConnectionPool::new(3);
        pool.prewarm().await;
        let (available, _) = pool.get_stats().await;
        assert_eq!(available, 3);
    }

    #[tokio::test]
    async fn test_connection_pool_get_and_return() {
        let pool = ConnectionPool::new(2);
        pool.prewarm().await;
        let conn = pool.get_connection().await;
        assert!(conn.is_some());
        let (available, _) = pool.get_stats().await;
        assert_eq!(available, 1);
        pool.return_connection(conn.unwrap()).await;
        let (available, _) = pool.get_stats().await;
        assert_eq!(available, 2);
    }

    #[tokio::test]
    async fn test_connection_pool_return_resets_state() {
        let pool = ConnectionPool::new(1);
        pool.prewarm().await;
        let mut conn = pool.get_connection().await.unwrap();
        conn.user_id = Some("user".into());
        conn.is_authenticated = true;
        pool.return_connection(conn).await;
        let returned = pool.get_connection().await.unwrap();
        assert!(returned.user_id.is_none());
        assert!(!returned.is_authenticated);
    }

    #[tokio::test]
    async fn test_connection_pool_return_clears_principal() {
        // The unauthenticated-message gate depends on auth_context being
        // cleared when a connection returns to the pool (audit item 1): a
        // stale authenticated principal must never leak into the next
        // session reusing the pooled connection.
        let pool = ConnectionPool::new(1);
        pool.prewarm().await;
        let mut conn = pool.get_connection().await.unwrap();
        conn.authenticate_with(
            "user".into(),
            Some("api_key".into()),
            crate::auth::types::AuthContext::ApiKey {
                key_id: "k1".into(),
                key_source: "local".into(),
                expires_at: None,
                permissions: crate::auth::authorization::ApiKeyPermissions::full(),
            },
        );
        assert!(conn.auth_context.is_some());
        pool.return_connection(conn).await;
        let returned = pool.get_connection().await.unwrap();
        assert!(returned.auth_context.is_none());
        assert!(returned.auth_method.is_none());
        assert!(!returned.is_authenticated());
    }

    #[tokio::test]
    async fn test_connection_pool_return_when_full() {
        let pool = ConnectionPool::new(1);
        pool.prewarm().await;
        pool.get_connection().await.unwrap();
        pool.return_connection(Connection::new(None, None, None))
            .await;
        let (available, _) = pool.get_stats().await;
        assert_eq!(available, 1);
    }

    #[tokio::test]
    async fn test_connection_pool_empty_get() {
        let pool = ConnectionPool::new(0);
        let conn = pool.get_connection().await;
        assert!(conn.is_none());
    }
}
