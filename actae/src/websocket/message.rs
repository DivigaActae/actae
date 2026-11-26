// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde::Serialize;
use sonic_rs::{from_str, to_string, JsonContainerTrait, JsonValueMutTrait, JsonValueTrait, Value};
use std::sync::Arc;
use uuid::Uuid;

// Message pool for object reuse to reduce allocations
static MESSAGE_POOL: Lazy<Arc<DashMap<String, Vec<Message>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Pre-allocated JSON template cache for common message patterns
static JSON_TEMPLATE_CACHE: Lazy<Arc<DashMap<String, Bytes>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Helper function to create sonic-rs errors
fn make_error(msg: &str) -> sonic_rs::Error {
    // Create a proper error by attempting to parse invalid JSON
    match sonic_rs::from_str::<sonic_rs::Value>("invalid_json") {
        Ok(_) => {
            // Fallback - should not happen, but create a default error
            // Use a simple approach since ErrorKind is not available in sonic_rs
            sonic_rs::from_str::<sonic_rs::Value>(&format!("{{\"error\": \"{msg}\"}}")).unwrap_err()
        }
        Err(e) => e,
    }
}

/// A single channel subscription within a `subscribe_many` request.
#[derive(Debug, Clone, Serialize)]
pub struct SubscribeTopic {
    pub topic: String,
    pub cursor: Option<i64>,
}

/// Core message types for WebSocket communication
/// Compatible with the Go Actae message format
#[derive(Debug, Clone, Serialize)]
pub enum Message {
    /// Client subscribes to a channel, optionally with a cursor for resume
    Subscribe {
        topic: String,
        cursor: Option<i64>,
        timestamp: Option<i64>,
    },

    /// Versioned multi-channel subscription (additive, actae.api.v1): a list
    /// of `{topic, cursor}` entries, equivalent to issuing one subscribe per
    /// entry. `request_id` is echoed in the summary frame for correlation.
    /// No second WS endpoint is created (D-001).
    SubscribeMany {
        topics: Vec<SubscribeTopic>,
        request_id: Option<String>,
    },

    /// Client unsubscribes from a channel
    Unsubscribe {
        topic: String,
        timestamp: Option<i64>,
    },

    /// Client broadcasts a message
    Broadcast {
        topic: String,
        payload: sonic_rs::Value,
        timestamp: Option<i64>,
        /// Optional client-supplied idempotency key: retrying a publish
        /// with the same (topic, operation_id) replays the original event
        /// instead of duplicating (mirrors the HTTP record path).
        operation_id: Option<Uuid>,
        /// Optional client-supplied correlation id. Echoed verbatim in the
        /// Ack so concurrent publishes can match their own response instead
        /// of consuming the next FIFO acknowledgement (audit items 7, 8).
        request_id: Option<String>,
    },

    /// Direct message to specific connection
    DirectMessage {
        topic: String,
        payload: sonic_rs::Value,
        timestamp: Option<i64>,
    },

    /// Database change event
    DatabaseChange {
        topic: String,
        event: String,
        payload: sonic_rs::Value,
        timestamp: Option<i64>,
    },

    /// Server response messages
    Subscription {
        topic: String,
        event: String,
        payload: sonic_rs::Value,
    },

    /// Connection management
    Connection {
        event: String,
        payload: sonic_rs::Value,
    },

    /// Authentication request
    Auth {
        token: Option<String>,
        api_key: Option<String>,
        payload: sonic_rs::Value,
    },

    /// Server informs client of the latest cursor for a topic
    CursorSync { topic: String, cursor: i64 },

    /// Error message
    Error {
        message: String,
        code: Option<String>,
        details: Option<sonic_rs::Value>,
    },

    /// Acknowledgement with full event data for exactly-once delivery semantics.
    /// Carries the persisted event so clients get cursor/timestamp/channel_cursor
    /// without a separate HTTP replay round-trip.
    Ack {
        id: Uuid,
        cursor: Option<i64>,
        channel_cursor: Option<i64>,
        timestamp: Option<String>,
        payload: Option<sonic_rs::Value>,
        channel_id: Option<String>,
        event_type: Option<String>,
        actor: Option<String>,
        /// Echo of the originating publish's `request_id` (None for legacy
        /// clients that do not send one).
        request_id: Option<String>,
    },

    /// Negative acknowledgement – message was not processed
    Nack { id: Uuid, reason: Option<String> },
}

/// Internal broadcast message for the server
#[derive(Debug, Clone)]
pub struct BroadcastMessage {
    /// Unique message identifier for idempotency / ACK handling
    pub id: uuid::Uuid,
    pub topic: String,
    pub message: Message,
    pub sender_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

/// Message validation results
#[derive(Debug, PartialEq)]
pub enum MessageValidation {
    Valid,
    InvalidFormat(String),
    MissingRequired(String),
    InvalidTopic(String),
    PayloadTooLarge(usize),
    InvalidCharacters(String),
    SecurityViolation(String),
    RateLimitExceeded(String),
}

impl Message {
    /// Create a subscription confirmation message
    pub fn subscription_confirmed(topic: &str) -> Self {
        Message::Subscription {
            topic: topic.to_string(),
            event: "subscribed".to_string(),
            payload: sonic_rs::json!({"success": true}),
        }
    }

    /// Create a subscription error message
    pub fn subscription_error(topic: &str, error: &str) -> Self {
        Message::Subscription {
            topic: topic.to_string(),
            event: "error".to_string(),
            payload: sonic_rs::json!({"error": error}),
        }
    }

    /// Create an error message
    pub fn error(message: String) -> Self {
        Message::Error {
            message,
            code: None,
            details: None,
        }
    }

    /// Create an error message with code
    pub fn error_with_code(message: &str, code: Option<&str>) -> Self {
        Message::Error {
            message: message.to_string(),
            code: code.map(|c| c.to_string()),
            details: None,
        }
    }

    /// Create a connection event message
    pub fn connection_event(event: &str, payload: sonic_rs::Value) -> Self {
        Message::Connection {
            event: event.to_string(),
            payload,
        }
    }

    /// Create an auth success message
    pub fn auth_success() -> Self {
        Message::Connection {
            event: "authenticated".to_string(),
            payload: sonic_rs::json!({"status": "success"}),
        }
    }

    /// Create an empty message for object pooling
    pub fn create_empty() -> Self {
        Message::Connection {
            event: String::new(),
            payload: sonic_rs::Value::default(),
        }
    }

    /// Reset message content for reuse in object pool
    pub fn reset(&mut self) {
        match self {
            Message::Subscribe {
                topic,
                cursor,
                timestamp,
            } => {
                topic.clear();
                *cursor = None;
                *timestamp = None;
            }
            Message::SubscribeMany { topics, request_id } => {
                topics.clear();
                *request_id = None;
            }
            Message::Unsubscribe { topic, timestamp } => {
                topic.clear();
                *timestamp = None;
            }
            Message::Broadcast {
                topic,
                payload,
                timestamp,
                operation_id: _,
                request_id: _,
            } => {
                topic.clear();
                *payload = sonic_rs::Value::default();
                *timestamp = None;
            }
            Message::DirectMessage {
                topic,
                payload,
                timestamp,
            } => {
                topic.clear();
                *payload = sonic_rs::Value::default();
                *timestamp = None;
            }
            Message::DatabaseChange {
                topic,
                event,
                payload,
                timestamp,
            } => {
                topic.clear();
                event.clear();
                *payload = sonic_rs::Value::default();
                *timestamp = None;
            }
            Message::Subscription {
                topic,
                event,
                payload,
            } => {
                topic.clear();
                event.clear();
                *payload = sonic_rs::Value::default();
            }
            Message::Connection { event, payload } => {
                event.clear();
                *payload = sonic_rs::Value::default();
            }
            Message::Auth {
                token,
                api_key,
                payload,
            } => {
                *token = None;
                *api_key = None;
                *payload = sonic_rs::Value::default();
            }
            Message::CursorSync { topic, cursor } => {
                topic.clear();
                *cursor = 0;
            }
            Message::Error {
                message,
                code,
                details,
            } => {
                message.clear();
                *code = None;
                *details = None;
            }
            Message::Ack {
                id,
                cursor,
                channel_cursor,
                timestamp,
                payload,
                channel_id,
                event_type,
                actor,
                request_id: _,
            } => {
                *id = uuid::Uuid::nil();
                *cursor = None;
                *channel_cursor = None;
                *timestamp = None;
                *payload = None;
                *channel_id = None;
                *event_type = None;
                *actor = None;
            }
            Message::Nack { id, reason } => {
                *id = uuid::Uuid::nil();
                *reason = None;
            }
        }
    }

    /// Create a subscribe success message
    pub fn subscribe_success(topic: String) -> Self {
        Message::Subscription {
            topic,
            event: "subscribed".to_string(),
            payload: sonic_rs::json!({"status": "success"}),
        }
    }

    /// Create an unsubscribe success message
    pub fn unsubscribe_success(topic: String) -> Self {
        Message::Subscription {
            topic,
            event: "unsubscribed".to_string(),
            payload: sonic_rs::json!({"status": "success"}),
        }
    }

    /// Create a ping message
    pub fn ping() -> Self {
        Message::Connection {
            event: "ping".to_string(),
            payload: sonic_rs::json!({}),
        }
    }

    /// Create a pong message
    pub fn pong() -> Self {
        Message::Connection {
            event: "pong".to_string(),
            payload: sonic_rs::json!({}),
        }
    }

    /// Helper method to create a direct message
    pub fn direct_message(topic: String, payload: sonic_rs::Value) -> Self {
        Message::DirectMessage {
            topic,
            payload,
            timestamp: Some(chrono::Utc::now().timestamp()),
        }
    }

    /// Helper method to create a broadcast message
    pub fn broadcast_message(topic: String, payload: sonic_rs::Value) -> Self {
        Message::Broadcast {
            topic,
            payload,
            timestamp: Some(chrono::Utc::now().timestamp()),
            operation_id: None,
            request_id: None,
        }
    }

    /// Get the message type as a string
    pub fn message_type(&self) -> &str {
        match self {
            Message::Subscribe { .. } => "subscribe",
            Message::SubscribeMany { .. } => "subscribe_many",
            Message::Unsubscribe { .. } => "unsubscribe",
            Message::Broadcast { .. } => "broadcast",
            Message::DirectMessage { .. } => "message",
            Message::DatabaseChange { .. } => "database_change",
            Message::Subscription { .. } => "subscription",
            Message::Connection { .. } => "connection",
            Message::Auth { .. } => "auth",
            Message::CursorSync { .. } => "cursor_sync",
            Message::Error { .. } => "error",
            Message::Ack { .. } => "ack",
            Message::Nack { .. } => "nack",
        }
    }

    /// Get the topic from the message if it has one
    pub fn topic(&self) -> Option<&str> {
        match self {
            Message::Subscribe { topic, .. }
            | Message::Unsubscribe { topic, .. }
            | Message::Broadcast { topic, .. }
            | Message::DirectMessage { topic, .. }
            | Message::DatabaseChange { topic, .. }
            | Message::Subscription { topic, .. }
            | Message::CursorSync { topic, .. } => Some(topic),
            _ => None,
        }
    }
    pub fn payload(&self) -> Option<&sonic_rs::Value> {
        match self {
            Message::Broadcast { payload, .. }
            | Message::DirectMessage { payload, .. }
            | Message::DatabaseChange { payload, .. }
            | Message::Subscription { payload, .. }
            | Message::Connection { payload, .. }
            | Message::Auth { payload, .. } => Some(payload),
            Message::Error { details, .. } => details.as_ref(),
            _ => None,
        }
    }

    /// Maximum number of topics in a single `subscribe_many` message — bounds
    /// the fan-out of one frame (section 8: bounded frames/concurrency).
    pub const MAX_SUBSCRIBE_MANY_TOPICS: usize = 100;

    /// Validate message structure and content with comprehensive security checks
    pub fn validate(&self, max_payload_size: usize) -> MessageValidation {
        // Check topic validity for topic-based messages
        if let Some(topic) = self.topic() {
            if topic.is_empty() {
                return MessageValidation::InvalidTopic("Topic cannot be empty".to_string());
            }
            if topic.len() > 256 {
                return MessageValidation::InvalidTopic(
                    "Topic too long (max 256 characters)".to_string(),
                );
            }

            // Enhanced topic validation
            if let Some(validation_error) = Self::validate_topic_security(topic) {
                return validation_error;
            }
        }

        // Check payload size and security
        if let Some(payload) = self.payload() {
            if let Ok(json_str) = to_string(payload) {
                if json_str.len() > max_payload_size {
                    return MessageValidation::PayloadTooLarge(json_str.len());
                }

                // Enhanced payload validation
                if let Some(validation_error) = Self::validate_payload_security(payload) {
                    return validation_error;
                }
            }
        }

        // Message type specific validation
        match self {
            Message::Subscribe { topic, .. } | Message::Unsubscribe { topic, .. } => {
                if let Some(validation_error) = Self::validate_subscription_topic(topic) {
                    return validation_error;
                }
            }
            Message::SubscribeMany { topics, .. } => {
                if topics.is_empty() {
                    return MessageValidation::InvalidFormat(
                        "subscribe_many requires at least one topic".to_string(),
                    );
                }
                if topics.len() > Self::MAX_SUBSCRIBE_MANY_TOPICS {
                    return MessageValidation::InvalidFormat(format!(
                        "subscribe_many exceeds the {} topic limit",
                        Self::MAX_SUBSCRIBE_MANY_TOPICS
                    ));
                }
                for t in topics {
                    if t.topic.is_empty() {
                        return MessageValidation::InvalidTopic(
                            "Topic cannot be empty".to_string(),
                        );
                    }
                    if t.topic.len() > 256 {
                        return MessageValidation::InvalidTopic(
                            "Topic too long (max 256 characters)".to_string(),
                        );
                    }
                    if let Some(validation_error) = Self::validate_topic_security(&t.topic) {
                        return validation_error;
                    }
                    if let Some(validation_error) = Self::validate_subscription_topic(&t.topic) {
                        return validation_error;
                    }
                }
            }
            Message::CursorSync { .. } => {}
            Message::Broadcast { topic, payload, .. }
            | Message::DirectMessage { topic, payload, .. } => {
                if let Some(validation_error) = Self::validate_broadcast_security(topic, payload) {
                    return validation_error;
                }
            }
            Message::DatabaseChange {
                topic,
                event,
                payload,
                ..
            } => {
                if let Some(validation_error) =
                    Self::validate_database_change_security(topic, event, payload)
                {
                    return validation_error;
                }
            }
            _ => {} // Other message types are considered safe
        }

        MessageValidation::Valid
    }

    /// Validate topic name for security issues.
    /// Topics are channel ids and must satisfy the shared channel grammar
    /// (audit item 10): 1..=256 bytes of `[A-Za-z0-9._:-]` — no `/`, no
    /// whitespace — so a channel created here is always addressable over HTTP
    /// and vice versa.
    fn validate_topic_security(topic: &str) -> Option<MessageValidation> {
        // Check for path traversal attempts
        if topic.contains("..") || topic.contains("//") {
            return Some(MessageValidation::SecurityViolation(
                "Topic contains path traversal patterns".to_string(),
            ));
        }

        // Check for invalid characters
        if !topic
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._:-".contains(c))
        {
            return Some(MessageValidation::InvalidCharacters(
                "Topic contains invalid characters (only [A-Za-z0-9._:-] allowed — no slashes or whitespace)"
                    .to_string(),
            ));
        }

        // Check for SQL injection patterns
        let lower_topic = topic.to_lowercase();
        let sql_keywords = [
            "select",
            "insert",
            "update",
            "delete",
            "drop",
            "union",
            "script",
            "javascript",
        ];
        for keyword in &sql_keywords {
            if lower_topic.contains(keyword) {
                return Some(MessageValidation::SecurityViolation(format!(
                    "Topic contains potentially dangerous keyword: {keyword}"
                )));
            }
        }

        None
    }

    /// Validate payload for security issues
    fn validate_payload_security(payload: &Value) -> Option<MessageValidation> {
        // Check for dangerous object keys
        if let Some(obj) = payload.as_object() {
            for (key, _) in obj {
                if key.starts_with("__") || key == "constructor" || key == "prototype" {
                    return Some(MessageValidation::SecurityViolation(format!(
                        "Payload contains dangerous key: {key}"
                    )));
                }
            }
        }

        // Check payload content recursively
        if let Some(validation_error) = Self::validate_payload_content_recursive(payload, 0) {
            return Some(validation_error);
        }

        None
    }

    /// Recursively validate payload content
    fn validate_payload_content_recursive(
        value: &Value,
        depth: usize,
    ) -> Option<MessageValidation> {
        // Prevent excessive nesting
        if depth > 20 {
            return Some(MessageValidation::SecurityViolation(
                "Payload has excessive nesting (max 20 levels)".to_string(),
            ));
        }

        // Check if value is a string
        if let Some(s) = value.as_str() {
            // Check for XSS patterns
            let lower_str = s.to_lowercase();
            let xss_patterns = ["<script", "javascript:", "onclick", "onerror", "onload"];
            for pattern in &xss_patterns {
                if lower_str.contains(pattern) {
                    return Some(MessageValidation::SecurityViolation(format!(
                        "Payload contains XSS pattern: {pattern}"
                    )));
                }
            }

            // Check for SQL injection patterns
            let sql_patterns = ["'; drop", "union select", "1=1", "or '1'='1"];
            for pattern in &sql_patterns {
                if lower_str.contains(pattern) {
                    return Some(MessageValidation::SecurityViolation(format!(
                        "Payload contains SQL injection pattern: {pattern}"
                    )));
                }
            }

            // Check string length
            if s.len() > 100000 {
                return Some(MessageValidation::PayloadTooLarge(s.len()));
            }
        } else if let Some(obj) = value.as_object() {
            for (key, val) in obj {
                // Validate key
                if key.len() > 1000 {
                    return Some(MessageValidation::InvalidCharacters(
                        "Object key too long (max 1000 characters)".to_string(),
                    ));
                }

                // Recursively validate value
                if let Some(error) = Self::validate_payload_content_recursive(val, depth + 1) {
                    return Some(error);
                }
            }
        } else if let Some(arr) = value.as_array() {
            // Check array size
            if arr.len() > 10000 {
                return Some(MessageValidation::PayloadTooLarge(arr.len()));
            }

            for val in arr {
                if let Some(error) = Self::validate_payload_content_recursive(val, depth + 1) {
                    return Some(error);
                }
            }
        }
        // Numbers, booleans, null are safe - no additional validation needed

        None
    }

    /// Validate subscription topic
    fn validate_subscription_topic(topic: &str) -> Option<MessageValidation> {
        // Check for wildcard abuse
        if topic.matches('*').count() > 3 {
            return Some(MessageValidation::InvalidTopic(
                "Too many wildcards in subscription topic (max 3)".to_string(),
            ));
        }

        // Check for system topics
        if topic.starts_with("system.") || topic.starts_with("admin.") {
            return Some(MessageValidation::SecurityViolation(
                "Cannot subscribe to system or admin topics".to_string(),
            ));
        }

        None
    }

    /// Validate broadcast message security
    fn validate_broadcast_security(topic: &str, payload: &Value) -> Option<MessageValidation> {
        // Check for broadcast to system topics
        if topic.starts_with("system.") || topic.starts_with("admin.") {
            return Some(MessageValidation::SecurityViolation(
                "Cannot broadcast to system or admin topics".to_string(),
            ));
        }

        // Check for command injection in payload
        if let Some(obj) = payload.as_object() {
            if obj.contains_key(&"cmd") || obj.contains_key(&"command") || obj.contains_key(&"exec")
            {
                return Some(MessageValidation::SecurityViolation(
                    "Payload contains command execution fields".to_string(),
                ));
            }
        }

        None
    }

    /// Validate database change message security
    fn validate_database_change_security(
        topic: &str,
        event: &str,
        _payload: &Value,
    ) -> Option<MessageValidation> {
        // Only allow specific database event types
        if !["INSERT", "UPDATE", "DELETE"].contains(&event.to_uppercase().as_str()) {
            return Some(MessageValidation::InvalidFormat(
                "Invalid database event type (only INSERT, UPDATE, DELETE allowed)".to_string(),
            ));
        }

        // Validate table name in topic
        if topic.contains("information_schema") || topic.contains("pg_") || topic.contains("mysql")
        {
            return Some(MessageValidation::SecurityViolation(
                "Cannot access system database tables".to_string(),
            ));
        }

        None
    }

    /// Serialize message to JSON bytes using zero-copy optimization
    pub fn to_json_bytes(&self) -> Result<Bytes, sonic_rs::Error> {
        // Check cache first for common patterns
        let cache_key = self.get_cache_key();
        if let Some(cached) = JSON_TEMPLATE_CACHE.get(&cache_key) {
            return Ok(cached.clone());
        }

        // Use a pre-allocated buffer for zero-copy serialization
        let mut buf = BytesMut::with_capacity(1024); // Most messages are < 1KB

        match self {
            // Fast paths for common message types
            Message::Subscribe {
                topic,
                cursor,
                timestamp,
            } => {
                buf.extend_from_slice(b"{\"type\":\"subscribe\",\"topic\":\"");
                buf.extend_from_slice(topic.as_bytes());
                buf.extend_from_slice(b"\"");
                if let Some(c) = cursor {
                    buf.extend_from_slice(b",\"cursor\":");
                    buf.extend_from_slice(c.to_string().as_bytes());
                }
                if let Some(ts) = timestamp {
                    buf.extend_from_slice(b",\"timestamp\":");
                    buf.extend_from_slice(ts.to_string().as_bytes());
                }
                buf.extend_from_slice(b"}");
            }
            Message::Unsubscribe { topic, timestamp } => {
                buf.extend_from_slice(b"{\"type\":\"unsubscribe\",\"topic\":\"");
                buf.extend_from_slice(topic.as_bytes());
                buf.extend_from_slice(b"\"");
                if let Some(ts) = timestamp {
                    buf.extend_from_slice(b",\"timestamp\":");
                    buf.extend_from_slice(ts.to_string().as_bytes());
                }
                buf.extend_from_slice(b"}");
            }
            Message::Connection { event, payload }
                if payload.is_null()
                    || (payload.is_object()
                        && payload.as_object().is_some_and(|o| o.is_empty())) =>
            {
                // Fast path for empty payload connection messages
                buf.extend_from_slice(b"{\"type\":\"connection\",\"event\":\"");
                buf.extend_from_slice(event.as_bytes());
                buf.extend_from_slice(b"\",\"payload\":{}}");
            }
            // Fall back to standard serialization for complex messages
            _ => {
                let json_str = self.to_json_string_fallback()?;
                buf.extend_from_slice(json_str.as_bytes());
            }
        }

        let result = buf.freeze();

        // Cache common patterns (but avoid caching user-specific data)
        if self.is_cacheable() {
            JSON_TEMPLATE_CACHE.insert(cache_key, result.clone());
        }

        Ok(result)
    }

    /// Get cache key for this message type
    fn get_cache_key(&self) -> String {
        match self {
            Message::Subscribe {
                topic,
                cursor,
                timestamp,
            } => {
                format!("sub_{}_{}_{}", topic, cursor.is_some(), timestamp.is_some())
            }
            Message::Unsubscribe { topic, timestamp } => {
                format!("unsub_{}_{}", topic, timestamp.is_some())
            }
            Message::Connection { event, payload } => {
                format!(
                    "conn_{}_{}",
                    event,
                    payload.is_null()
                        || (payload.is_object()
                            && payload.as_object().is_some_and(|o| o.is_empty()))
                )
            }
            _ => format!("{}_generic", self.message_type()),
        }
    }

    /// Check if this message can be cached
    fn is_cacheable(&self) -> bool {
        match self {
            Message::Subscribe { .. } | Message::Unsubscribe { .. } => true,
            Message::Connection { payload, .. } => {
                payload.is_null()
                    || (payload.is_object() && payload.as_object().is_some_and(|o| o.is_empty()))
            }
            _ => false,
        }
    }

    /// Standard JSON serialization as fallback
    fn to_json_string_fallback(&self) -> Result<String, sonic_rs::Error> {
        // Create a tagged object with type field
        let mut obj = sonic_rs::object! {};

        match self {
            Message::Subscribe {
                topic,
                cursor,
                timestamp,
            } => {
                obj.insert("type", "subscribe");
                obj.insert("topic", topic);
                if let Some(c) = cursor {
                    obj.insert("cursor", *c);
                }
                if let Some(ts) = timestamp {
                    obj.insert("timestamp", *ts);
                }
            }
            Message::SubscribeMany { topics, request_id } => {
                obj.insert("type", "subscribe_many");
                let mut topics_val: Vec<sonic_rs::Value> = Vec::with_capacity(topics.len());
                for t in topics {
                    let mut entry = sonic_rs::json!({"topic": &t.topic});
                    if let Some(c) = t.cursor {
                        if let Some(entry_obj) = entry.as_object_mut() {
                            entry_obj.insert("cursor", c);
                        }
                    }
                    topics_val.push(entry);
                }
                obj.insert("topics", topics_val);
                if let Some(rid) = request_id {
                    obj.insert("request_id", Value::from(rid.as_str()));
                }
            }
            Message::Unsubscribe { topic, timestamp } => {
                obj.insert("type", "unsubscribe");
                obj.insert("topic", topic);
                if let Some(ts) = timestamp {
                    obj.insert("timestamp", *ts);
                }
            }
            Message::Broadcast {
                topic,
                payload,
                timestamp,
                operation_id,
                request_id,
            } => {
                obj.insert("type", "broadcast");
                obj.insert("topic", topic);
                obj.insert("payload", payload.clone());
                if let Some(ts) = timestamp {
                    obj.insert("timestamp", *ts);
                }
                if let Some(op) = operation_id {
                    obj.insert("operation_id", sonic_rs::json!(op.to_string()));
                }
                if let Some(rid) = request_id {
                    obj.insert("request_id", sonic_rs::json!(rid));
                }
            }
            Message::DirectMessage {
                topic,
                payload,
                timestamp,
            } => {
                obj.insert("type", "message");
                obj.insert("topic", topic);
                obj.insert("payload", payload.clone());
                if let Some(ts) = timestamp {
                    obj.insert("timestamp", *ts);
                }
            }
            Message::DatabaseChange {
                topic,
                event,
                payload,
                timestamp,
            } => {
                obj.insert("type", "database_change");
                obj.insert("topic", topic);
                obj.insert("event", event);
                obj.insert("payload", payload.clone());
                if let Some(ts) = timestamp {
                    obj.insert("timestamp", *ts);
                }
            }
            Message::Subscription {
                topic,
                event,
                payload,
            } => {
                obj.insert("type", "subscription");
                obj.insert("topic", topic);
                obj.insert("event", event);
                obj.insert("payload", payload.clone());
            }
            Message::Connection { event, payload } => {
                obj.insert("type", "connection");
                obj.insert("event", event);
                obj.insert("payload", payload.clone());
            }
            Message::Auth {
                token,
                api_key,
                payload,
            } => {
                obj.insert("type", "auth");
                if let Some(t) = token {
                    obj.insert("token", t);
                } else {
                    obj.insert("token", sonic_rs::Value::default());
                }
                if let Some(key) = api_key {
                    obj.insert("api_key", key);
                } else {
                    obj.insert("api_key", sonic_rs::Value::default());
                }
                obj.insert("payload", payload.clone());
            }
            Message::CursorSync { topic, cursor } => {
                obj.insert("type", "cursor_sync");
                obj.insert("topic", topic);
                obj.insert("cursor", *cursor);
            }
            Message::Error {
                message,
                code,
                details,
            } => {
                obj.insert("type", "error");
                obj.insert("message", message);
                if let Some(c) = code {
                    obj.insert("code", c);
                }
                if let Some(d) = details {
                    obj.insert("details", d.clone());
                }
            }
            Message::Ack {
                id,
                cursor,
                channel_cursor,
                timestamp,
                payload,
                channel_id,
                event_type,
                actor,
                request_id,
            } => {
                obj.insert("type", "ack");
                obj.insert("id", Value::from(id.to_string().as_str()));
                if let Some(c) = cursor {
                    obj.insert("cursor", *c);
                }
                if let Some(cc) = channel_cursor {
                    obj.insert("channel_cursor", *cc);
                }
                if let Some(ref ts) = timestamp {
                    obj.insert("timestamp", Value::from(ts.as_str()));
                }
                if let Some(ref p) = payload {
                    obj.insert("payload", p.clone());
                }
                if let Some(ref ch) = channel_id {
                    obj.insert("channel_id", Value::from(ch.as_str()));
                }
                if let Some(ref et) = event_type {
                    obj.insert("event_type", Value::from(et.as_str()));
                }
                if let Some(ref a) = actor {
                    obj.insert("actor", Value::from(a.as_str()));
                }
                if let Some(rid) = request_id {
                    obj.insert("request_id", Value::from(rid.as_str()));
                }
            }
            Message::Nack { id, reason } => {
                obj.insert("type", "nack");
                obj.insert("id", Value::from(id.to_string().as_str()));
                if let Some(r) = reason {
                    obj.insert("reason", Value::from(r.as_str()));
                }
            }
        }

        to_string(&obj)
    }

    /// Serialize message to JSON string using sonic-rs (backward compatibility)  
    pub fn to_json(&self) -> Result<String, sonic_rs::Error> {
        // Use zero-copy serialization and convert to string
        let bytes = self.to_json_bytes()?;
        Ok(String::from_utf8_lossy(&bytes).to_string())
    }

    /// Get a message from the object pool or create a new one
    pub fn from_pool(message_type: &str) -> Self {
        if let Some(mut pool) = MESSAGE_POOL.get_mut(message_type) {
            if let Some(message) = pool.pop() {
                return message;
            }
        }

        // Create new message if pool is empty
        match message_type {
            "subscribe" => Message::Subscribe {
                topic: String::new(),
                cursor: None,
                timestamp: None,
            },
            "unsubscribe" => Message::Unsubscribe {
                topic: String::new(),
                timestamp: None,
            },
            "connection" => Message::Connection {
                event: String::new(),
                payload: sonic_rs::Value::default(),
            },
            "broadcast" => Message::Broadcast {
                topic: String::new(),
                payload: sonic_rs::Value::default(),
                timestamp: None,
                operation_id: None,
                request_id: None,
            },
            "message" => Message::DirectMessage {
                topic: String::new(),
                payload: sonic_rs::Value::default(),
                timestamp: None,
            },
            _ => Message::create_empty(),
        }
    }

    /// Return a message to the object pool for reuse
    pub fn return_to_pool(mut self) {
        let message_type = self.message_type().to_string();

        // Reset the message content
        self.reset();

        // Add to pool if not full
        let mut pool = MESSAGE_POOL.entry(message_type).or_default();
        if pool.len() < 100 {
            // Limit pool size to prevent memory bloat
            pool.push(self);
        }
    }

    /// Deserialize message from JSON string using sonic-rs with null payload handling
    pub fn from_json(json: &str) -> Result<Self, sonic_rs::Error> {
        // Guard against deeply nested payloads: `from_str` into a Value
        // recurses unboundedly and overflows the stack (~40 levels), which
        // would let a hostile WS client crash the server with a deep publish
        // payload.
        if crate::database::json_compat::json_nesting_depth(json)
            > crate::database::json_compat::MAX_JSON_DEPTH
        {
            return Err(make_error("JSON message exceeds nesting depth limit"));
        }
        // Parse as generic Value first
        let mut value: Value = from_str(json)?;

        // Get the object representation
        let obj = value
            .as_object_mut()
            .ok_or_else(|| make_error("Expected JSON object"))?;

        // Get the message type first
        let msg_type = obj
            .get(&"type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| make_error("Missing or invalid 'type' field"))?
            .to_string(); // Convert to owned String to avoid borrowing issues

        // Handle null or missing payload for auth messages
        if msg_type == "auth" {
            match obj.get(&"payload") {
                Some(payload) if payload.is_null() => {
                    obj.insert("payload", sonic_rs::json!({}));
                }
                None => {
                    obj.insert("payload", sonic_rs::json!({}));
                }
                _ => {}
            }
        }

        // Reconstruct the message based on type
        match msg_type.as_str() {
            "subscribe" => {
                let topic = obj
                    .get(&"topic")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing topic for subscribe"))?
                    .to_string();
                let cursor = obj.get(&"cursor").and_then(|v| v.as_i64());
                let timestamp = obj.get(&"timestamp").and_then(|v| v.as_i64());
                Ok(Message::Subscribe {
                    topic,
                    cursor,
                    timestamp,
                })
            }
            "unsubscribe" => {
                let topic = obj
                    .get(&"topic")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing topic for unsubscribe"))?
                    .to_string();
                let timestamp = obj.get(&"timestamp").and_then(|v| v.as_i64());
                Ok(Message::Unsubscribe { topic, timestamp })
            }
            "subscribe_many" => {
                let topics = obj
                    .get(&"topics")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| make_error("Missing topics for subscribe_many"))?
                    .iter()
                    .map(|t| {
                        let topic = t
                            .get("topic")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| make_error("Missing topic in subscribe_many entry"))?
                            .to_string();
                        let cursor = t.get("cursor").and_then(|v| v.as_i64());
                        Ok(SubscribeTopic { topic, cursor })
                    })
                    .collect::<Result<Vec<SubscribeTopic>, sonic_rs::Error>>()?;
                let request_id = obj
                    .get(&"request_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                Ok(Message::SubscribeMany { topics, request_id })
            }
            "broadcast" => {
                let topic = obj
                    .get(&"topic")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing topic for broadcast"))?
                    .to_string();
                let payload = obj
                    .get(&"payload")
                    .ok_or_else(|| make_error("Missing payload for broadcast"))?
                    .clone();
                let timestamp = obj.get(&"timestamp").and_then(|v| v.as_i64());
                let operation_id = obj
                    .get(&"operation_id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok());
                let request_id = obj
                    .get(&"request_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                Ok(Message::Broadcast {
                    topic,
                    payload,
                    timestamp,
                    operation_id,
                    request_id,
                })
            }
            "message" => {
                let topic = obj
                    .get(&"topic")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing topic for message"))?
                    .to_string();
                let payload = obj
                    .get(&"payload")
                    .ok_or_else(|| make_error("Missing payload for message"))?
                    .clone();
                let timestamp = obj.get(&"timestamp").and_then(|v| v.as_i64());
                Ok(Message::DirectMessage {
                    topic,
                    payload,
                    timestamp,
                })
            }
            "database_change" => {
                let topic = obj
                    .get(&"topic")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing topic for database_change"))?
                    .to_string();
                let event = obj
                    .get(&"event")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing event for database_change"))?
                    .to_string();
                let payload = obj
                    .get(&"payload")
                    .ok_or_else(|| make_error("Missing payload for database_change"))?
                    .clone();
                let timestamp = obj.get(&"timestamp").and_then(|v| v.as_i64());
                Ok(Message::DatabaseChange {
                    topic,
                    event,
                    payload,
                    timestamp,
                })
            }
            "subscription" => {
                let topic = obj
                    .get(&"topic")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing topic for subscription"))?
                    .to_string();
                let event = obj
                    .get(&"event")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing event for subscription"))?
                    .to_string();
                let payload = obj
                    .get(&"payload")
                    .ok_or_else(|| make_error("Missing payload for subscription"))?
                    .clone();
                Ok(Message::Subscription {
                    topic,
                    event,
                    payload,
                })
            }
            "connection" => {
                let event = obj
                    .get(&"event")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing event for connection"))?
                    .to_string();
                let payload = obj
                    .get(&"payload")
                    .ok_or_else(|| make_error("Missing payload for connection"))?
                    .clone();
                Ok(Message::Connection { event, payload })
            }
            "ping" => {
                // Keepalive shorthand (`{"type":"ping"}`) — equivalent to
                // the canonical `{"type":"connection","event":"ping"}`; the
                // handler replies with a pong. Accepted so the dashboard
                // heartbeat and any raw WS client work without error spam.
                Ok(Message::Connection {
                    event: "ping".to_string(),
                    payload: obj
                        .get(&"payload")
                        .cloned()
                        .unwrap_or_else(sonic_rs::Value::default),
                })
            }
            "auth" => {
                let token = obj.get(&"token").and_then(|v| {
                    if v.is_null() {
                        None
                    } else {
                        v.as_str().map(|s| s.to_string())
                    }
                });
                let api_key = obj.get(&"api_key").and_then(|v| {
                    if v.is_null() {
                        None
                    } else {
                        v.as_str().map(|s| s.to_string())
                    }
                });
                let payload = obj
                    .get(&"payload")
                    .ok_or_else(|| make_error("Missing payload for auth"))?
                    .clone();
                Ok(Message::Auth {
                    token,
                    api_key,
                    payload,
                })
            }
            "cursor_sync" => {
                let topic = obj
                    .get(&"topic")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing topic for cursor_sync"))?
                    .to_string();
                let cursor = obj
                    .get(&"cursor")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| make_error("Missing cursor for cursor_sync"))?;
                Ok(Message::CursorSync { topic, cursor })
            }
            "error" => {
                let message = obj
                    .get(&"message")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| make_error("Missing message for error"))?
                    .to_string();
                let code = obj
                    .get(&"code")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                let details = obj.get(&"details").cloned();
                Ok(Message::Error {
                    message,
                    code,
                    details,
                })
            }
            "ack" => {
                let id = obj
                    .get(&"id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .ok_or_else(|| make_error("Missing or invalid id for ack"))?;
                let cursor = obj.get(&"cursor").and_then(|v| v.as_i64());
                let channel_cursor = obj.get(&"channel_cursor").and_then(|v| v.as_i64());
                let timestamp = obj
                    .get(&"timestamp")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                let payload = obj.get(&"payload").cloned();
                let channel_id = obj
                    .get(&"channel_id")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                let event_type = obj
                    .get(&"event_type")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                let actor = obj
                    .get(&"actor")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                let request_id = obj
                    .get(&"request_id")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                Ok(Message::Ack {
                    id,
                    cursor,
                    channel_cursor,
                    timestamp,
                    payload,
                    channel_id,
                    event_type,
                    actor,
                    request_id,
                })
            }
            "nack" => {
                let id = obj
                    .get(&"id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .ok_or_else(|| make_error("Missing or invalid id for nack"))?;
                let reason = obj
                    .get(&"reason")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                Ok(Message::Nack { id, reason })
            }
            _ => Err(make_error(&format!("Unknown message type: {msg_type}"))),
        }
    }

    /// Convert message to its serialized JSON string for sending over WebSocket
    pub fn to_websocket_message(&self) -> Result<String, anyhow::Error> {
        self.to_json()
            .map_err(|e| anyhow::anyhow!("Failed to serialize message: {e}"))
    }
}

impl BroadcastMessage {
    /// Create a new broadcast message
    pub fn new(topic: String, message: Message, sender_id: Option<Uuid>) -> Self {
        Self {
            id: Uuid::new_v4(),
            topic,
            message,
            sender_id,
            created_at: Utc::now(),
        }
    }

    /// Convert to JSON for cross-instance broadcasting
    pub fn to_json(&self) -> Result<String, sonic_rs::Error> {
        // Serialize the nested message using our custom to_json method
        let message_json = self.message.to_json()?;
        let message_value: Value = from_str(&message_json)?;

        let obj = sonic_rs::object! {
            "id": self.id.to_string(),
            "topic": &self.topic,
            "message": message_value,
            "sender_id": self.sender_id.map(|id| id.to_string()),
            "created_at": self.created_at.to_rfc3339()
        };
        to_string(&obj)
    }
}

impl MessageValidation {
    /// Check if the validation result is valid
    pub fn is_valid(&self) -> bool {
        matches!(self, MessageValidation::Valid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: Message) {
        let json = msg.to_json().unwrap();
        let deserialized = Message::from_json(&json).unwrap();
        assert_eq!(msg.message_type(), deserialized.message_type());
        assert_eq!(msg.topic(), deserialized.topic());
    }

    #[test]
    fn test_subscribe_roundtrip() {
        roundtrip(Message::Subscribe {
            topic: "t".into(),
            cursor: None,
            timestamp: None,
        });
        roundtrip(Message::Subscribe {
            topic: "t".into(),
            cursor: Some(99),
            timestamp: Some(12345),
        });
    }

    #[test]
    fn test_subscribe_many_roundtrip() {
        let msg = Message::SubscribeMany {
            topics: vec![
                SubscribeTopic {
                    topic: "ch:a".into(),
                    cursor: None,
                },
                SubscribeTopic {
                    topic: "ch:b".into(),
                    cursor: Some(7),
                },
            ],
            request_id: Some("req-1".into()),
        };
        let json = msg.to_json().unwrap();
        let parsed = Message::from_json(&json).unwrap();
        match parsed {
            Message::SubscribeMany { topics, request_id } => {
                assert_eq!(topics.len(), 2);
                assert_eq!(topics[0].topic, "ch:a");
                assert_eq!(topics[0].cursor, None);
                assert_eq!(topics[1].topic, "ch:b");
                assert_eq!(topics[1].cursor, Some(7));
                assert_eq!(request_id.as_deref(), Some("req-1"));
            }
            other => panic!("expected SubscribeMany, got {:?}", other.message_type()),
        }
        assert_eq!(msg.message_type(), "subscribe_many");
        assert!(msg.topic().is_none());
    }

    #[test]
    fn test_subscribe_many_validation_bounds() {
        use super::MessageValidation;
        let empty = Message::SubscribeMany {
            topics: vec![],
            request_id: None,
        };
        assert!(matches!(
            empty.validate(1024),
            MessageValidation::InvalidFormat(_)
        ));
        let too_many: Vec<SubscribeTopic> = (0..(Message::MAX_SUBSCRIBE_MANY_TOPICS + 1))
            .map(|i| SubscribeTopic {
                topic: format!("ch:{i}"),
                cursor: None,
            })
            .collect();
        let big = Message::SubscribeMany {
            topics: too_many,
            request_id: None,
        };
        assert!(matches!(
            big.validate(1024),
            MessageValidation::InvalidFormat(_)
        ));
        // A valid multi-subscribe passes validation.
        let valid = Message::SubscribeMany {
            topics: vec![SubscribeTopic {
                topic: "ch:ok".into(),
                cursor: Some(1),
            }],
            request_id: None,
        };
        assert!(valid.validate(1024).is_valid());
    }

    #[test]
    fn test_unsubscribe_roundtrip() {
        roundtrip(Message::Unsubscribe {
            topic: "t".into(),
            timestamp: None,
        });
        roundtrip(Message::Unsubscribe {
            topic: "t".into(),
            timestamp: Some(999),
        });
    }

    #[test]
    fn test_broadcast_roundtrip() {
        roundtrip(Message::Broadcast {
            topic: "ch:general".into(),
            payload: sonic_rs::json!({"m": "hello"}),
            timestamp: Some(123),
            operation_id: None,
            request_id: None,
        });
        roundtrip(Message::Broadcast {
            topic: "empty".into(),
            payload: sonic_rs::json!({}),
            timestamp: None,
            operation_id: None,
            request_id: None,
        });
    }

    #[test]
    fn test_broadcast_operation_id_roundtrip() {
        let op = Uuid::new_v4();
        let msg = Message::Broadcast {
            topic: "ch:op".into(),
            payload: sonic_rs::json!({"n": 1}),
            timestamp: None,
            operation_id: Some(op),
            request_id: None,
        };
        let json = msg.to_json().expect("serialize");
        let parsed = Message::from_json(&json).expect("parse");
        match parsed {
            Message::Broadcast {
                topic,
                operation_id,
                ..
            } => {
                assert_eq!(topic, "ch:op");
                assert_eq!(operation_id, Some(op));
            }
            other => panic!("expected Broadcast, got {:?}", other.message_type()),
        }
    }

    #[test]
    fn test_direct_message_roundtrip() {
        roundtrip(Message::DirectMessage {
            topic: "dm:user1".into(),
            payload: sonic_rs::json!({"text": "hi"}),
            timestamp: None,
        });
    }

    #[test]
    fn test_database_change_roundtrip() {
        roundtrip(Message::DatabaseChange {
            topic: "public.users".into(),
            event: "INSERT".into(),
            payload: sonic_rs::json!({"id": 1}),
            timestamp: Some(456),
        });
    }

    #[test]
    fn test_subscription_message_roundtrip() {
        roundtrip(Message::Subscription {
            topic: "t".into(),
            event: "subscribed".into(),
            payload: sonic_rs::json!({"success": true}),
        });
        roundtrip(Message::Subscription {
            topic: "t".into(),
            event: "error".into(),
            payload: sonic_rs::json!({"error": "denied"}),
        });
    }

    #[test]
    fn test_connection_message_roundtrip() {
        roundtrip(Message::Connection {
            event: "connected".into(),
            payload: sonic_rs::json!({"id": "abc"}),
        });
        roundtrip(Message::Connection {
            event: "ping".into(),
            payload: sonic_rs::json!({}),
        });
    }

    #[test]
    fn test_auth_message_roundtrip() {
        roundtrip(Message::Auth {
            token: Some("jwt-token".into()),
            api_key: None,
            payload: sonic_rs::json!({}),
        });
        roundtrip(Message::Auth {
            token: None,
            api_key: Some("key-123".into()),
            payload: sonic_rs::json!({}),
        });
    }

    #[test]
    fn test_cursor_sync_roundtrip() {
        roundtrip(Message::CursorSync {
            topic: "t".into(),
            cursor: 42,
        });
        roundtrip(Message::CursorSync {
            topic: "empty".into(),
            cursor: 0,
        });
    }

    #[test]
    fn test_error_message_roundtrip() {
        roundtrip(Message::Error {
            message: "not found".into(),
            code: Some("404".into()),
            details: Some(sonic_rs::json!({"field": "id"})),
        });
        roundtrip(Message::Error {
            message: "fail".into(),
            code: None,
            details: None,
        });
    }

    #[test]
    fn test_ack_nack_roundtrip() {
        let uid = Uuid::new_v4();
        roundtrip(Message::Ack {
            id: uid,
            cursor: Some(42),
            channel_cursor: Some(7),
            timestamp: Some("2026-01-01T00:00:00Z".into()),
            payload: Some(sonic_rs::json!({"key": "value"})),
            channel_id: Some("test-channel".into()),
            event_type: Some("test.event".into()),
            actor: Some("test-actor".into()),
            request_id: Some("req-1".into()),
        });
        roundtrip(Message::Nack {
            id: uid,
            reason: Some("timeout".into()),
        });
        roundtrip(Message::Nack {
            id: Uuid::nil(),
            reason: None,
        });
    }

    #[test]
    fn test_validation_valid() {
        let msg = Message::Subscribe {
            topic: "valid".into(),
            cursor: None,
            timestamp: None,
        };
        assert_eq!(msg.validate(1024), MessageValidation::Valid);
    }

    #[test]
    fn test_validation_empty_topic() {
        let msg = Message::Subscribe {
            topic: "".into(),
            cursor: None,
            timestamp: None,
        };
        assert!(matches!(
            msg.validate(1024),
            MessageValidation::InvalidTopic(_)
        ));
    }

    #[test]
    fn test_validation_topic_too_long() {
        let msg = Message::Subscribe {
            topic: "x".repeat(1001),
            cursor: None,
            timestamp: None,
        };
        assert!(matches!(
            msg.validate(1024),
            MessageValidation::InvalidTopic(_)
        ));
    }

    #[test]
    fn test_validation_payload_too_large() {
        let msg = Message::Broadcast {
            topic: "t".into(),
            payload: sonic_rs::json!({"data": "x".repeat(2000)}),
            timestamp: None,
            operation_id: None,
            request_id: None,
        };
        assert!(matches!(
            msg.validate(100),
            MessageValidation::PayloadTooLarge(_)
        ));
    }

    #[test]
    fn test_validation_topic_path_traversal() {
        let msg = Message::Subscribe {
            topic: "../etc/passwd".into(),
            cursor: None,
            timestamp: None,
        };
        let v = msg.validate(1024);
        assert!(!matches!(v, MessageValidation::Valid));
    }

    #[test]
    fn test_message_type_strings() {
        assert_eq!(
            Message::Subscribe {
                topic: "".into(),
                cursor: None,
                timestamp: None
            }
            .message_type(),
            "subscribe"
        );
        assert_eq!(
            Message::Ack {
                id: Uuid::nil(),
                cursor: None,
                channel_cursor: None,
                timestamp: None,
                payload: None,
                channel_id: None,
                event_type: None,
                actor: None,
                request_id: None
            }
            .message_type(),
            "ack"
        );
        assert_eq!(
            Message::Nack {
                id: Uuid::nil(),
                reason: None
            }
            .message_type(),
            "nack"
        );
        assert_eq!(
            Message::Error {
                message: "".into(),
                code: None,
                details: None
            }
            .message_type(),
            "error"
        );
    }

    #[test]
    fn test_broadcast_message_creation() {
        let msg = Message::Broadcast {
            topic: "t".into(),
            payload: sonic_rs::json!({"k": "v"}),
            timestamp: None,
            operation_id: None,
            request_id: None,
        };
        let bm = BroadcastMessage::new("t".into(), msg, None);
        assert_eq!(bm.topic, "t");
        assert!(bm.sender_id.is_none());
    }

    #[test]
    fn test_message_helpers() {
        let confirmed = Message::subscription_confirmed("t");
        assert_eq!(confirmed.message_type(), "subscription");

        let error = Message::subscription_error("t", "denied");
        assert_eq!(error.message_type(), "subscription");

        let auth = Message::auth_success();
        assert_eq!(auth.message_type(), "connection");

        let ping = Message::ping();
        assert_eq!(ping.message_type(), "connection");

        let pong = Message::pong();
        assert_eq!(pong.message_type(), "connection");
    }

    #[test]
    fn test_from_json_missing_type() {
        let result = Message::from_json(r#"{"topic": "test"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_json_invalid_json() {
        let result = Message::from_json("not-json");
        assert!(result.is_err());
    }

    #[test]
    fn test_from_json_rejects_deep_nesting() {
        // A deeply nested payload must be rejected by the depth guard, not
        // overflow the stack.
        let mut json = String::from(r#"{"type":"broadcast","topic":"t","payload":{"leaf":1}}"#);
        for _ in 0..50 {
            json = format!(r#"{{"type":"broadcast","topic":"t","payload":{{"c":{json}}}}}"#);
        }
        let result = Message::from_json(&json);
        assert!(result.is_err(), "deep message must be rejected");
    }

    #[test]
    fn test_from_json_accepts_normal_broadcast() {
        let result = Message::from_json(r#"{"type":"broadcast","topic":"t","payload":{"n":1}}"#);
        assert!(result.is_ok(), "normal broadcast must parse");
    }

    #[test]
    fn test_from_json_unknown_type() {
        let result = Message::from_json(r#"{"type": "unknown_type", "topic": "test"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_json_ping_shorthand() {
        let result = Message::from_json(r#"{"type":"ping"}"#).unwrap();
        assert_eq!(result.message_type(), "connection");
        if let Message::Connection { event, .. } = result {
            assert_eq!(event, "ping");
        } else {
            panic!("expected Connection message");
        }
    }

    #[test]
    fn test_message_pool() {
        let msg = Message::from_pool("subscribe");
        assert_eq!(msg.message_type(), "subscribe");
        msg.return_to_pool();
        let msg2 = Message::from_pool("subscribe");
        assert_eq!(msg2.message_type(), "subscribe");
    }

    #[test]
    fn test_message_pool_unknown_type() {
        let msg = Message::from_pool("nonexistent_type");
        assert_eq!(msg.message_type(), "connection");
    }

    #[test]
    fn test_error_helpers_and_reset() {
        let err = Message::error("boom".into());
        assert_eq!(err.message_type(), "error");
        let coded = Message::error_with_code("boom", Some("member_fenced"));
        assert_eq!(coded.message_type(), "error");
        if let Message::Error { code, message, .. } = coded {
            assert_eq!(code.as_deref(), Some("member_fenced"));
            assert_eq!(message, "boom");
        } else {
            panic!("expected error message");
        }
    }

    #[test]
    fn test_connection_event_helper() {
        let ev = Message::connection_event("ping", sonic_rs::json!({"ts": 1}));
        assert_eq!(ev.message_type(), "connection");
        if let Message::Connection { event, payload } = ev {
            assert_eq!(event, "ping");
            assert_eq!(payload["ts"], 1);
        } else {
            panic!("expected connection message");
        }
    }

    #[test]
    fn test_subscribe_success_and_empty_reset() {
        let ok = Message::subscribe_success("ch".into());
        assert_eq!(ok.message_type(), "subscription");
        let unsub = Message::unsubscribe_success("ch".into());
        assert_eq!(unsub.message_type(), "subscription");

        let mut empty = Message::create_empty();
        assert_eq!(empty.message_type(), "connection");
        empty.reset();
        assert_eq!(empty.message_type(), "connection");
    }

    #[test]
    fn test_from_json_ignores_unknown_fields() {
        let result = Message::from_json(
            r#"{"type":"subscribe","topic":"ch","cursor":5,"future_field":true}"#,
        )
        .unwrap();
        assert_eq!(result.message_type(), "subscribe");
        if let Message::Subscribe { topic, cursor, .. } = result {
            assert_eq!(topic, "ch");
            assert_eq!(cursor, Some(5));
        } else {
            panic!("expected subscribe message");
        }
    }
}
