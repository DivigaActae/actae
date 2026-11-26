// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use super::{connection::OutgoingMessage, connection_manager::ConnectionManager, message::Message};
use crate::auth::websocket_auth::{AuthMethod, WebSocketAuthenticator};
use crate::events::handler::EventHandler;
use crate::metrics;
use crate::security::{SecurityManager, SecurityValidationResult};
use crate::state::AppState;
use anyhow::Result;
use sonic_rs::{json, JsonValueMutTrait, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

/// WebSocket message handler
/// Processes incoming messages and routes them appropriately
#[derive(Clone)]
pub struct MessageHandler {
    authenticator: Arc<WebSocketAuthenticator>,
    security_manager: Option<Arc<SecurityManager>>,
    event_handler: Arc<RwLock<Option<Arc<crate::events::handler::EventHandler>>>>,
    /// App state (control-plane federation source policy access for federated
    /// subscribers — Phase 4).
    app_state: Arc<AppState>,
}

impl MessageHandler {
    /// Create a new message handler
    pub fn new(app_state: AppState) -> Self {
        let authenticator = Arc::new(WebSocketAuthenticator::new(app_state.clone()));
        Self {
            authenticator,
            security_manager: None,
            event_handler: Arc::new(RwLock::new(None)),
            app_state: Arc::new(app_state),
        }
    }

    /// Create a new message handler with security manager
    pub fn with_security_manager(
        app_state: AppState,
        security_manager: Arc<SecurityManager>,
    ) -> Self {
        let authenticator = Arc::new(WebSocketAuthenticator::new(app_state.clone()));
        Self {
            authenticator,
            security_manager: Some(security_manager),
            event_handler: Arc::new(RwLock::new(None)),
            app_state: Arc::new(app_state),
        }
    }

    pub async fn set_event_handler(&self, eh: Arc<EventHandler>) {
        *self.event_handler.write().await = Some(eh);
    }

    /// Handle incoming WebSocket message
    #[instrument(skip(self, message, connection_manager), fields(conn_id = %connection_id, msg_type = %message.message_type()))]
    pub async fn handle_message(
        &self,
        connection_id: &Uuid,
        message: Message,
        connection_manager: &ConnectionManager,
    ) -> Result<()> {
        debug!(
            "Processing message from connection {}: {:?}",
            connection_id, message
        );

        // Validate message first
        let validation = message.validate(1024 * 1024); // 1MB max payload
        if validation != super::message::MessageValidation::Valid {
            warn!(
                "Invalid message from connection {}: {:?}",
                connection_id, validation
            );
            let error_msg = Message::error("Invalid message format".to_string());
            connection_manager
                .send_to_connection(connection_id, error_msg)
                .await?;
            return Ok(());
        }

        // Authorization gate (audit item 1): every message other than the
        // auth handshake itself and keepalive pings requires a completed
        // authentication handshake. Previously an anonymous connection could
        // subscribe to and replay any channel's full event history.
        let requires_handshake = !matches!(&message, Message::Auth { .. })
            && !matches!(
                &message,
                Message::Connection { event, .. } if event == "ping"
            );
        if requires_handshake {
            let conn = connection_manager.get_connection(connection_id).await;
            let handshake_complete = conn
                .as_ref()
                .map(|c| c.auth_context.is_some())
                .unwrap_or(false);
            if !handshake_complete {
                warn!(
                    "Rejecting {} from unauthenticated connection {}",
                    message.message_type(),
                    connection_id
                );
                let error_msg = Message::error_with_code(
                    "Authentication required — send an auth message first",
                    Some("AUTH_REQUIRED"),
                );
                connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
        }

        // Route message based on type
        match &message {
            Message::Auth { .. } => {
                debug!("Routing auth message for connection {}", connection_id);
                self.handle_auth_message(connection_id, message, connection_manager)
                    .await
            }
            Message::Subscribe { .. } => {
                self.handle_subscribe_message(connection_id, message, connection_manager)
                    .await
            }
            Message::SubscribeMany { .. } => {
                self.handle_subscribe_many_message(connection_id, message, connection_manager)
                    .await
            }
            Message::Unsubscribe { .. } => {
                self.handle_unsubscribe_message(connection_id, message, connection_manager)
                    .await
            }
            Message::Broadcast { .. } => {
                self.handle_broadcast_message(connection_id, message, connection_manager)
                    .await
            }
            Message::Connection { event, .. } if event == "ping" => {
                self.handle_ping_message(connection_id, message, connection_manager)
                    .await
            }
            Message::Connection { event, .. } if event == "presence_request" => {
                // Presence tracking removed - implement through normal message routing
                info!("Presence request received - consider implementing through broadcast system");
                Ok(())
            }
            Message::Connection { event, .. } if event == "status_update" => {
                // Status updates removed - implement through normal message routing
                info!("Status update received - consider implementing through broadcast system");
                Ok(())
            }
            Message::Ack { id, .. } => {
                metrics::basic_metrics::BROADCAST_ACKED_TOTAL.inc();
                debug!(
                    delivery_id = %id,
                    connection_id = %connection_id,
                    "Received Ack"
                );
                Ok(())
            }
            Message::Nack { id, reason } => {
                metrics::basic_metrics::BROADCAST_NACKED_TOTAL.inc();
                warn!(
                    delivery_id = %id,
                    connection_id = %connection_id,
                    reason = ?reason,
                    "Received Nack"
                );
                Ok(())
            }
            _ => {
                warn!(
                    "Unhandled message type from connection {}: {:?}",
                    connection_id, message
                );
                let error_msg = Message::error("Unsupported message type".to_string());
                connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                Ok(())
            }
        }
    }

    /// Handle authentication message
    #[instrument(skip(self, message, connection_manager), fields(conn_id = %connection_id))]
    async fn handle_auth_message(
        &self,
        connection_id: &Uuid,
        message: Message,
        connection_manager: &ConnectionManager,
    ) -> Result<()> {
        if let Message::Auth { token, api_key, .. } = message {
            info!(
                "Authentication attempt from connection {} (api_key={:?}, has_token={})",
                connection_id,
                api_key.as_deref().map(|s| &s[..8.min(s.len())]),
                token.is_some()
            );

            // Get connection info for security validation
            let connection_info = connection_manager.get_connection(connection_id).await;
            let ip_addr = connection_info
                .as_ref()
                .and_then(|conn| conn.ip_address.as_ref()?.parse().ok());

            // If we have a security manager, validate the authentication attempt first
            if let Some(security_manager) = &self.security_manager {
                let auth_method = if token.is_some() {
                    AuthMethod::JWT
                } else if api_key.is_some() {
                    AuthMethod::ApiKey
                } else {
                    AuthMethod::Anonymous
                };

                // Validate authentication attempt with SecurityManager
                match security_manager
                    .validate_auth_attempt(
                        ip_addr,
                        None, // user_id not known yet
                        auth_method.clone(),
                    )
                    .await
                {
                    Ok(SecurityValidationResult::Allowed) => {
                        debug!(
                            "Authentication attempt allowed for connection {}",
                            connection_id
                        );
                    }
                    Ok(SecurityValidationResult::Blocked(reason)) => {
                        warn!("Authentication blocked for {}: {}", connection_id, reason);

                        // Record auth failure
                        security_manager.record_auth_failure(
                            ip_addr,
                            None,
                            auth_method,
                            reason.clone(),
                        );

                        let error_msg = Message::error(format!("Authentication blocked: {reason}"));
                        connection_manager
                            .send_to_connection(connection_id, error_msg)
                            .await?;
                        return Ok(());
                    }
                    Ok(SecurityValidationResult::RateLimited(info)) => {
                        warn!(
                            "Authentication rate limited for {}: {:?}",
                            connection_id, info
                        );

                        // Record auth failure
                        security_manager.record_auth_failure(
                            ip_addr,
                            None,
                            auth_method,
                            format!("Rate limit exceeded: {}", info.limit_type),
                        );

                        let error_msg = Message::error(format!(
                            "Too many authentication attempts. Try again in {} seconds.",
                            info.retry_after_seconds
                        ));
                        connection_manager
                            .send_to_connection(connection_id, error_msg)
                            .await?;
                        return Ok(());
                    }
                    Ok(SecurityValidationResult::RequiresAdditionalAuth) => {
                        debug!("Additional authentication required for {}", connection_id);
                        // Continue with standard auth process
                    }
                    Err(e) => {
                        error!(
                            "Authentication security validation error for {}: {}",
                            connection_id, e
                        );

                        // Record auth failure
                        security_manager.record_auth_failure(
                            ip_addr,
                            None,
                            auth_method,
                            format!("Security validation error: {e}"),
                        );

                        let error_msg = Message::error("Authentication failed".to_string());
                        connection_manager
                            .send_to_connection(connection_id, error_msg)
                            .await?;
                        return Ok(());
                    }
                }
            }

            // Use the new high-performance authenticator
            // Wrap in a timeout so failed auth doesn't hang the connection
            let auth_fut =
                self.authenticator
                    .authenticate(token.clone(), api_key.clone(), None, None);
            let auth_result = match tokio::time::timeout(Duration::from_secs(10), auth_fut).await {
                Ok(result) => result,
                Err(_) => {
                    warn!("Authentication timed out for connection {}", connection_id);
                    let error_msg = Message::error("Authentication timed out".to_string());
                    connection_manager
                        .send_to_connection(connection_id, error_msg)
                        .await?;
                    return Ok(());
                }
            };
            match auth_result {
                Ok(auth_result) => {
                    // Record successful authentication with SecurityManager
                    if let Some(security_manager) = &self.security_manager {
                        let auth_method = if token.is_some() {
                            AuthMethod::JWT
                        } else if api_key.is_some() {
                            AuthMethod::ApiKey
                        } else {
                            AuthMethod::Anonymous
                        };

                        security_manager.record_auth_success(
                            ip_addr,
                            auth_result.user_id.clone(),
                            auth_method,
                        );

                        // Cache successful auth if we have a token
                        if let Some(ref token_str) = token {
                            security_manager.cache_auth(token_str, auth_result.clone());
                        }
                    }

                    // Authenticate the connection with the full authorization
                    // principal so later subscribe/publish operations are
                    // checked against the real key scopes (audit items 1, 3).
                    let auth_context = auth_result.to_auth_context();
                    connection_manager
                        .authenticate_connection(
                            connection_id,
                            auth_result.user_id.clone(),
                            Some(auth_result.auth_method.to_string()),
                            auth_context,
                        )
                        .await?;

                    // Send authentication success response
                    let auth_response = Message::Connection {
                        event: "authenticated".to_string(),
                        payload: json!({
                            "user_id": auth_result.user_id,
                            "auth_method": auth_result.auth_method,
                            "permissions": auth_result.permissions,
                            "expires_at": auth_result.expires_at,
                            "timestamp": chrono::Utc::now().timestamp_millis()
                        }),
                    };

                    connection_manager
                        .send_to_connection(connection_id, auth_response)
                        .await?;

                    info!(
                        "Connection {} authenticated as user {} via {:?}",
                        connection_id, auth_result.user_id, auth_result.auth_method
                    );
                }
                Err(e) => {
                    warn!(
                        "Authentication failed for connection {}: {}",
                        connection_id, e
                    );

                    // Record authentication failure with SecurityManager
                    if let Some(security_manager) = &self.security_manager {
                        let auth_method = if token.is_some() {
                            AuthMethod::JWT
                        } else if api_key.is_some() {
                            AuthMethod::ApiKey
                        } else {
                            AuthMethod::Anonymous
                        };

                        security_manager.record_auth_failure(
                            ip_addr,
                            None, // user_id unknown on failure
                            auth_method,
                            format!("Authentication failed: {e}"),
                        );
                    }

                    // Never leak backend/database detail to the client (audit
                    // item 15) — the anyhow error may embed DB or config
                    // internals. Log it server-side; send a stable message.
                    // The one exception is the control-plane subscription lock:
                    // its reason is customer-facing (HTTP 402 returns the same
                    // text) and a locked customer must know why the connection
                    // was rejected.
                    let reason = e
                        .downcast_ref::<crate::auth::websocket_auth::SubscriptionBlocked>()
                        .map(|blocked| blocked.0.clone());
                    let error_msg = Message::error(match reason {
                        Some(msg) => msg,
                        None => "Authentication failed".to_string(),
                    });
                    connection_manager
                        .send_to_connection(connection_id, error_msg)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Handle subscribe message with optional cursor resume
    #[instrument(skip(self, message, connection_manager), fields(conn_id = %connection_id, topic))]
    async fn handle_subscribe_message(
        &self,
        connection_id: &Uuid,
        message: Message,
        connection_manager: &ConnectionManager,
    ) -> Result<()> {
        if let Message::Subscribe { topic, cursor, .. } = message {
            self.subscribe_one(connection_id, &topic, cursor, connection_manager)
                .await?;
        }
        Ok(())
    }

    /// Versioned multi-channel subscription (additive, actae.api.v1; D-001 —
    /// no second WS endpoint). Each entry receives the exact same treatment as
    /// a single `subscribe` (authorization, registration, cursor replay,
    /// confirmation); a summary frame is then sent with per-topic outcomes and
    /// the echoed `request_id`. Topics are processed sequentially so the
    /// connection's outbound ordering stays deterministic.
    async fn handle_subscribe_many_message(
        &self,
        connection_id: &Uuid,
        message: Message,
        connection_manager: &ConnectionManager,
    ) -> Result<()> {
        if let Message::SubscribeMany { topics, request_id } = message {
            info!(
                "subscribe_many request from connection {} for {} topics",
                connection_id,
                topics.len()
            );
            let mut results: Vec<sonic_rs::Value> = Vec::with_capacity(topics.len());
            for t in &topics {
                let outcome = match self
                    .subscribe_one(connection_id, &t.topic, t.cursor, connection_manager)
                    .await
                {
                    Ok(()) => sonic_rs::json!({"topic": t.topic, "status": "subscribed"}),
                    Err(e) => {
                        sonic_rs::json!({"topic": t.topic, "status": "error", "error": e.to_string()})
                    }
                };
                results.push(outcome);
            }
            let mut payload = sonic_rs::json!({});
            if let Some(payload_obj) = payload.as_object_mut() {
                payload_obj.insert("results", results);
                if let Some(rid) = &request_id {
                    payload_obj.insert("request_id", Value::from(rid.as_str()));
                }
            }
            let summary = Message::Connection {
                event: "subscribed_many".to_string(),
                payload,
            };
            connection_manager
                .send_to_connection(connection_id, summary)
                .await?;
        }
        Ok(())
    }

    /// Subscribe a single connection to a single topic with cursor resume.
    /// Shared by `subscribe` and `subscribe_many` so both transports exercise
    /// the exact same authorization + registration + replay path.
    #[instrument(skip(self, connection_manager), fields(conn_id = %connection_id))]
    async fn subscribe_one(
        &self,
        connection_id: &Uuid,
        topic: &str,
        cursor: Option<i64>,
        connection_manager: &ConnectionManager,
    ) -> Result<()> {
        {
            info!(
                "Subscription request from connection {} to topic {}",
                connection_id, topic
            );

            // Authorization: the connection's established principal must be
            // able to READ the channel before any replay or registration
            // happens (audit items 1 and 9). The gate in handle_message
            // guarantees the handshake completed; here we get the real
            // principal for the channel-scoped check.
            let Some(conn) = connection_manager.get_connection(connection_id).await else {
                warn!("Connection not found for subscription: {}", connection_id);
                let error_msg = Message::error("Connection not found".to_string());
                connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            };
            let Some(auth_context) = conn.auth_context.clone() else {
                let error_msg = Message::error_with_code(
                    "Authentication required — send an auth message first",
                    Some("AUTH_REQUIRED"),
                );
                connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            };
            let user_id = conn.user_id.as_deref().unwrap_or("anonymous");
            let can_write = auth_context.can_write(topic);

            // SYNCHRONOUS authorization gate: the principal must be able to
            // READ the channel BEFORE the connection is registered as a live
            // subscriber. Registration inserts the connection into the topic
            // subscriber set, so a delayed/rejected replay would otherwise
            // still leak live events to an out-of-scope key (audit: WS
            // subscription race). The EventHandler re-checks authoritatively
            // before replay; this in-memory check guarantees no registration
            // ever happens for a forbidden channel.
            // Parity with HTTP: `EventHandler::validate_auth` short-circuits
            // `AuthContext::Anonymous` to allowed whenever anonymous access is
            // enabled. Mirror that here so `ENABLE_ANONYMOUS=true` grants WS
            // read (subscribe) too, instead of accepting the anonymous
            // handshake and then denying every subscription.
            let anonymous_allowed = auth_context == crate::auth::types::AuthContext::Anonymous
                && self.app_state.config.enable_anonymous;
            if !anonymous_allowed && !auth_context.can_read(topic) {
                warn!(
                    "Connection {} denied subscription to topic '{}' - missing read permission",
                    connection_id, topic
                );
                let error_msg = Message::subscription_error(
                    topic,
                    "Not authorized to subscribe to this channel",
                );
                connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }

            // Federation source-policy gate (Phase 4, PR-009): a federated principal
            // may subscribe only to policy-allowed channels. Live frames for
            // federated subscribers that need per-frame redaction/filtering
            // are transformed INSIDE the source instance per subscriber
            // (events/handler.rs `transform_for_federated`) — the shared local
            // fan-out is never weakened, and bytes leave the instance only
            // after the policy has been applied. HTTP replay/query always
            // apply the policy per event.
            let is_federated = auth_context.is_federated();
            if is_federated {
                let policy = self.app_state.control_plane.effective_source_policy();
                if !policy.enabled || !policy.channel_allowed(topic) {
                    let error_msg = Message::subscription_error(
                        topic,
                        "Not authorized to subscribe to this channel (source policy)",
                    );
                    connection_manager
                        .send_to_connection(connection_id, error_msg)
                        .await?;
                    return Ok(());
                }
            }

            debug!(
                "User {} authorized subscription to topic '{}' (can_write={})",
                user_id, topic, can_write
            );

            // Register the real-time subscription.
            let subscribed = match connection_manager
                .subscribe_to_topic(connection_id, topic)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    error!(
                        "Failed to subscribe connection {} to topic {}: {}",
                        connection_id, topic, e
                    );
                    let error_response = Message::subscription_error(topic, "Subscription failed");
                    connection_manager
                        .send_to_connection(connection_id, error_response)
                        .await?;
                    return Ok(());
                }
            };

            if !subscribed {
                warn!(
                    "Connection {} already subscribed to topic {}",
                    connection_id, topic
                );
                let error_response =
                    Message::subscription_error(topic, "Already subscribed to this topic");
                connection_manager
                    .send_to_connection(connection_id, error_response)
                    .await?;
                return Ok(());
            }

            // Replay + latest cursor through the authorized event handler.
            let (mut replayed, latest_cursor) = {
                let event_handler_opt = self.event_handler.read().await;
                if let Some(ref eh) = *event_handler_opt {
                    match eh.subscribe_with_cursor(&auth_context, topic, cursor).await {
                        Ok(result) => result,
                        Err(e) => {
                            warn!(
                                "Subscription rejected for connection {} on {}: {}",
                                connection_id, topic, e
                            );
                            // Roll back the registration on authorization failure.
                            let _ = connection_manager
                                .unsubscribe_from_topic(connection_id, topic)
                                .await;
                            let client_msg = match &e {
                                crate::events::handler::EventError::Forbidden(_) => {
                                    "Not authorized to subscribe to this channel"
                                }
                                crate::events::handler::EventError::Unauthenticated(_) => {
                                    "Authentication required"
                                }
                                _ => "Subscription failed",
                            };
                            let error_response = Message::subscription_error(topic, client_msg);
                            connection_manager
                                .send_to_connection(connection_id, error_response)
                                .await?;
                            return Ok(());
                        }
                    }
                } else {
                    warn!("EventHandler not wired — replay unavailable");
                    (Vec::new(), 0)
                }
            };

            info!("Connection {} subscribed to topic {}", connection_id, topic);

            // Federated catch-up replay gets the same per-event source-policy
            // treatment as HTTP reads (Phase 4). For a no-transform policy
            // this is a no-op.
            if is_federated {
                let policy = self.app_state.control_plane.effective_source_policy();
                replayed.retain_mut(|e| policy.apply_to_event(e));
            }

            // Send replayed events from cursor catch-up
            for event in &replayed {
                let replay_msg = Message::Broadcast {
                    topic: topic.to_string(),
                    payload: json!({
                        "event": {
                            "id": event.id.to_string(),
                            "type": event.event_type,
                            "payload": event.payload,
                            "actor": event.metadata.actor,
                            "agent_id": event.metadata.agent_id,
                            "metadata": event.metadata.metadata,
                            "cursor": event.cursor,
                            "channel_cursor": event.cursor,
                            "timestamp": event.timestamp.to_rfc3339(),
                        }
                    }),
                    timestamp: Some(event.timestamp.timestamp()),
                    operation_id: None,
                    request_id: None,
                };
                connection_manager
                    .send_to_connection(connection_id, replay_msg)
                    .await?;
            }

            if latest_cursor > 0 {
                let sync_msg = Message::CursorSync {
                    topic: topic.to_string(),
                    cursor: latest_cursor,
                };
                connection_manager
                    .send_to_connection(connection_id, sync_msg)
                    .await?;
            }

            // Send subscription confirmation
            let confirm_response = Message::Subscription {
                topic: topic.to_string(),
                event: "subscribed".to_string(),
                payload: json!({
                    "topic": topic,
                    "timestamp": chrono::Utc::now().timestamp_millis(),
                    "subscriber_count": connection_manager.topic_subscriber_count(topic).await,
                    "can_write": can_write,
                    "cursor": latest_cursor,
                }),
            };

            connection_manager
                .send_to_connection(connection_id, confirm_response)
                .await?;
        }

        Ok(())
    }

    /// Handle unsubscribe message
    async fn handle_unsubscribe_message(
        &self,
        connection_id: &Uuid,
        message: Message,
        connection_manager: &ConnectionManager,
    ) -> Result<()> {
        if let Message::Unsubscribe { topic, .. } = message {
            info!(
                "Unsubscribe request from connection {} from topic {}",
                connection_id, topic
            );

            // Unsubscribe from topic
            match connection_manager
                .unsubscribe_from_topic(connection_id, &topic)
                .await
            {
                Ok(true) => {
                    info!(
                        "Connection {} unsubscribed from topic {}",
                        connection_id, topic
                    );

                    // Send unsubscription confirmation
                    let confirm_response = Message::Subscription {
                        topic: topic.clone(),
                        event: "unsubscribed".to_string(),
                        payload: json!({
                            "topic": topic,
                            "timestamp": chrono::Utc::now().timestamp_millis(),
                            "subscriber_count": connection_manager.topic_subscriber_count(&topic).await
                        }),
                    };

                    // 🆕 Remove cached permissions when unsubscribing
                    connection_manager
                        .remove_cached_permissions(connection_id, &topic)
                        .await;

                    connection_manager
                        .send_to_connection(connection_id, confirm_response)
                        .await?;
                }
                Ok(false) => {
                    warn!(
                        "Connection {} was not subscribed to topic {}",
                        connection_id, topic
                    );
                    let error_response =
                        Message::subscription_error(&topic, "Not subscribed to this topic");
                    connection_manager
                        .send_to_connection(connection_id, error_response)
                        .await?;
                }
                Err(e) => {
                    error!(
                        "Failed to unsubscribe connection {} from topic {}: {}",
                        connection_id, topic, e
                    );
                    let error_response =
                        Message::subscription_error(&topic, "Unsubscription failed");
                    connection_manager
                        .send_to_connection(connection_id, error_response)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Handle broadcast message
    #[instrument(skip(self, message, connection_manager), fields(conn_id = %connection_id, topic))]
    async fn handle_broadcast_message(
        &self,
        connection_id: &Uuid,
        message: Message,
        connection_manager: &ConnectionManager,
    ) -> Result<()> {
        if let Message::Broadcast {
            topic,
            payload,
            timestamp: _,
            operation_id,
            request_id,
        } = message
        {
            info!(
                "Broadcast request from connection {} to topic {}",
                connection_id, topic
            );

            // Retrieve the connection and its real authorization principal
            // (audit items 1 and 3). The gate in handle_message guarantees a
            // completed handshake; a read-only or channel-scoped key must not
            // be able to publish.
            let Some(connection) = connection_manager.get_connection(connection_id).await else {
                let error_msg = Message::error("Connection not found".to_string());
                connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            };
            let Some(auth_context) = connection.auth_context.clone() else {
                let error_msg = Message::error_with_code(
                    "Authentication required — send an auth message first",
                    Some("AUTH_REQUIRED"),
                );
                connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            };

            let event_handler_opt = self.event_handler.read().await;
            if let Some(ref eh) = *event_handler_opt {
                use crate::events::schema::{EventMetadata, PublishEventRequest};

                // Parity with HTTP anonymous write (see the subscribe gate).
                let anonymous_allowed = auth_context == crate::auth::types::AuthContext::Anonymous
                    && self.app_state.config.enable_anonymous;
                if !anonymous_allowed && !auth_context.can_write(&topic) {
                    warn!(
                        "Connection {} denied broadcast to topic '{}' - missing write permission",
                        connection_id, topic
                    );
                    let error_msg = Message::error(format!(
                        "Write access denied to topic: {topic}. Missing write permission."
                    ));
                    connection_manager
                        .send_to_connection(connection_id, error_msg)
                        .await?;
                    return Ok(());
                }

                let request = PublishEventRequest {
                    channel_id: topic.clone(),
                    event_type: "broadcast".to_string(),
                    payload: payload.clone(),
                    metadata: EventMetadata {
                        actor: connection
                            .user_id
                            .clone()
                            .unwrap_or_else(|| "anonymous".to_string()),
                        agent_id: None,
                        user_id: connection.user_id.clone(),
                        metadata: None,
                    },
                    depends_on: None,
                    dependencies: Vec::new(),
                    operation_id,
                    step_number: None,
                };

                match eh
                    .record(request, &auth_context, Some(*connection_id))
                    .await
                {
                    Ok(event) => {
                        info!(
                            "WS broadcast persisted {} cursor={}",
                            event.id, event.cursor
                        );
                        let ack = Message::Ack {
                            id: event.id,
                            cursor: Some(event.cursor),
                            channel_cursor: Some(event.cursor),
                            timestamp: Some(event.timestamp.to_rfc3339()),
                            payload: Some(event.payload.clone()),
                            channel_id: Some(event.channel_id.clone()),
                            event_type: Some(event.event_type.clone()),
                            actor: Some(event.metadata.actor.clone()),
                            request_id,
                        };
                        let conn_mgr = (*connection_manager).clone();
                        let sender_id = *connection_id;
                        tokio::spawn(async move {
                            let _ = conn_mgr.try_send_to_connection(
                                &sender_id,
                                OutgoingMessage::Message(Arc::new(ack)),
                            );
                        });
                    }
                    Err(e) => {
                        error!("WS broadcast record failed: {}", e);
                        // Log the full error server-side; the client gets a
                        // stable message (audit item 15).
                        let error_msg = Message::error("Record failed".to_string());
                        connection_manager
                            .send_to_connection(connection_id, error_msg)
                            .await?;
                    }
                }
            } else {
                error!("EventHandler not wired — broadcast via WebSocket is not available");
                let error_msg = Message::error(
                    "Broadcast unavailable: EventHandler not configured".to_string(),
                );
                connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
            }
        }

        Ok(())
    }

    /// Handle ping message
    async fn handle_ping_message(
        &self,
        connection_id: &Uuid,
        _message: Message,
        connection_manager: &ConnectionManager,
    ) -> Result<()> {
        debug!("Ping received from connection {}", connection_id);

        // Send pong response
        let pong_response = Message::Connection {
            event: "pong".to_string(),
            payload: json!({
                "timestamp": chrono::Utc::now().timestamp_millis()
            }),
        };

        connection_manager
            .send_to_connection(connection_id, pong_response)
            .await?;

        Ok(())
    }

    // Presence and cursor handling methods removed.
    // These should be implemented through the normal broadcast system:
    // 1. Clients send "presence" or "cursor" broadcast messages
    // 2. Server routes them like any other message
    // 3. Subscribers receive them through normal topic subscriptions
    // This keeps Actae focused on its core real-time messaging mission.
}

// Note: Default implementation removed to prevent panic in production.
// MessageHandler requires a valid AppState which must be initialized asynchronously.
// Always use MessageHandler::new(app_state) or MessageHandler::with_security_manager().

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::database::repositories::{ApiKeyRepository, SessionRepository, UserRepository};
    use crate::database::DatabaseManager;
    use deadpool_postgres::{Config, Runtime};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use tokio_postgres::NoTls;

    async fn create_mock_app_state() -> AppState {
        let app_config = AppConfig::default();
        let mut main_cfg = Config::new();
        main_cfg.host = Some("localhost".to_string());
        main_cfg.port = Some(5432);
        main_cfg.user = Some("postgres".to_string());
        main_cfg.password = Some("postgres".to_string());
        main_cfg.dbname = Some("actae".to_string());
        let main_pool = main_cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

        let mut client_cfg = Config::new();
        client_cfg.host = Some("localhost".to_string());
        client_cfg.port = Some(5433);
        client_cfg.user = Some("postgres".to_string());
        client_cfg.password = Some("postgres".to_string());
        client_cfg.dbname = Some("actae".to_string());
        let client_pool = client_cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .unwrap();

        let db_manager = Arc::new(DatabaseManager {
            main_pool,
            client_pool,
            pool_config: Arc::new(crate::database::PoolConfig {
                max_connections: 10,
                min_connections: 2,
                connection_timeout: Duration::from_secs(5),
                idle_timeout: Duration::from_secs(300),
                enable_connection_warmup: false,
            }),
            circuit_breakers: Arc::new(std::sync::RwLock::new(HashMap::new())),
            db_semaphore: Arc::new(Semaphore::new(200)),
        });

        let batch_broadcaster = Arc::new(
            crate::websocket::batch_broadcaster::BatchBroadcaster::new(
                crate::websocket::batch_broadcaster::BatchBroadcastConfig::default(),
            )
            .await
            .unwrap(),
        );

        AppState {
            config: Arc::new(app_config),
            db_manager: db_manager.clone(),
            user_repo: Arc::new(UserRepository::new(db_manager.clone())),
            api_key_repo: Arc::new(ApiKeyRepository::new(db_manager.clone())),
            session_repo: Arc::new(SessionRepository::new(db_manager.clone())),
            batch_broadcaster,
            password_hasher: Arc::new(
                crate::auth::password_hash::SecurePasswordHasher::with_secure_defaults(),
            ),
            control_plane: Arc::new(
                crate::control_plane::ControlPlaneManager::new(
                    Arc::new(crate::config::AppConfig::default()),
                    db_manager.client_pool.clone(),
                )
                .unwrap(),
            ),
            auth_manager: Arc::new(crate::auth::AuthManager::new(db_manager, vec![])),
        }
    }

    #[tokio::test]
    #[ignore] // Skip this test — requires live database
    async fn test_handler_creation() {
        let app_state = create_mock_app_state().await;
        let _handler = MessageHandler::new(app_state);
        // Basic test that handler can be created
        assert!(true);
    }

    #[tokio::test]
    async fn test_auth_message_handling() {
        // Test successful JWT authentication
        let jwt_auth_msg = Message::Auth {
            token: Some("valid_jwt_token".to_string()),
            api_key: None,
            payload: sonic_rs::json!({}),
        };

        // For now, just test that the handler can process auth messages without panicking
        // In a real test, we would mock the authentication and database calls
        assert!(jwt_auth_msg.validate(1024 * 1024).is_valid());

        // Test successful API key authentication
        let api_key_auth_msg = Message::Auth {
            token: None,
            api_key: Some("valid_api_key".to_string()),
            payload: sonic_rs::json!({}),
        };

        assert!(api_key_auth_msg.validate(1024 * 1024).is_valid());

        // Test invalid authentication message
        let invalid_auth_msg = Message::Auth {
            token: None,
            api_key: None,
            payload: sonic_rs::json!({}),
        };

        assert!(invalid_auth_msg.validate(1024 * 1024).is_valid()); // Structure is valid, but content is not
    }

    #[tokio::test]
    async fn test_subscription_message_validation() {
        let subscribe_msg = Message::Subscribe {
            topic: "test_topic".to_string(),
            cursor: None,
            timestamp: None,
        };

        assert!(subscribe_msg.validate(1024 * 1024).is_valid());

        // Test invalid topic names
        let invalid_subscribe_msg = Message::Subscribe {
            topic: "".to_string(),
            cursor: None,
            timestamp: None,
        };

        match invalid_subscribe_msg.validate(1024 * 1024) {
            crate::websocket::message::MessageValidation::InvalidTopic(_) => {
                // Expected
            }
            other => {
                assert!(
                    false,
                    "Expected InvalidTopic validation error, got {other:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_broadcast_message_validation() {
        let broadcast_msg = Message::Broadcast {
            topic: "test_topic".to_string(),
            payload: sonic_rs::json!({"message": "test"}),
            timestamp: Some(chrono::Utc::now().timestamp_millis()),
            operation_id: None,
            request_id: None,
        };

        assert!(broadcast_msg.validate(1024 * 1024).is_valid());

        // Test invalid topic name
        let invalid_broadcast_msg = Message::Broadcast {
            topic: "".to_string(),
            payload: sonic_rs::json!({"message": "test"}),
            timestamp: Some(chrono::Utc::now().timestamp_millis()),
            operation_id: None,
            request_id: None,
        };

        match invalid_broadcast_msg.validate(1024 * 1024) {
            crate::websocket::message::MessageValidation::InvalidTopic(_) => {
                // Expected
            }
            other => {
                assert!(
                    false,
                    "Expected InvalidTopic validation error, got {other:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_message_size_validation() {
        // Create a large payload that exceeds the limit
        let large_payload = sonic_rs::json!({
            "data": "x".repeat(2 * 1024 * 1024) // 2MB of data
        });

        let large_msg = Message::Broadcast {
            topic: "test_topic".to_string(),
            payload: large_payload,
            timestamp: Some(chrono::Utc::now().timestamp_millis()),
            operation_id: None,
            request_id: None,
        };

        match large_msg.validate(1024 * 1024) {
            // 1MB limit
            crate::websocket::message::MessageValidation::PayloadTooLarge(_) => {
                // Expected
            }
            other => {
                assert!(
                    false,
                    "Expected PayloadTooLarge validation error, got {other:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_ping_pong_messages() {
        let ping_msg = Message::ping();
        assert!(ping_msg.validate(1024 * 1024).is_valid());

        let pong_msg = Message::pong();
        assert!(pong_msg.validate(1024 * 1024).is_valid());
    }

    #[tokio::test]
    async fn test_error_message_creation() {
        let error_msg = Message::error("Test error message".to_string());
        assert!(error_msg.validate(1024 * 1024).is_valid());

        match error_msg {
            Message::Error { message, .. } => {
                assert_eq!(message, "Test error message");
            }
            other => {
                assert!(false, "Expected Error message, got {other:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_connection_message_creation() {
        let conn_msg =
            Message::connection_event("connected", sonic_rs::json!({"connection_id": "test_id"}));

        assert!(conn_msg.validate(1024 * 1024).is_valid());

        match conn_msg {
            Message::Connection { event, .. } => {
                assert_eq!(event, "connected");
            }
            other => {
                assert!(false, "Expected Connection message, got {other:?}");
            }
        }
    }

    // ── Subscription authorization-before-registration (audit race) ──

    fn restricted_key(read: &[&str]) -> crate::auth::types::AuthContext {
        let patterns: Vec<&str> = read.to_vec();
        crate::auth::types::AuthContext::ApiKey {
            key_id: "k-restricted".into(),
            key_source: "local".into(),
            expires_at: None,
            permissions: crate::auth::parse_api_key_permissions(&sonic_rs::json!({
                "read": patterns,
                "write": [],
            })),
        }
    }

    #[tokio::test]
    async fn subscribe_to_forbidden_channel_never_registers() {
        // A key scoped to `allowed.*` must NOT become a live subscriber of a
        // forbidden channel. Registration inserts the connection into the
        // topic subscriber set; if it ever happened before the read check, a
        // concurrent broadcast could leak live events to the out-of-scope
        // socket even though the replay is later rejected (audit race).
        let app_state = create_mock_app_state().await;
        let handler = MessageHandler::new(app_state);
        let cm = Arc::new(ConnectionManager::new(10, 100));

        let conn_id = Uuid::new_v4();
        let conn = crate::websocket::connection::Connection::anonymous(None, None);
        let _rx = cm.add_connection(conn_id, conn).await.unwrap();
        cm.authenticate_connection(
            &conn_id,
            "k-restricted".into(),
            Some("api_key".into()),
            restricted_key(&["allowed.*"]),
        )
        .await
        .unwrap();

        handler
            .handle_subscribe_message(
                &conn_id,
                Message::Subscribe {
                    topic: "forbidden.ch".into(),
                    cursor: None,
                    timestamp: None,
                },
                &cm,
            )
            .await
            .unwrap();

        // The connection must NOT be in the forbidden topic's subscriber set
        // and must have no subscription recorded on it.
        assert_eq!(
            cm.topic_subscriber_count("forbidden.ch").await,
            0,
            "denied subscribe must not register a live subscriber"
        );
        let conn = cm.get_connection(&conn_id).await.expect("connection gone");
        assert!(!conn.is_subscribed("forbidden.ch").await);
    }

    #[tokio::test]
    async fn subscribe_to_allowed_channel_registers() {
        // Sanity: the same path DOES register when the key is in scope, so
        // the negative test above is not a false positive.
        let app_state = create_mock_app_state().await;
        let handler = MessageHandler::new(app_state);
        let cm = Arc::new(ConnectionManager::new(10, 100));

        let conn_id = Uuid::new_v4();
        let conn = crate::websocket::connection::Connection::anonymous(None, None);
        let _rx = cm.add_connection(conn_id, conn).await.unwrap();
        cm.authenticate_connection(
            &conn_id,
            "k-restricted".into(),
            Some("api_key".into()),
            restricted_key(&["allowed.*"]),
        )
        .await
        .unwrap();

        handler
            .handle_subscribe_message(
                &conn_id,
                Message::Subscribe {
                    topic: "allowed.ch".into(),
                    cursor: None,
                    timestamp: None,
                },
                &cm,
            )
            .await
            .unwrap();

        assert_eq!(
            cm.topic_subscriber_count("allowed.ch").await,
            1,
            "in-scope subscribe must register a live subscriber"
        );
    }
}
