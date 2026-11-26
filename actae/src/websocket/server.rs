// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use axum::extract::ws::{Message as AxumWsMessage, WebSocket as AxumWebSocket};
use axum::http::HeaderMap;

use super::connection::{Connection, OutgoingMessage};
use super::connection_manager::ConnectionManager;
use super::handler::MessageHandler;
use super::message::{Message, MessageValidation};
use super::security::WebSocketSecurity;
use crate::auth::{self, websocket_auth::AuthMethod};
use crate::config::AppConfig;
use crate::security::{SecurityManager, SecurityValidationResult};
use crate::state::AppState;
use crate::websocket::AuthContext;
use chrono::{DateTime, Utc};
use sonic_rs::{to_string, JsonContainerTrait, JsonValueTrait};
use std::sync::atomic::{AtomicU64, Ordering};

/// WebSocket server core with optimized authentication and memory pooling
pub struct WebSocketServer {
    config: AppConfig,
    connection_manager: Arc<ConnectionManager>,
    message_handler: MessageHandler,
    shutdown_tx: broadcast::Sender<()>,
    is_running: Arc<RwLock<bool>>,
    app_state: AppState,
    auth_manager: Arc<auth::AuthManager>,
    // Security layer
    security: Arc<WebSocketSecurity>,
    security_manager: Arc<SecurityManager>,
    connection_info: Arc<RwLock<HashMap<String, ConnectionInfo>>>,
    message_stats: Arc<AtomicU64>,
    bytes_transferred: Arc<AtomicU64>,
    // Connection processing pool
    connection_pool_size: usize,
}

/// Connection information for usage tracking
#[derive(Debug, Clone)]
struct ConnectionInfo {
    connected_at: DateTime<Utc>,
    last_activity: DateTime<Utc>,
    messages_sent: u64,
    messages_received: u64,
    bytes_sent: u64,
    bytes_received: u64,
}

impl WebSocketServer {
    pub async fn new(
        config: &AppConfig,
        connection_manager: Arc<ConnectionManager>,
        app_state: AppState,
    ) -> Result<Self> {
        let (shutdown_tx, _) = broadcast::channel(16);

        // Initialize WebSocket server components
        info!(" Initializing WebSocket server...");

        let connection_pool_size = config
            .worker_count
            .unwrap_or_else(|| num_cpus::get() as u32) as usize;
        info!(" Connection pool size: {} workers", connection_pool_size);

        // Initialize security layer
        let auth_manager = Arc::new(auth::AuthManager::new(
            app_state.db_manager.clone(),
            app_state.config.api_key_hash_secret.as_bytes().to_vec(),
        ));
        let security = Arc::new(WebSocketSecurity::new(config.clone(), auth_manager.clone()));

        // Initialize comprehensive security manager
        let security_manager = Arc::new(SecurityManager::new(Arc::new(config.clone()))?);
        info!(" WebSocket security layer initialized with comprehensive SecurityManager");

        // Update message handler to use security manager
        let message_handler =
            MessageHandler::with_security_manager(app_state.clone(), security_manager.clone());

        Ok(Self {
            config: config.clone(),
            connection_manager,
            message_handler,
            shutdown_tx,
            is_running: Arc::new(RwLock::new(false)),
            auth_manager,
            security,
            security_manager,
            app_state,
            connection_info: Arc::new(RwLock::new(HashMap::new())),
            message_stats: Arc::new(AtomicU64::new(0)),
            bytes_transferred: Arc::new(AtomicU64::new(0)),
            connection_pool_size,
        })
    }

    pub async fn set_event_handler(&self, eh: Arc<crate::events::handler::EventHandler>) {
        self.message_handler.set_event_handler(eh).await;
    }

    /// Start background tasks — connection cleanup, stats, and security cleanup.
    /// No TCP listener; WebSocket connections are handled via axum upgrade on the main HTTP port.
    pub async fn start_background(&self) -> Result<()> {
        info!(" Starting WebSocket background tasks");

        // Set running state
        {
            let mut running = self.is_running.write().await;
            *running = true;
        }

        // Start background tasks
        self.start_background_tasks().await;

        // Start security cleanup task
        self.start_security_cleanup_task().await;

        info!(" WebSocket background tasks started");
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        let drain_timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();

        info!("Stopping WebSocket server — initiating graceful drain");

        // Phase 1: Signal all accept loops and connections to stop
        let _ = self.shutdown_tx.send(());

        // Phase 2: Wait for active connections to drain, bounded by the
        // drain timeout. Polling the live connection count (instead of a
        // fixed sleep) makes clean shutdowns with no sockets instantaneous.
        loop {
            let remaining = self.connection_manager.connection_count().await;
            if remaining == 0 {
                break;
            }
            if start.elapsed() >= drain_timeout {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        // Phase 3: Force-close remaining connections
        let remaining = self.connection_manager.cleanup_all_connections().await;
        if remaining > 0 {
            info!(
                "Forcefully closed {} remaining connections during drain",
                remaining
            );
        }

        info!("WebSocket server stopped");
        Ok(())
    }

    async fn start_background_tasks(&self) {
        let shutdown_tx = self.shutdown_tx.clone();

        // Connection cleanup task
        let connection_manager = self.connection_manager.clone();
        let mut cleanup_interval = interval(Duration::from_secs(60));

        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_tx.subscribe();
            loop {
                tokio::select! {
                    _ = cleanup_interval.tick() => {
                        let cleaned = connection_manager
                            .cleanup_inactive_connections(Duration::from_secs(300))
                            .await;
                        if cleaned > 0 {
                            debug!("Cleaned up {} inactive connections", cleaned);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Connection cleanup task shutting down");
                        break;
                    }
                }
            }
        });

        // Stats logging task
        let stats_shutdown_tx = self.shutdown_tx.clone();
        let connection_manager = self.connection_manager.clone();
        let mut stats_interval = interval(Duration::from_secs(30));

        tokio::spawn(async move {
            let mut shutdown_rx = stats_shutdown_tx.subscribe();
            loop {
                tokio::select! {
                    _ = stats_interval.tick() => {
                        let count = connection_manager.connection_count().await;
                        let topic_stats = connection_manager.get_topic_stats().await;
                        debug!("Server stats: {} connections, {} topics", count, topic_stats.len());
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Stats logging task shutting down");
                        break;
                    }
                }
            }
        });
    }

    /// Handle a WebSocket connection upgraded by the axum HTTP server.
    /// This replaces the old native TCP listener — all WS connections now arrive via the axum Router.
    pub async fn handle_axum_connection(
        &self,
        ws: AxumWebSocket,
        addr: SocketAddr,
        headers: &HeaderMap,
    ) -> Result<()> {
        debug!("New axum WS connection from: {}", addr);

        // Validate connection security first
        let connection_id = uuid::Uuid::new_v4();
        let ip = addr.ip();
        let user_agent = headers
            .get("user-agent")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());
        let origin = headers
            .get("origin")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());
        let host_header = headers
            .get("host")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        // Check connection security with SecurityManager
        match self
            .security_manager
            .validate_connection(
                connection_id,
                Some(ip),
                user_agent.clone(),
                origin,
                host_header,
            )
            .await
        {
            Ok(SecurityValidationResult::Allowed) => {
                debug!("Connection security validation passed for {}", addr);
            }
            Ok(SecurityValidationResult::Blocked(reason)) => {
                warn!("Connection blocked from {}: {}", addr, reason);
                return Err(anyhow::anyhow!("Connection blocked: {reason}"));
            }
            Ok(SecurityValidationResult::RateLimited(info)) => {
                warn!("Connection rate limited from {}: {:?}", addr, info);
                return Err(anyhow::anyhow!("Rate limited: {}", info.limit_type));
            }
            Ok(SecurityValidationResult::RequiresAdditionalAuth) => {
                debug!("Connection requires additional auth for {}", addr);
            }
            Err(e) => {
                error!("Security validation error for {}: {}", addr, e);
                return Err(e);
            }
        }

        // Create connection (initially anonymous)
        let mut connection = Connection::anonymous(Some(addr.ip().to_string()), user_agent);
        connection.id = connection_id;

        info!("WebSocket connection established: {}", connection.info());

        // Add to connection manager
        let message_receiver = match self
            .connection_manager
            .add_connection(connection.id, connection.clone())
            .await
        {
            Ok(receiver) => receiver,
            Err(e) => {
                error!("Failed to add connection: {}", e);
                return Err(e);
            }
        };

        // Store connection ID for cleanup
        let connection_id = connection.id;

        // Handle the connection with axum WebSocket
        let result = self
            .handle_axum_websocket_connection(connection, ws, message_receiver)
            .await;

        // Clean up connection
        if let Err(e) = self
            .connection_manager
            .remove_connection(&connection_id)
            .await
        {
            error!("Failed to remove connection {}: {}", connection_id, e);
        }

        info!("Connection closed: {}", connection_id);
        result
    }

    async fn handle_axum_websocket_connection(
        &self,
        connection: Connection,
        ws: AxumWebSocket,
        message_receiver: flume::Receiver<crate::websocket::connection::OutgoingMessage>,
    ) -> Result<()> {
        let connection_id = connection.id;
        let (mut ws_sender, mut ws_reader) = ws.split();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let aggregation_window = Duration::from_micros(200);

        // Send welcome message
        let welcome_msg = Message::connection_event(
            "connected",
            sonic_rs::json!({
                "connection_id": connection_id,
                "timestamp": chrono::Utc::now().timestamp_millis()
            }),
        );

        if let Err(e) = self
            .connection_manager
            .send_to_connection(&connection_id, welcome_msg)
            .await
        {
            warn!("Failed to send welcome message: {}", e);
        }

        debug!(
            "[WS_CONN] Starting select! loop for connection {}",
            connection_id
        );

        loop {
            tokio::select! {
                ws_msg = ws_reader.next() => {
                    match ws_msg {
                        Some(Ok(AxumWsMessage::Text(text))) => {
                            debug!("[WS_CONN] Received text message on connection {}", connection_id);
                            debug!("[WS_CONN] Text payload length={} for connection {}", text.len(), connection_id);
                            match self
                                .handle_incoming_message(&connection_id, text.to_string())
                                .await
                            {
                                Ok(()) => debug!("[WS_CONN] handle_incoming_message OK for {}", connection_id),
                                Err(e) => error!("[WS_CONN] handle_incoming_message ERROR for {}: {}", connection_id, e),
                            }
                        }
                        Some(Ok(AxumWsMessage::Close(_))) => {
                            info!("[WS_CONN] Received close frame from {}", connection_id);
                            break;
                        }
                        Some(Ok(AxumWsMessage::Ping(_))) => {
                            debug!("[WS_CONN] Ping received from {}", connection_id);
                        }
                        Some(Ok(AxumWsMessage::Pong(_))) => {
                            debug!("[WS_CONN] Pong received from {}", connection_id);
                        }
                        Some(Ok(AxumWsMessage::Binary(_))) => {
                            debug!("[WS_CONN] Binary message from {} (unsupported)", connection_id);
                        }
                        Some(Err(e)) => {
                            debug!("[WS_CONN] WebSocket error from {}: {}", connection_id, e);
                            break;
                        }
                        None => {
                            debug!("[WS_CONN] WebSocket stream ended for {}", connection_id);
                            break;
                        }
                    }
                }
                outgoing = message_receiver.recv_async() => {
                    match outgoing {
                        Ok(first) => {
                            let mut batch = Vec::with_capacity(16);
                            batch.push(first);
                            let deadline = tokio::time::Instant::now() + aggregation_window;
                            loop {
                                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                                if remaining.is_zero() { break; }
                                match tokio::time::timeout(remaining, message_receiver.recv_async()).await {
                                    Ok(Ok(msg)) => batch.push(msg),
                                    _ => break,
                                }
                            }
                            self.message_stats.fetch_add(batch.len() as u64, Ordering::Relaxed);
                            debug!("[WS_CONN] Sending batch of {} messages to {}", batch.len(), connection_id);
                            for outgoing_msg in batch {
                                let result = match outgoing_msg {
                                    OutgoingMessage::Serialized(s) => {
                                        debug!("[WS_CONN] Sending serialized message to {}", connection_id);
                                        let data = Arc::try_unwrap(s).unwrap_or_else(|arc| (*arc).clone());
                                        ws_sender.send(AxumWsMessage::Text(data.into())).await
                                    }
                                    OutgoingMessage::Message(m) => {
                                        debug!("[WS_CONN] Sending Message to {}, type={}", connection_id, m.message_type());
                                        match to_string(&*m) {
                                            Ok(json) => {
                                                debug!("[WS_CONN] Serialized message for {}: {} bytes", connection_id, json.len());
                                                ws_sender.send(AxumWsMessage::Text(json.into())).await
                                            }
                                            Err(_) => continue,
                                        }
                                    }
                                };
                                if let Err(e) = result {
                                    debug!("[WS_CONN] Send failed to {}: {}", connection_id, e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            debug!("[WS_CONN] Outgoing channel closed for {}: {}", connection_id, e);
                            break;
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    debug!("[WS_CONN] Shutdown signal received for connection {}", connection_id);
                    break;
                }
            }
        }

        Ok(())
    }
    async fn handle_incoming_message(
        &self,
        connection_id: &Uuid,
        raw_message: String,
    ) -> Result<()> {
        info!(
            "[HANDLE_MSG] Entering handle_incoming_message for connection {}",
            connection_id
        );
        info!(
            "[HANDLE_MSG] Raw message (len={}): {}",
            raw_message.len(),
            &raw_message[..raw_message.len().min(200)]
        );

        // Track message received
        self.message_stats.fetch_add(1, Ordering::Relaxed);

        // Get connection info for security validation
        let connection_info = self.connection_manager.get_connection(connection_id).await;
        let (user_id, ip_addr) = if let Some(conn) = &connection_info {
            (
                conn.user_id.as_deref(),
                conn.ip_address
                    .as_ref()
                    .and_then(|ip_str| ip_str.parse().ok()),
            )
        } else {
            (None, None)
        };

        // Validate message security with SecurityManager
        match self
            .security_manager
            .validate_message(*connection_id, ip_addr, user_id, raw_message.len())
            .await
        {
            Ok(SecurityValidationResult::Allowed) => {
                // Message validation passed
            }
            Ok(SecurityValidationResult::Blocked(reason)) => {
                warn!("Message blocked from {}: {}", connection_id, reason);
                let error_msg = Message::error_with_code(&reason, Some("MESSAGE_BLOCKED"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
            Ok(SecurityValidationResult::RateLimited(info)) => {
                warn!("Message rate limited from {}: {:?}", connection_id, info);
                let error_msg = Message::error_with_code(
                    &format!("Rate limit exceeded: {}", info.limit_type),
                    Some("RATE_LIMITED"),
                );
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
            Ok(SecurityValidationResult::RequiresAdditionalAuth) => {
                // Allow message to proceed for auth
            }
            Err(e) => {
                error!(
                    "Message security validation error for {}: {}",
                    connection_id, e
                );
                let error_msg =
                    Message::error_with_code("Security validation failed", Some("SECURITY_ERROR"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
        }

        // Parse message using sonic-rs for better performance with null payload handling
        info!(
            "[HANDLE_MSG] Attempting to parse message for connection {}",
            connection_id
        );
        let message = match Message::from_json(&raw_message) {
            Ok(msg) => {
                info!(
                    "[HANDLE_MSG] Parsed OK: type={}, msg={:?}",
                    msg.message_type(),
                    msg
                );
                msg
            }
            Err(e) => {
                error!(
                    "[HANDLE_MSG] Failed to parse message from {}: {}",
                    connection_id, e
                );
                // Don't log the raw body verbatim — a hostile client can send
                // a megabyte payload and turn this into a log flood. Truncate.
                let preview: String = raw_message.chars().take(500).collect();
                error!(
                    "[HANDLE_MSG] Raw incoming message content (truncated): {}",
                    preview
                );

                // Try to parse as generic JSON to see if it's valid JSON at all.
                // Guard depth first — the diagnosis parse must never overflow.
                let json_value = if crate::database::json_compat::json_nesting_depth(&raw_message)
                    > crate::database::json_compat::MAX_JSON_DEPTH
                {
                    None
                } else {
                    sonic_rs::from_str::<sonic_rs::Value>(&raw_message).ok()
                };
                match json_value {
                    Some(json_value) => {
                        error!("JSON parsed but Message mapping failed: {:?}", json_value);
                        // Try to identify the specific issue
                        if let Some(obj) = json_value.as_object() {
                            let type_key = String::from("type");
                            if let Some(msg_type) = obj.get(&type_key) {
                                debug!("Message type: {:?}", msg_type);
                                if msg_type.as_str() == Some("auth") {
                                    debug!("Auth message structure: {:?}", obj);
                                    // Check if payload is the issue
                                    let payload_key = String::from("payload");
                                    if let Some(payload) = obj.get(&payload_key) {
                                        debug!("Payload type: {:?}", payload);
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        debug!("Message exceeds nesting depth limit");
                    }
                }

                let error_msg =
                    Message::error_with_code("Invalid message format", Some("INVALID_JSON"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
        };

        // Validate message
        match message.validate(1024 * 1024) {
            // 1MB max payload
            MessageValidation::Valid => {}
            MessageValidation::InvalidFormat(err) => {
                let error_msg = Message::error_with_code(&err, Some("INVALID_FORMAT"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
            MessageValidation::InvalidTopic(err) => {
                let error_msg = Message::error_with_code(&err, Some("INVALID_TOPIC"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
            MessageValidation::PayloadTooLarge(size) => {
                let error_msg = Message::error_with_code(
                    &format!("Payload too large: {size} bytes"),
                    Some("PAYLOAD_TOO_LARGE"),
                );
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
            MessageValidation::MissingRequired(field) => {
                let error_msg = Message::error_with_code(
                    &format!("Missing required field: {field}"),
                    Some("MISSING_REQUIRED"),
                );
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
            MessageValidation::InvalidCharacters(err) => {
                let error_msg = Message::error_with_code(&err, Some("INVALID_CHARACTERS"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
            MessageValidation::SecurityViolation(err) => {
                let error_msg = Message::error_with_code(&err, Some("SECURITY_VIOLATION"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
            MessageValidation::RateLimitExceeded(err) => {
                let error_msg = Message::error_with_code(&err, Some("RATE_LIMIT_EXCEEDED"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Ok(());
            }
        }

        info!(
            "[HANDLE_MSG] Routing message to default handler for {} (type={})",
            connection_id,
            message.message_type()
        );
        match self
            .message_handler
            .handle_message(connection_id, message, &self.connection_manager)
            .await
        {
            Ok(()) => info!("[HANDLE_MSG] Handler returned OK for {}", connection_id),
            Err(e) => {
                error!("[HANDLE_MSG] Handler error for {}: {}", connection_id, e);
                let error_msg =
                    Message::error_with_code("Internal server error", Some("INTERNAL_ERROR"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
            }
        }

        info!(
            "[HANDLE_MSG] Exiting handle_incoming_message for {}",
            connection_id
        );

        Ok(())
    }

    /// Get server statistics
    pub async fn get_stats(&self) -> ServerStats {
        let connection_count = self.connection_manager.connection_count().await;
        let _topic_stats = self.connection_manager.get_topic_stats().await;
        let _is_running = *self.is_running.read().await;

        ServerStats {
            active_connections: connection_count,
            total_messages: self.message_stats.load(Ordering::Relaxed),
            total_bytes: self.bytes_transferred.load(Ordering::Relaxed),
        }
    }

    /// Get connection manager reference
    pub fn get_connection_manager(&self) -> &Arc<ConnectionManager> {
        &self.connection_manager
    }

    /// Broadcast message to all connections (for feature system)
    pub async fn broadcast_message(&self, message: &Message) -> Result<()> {
        // For now, broadcast to all connections via a default topic
        self.connection_manager
            .broadcast_to_topic("__global__", message.clone(), None)
            .await?;
        Ok(())
    }

    /// Broadcast message to specific topic (for feature system)
    pub async fn broadcast_to_topic(&self, topic: &str, message: &Message) -> Result<()> {
        self.connection_manager
            .broadcast_to_topic(topic, message.clone(), None)
            .await?;
        Ok(())
    }

    /// Get connection count (for feature system)
    pub async fn get_connection_count(&self) -> usize {
        self.connection_manager.connection_count().await
    }

    /// Get topic subscribers (for feature system)
    pub async fn get_topic_subscribers(&self, topic: &str) -> Vec<String> {
        // Return subscriber count as a vector with one element for now
        // This is a simplified implementation
        let topic_stats = self.connection_manager.get_topic_stats().await;
        let count = topic_stats.get(topic).copied().unwrap_or(0);
        vec![format!("subscribers: {}", count)]
    }

    /// Handle new WebSocket connection with usage tracking
    pub async fn handle_authenticated_connection(
        &self,
        connection_id: String,
        auth_context: AuthContext,
    ) -> anyhow::Result<()> {
        let user_id = match &auth_context {
            AuthContext::Authenticated { user_id, .. } => user_id.clone(),
            AuthContext::FederatedObservation { principal, .. }
            | AuthContext::FederatedCommand { principal, .. } => principal.clone(),
            AuthContext::ApiKey { .. } => {
                return Err(anyhow::anyhow!(
                    "API key authentication not supported for WebSocket connections"
                ));
            }
            AuthContext::Anonymous => {
                return Err(anyhow::anyhow!(
                    "Anonymous authentication not supported for WebSocket connections"
                ));
            }
        };

        // Create connection info
        let connection_info = ConnectionInfo {
            connected_at: Utc::now(),
            last_activity: Utc::now(),
            messages_sent: 0,
            messages_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
        };

        // Store connection info
        {
            let mut connections = self.connection_info.write().await;
            connections.insert(connection_id.clone(), connection_info.clone());
        }

        info!(
            " WebSocket connection established: {} (user: {})",
            connection_id, user_id
        );

        Ok(())
    }

    /// Handle message sent through WebSocket
    pub async fn handle_message_sent(
        &self,
        connection_id: &str,
        message: &Message,
    ) -> anyhow::Result<()> {
        // Get connection info
        let connection_exists = {
            let connections = self.connection_info.read().await;
            connections.contains_key(connection_id)
        };

        if !connection_exists {
            return Err(anyhow::anyhow!("Connection not found: {connection_id}"));
        }

        // Calculate message size
        let message_size = self.calculate_message_size(message);

        // Update connection stats
        {
            let mut connections = self.connection_info.write().await;
            if let Some(info) = connections.get_mut(connection_id) {
                info.messages_sent += 1;
                info.bytes_sent += message_size;
                info.last_activity = Utc::now();
            }
        }

        // Update global stats
        self.message_stats.fetch_add(1, Ordering::Relaxed);
        self.bytes_transferred
            .fetch_add(message_size, Ordering::Relaxed);

        debug!(
            " Message sent - connection: {}, size: {} bytes, topic: {:?}",
            connection_id,
            message_size,
            message.topic()
        );

        Ok(())
    }

    /// Handle message received through WebSocket
    pub async fn handle_message_received(
        &self,
        connection_id: &str,
        message: &Message,
    ) -> anyhow::Result<()> {
        // Get connection info
        let connection_exists = {
            let connections = self.connection_info.read().await;
            connections.contains_key(connection_id)
        };

        if !connection_exists {
            return Err(anyhow::anyhow!("Connection not found: {connection_id}"));
        }

        // Calculate message size
        let message_size = self.calculate_message_size(message);

        // Update connection stats
        {
            let mut connections = self.connection_info.write().await;
            if let Some(info) = connections.get_mut(connection_id) {
                info.messages_received += 1;
                info.bytes_received += message_size;
                info.last_activity = Utc::now();
            }
        }

        debug!(
            " Message received - connection: {}, size: {} bytes, topic: {:?}",
            connection_id,
            message_size,
            message.topic()
        );

        Ok(())
    }

    /// Handle WebSocket disconnection
    pub async fn handle_disconnection(&self, connection_id: &str) -> anyhow::Result<()> {
        // Get and remove connection info
        let connection_info = {
            let mut connections = self.connection_info.write().await;
            connections.remove(connection_id)
        };

        if let Some(info) = connection_info {
            let session_duration = (Utc::now() - info.connected_at).num_seconds() as u64;

            info!(
                " WebSocket disconnected: {} (duration: {}s, sent: {} msgs/{} bytes, received: {} msgs/{} bytes)",
                connection_id,
                session_duration,
                info.messages_sent,
                info.bytes_sent,
                info.messages_received,
                info.bytes_received
            );
        }

        Ok(())
    }

    /// Handle API request for usage tracking
    pub async fn track_api_request(
        &self,
        _user_id: Option<&str>,
        _endpoint: &str,
        _method: &str,
        _response_size: u64,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Calculate message size for usage tracking
    fn calculate_message_size(&self, message: &Message) -> u64 {
        match message {
            Message::Broadcast { topic, payload, .. } => {
                topic.len() as u64 + self.calculate_sonic_value_size(payload)
            }
            Message::Subscribe { topic, .. } => {
                topic.len() as u64 + 20 // Base overhead
            }
            Message::Unsubscribe { topic, .. } => {
                topic.len() as u64 + 20 // Base overhead
            }
            Message::Error { message, .. } => {
                message.len() as u64 + 20 // Base overhead
            }
            _ => 10, // Default size for any other message type
        }
    }

    /// Calculate sonic_rs::Value size approximation
    fn calculate_sonic_value_size(&self, value: &sonic_rs::Value) -> u64 {
        if value.is_null() {
            4
        } else if value.as_bool().is_some() {
            5
        } else if value.is_number() {
            8
        } else if value.is_str() {
            value.as_str().unwrap_or("").len() as u64
        } else if value.is_array() {
            let default_array = sonic_rs::Array::new();
            let arr = value.as_array().unwrap_or(&default_array);
            arr.iter()
                .map(|v| self.calculate_sonic_value_size(v))
                .sum::<u64>()
                + 10 // Array overhead
        } else if value.is_object() {
            value
                .as_object()
                .unwrap_or(&Default::default())
                .iter()
                .map(|(k, v)| k.len() as u64 + self.calculate_sonic_value_size(v))
                .sum::<u64>()
                + 10 // Object overhead
        } else {
            0
        }
    }

    /// Subscribe to the server shutdown signal.
    pub fn subscribe_shutdown(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Get current server statistics
    pub async fn get_server_stats(&self) -> ServerStats {
        let connections = self.connection_info.read().await;
        let active_connections = connections.len();
        let total_messages = self.message_stats.load(Ordering::Relaxed);
        let total_bytes = self.bytes_transferred.load(Ordering::Relaxed);

        ServerStats {
            active_connections,
            total_messages,
            total_bytes,
        }
    }

    /// Start background tasks + periodic usage reporting.
    /// WebSocket connections arrive via axum upgrade on the main HTTP port (no separate TCP listener).
    pub async fn start_background_with_usage_tracking(&self) -> anyhow::Result<()> {
        info!(" Starting WebSocket background tasks with usage tracking");

        // Start background tasks (cleanup, stats, security cleanup)
        self.start_background().await?;

        // Start periodic usage reporting
        let connection_manager = Arc::clone(&self.connection_info);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                let connections = connection_manager.read().await;
                let connection_count = connections.len() as u64;
                drop(connections);

                debug!(" Reported active connections: {}", connection_count);
            }
        });

        info!(" WebSocket background tasks started with usage tracking");
        Ok(())
    }

    /// Get reference to security layer
    pub fn get_security(&self) -> &Arc<WebSocketSecurity> {
        &self.security
    }

    /// Get reference to comprehensive security manager
    pub fn get_security_manager(&self) -> &Arc<SecurityManager> {
        &self.security_manager
    }

    /// Handle authentication with enhanced security validation
    pub async fn handle_secure_authentication(
        &self,
        connection_id: &Uuid,
        auth_method: AuthMethod,
        token: Option<&str>,
        api_key: Option<&str>,
        user_id: Option<&str>,
        ip_addr: Option<std::net::IpAddr>,
    ) -> Result<()> {
        // Save auth_method for later use since it will be moved
        let auth_method_display = format!("{auth_method:?}");

        // Validate authentication attempt with SecurityManager
        match self
            .security_manager
            .validate_auth_attempt(ip_addr, user_id, auth_method.clone())
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
                self.security_manager.record_auth_failure(
                    ip_addr,
                    user_id,
                    auth_method.clone(),
                    reason.clone(),
                );

                let error_msg = Message::error_with_code(&reason, Some("AUTH_BLOCKED"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Err(anyhow::anyhow!("Authentication blocked: {reason}"));
            }
            Ok(SecurityValidationResult::RateLimited(info)) => {
                warn!(
                    "Authentication rate limited for {}: {:?}",
                    connection_id, info
                );

                // Record auth failure
                self.security_manager.record_auth_failure(
                    ip_addr,
                    user_id,
                    auth_method,
                    format!("Rate limit exceeded: {}", info.limit_type),
                );

                let error_msg = Message::error_with_code(
                    &format!(
                        "Too many authentication attempts. Try again in {} seconds.",
                        info.retry_after_seconds
                    ),
                    Some("AUTH_RATE_LIMITED"),
                );
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Err(anyhow::anyhow!("Authentication rate limited"));
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
                self.security_manager.record_auth_failure(
                    ip_addr,
                    user_id,
                    auth_method,
                    format!("Security validation error: {e}"),
                );

                let error_msg =
                    Message::error_with_code("Authentication failed", Some("AUTH_ERROR"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;
                return Err(e);
            }
        }

        // Perform standard authentication
        let auth_result = if let Some(token) = token {
            self.auth_manager
                .authenticate_websocket(Some(token.to_string()), None, None, None)
                .await
        } else if let Some(api_key) = api_key {
            self.auth_manager
                .authenticate_websocket(None, Some(api_key.to_string()), None, None)
                .await
        } else {
            return Err(anyhow::anyhow!("No authentication credentials provided"));
        };

        match auth_result {
            Ok(result) => {
                // Record successful authentication
                self.security_manager.record_auth_success(
                    ip_addr,
                    result.user_id.clone(),
                    auth_method.clone(),
                );

                // Cache successful auth
                if let Some(token) = token {
                    self.security_manager.cache_auth(token, result.clone());
                }

                // Update connection with auth info
                let auth_context = result.to_auth_context();
                if let Err(e) = self
                    .connection_manager
                    .authenticate_connection(
                        connection_id,
                        result.user_id,
                        Some(result.auth_method.to_string()),
                        auth_context,
                    )
                    .await
                {
                    error!(
                        "Failed to update connection auth for {}: {}",
                        connection_id, e
                    );
                    return Err(e);
                }

                let success_msg = Message::connection_event(
                    "authenticated",
                    sonic_rs::json!({
                        "status": "success",
                        "auth_method": auth_method_display,
                        "timestamp": chrono::Utc::now().timestamp_millis()
                    }),
                );

                self.connection_manager
                    .send_to_connection(connection_id, success_msg)
                    .await?;

                info!(
                    " Authentication successful for connection {}",
                    connection_id
                );
                Ok(())
            }
            Err(e) => {
                // Record auth failure
                self.security_manager.record_auth_failure(
                    ip_addr,
                    user_id,
                    auth_method,
                    format!("Authentication failed: {e}"),
                );

                let error_msg =
                    Message::error_with_code("Authentication failed", Some("AUTH_FAILED"));
                self.connection_manager
                    .send_to_connection(connection_id, error_msg)
                    .await?;

                Err(e.into())
            }
        }
    }

    /// Periodic security cleanup task
    pub async fn start_security_cleanup_task(&self) {
        let security_manager = Arc::clone(&self.security_manager);
        let shutdown_tx = self.shutdown_tx.clone();

        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_tx.subscribe();
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        security_manager.cleanup();
                        debug!("Security cleanup performed");
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Security cleanup task shutting down");
                        break;
                    }
                }
            }
        });
    }
}

/// Server statistics for monitoring
#[derive(Debug, Clone)]
pub struct ServerStats {
    pub active_connections: usize,
    pub total_messages: u64,
    pub total_bytes: u64,
}

// Make WebSocketServer cloneable for use in async tasks
impl Clone for WebSocketServer {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            connection_manager: self.connection_manager.clone(),
            message_handler: self.message_handler.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            is_running: self.is_running.clone(),
            app_state: self.app_state.clone(),
            auth_manager: self.auth_manager.clone(),
            security: self.security.clone(),
            security_manager: self.security_manager.clone(),
            connection_info: self.connection_info.clone(),
            message_stats: self.message_stats.clone(),
            bytes_transferred: self.bytes_transferred.clone(),
            connection_pool_size: self.connection_pool_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::database::repositories::{ApiKeyRepository, SessionRepository, UserRepository};
    use crate::database::DatabaseManager;
    use crate::state::AppState;
    use deadpool_postgres::{Config, Runtime};
    use std::sync::Arc;
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
        let main_pool = main_cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .expect("Failed to create main pool for tests");

        let mut client_cfg = Config::new();
        client_cfg.host = Some("localhost".to_string());
        client_cfg.port = Some(5433);
        client_cfg.user = Some("postgres".to_string());
        client_cfg.password = Some("postgres".to_string());
        client_cfg.dbname = Some("actae".to_string());
        let client_pool = client_cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .expect("Failed to create client pool for tests");

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

        AppState {
            config: Arc::new(app_config),
            db_manager: db_manager.clone(),
            user_repo: Arc::new(UserRepository::new(db_manager.clone())),
            api_key_repo: Arc::new(ApiKeyRepository::new(db_manager.clone())),
            session_repo: Arc::new(SessionRepository::new(db_manager.clone())),
            batch_broadcaster: Arc::new(
                crate::websocket::batch_broadcaster::BatchBroadcaster::new(
                    crate::websocket::batch_broadcaster::BatchBroadcastConfig::default(),
                )
                .await
                .unwrap(),
            ),
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
    async fn test_server_creation() {
        let config = AppConfig::default();
        let app_state = create_mock_app_state().await;
        let server = WebSocketServer::new(
            &config,
            Arc::new(ConnectionManager::new(100, 1024)),
            app_state,
        )
        .await
        .unwrap();

        let stats = server.get_stats().await;
        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.total_messages, 0);
        assert_eq!(stats.total_bytes, 0);
    }

    #[test]
    fn test_server_stats_construction() {
        let stats = ServerStats {
            active_connections: 10,
            total_messages: 300,
            total_bytes: 3072,
        };
        assert_eq!(stats.active_connections, 10);
        assert_eq!(stats.total_messages, 300);
        assert_eq!(stats.total_bytes, 3072);
    }

    #[test]
    fn test_server_stats_empty() {
        let stats = ServerStats {
            active_connections: 0,
            total_messages: 0,
            total_bytes: 0,
        };
        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.total_messages, 0);
        assert_eq!(stats.total_bytes, 0);
    }

    #[test]
    fn test_server_stats_debug() {
        let stats = ServerStats {
            active_connections: 1,
            total_messages: 2,
            total_bytes: 3,
        };
        let _ = format!("{stats:?}");
    }

    #[test]
    fn test_server_stats_clone() {
        let stats = ServerStats {
            active_connections: 10,
            total_messages: 100,
            total_bytes: 1000,
        };
        let cloned = stats.clone();
        assert_eq!(cloned.active_connections, stats.active_connections);
    }

    #[tokio::test]
    async fn test_message_validation() {
        // Test message validation without requiring backplane
        let valid_msg = Message::Subscribe {
            topic: "test:topic".to_string(),
            cursor: None,
            timestamp: Some(123456789),
        };

        match valid_msg.validate(1024) {
            MessageValidation::Valid => {}
            other => {
                assert!(false, "Valid message failed validation, got {other:?}");
            }
        }

        // Test invalid message
        let invalid_msg = Message::Subscribe {
            topic: "".to_string(),
            cursor: None,
            timestamp: Some(123456789),
        };

        if invalid_msg.validate(1024) == MessageValidation::Valid {
            panic!("Empty topic should be invalid");
        }
    }
}
