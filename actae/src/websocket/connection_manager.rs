// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use super::backpressure::{
    BackPressureConfig, BackPressureManager, ConsumerStats, GlobalBackPressureStats,
};
use crate::auth::websocket_auth::AuthResult;
use crate::metrics;
use chrono::Utc;
use dashmap::{DashMap, DashSet};
use flume::{Receiver, Sender};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use super::batching::{BatchingConfig, BatchingManager};
use super::connection::{Connection, ConnectionMemoryStats, ConnectionPool, OutgoingMessage};
use super::message::Message;

/// Topics with more than this many subscribers switch to ring-buffer (broadcast) delivery.
/// The ring provides O(1) publish cost regardless of subscriber count.
const RING_FANOUT_THRESHOLD: usize = 100;

/// Default capacity for per-topic ring buffers (power of 2).
const RING_DEFAULT_CAPACITY: usize = 65536;

/// Connection manager for handling WebSocket connections
#[derive(Clone, Debug)]
pub struct ConnectionManager {
    pub(crate) connections: Arc<DashMap<Uuid, Connection>>,
    pub(crate) topics: Arc<DashMap<String, HashSet<Uuid>>>,
    /// Per-connection outgoing channel. Supports either a Message (Arc<Message>) or a
    /// pre-serialized payload (Arc<String>) to avoid re-serializing broadcasts.
    pub(crate) message_senders: Arc<DashMap<Uuid, Sender<OutgoingMessage>>>,
    /// Track consecutive send failures per connection for slow-consumer detection
    pub(crate) send_failures: Arc<DashMap<Uuid, u32>>,
    pub(crate) max_connections: usize,
    pub(crate) broadcast_capacity: usize,
    /// Back-pressure manager for handling slow consumers
    pub(crate) back_pressure_manager: Arc<BackPressureManager>,
    /// Batching manager for high-throughput scenarios
    pub(crate) batching_manager: Option<Arc<BatchingManager>>,
    /// Pre-allocated connection pool for optimization
    pub(crate) connection_pool: Arc<ConnectionPool>,

    // ── Ring-buffer (broadcast) fanout for high-subscriber topics (>100) ──
    /// Per-topic broadcast senders. Present only when subscriber count >= RING_FANOUT_THRESHOLD.
    /// Message delivery via ring is O(1) for the publisher vs O(N) with per-subscriber channels.
    pub(crate) topic_rings: Arc<DashMap<String, broadcast::Sender<Arc<String>>>>,

    /// EventStore for cursor-based replay on subscribe/resume.
    /// Uses interior-mutability (RwLock) so it can be set after construction
    /// while the ConnectionManager is already shared via Arc.
    pub(crate) event_store: Arc<RwLock<Option<Arc<crate::events::EventStore>>>>,

    /// Serializes reauthorize_all sweeps. The 30s credential_reauthorizer
    /// task and the control-plane notify watcher both call reauthorize_all
    /// and could otherwise run concurrently — duplicate DB queries, double
    /// AUTH_REVOKED frames, and interleaved principal updates.
    pub(crate) reauthorize_lock: Arc<tokio::sync::Mutex<()>>,
}

impl ConnectionManager {
    pub fn new(max_connections: usize, broadcast_capacity: usize) -> Self {
        // Create back-pressure configuration optimized for WebSocket connections
        let back_pressure_config = BackPressureConfig {
            max_pending_messages: broadcast_capacity / 2, // Allow up to half the queue to be pending
            max_queue_size: max_connections * broadcast_capacity, // Global queue limit
            slow_consumer_threshold: 10.0,                // 10 messages per second minimum
            max_consumer_lag: std::time::Duration::from_secs(30), // 30 second max lag
            measurement_window: std::time::Duration::from_secs(60), // 1 minute measurement window
            drop_messages_for_slow_consumers: true,       // Drop messages instead of blocking
            buffer_size: broadcast_capacity,              // Individual consumer buffer size
        };

        let back_pressure_manager = Arc::new(
            BackPressureManager::new(back_pressure_config)
                .expect("Failed to create back-pressure manager"),
        );

        // Create and pre-warm connection pool
        let connection_pool = Arc::new(ConnectionPool::new(50)); // Pre-allocate 50 connections

        Self {
            connections: Arc::new(DashMap::new()),
            topics: Arc::new(DashMap::new()),
            message_senders: Arc::new(DashMap::new()),
            send_failures: Arc::new(DashMap::new()),
            max_connections,
            broadcast_capacity,
            back_pressure_manager,
            batching_manager: None, // Will be initialized later if needed
            connection_pool,
            topic_rings: Arc::new(DashMap::new()),
            event_store: Arc::new(RwLock::new(None)),
            reauthorize_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Create a new optimized connection manager with pre-warmed pool
    pub async fn new_optimized(max_connections: usize, broadcast_capacity: usize) -> Self {
        let manager = Self::new(max_connections, broadcast_capacity);

        // Pre-warm the connection pool
        manager.connection_pool.prewarm().await;
        info!("Optimized connection manager initialized with pre-warmed pool");

        manager
    }

    /// Fast-path connection addition for authenticated users
    pub async fn add_authenticated_connection_optimized(
        &self,
        connection_id: Uuid,
        auth_result: AuthResult,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> anyhow::Result<Receiver<OutgoingMessage>> {
        // Check connection limit early
        if self.connections.len() >= self.max_connections {
            return Err(anyhow::anyhow!("Maximum connections reached"));
        }

        // Pre-allocate message channel
        let (sender, receiver) = flume::bounded(self.broadcast_capacity);

        // Try to get pre-allocated connection from pool
        let connection = match self.connection_pool.get_connection().await {
            Some(mut conn) => {
                // Configure the pre-allocated connection
                conn.id = connection_id;
                conn.user_id = Some(auth_result.user_id.clone());
                conn.ip_address = ip_address;
                conn.user_agent = user_agent;
                conn.connected_at = Utc::now();
                conn.last_activity = Utc::now();
                conn.is_authenticated = true;
                conn.auth_method = Some(auth_result.auth_method.to_string());
                conn.auth_context = Some(auth_result.to_auth_context());
                conn
            }
            None => {
                // Fallback: create new connection if pool is empty
                Connection {
                    id: connection_id,
                    user_id: Some(auth_result.user_id.clone()),
                    ip_address,
                    user_agent,
                    connected_at: Utc::now(),
                    last_activity: Utc::now(),
                    subscriptions: Arc::new(RwLock::new(HashSet::new())),
                    is_authenticated: true,
                    auth_method: Some(auth_result.auth_method.to_string()),
                    auth_context: Some(auth_result.to_auth_context()),
                    metadata: Arc::new(RwLock::new(sonic_rs::json!({
                        "auth_method": auth_result.auth_method,
                        "authenticated_at": Utc::now().timestamp_millis()
                    }))),
                    validated_write_topics: Arc::new(DashSet::new()),
                    validated_read_topics: Arc::new(DashSet::new()),
                    permission_cache_expiry: None,
                    topic_cursors: Arc::new(RwLock::new(HashMap::new())),
                }
            }
        };

        // ULTRA-PERFORMANCE FIX: Don't register as consumer during connection setup.
        // Connections are only registered as consumers when they SUBSCRIBE to topics.
        // This prevents broadcasters (publishers) from being throttled by backpressure.

        // Store connection and sender atomically
        self.connections.insert(connection_id, connection);
        self.message_senders.insert(connection_id, sender);

        crate::metrics::CONNECTED_CLIENTS.inc();

        info!(
            "Fast-path authenticated connection added: {} (user: {})",
            connection_id, auth_result.user_id
        );

        Ok(receiver)
    }

    /// Optimized connection addition with minimal overhead
    pub async fn add_connection_optimized(
        &self,
        connection_id: Uuid,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> anyhow::Result<Receiver<OutgoingMessage>> {
        // Check connection limit early
        if self.connections.len() >= self.max_connections {
            return Err(anyhow::anyhow!("Maximum connections reached"));
        }

        // Pre-allocate message channel
        let (sender, receiver) = flume::bounded(self.broadcast_capacity);

        // Try to get pre-allocated connection from pool
        let mut connection = match self.connection_pool.get_connection().await {
            Some(mut conn) => {
                // Configure the pre-allocated connection. Defensively clear
                // every principal field in case an older pool-reset path ever
                // misses one — a stale auth_context must never leak across
                // connections (audit item 1).
                conn.id = connection_id;
                conn.ip_address = ip_address;
                conn.user_agent = user_agent;
                conn.connected_at = Utc::now();
                conn.last_activity = Utc::now();
                conn.user_id = None;
                conn.is_authenticated = false;
                conn.auth_method = None;
                conn.auth_context = None;
                conn.validated_read_topics.clear();
                conn.validated_write_topics.clear();
                conn.permission_cache_expiry = None;
                conn
            }
            None => {
                // Fallback: create new connection if pool is empty
                Connection::anonymous(ip_address, user_agent)
            }
        };

        // Ensure the connection has the right ID
        connection.id = connection_id;

        // ULTRA-PERFORMANCE FIX: Don't register as consumer during connection setup.
        // Connections are only registered as consumers when they SUBSCRIBE to topics.
        // This prevents broadcasters (publishers) from being throttled by backpressure.

        // Store connection and sender atomically
        self.connections.insert(connection_id, connection);
        self.message_senders.insert(connection_id, sender);

        crate::metrics::CONNECTED_CLIENTS.inc();

        debug!("Optimized connection added: {}", connection_id);

        Ok(receiver)
    }

    /// Optimized connection removal with connection pooling
    pub async fn remove_connection_optimized(&self, connection_id: &Uuid) -> anyhow::Result<()> {
        // Get connection for potential reuse
        let connection = self.connections.get(connection_id).map(|c| c.clone());

        // Clear failure counter
        self.send_failures.remove(connection_id);

        // Remove from all topics
        let subscriptions = if let Some(connection) = &connection {
            connection.get_subscriptions().await
        } else {
            HashSet::new()
        };

        for topic in &subscriptions {
            self.unsubscribe_from_topic_internal(connection_id, topic)
                .await;
        }

        // Unregister from back-pressure manager (fire-and-forget)
        let consumer_id = connection_id.to_string();
        let bp_manager = self.back_pressure_manager.clone();
        tokio::spawn(async move {
            if let Err(e) = bp_manager.unregister_consumer(&consumer_id).await {
                warn!(
                    "Failed to unregister consumer from back-pressure manager: {}",
                    e
                );
            }
        });

        // Remove connection and sender
        self.connections.remove(connection_id);
        self.message_senders.remove(connection_id);

        // Return connection to pool for reuse if possible
        if let Some(conn) = connection {
            let pool = self.connection_pool.clone();
            tokio::spawn(async move {
                pool.return_connection(conn).await;
            });
        }

        crate::metrics::CONNECTED_CLIENTS.dec();
        debug!("Optimized connection removed: {}", connection_id);
        Ok(())
    }

    /// Get connection pool statistics
    pub async fn get_connection_pool_stats(&self) -> (usize, usize) {
        self.connection_pool.get_stats().await
    }

    /// Add a new connection - delegates to optimized version
    pub async fn add_connection(
        &self,
        connection_id: Uuid,
        connection: Connection,
    ) -> anyhow::Result<Receiver<OutgoingMessage>> {
        // Delegate to optimized version for better performance
        self.add_connection_optimized(
            connection_id,
            connection.ip_address.clone(),
            connection.user_agent.clone(),
        )
        .await
    }

    /// Remove a connection - delegates to optimized version
    pub async fn remove_connection(&self, connection_id: &Uuid) -> anyhow::Result<()> {
        // Delegate to optimized version for better performance and connection pooling
        self.remove_connection_optimized(connection_id).await
    }

    /// Get a connection
    pub async fn get_connection(&self, connection_id: &Uuid) -> Option<Connection> {
        self.connections.get(connection_id).map(|c| c.clone())
    }

    /// Authenticate a connection with a full authorization principal.
    ///
    /// The `AuthContext` is retained so every subsequent subscribe/publish on
    /// this socket is authorized with the real key scopes rather than a
    /// synthesized full-access permission list (audit items 1 and 3).
    pub async fn authenticate_connection(
        &self,
        connection_id: &Uuid,
        user_id: String,
        auth_method: Option<String>,
        auth_context: crate::auth::types::AuthContext,
    ) -> anyhow::Result<()> {
        if let Some(mut connection) = self.connections.get_mut(connection_id) {
            connection.authenticate_with(user_id, auth_method, auth_context);
            info!("Authenticated connection: {}", connection.info());
            Ok(())
        } else {
            Err(anyhow::anyhow!("Connection not found: {connection_id}"))
        }
    }

    /// Check if connection is authenticated
    pub async fn is_connection_authenticated(&self, connection_id: &Uuid) -> bool {
        self.connections
            .get(connection_id)
            .map(|c| c.is_authenticated())
            .unwrap_or(false)
    }

    /// Subscribe connection to topic
    #[instrument(level = "debug", skip(self), fields(topic, conn_id = %connection_id))]
    pub async fn subscribe_to_topic(
        &self,
        connection_id: &Uuid,
        topic: &str,
    ) -> anyhow::Result<bool> {
        // Check if connection exists and subscribe
        let subscribed = if let Some(connection) = self.connections.get(connection_id) {
            connection.subscribe(topic).await
        } else {
            return Err(anyhow::anyhow!("Connection not found: {connection_id}"));
        };

        if subscribed {
            // ULTRA-PERFORMANCE FIX: Register as consumer ONLY when subscribing to topics.
            // This prevents pure broadcasters from being throttled by backpressure.
            // CRITICAL: Registration MUST complete BEFORE subscription proceeds to avoid race condition.
            let consumer_id = connection_id.to_string();
            if let Err(e) = self
                .back_pressure_manager
                .register_consumer(consumer_id)
                .await
            {
                warn!(
                    "Failed to register consumer with back-pressure manager: {}",
                    e
                );
            }

            let subscriber_count = {
                let mut topic_subscribers = self.topics.entry(topic.to_string()).or_default();
                let _is_new_topic = topic_subscribers.is_empty();
                topic_subscribers.insert(*connection_id);

                topic_subscribers.len()
            };

            // Ring-buffer fanout activation: when subscriber count crosses the threshold,
            // create a broadcast channel for this topic and subscribe ALL existing connections.
            if subscriber_count >= RING_FANOUT_THRESHOLD {
                let is_new_ring = !self.topic_rings.contains_key(topic);
                if is_new_ring {
                    info!(
                        "Topic '{}' reached {} subscribers — activating ring-buffer fanout",
                        topic, RING_FANOUT_THRESHOLD
                    );
                    let (ring_tx, _) = broadcast::channel::<Arc<String>>(RING_DEFAULT_CAPACITY);
                    let topic_key = topic.to_string();

                    // Subscribe all existing subscribers to the ring
                    if let Some(subscribers) = self.topics.get(&topic_key) {
                        for sub_id in subscribers.iter() {
                            // Spawn a ring-reader task to bridge broadcast -> flume
                            if let Some(sender) = self.message_senders.get(sub_id) {
                                let rx = ring_tx.subscribe();
                                Self::spawn_ring_reader(*sub_id, rx, sender.value().clone());
                            }
                        }
                    }
                    self.topic_rings.insert(topic_key, ring_tx);
                } else {
                    // Topic already has a ring — attach new subscriber
                    if let Some(ring_tx) = self.topic_rings.get(topic) {
                        if let Some(sender) = self.message_senders.get(connection_id) {
                            let rx = ring_tx.subscribe();
                            Self::spawn_ring_reader(*connection_id, rx, sender.value().clone());
                        }
                    }
                }
            }

            debug!(
                "Connection {} subscribed to topic: {}",
                connection_id, topic
            );
        }

        Ok(subscribed)
    }

    /// Subscribe a connection to a topic with optional cursor-based resume.
    /// 1. Subscribes to the real-time topic (same as subscribe_to_topic)
    /// 2. If cursor is Some and EventStore is configured, replays events
    ///    from that cursor onward.
    ///
    /// Returns (subscribed, replayed_events, latest_cursor_at_subscription_time).
    pub async fn subscribe_with_cursor(
        &self,
        connection_id: &Uuid,
        topic: &str,
        cursor: Option<i64>,
    ) -> anyhow::Result<(
        bool,                                   // subscribed
        Vec<crate::events::schema::AgentEvent>, // replayed events
        i64,                                    // latest cursor
    )> {
        let subscribed = self.subscribe_to_topic(connection_id, topic).await?;

        let (replayed, latest_cursor) = if let Some(ref store) = *self.event_store.read().await {
            if let Some(from_cursor) = cursor {
                let request = crate::events::schema::ReplayRequest {
                    channel_id: topic.to_string(),
                    cursor: Some(from_cursor),
                    channel_cursor: None,
                    limit: Some(1000),
                    event_type: None,
                };
                match store.replay(&request).await {
                    Ok(events) => {
                        let latest = store
                            .get_latest_cursor(topic)
                            .await
                            .unwrap_or(None)
                            .unwrap_or(0);
                        (events, latest)
                    }
                    Err(e) => {
                        warn!(
                            "Failed to replay events for {} on topic {} (cursor={:?}): {}",
                            connection_id, topic, cursor, e
                        );
                        let latest = store
                            .get_latest_cursor(topic)
                            .await
                            .unwrap_or(None)
                            .unwrap_or(0);
                        (vec![], latest)
                    }
                }
            } else {
                let latest = store
                    .get_latest_cursor(topic)
                    .await
                    .unwrap_or(None)
                    .unwrap_or(0);
                (vec![], latest)
            }
        } else {
            (vec![], 0)
        };

        Ok((subscribed, replayed, latest_cursor))
    }

    /// Spawn a background task that bridges a broadcast (ring) receiver to a flume sender.
    /// This allows the ring's O(1) broadcast delivery to feed into the existing per-subscriber
    /// flume channel that `handle_outgoing` reads from — without modifying `handle_outgoing`.
    fn spawn_ring_reader(
        connection_id: Uuid,
        mut rx: broadcast::Receiver<Arc<String>>,
        tx: flume::Sender<OutgoingMessage>,
    ) {
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        // Non-blocking try_send. If the flume channel is full (subscriber slow),
                        // drop the message — same real-time semantics as batch broadcaster.
                        if tx.try_send(OutgoingMessage::Serialized(msg)).is_err() {
                            debug!("Ring reader {}: channel closed, exiting", connection_id);
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("Ring reader {}: broadcast channel closed", connection_id);
                        return;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        debug!(
                            "Ring reader {} lagged, skipped {} messages",
                            connection_id, n
                        );
                        continue;
                    }
                }
            }
        });
    }

    /// Unsubscribe connection from topic
    pub async fn unsubscribe_from_topic(
        &self,
        connection_id: &Uuid,
        topic: &str,
    ) -> anyhow::Result<bool> {
        // Check if connection exists and unsubscribe
        let unsubscribed = if let Some(connection) = self.connections.get(connection_id) {
            connection.unsubscribe(topic).await
        } else {
            return Err(anyhow::anyhow!("Connection not found: {connection_id}"));
        };

        if unsubscribed {
            self.unsubscribe_from_topic_internal(connection_id, topic)
                .await;
            debug!(
                "Connection {} unsubscribed from topic: {}",
                connection_id, topic
            );
        }

        Ok(unsubscribed)
    }

    /// Internal unsubscribe helper
    async fn unsubscribe_from_topic_internal(&self, connection_id: &Uuid, topic: &str) {
        if let Some(mut subscribers) = self.topics.get_mut(topic) {
            subscribers.remove(connection_id);
            if subscribers.is_empty() {
                // Remove the topic entry if no subscribers remain
                drop(subscribers); // Release the mutable reference
                self.topics.remove(topic);
                // Clean up ring for this topic
                self.topic_rings.remove(topic);
            }
        }
    }

    /// Cache validated permissions for a connection (called at subscription time)
    pub async fn cache_validated_permissions(
        &self,
        connection_id: &Uuid,
        topic: &str,
        can_read: bool,
        can_write: bool,
    ) {
        if let Some(mut conn_ref) = self.connections.get_mut(connection_id) {
            if can_read {
                conn_ref.validated_read_topics.insert(topic.to_string());
            }
            if can_write {
                conn_ref.validated_write_topics.insert(topic.to_string());
            }
            // Set or refresh a permission cache expiry (1 hour from now) to allow dynamic permission revocation
            conn_ref.permission_cache_expiry = Some(Utc::now() + chrono::Duration::hours(1));
        }
    }

    /// Remove cached permissions when unsubscribing
    pub async fn remove_cached_permissions(&self, connection_id: &Uuid, topic: &str) {
        if let Some(conn_ref) = self.connections.get_mut(connection_id) {
            let _ = conn_ref.validated_read_topics.remove(topic);
            let _ = conn_ref.validated_write_topics.remove(topic);
        }
    }

    /// Invalidate cached permissions for a specific topic across all connections.
    /// This is used by admin hooks / pubsub when RBAC rules change.
    pub async fn invalidate_permissions_for_topic(&self, topic: &str) {
        let mut invalidated = 0usize;
        for mut entry in self.connections.iter_mut() {
            let conn = entry.value_mut();
            let removed_read = conn.validated_read_topics.remove(topic);
            let removed_write = conn.validated_write_topics.remove(topic);
            if removed_read.is_some() || removed_write.is_some() {
                conn.permission_cache_expiry = None;
                invalidated += 1;
            }
        }
        info!(
            "Invalidated permission cache for topic '{}' on {} connections",
            topic, invalidated
        );
    }

    /// Invalidate cached permissions for a specific user across all connections.
    pub async fn invalidate_permissions_for_user(&self, user_id: &str) {
        let mut invalidated = 0usize;
        for mut entry in self.connections.iter_mut() {
            let conn = entry.value_mut();
            if let Some(uid) = &conn.user_id {
                if uid == user_id {
                    conn.validated_read_topics.clear();
                    conn.validated_write_topics.clear();
                    conn.permission_cache_expiry = None;
                    invalidated += 1;
                }
            }
        }
        info!(
            "Invalidated permission cache for user '{}' on {} connections",
            user_id, invalidated
        );
    }

    /// Re-authorize every authenticated connection (audit item 11).
    ///
    /// Sockets authenticated with an API key are re-checked against the DB;
    /// revoked, missing, or expired keys are de-authenticated (principal
    /// cleared, subscriptions dropped) and sent an error frame. Called by the
    /// periodic sweep and immediately after control-plane syncs. Returns the
    /// Re-authorize every authenticated connection (audit item 11 + scope
    /// reduction). Called by the periodic sweep and immediately after
    /// control-plane syncs.
    ///
    /// For each API-key-authenticated socket it re-resolves the FULL key row
    /// (status, permissions, expiry) and then either:
    /// - **de-authenticates** a revoked/expired/deleted key (principal
    ///   cleared, subscriptions dropped, `AUTH_REVOKED` error frame), or
    /// - **atomically replaces** the connection's `AuthContext` with the
    ///   key's CURRENT permissions, drops subscriptions no longer covered by
    ///   the new read scope, and clears permission caches — so a scope
    ///   reduction (write → read-only, or a narrower channel set) takes
    ///   effect on open sockets without a reconnect.
    ///
    /// Returns the number of connections whose principal changed (revoked or
    /// re-scoped).
    ///
    /// DB-failure policy (explicit): if the pool cannot hand out a
    /// connection, or the single batched key query fails, the pass is
    /// ABORTED and an error is logged — the next sweep tick retries. We never
    /// blindly keep sockets on unverifiable permissions for long (a 30s
    /// retry), and we never kill every socket on a transient DB error (which
    /// would be its own DoS). Keys are fetched in ONE `ANY` query per pass
    /// (not N queries), so the sweep is O(keys + sockets) rather than
    /// O(sockets) round trips. New authentications are unaffected — they
    /// always hit the DB and fail closed.
    pub async fn reauthorize_all(&self, app_state: &crate::state::AppState) -> usize {
        // Serialize with any concurrently-running sweep (the 30s reauthorizer
        // task and the control-plane notify watcher). Held for the whole pass
        // so no two sweeps interleave principal updates or duplicate frames.
        let _guard = self.reauthorize_lock.lock().await;

        let connection_ids: Vec<Uuid> = self.connections.iter().map(|e| *e.key()).collect();
        let mut changed = 0usize;

        // Pass 1: collect the key id for every API-key-authenticated socket
        // (the only sockets this sweep revalidates) plus its current
        // principal for the change-detection log.
        let mut key_ids: Vec<String> = Vec::new();
        let mut conn_keys: Vec<(Uuid, String, Option<crate::auth::types::AuthContext>)> =
            Vec::new();
        for id in connection_ids {
            let conn = self.connections.get(&id);
            let Some(ac) = conn.and_then(|c| c.auth_context.clone()) else {
                continue;
            };
            let Some(key_id) = ac.key_id().map(str::to_string) else {
                continue;
            };
            key_ids.push(key_id.clone());
            conn_keys.push((id, key_id, Some(ac)));
        }
        if conn_keys.is_empty() {
            return 0;
        }
        key_ids.sort();
        key_ids.dedup();

        // Pass 2: fetch every key in ONE round trip (was N queries per
        // sweep). Acquisition failure aborts the pass (policy below); a
        // batch-query error aborts it too — with one query there is no
        // per-key fallback, and we must not leave a partially-revalidated
        // principal set.
        let client = match app_state.db_manager.get_client_connection().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("reauthorize_all: DB unavailable, aborting sweep pass: {e:#}");
                return changed;
            }
        };
        let rows = match client
            .query(
                "SELECT * FROM public.api_keys WHERE id = ANY($1)",
                &[&key_ids],
            )
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!(
                    "reauthorize_all: batch key query failed, aborting sweep pass: {e:#}"
                );
                return changed;
            }
        };
        let keys: std::collections::HashMap<String, crate::database::models::ApiKey> = rows
            .into_iter()
            .map(|r| {
                let k = crate::database::models::ApiKey::from(r);
                (k.id.clone(), k)
            })
            .collect();

        // Pass 3: apply each socket's current principal.
        for (id, key_id, old_auth_context) in conn_keys {
            // Key deleted → de-authenticate.
            let Some(api_key) = keys.get(&key_id) else {
                let _ = self.deauthenticate(&id).await;
                changed += 1;
                continue;
            };

            let active = api_key.status == "active"
                && api_key
                    .expires_at
                    .map(|exp| exp > chrono::Utc::now())
                    .unwrap_or(true);

            if !active {
                let _ = self.deauthenticate(&id).await;
                changed += 1;
                continue;
            }

            // Active key: build the CURRENT principal and atomically replace
            // the connection's AuthContext. Even an unchanged scope replaces
            // the object so any future permission change is picked up next
            // sweep.
            let new_auth = crate::auth::types::AuthContext::ApiKey {
                key_id: api_key.id.clone(),
                key_source: api_key.source.clone(),
                expires_at: api_key.expires_at,
                permissions: crate::auth::parse_api_key_permissions(&api_key.permissions),
            };
            {
                let mut guard = self.connections.get_mut(&id);
                if let Some(conn) = guard.as_deref_mut() {
                    conn.auth_context = Some(new_auth.clone());
                    conn.validated_read_topics.clear();
                    conn.validated_write_topics.clear();
                    conn.permission_cache_expiry = None;
                }
            }

            // Drop subscriptions the new (possibly narrower) read scope no
            // longer covers. Short-lived guard: clone the Arc, await the
            // read after the guard is released.
            let subs = match self.connections.get(&id) {
                Some(c) => c.subscriptions.clone(),
                None => continue,
            };
            let topics: Vec<String> = subs.read().await.iter().cloned().collect();
            for topic in topics {
                if !new_auth.can_read(&topic) {
                    let _ = self.unsubscribe_from_topic(&id, &topic).await;
                }
            }

            // Re-scoped (scope may be unchanged, but the principal is fresh).
            let old_changed = old_auth_context.map(|old| old != new_auth).unwrap_or(true);
            if old_changed {
                tracing::info!(
                    "Connection {} re-authorized (key {} permissions refreshed)",
                    id,
                    key_id
                );
            }
            changed += 1;
        }
        changed
    }

    /// De-authenticate a single connection: clear its principal, drop all
    /// subscriptions, and send an `AUTH_REVOKED` error frame.
    async fn deauthenticate(&self, connection_id: &Uuid) {
        // Clear principal + caches (short-lived guard: no awaits while held).
        {
            let mut guard = self.connections.get_mut(connection_id);
            if let Some(conn) = guard.as_deref_mut() {
                conn.auth_context = None;
                conn.is_authenticated = false;
                conn.user_id = None;
                conn.validated_read_topics.clear();
                conn.validated_write_topics.clear();
            }
        }
        // Drop subscriptions (clone the Arc first so no guard is held across
        // the await).
        let subs = match self.connections.get(connection_id) {
            Some(c) => c.subscriptions.clone(),
            None => return,
        };
        let topics: Vec<String> = subs.read().await.iter().cloned().collect();
        for topic in topics {
            let _ = self.unsubscribe_from_topic(connection_id, &topic).await;
        }

        let error_msg = crate::websocket::message::Message::error_with_code(
            "Authentication revoked or expired — re-authenticate",
            Some("AUTH_REVOKED"),
        );
        let _ = self.send_to_connection(connection_id, error_msg).await;
        tracing::info!(
            "Connection {} de-authenticated (key revoked/expired/deleted)",
            connection_id
        );
    }

    /// Invalidate all cached permissions across all connections (full cache flush)
    pub async fn invalidate_all_permissions(&self) {
        for mut entry in self.connections.iter_mut() {
            let conn = entry.value_mut();
            conn.validated_read_topics.clear();
            conn.validated_write_topics.clear();
            conn.permission_cache_expiry = None;
        }
        info!("Invalidated all connection permission caches");
    }

    /// Check whether a connection has cached write permission for a topic
    pub async fn has_write_permission_cached(&self, connection_id: &Uuid, topic: &str) -> bool {
        if let Some(mut conn) = self.connections.get_mut(connection_id) {
            // Check cache expiry and clear if expired
            if let Some(expiry) = conn.permission_cache_expiry {
                if Utc::now() > expiry {
                    // Clear cached permissions
                    conn.validated_read_topics.clear();
                    conn.validated_write_topics.clear();
                    conn.permission_cache_expiry = None;
                    return false;
                }
            }

            return conn.validated_write_topics.contains(topic);
        }

        false
    }

    /// Get total connection count
    pub async fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Get topic subscriber count
    pub async fn topic_subscriber_count(&self, topic: &str) -> usize {
        self.topics.get(topic).map(|subs| subs.len()).unwrap_or(0)
    }

    /// Get all connections subscribed to a topic (clones full Connection objects)
    pub async fn get_topic_subscribers(&self, topic: &str) -> Vec<Arc<Connection>> {
        let subscriber_ids: Vec<Uuid> = self
            .topics
            .get(topic)
            .map(|subs| subs.iter().copied().collect())
            .unwrap_or_default();

        subscriber_ids
            .into_iter()
            .filter_map(|id| self.connections.get(&id).map(|conn| Arc::new(conn.clone())))
            .collect()
    }

    /// ULTRA-FAST: Get only subscriber UUIDs without cloning Connection objects.
    /// Use this for high-throughput broadcast to avoid per-subscriber allocation.
    pub fn get_topic_subscriber_ids(&self, topic: &str) -> Vec<Uuid> {
        self.topics
            .get(topic)
            .map(|subs| {
                let ids: Vec<Uuid> = subs.iter().copied().collect();
                ids
            })
            .unwrap_or_default()
    }

    /// Update the tracked cursor for a topic on this connection.
    pub async fn update_topic_cursor(&self, connection_id: &Uuid, topic: &str, cursor: i64) {
        if let Some(conn) = self.connections.get_mut(connection_id) {
            conn.topic_cursors
                .write()
                .await
                .insert(topic.to_string(), cursor);
        }
    }

    /// Get the tracked cursor for a topic on a connection, if any.
    pub async fn get_topic_cursor(&self, connection_id: &Uuid, topic: &str) -> Option<i64> {
        if let Some(conn) = self.connections.get(connection_id) {
            conn.topic_cursors.read().await.get(topic).copied()
        } else {
            None
        }
    }

    /// Get topic statistics
    pub async fn get_topic_stats(&self) -> HashMap<String, usize> {
        self.topics
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().len()))
            .collect()
    }

    /// Clean up inactive connections with enhanced leak detection
    pub async fn cleanup_inactive_connections(&self, timeout: std::time::Duration) -> usize {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(timeout).unwrap_or_else(|_| chrono::Duration::minutes(5));

        let inactive_connections: Vec<Uuid> = self
            .connections
            .iter()
            .filter(|entry| entry.value().last_activity < cutoff)
            .map(|entry| *entry.key())
            .collect();

        let count = inactive_connections.len();
        for connection_id in inactive_connections {
            // Check for potential resource leaks before removal
            if let Some(connection) = self.connections.get(&connection_id) {
                let sub_count = connection.subscriptions.read().await.len();
                if sub_count > 0 {
                    warn!(
                        "Connection {} has {} subscriptions that will be cleaned up",
                        connection_id, sub_count
                    );
                }
            }

            if let Err(e) = self.remove_connection(&connection_id).await {
                error!(
                    "Failed to remove inactive connection {}: {}",
                    connection_id, e
                );
            }
        }

        if count > 0 {
            info!("Cleaned up {} inactive connections", count);
            // Log potential memory leaks
            self.detect_potential_leaks().await;
        }

        count
    }

    /// Detect potential memory leaks in connection management
    async fn detect_potential_leaks(&self) {
        let connection_count = self.connections.len();
        let topic_count = self.topics.len();
        let sender_count = self.message_senders.len();
        let failure_count = self.send_failures.len();

        // Check for mismatched counts that might indicate leaks
        if connection_count != sender_count {
            warn!(
                "Connection count mismatch: {} connections vs {} senders",
                connection_count, sender_count
            );
        }

        if failure_count > connection_count {
            warn!(
                "Excessive failure tracking: {} failures for {} connections",
                failure_count, connection_count
            );
        }

        // Check for orphaned topics (topics with no subscribers)
        let mut orphaned_topics = Vec::new();
        for topic_entry in self.topics.iter() {
            if topic_entry.value().is_empty() {
                orphaned_topics.push(topic_entry.key().clone());
            }
        }

        if !orphaned_topics.is_empty() {
            warn!(
                "Found {} orphaned topics: {:?}",
                orphaned_topics.len(),
                orphaned_topics
            );
            // Clean up orphaned topics
            for topic in orphaned_topics {
                self.topics.remove(&topic);
            }
        }

        // Log memory usage statistics
        debug!(
            "Memory usage: {} connections, {} topics, {} senders, {} failures",
            connection_count, topic_count, sender_count, failure_count
        );
    }

    /// Clean up all connections (for shutdown)
    pub async fn cleanup_all_connections(&self) -> usize {
        let all_connections: Vec<Uuid> =
            self.connections.iter().map(|entry| *entry.key()).collect();

        let count = all_connections.len();
        for connection_id in all_connections {
            if let Err(e) = self.remove_connection(&connection_id).await {
                error!("Failed to remove connection {}: {}", connection_id, e);
            }
        }

        count
    }

    /// Start background monitoring for back-pressure management
    pub async fn start_back_pressure_monitoring(&self) -> tokio::task::JoinHandle<()> {
        self.back_pressure_manager.start_monitoring().await
    }

    /// Get back-pressure statistics for monitoring
    pub async fn get_back_pressure_stats(&self) -> GlobalBackPressureStats {
        self.back_pressure_manager.get_global_stats().await
    }

    /// Get back-pressure statistics for a specific connection
    pub async fn get_connection_back_pressure_stats(
        &self,
        connection_id: &Uuid,
    ) -> Option<ConsumerStats> {
        let consumer_id = connection_id.to_string();
        self.back_pressure_manager
            .get_consumer_stats(&consumer_id)
            .await
    }

    /// Enable batching for high-throughput scenarios
    pub fn enable_batching(&self, config: BatchingConfig) -> Result<(), String> {
        // For now, just log that batching would be enabled
        info!(
            "Batching enabled with config: max_batch_size={}, max_delay={:?}",
            config.max_batch_size, config.max_batch_delay
        );
        Ok(())
    }

    pub async fn try_batch_message(
        &self,
        topic: &str,
        message: Arc<Message>,
    ) -> Result<bool, String> {
        if let Some(ref batching_manager) = self.batching_manager {
            batching_manager.add_message(topic, message).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn force_cleanup_connection(&self, connection_id: &Uuid) -> anyhow::Result<()> {
        warn!("Force cleaning up connection {}", connection_id);
        self.connections.remove(connection_id);
        self.message_senders.remove(connection_id);
        self.send_failures.remove(connection_id);
        let topics_to_check: Vec<String> = self
            .topics
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for topic in topics_to_check {
            if let Some(mut subscribers) = self.topics.get_mut(&topic) {
                subscribers.remove(connection_id);
                if subscribers.is_empty() {
                    drop(subscribers);
                    self.topics.remove(&topic);
                }
            }
        }
        let consumer_id = connection_id.to_string();
        let _ = self
            .back_pressure_manager
            .unregister_consumer(&consumer_id)
            .await;
        metrics::CONNECTED_CLIENTS.dec();
        Ok(())
    }

    pub async fn get_memory_stats(&self) -> ConnectionMemoryStats {
        let connection_count = self.connections.len();
        let topic_count = self.topics.len();
        let sender_count = self.message_senders.len();
        let failure_count = self.send_failures.len();
        let mut total_subscriptions = 0;
        for connection in self.connections.iter() {
            total_subscriptions += connection.subscriptions.read().await.len();
        }
        let avg_subscriptions = if connection_count > 0 {
            total_subscriptions as f64 / connection_count as f64
        } else {
            0.0
        };
        ConnectionMemoryStats {
            connection_count,
            topic_count,
            sender_count,
            failure_count,
            total_subscriptions,
            avg_subscriptions,
            max_connections: self.max_connections,
            utilization_percent: (connection_count as f64 / self.max_connections as f64) * 100.0,
        }
    }

    pub async fn monitor_connection_timeouts(&self, timeout: std::time::Duration) -> Vec<Uuid> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(timeout).unwrap_or_else(|_| chrono::Duration::minutes(5));
        let timed_out_connections: Vec<Uuid> = self
            .connections
            .iter()
            .filter(|entry| entry.value().last_activity < cutoff)
            .map(|entry| *entry.key())
            .collect();
        if !timed_out_connections.is_empty() {
            warn!(
                "Found {} connections that haven't been active since {}",
                timed_out_connections.len(),
                cutoff
            );
        }
        timed_out_connections
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_manager_new() {
        let cm = ConnectionManager::new(100, 256);
        assert_eq!(cm.max_connections, 100);
        assert_eq!(cm.broadcast_capacity, 256);
        assert!(cm.connections.is_empty());
        assert!(cm.topics.is_empty());
    }

    #[tokio::test]
    async fn test_connection_manager_new_optimized() {
        let cm = ConnectionManager::new_optimized(50, 128).await;
        assert_eq!(cm.max_connections, 50);
        assert_eq!(cm.broadcast_capacity, 128);
        let (available, _) = cm.get_connection_pool_stats().await;
        assert_eq!(available, 50);
    }

    #[tokio::test]
    async fn test_connection_count_starts_zero() {
        let cm = ConnectionManager::new(100, 256);
        assert_eq!(cm.connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_topic_subscriber_count_empty() {
        let cm = ConnectionManager::new(100, 256);
        let count = cm.topic_subscriber_count("nonexistent").await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_get_topic_subscribers_empty() {
        let cm = ConnectionManager::new(100, 256);
        let subscribers = cm.get_topic_subscribers("nonexistent").await;
        assert!(subscribers.is_empty());
    }

    #[test]
    fn test_get_topic_subscriber_ids_empty() {
        let cm = ConnectionManager::new(100, 256);
        let ids = cm.get_topic_subscriber_ids("nonexistent");
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn test_get_topic_stats_empty() {
        let cm = ConnectionManager::new(100, 256);
        let stats = cm.get_topic_stats().await;
        assert!(stats.is_empty());
    }

    #[tokio::test]
    async fn test_get_memory_stats_initial() {
        let cm = ConnectionManager::new(100, 256);
        let stats = cm.get_memory_stats().await;
        assert_eq!(stats.connection_count, 0);
        assert_eq!(stats.topic_count, 0);
        assert_eq!(stats.max_connections, 100);
        assert!((stats.utilization_percent - 0.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_get_connection_pool_stats() {
        let cm = ConnectionManager::new(100, 256);
        let (available, pool_size) = cm.get_connection_pool_stats().await;
        assert_eq!(available, 0);
        assert_eq!(pool_size, 50); // default pool size
    }

    #[tokio::test]
    async fn test_enable_batching() {
        let cm = ConnectionManager::new(100, 256);
        let config = BatchingConfig {
            max_batch_size: 100,
            max_batch_delay: std::time::Duration::from_millis(50),
            max_memory_usage: 1024 * 1024,
            enable_broadcast_batching: true,
            enable_database_batching: false,
            enable_direct_batching: false,
        };
        assert!(cm.enable_batching(config).is_ok());
    }

    #[tokio::test]
    async fn test_connection_manager_clone() {
        let cm = ConnectionManager::new(100, 256);
        let cloned = cm.clone();
        assert_eq!(cloned.max_connections, 100);
        assert_eq!(cloned.connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_add_and_get_connection() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("user-1".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        assert_eq!(cm.connection_count().await, 1);
        let retrieved = cm.get_connection(&id).await;
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_get_connection_nonexistent() {
        let cm = ConnectionManager::new(100, 256);
        let retrieved = cm.get_connection(&Uuid::new_v4()).await;
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_is_connection_authenticated() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("user-1".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        assert!(cm.is_connection_authenticated(&id).await);
    }

    #[tokio::test]
    async fn test_is_connection_authenticated_false() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::anonymous(None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        assert!(!cm.is_connection_authenticated(&id).await);
    }

    #[tokio::test]
    async fn test_monitor_connection_timeouts_empty() {
        let cm = ConnectionManager::new(100, 256);
        let timed_out = cm
            .monitor_connection_timeouts(std::time::Duration::from_secs(1))
            .await;
        assert!(timed_out.is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_inactive_connections_empty() {
        let cm = ConnectionManager::new(100, 256);
        let count = cm
            .cleanup_inactive_connections(std::time::Duration::from_secs(1))
            .await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_cleanup_all_connections_empty() {
        let cm = ConnectionManager::new(100, 256);
        let count = cm.cleanup_all_connections().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_add_remove_connection() {
        let cm = ConnectionManager::new(100, 256);
        let (sender, _receiver) = flume::bounded::<OutgoingMessage>(256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.message_senders.insert(id, sender);
        assert_eq!(cm.connection_count().await, 1);
        cm.remove_connection(&id).await.unwrap();
        assert_eq!(cm.connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_get_topic_cursor_nonexistent() {
        let cm = ConnectionManager::new(100, 256);
        let cursor = cm.get_topic_cursor(&Uuid::new_v4(), "topic").await;
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn test_update_topic_cursor() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.update_topic_cursor(&id, "topic-1", 42).await;
        let cursor = cm.get_topic_cursor(&id, "topic-1").await;
        assert_eq!(cursor, Some(42));
    }

    #[tokio::test]
    async fn test_force_cleanup_connection() {
        let cm = ConnectionManager::new(100, 256);
        let (sender, _receiver) = flume::bounded::<OutgoingMessage>(256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.message_senders.insert(id, sender);
        cm.force_cleanup_connection(&id).await.unwrap();
        assert!(cm.get_connection(&id).await.is_none());
    }

    #[tokio::test]
    async fn test_subscribe_and_get_topic_subscribers() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.subscribe_to_topic(&id, "topic-a").await.unwrap();
        assert_eq!(cm.topic_subscriber_count("topic-a").await, 1);
        assert_eq!(cm.topic_subscriber_count("topic-b").await, 0);
    }

    #[tokio::test]
    async fn test_subscribe_twice_same_topic() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.subscribe_to_topic(&id, "topic").await.unwrap();
        cm.subscribe_to_topic(&id, "topic").await.unwrap();
        assert_eq!(cm.topic_subscriber_count("topic").await, 1);
    }

    #[tokio::test]
    async fn test_unsubscribe_from_topic() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.subscribe_to_topic(&id, "topic").await.unwrap();
        cm.unsubscribe_from_topic(&id, "topic").await.unwrap();
        assert_eq!(cm.topic_subscriber_count("topic").await, 0);
    }

    #[tokio::test]
    async fn test_get_topic_subscriber_ids() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.subscribe_to_topic(&id, "topic").await.unwrap();
        let ids = cm.get_topic_subscriber_ids("topic");
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], id);
    }

    #[tokio::test]
    async fn test_get_topic_stats_with_data() {
        let cm = ConnectionManager::new(100, 256);
        let conn1 = Connection::new(Some("u1".into()), None, None);
        let conn2 = Connection::new(Some("u2".into()), None, None);
        let id1 = conn1.id;
        let id2 = conn2.id;
        cm.connections.insert(id1, conn1);
        cm.connections.insert(id2, conn2);
        cm.subscribe_to_topic(&id1, "topic-a").await.unwrap();
        cm.subscribe_to_topic(&id2, "topic-a").await.unwrap();
        cm.subscribe_to_topic(&id1, "topic-b").await.unwrap();
        let stats = cm.get_topic_stats().await;
        assert_eq!(stats.get("topic-a").copied().unwrap_or(0), 2);
        assert_eq!(stats.get("topic-b").copied().unwrap_or(0), 1);
    }

    #[tokio::test]
    async fn test_remove_connection_cleans_subscriptions() {
        let cm = ConnectionManager::new(100, 256);
        let (sender, _receiver) = flume::bounded::<OutgoingMessage>(256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.message_senders.insert(id, sender);
        cm.subscribe_to_topic(&id, "topic").await.unwrap();
        cm.remove_connection(&id).await.unwrap();
        assert_eq!(cm.topic_subscriber_count("topic").await, 0);
    }

    #[tokio::test]
    async fn test_cache_validated_permissions_and_check() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.cache_validated_permissions(&id, "chat:general", true, true)
            .await;
        assert!(cm.has_write_permission_cached(&id, "chat:general").await);
        assert!(!cm.has_write_permission_cached(&id, "other").await);
    }

    #[tokio::test]
    async fn test_remove_cached_permissions() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.cache_validated_permissions(&id, "topic", true, true)
            .await;
        assert!(cm.has_write_permission_cached(&id, "topic").await);
        cm.remove_cached_permissions(&id, "topic").await;
        assert!(!cm.has_write_permission_cached(&id, "topic").await);
    }

    #[tokio::test]
    async fn test_invalidate_permissions_for_topic() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.cache_validated_permissions(&id, "topic", true, true)
            .await;
        cm.invalidate_permissions_for_topic("topic").await;
        assert!(!cm.has_write_permission_cached(&id, "topic").await);
    }

    #[tokio::test]
    async fn test_invalidate_all_permissions() {
        let cm = ConnectionManager::new(100, 256);
        let conn = Connection::new(Some("u".into()), None, None);
        let id = conn.id;
        cm.connections.insert(id, conn);
        cm.cache_validated_permissions(&id, "t1", true, true).await;
        cm.cache_validated_permissions(&id, "t2", true, true).await;
        cm.invalidate_all_permissions().await;
        assert!(!cm.has_write_permission_cached(&id, "t1").await);
        assert!(!cm.has_write_permission_cached(&id, "t2").await);
    }
}
