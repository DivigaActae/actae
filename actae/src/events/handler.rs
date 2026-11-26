// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use super::schema::{
    AddExperimentMemberRequest, AgentEvent, ForkInfo, CancelExecutionRequest, CausalGraph,
    ChannelMetadata, ClaimExecutionRequest, ClaimedWork, CompareResponse, CompleteExecutionRequest,
    CreateExperimentRequest, CreateWakeupRequest, DecisionTrail, EventMetadata,
    ExecutionClaimResult, ExecutionGroup, ExecutionGroupMember, ExecutionInfo, ExperimentGroup,
    FailExecutionRequest,
    ForkReplayResult, ForkRequest, GroupForkReceipt, GroupInfo, GroupMessage, GroupOffsetInfo,
    HeartbeatExecutionRequest, MemberLease, PublishEventRequest, QueryRequest,
    RankedExperimentMember, ReplayRequest, SetOutcomeRequest, StateDiffResponse, TransitionRequest,
    UpdateMetadataRequest, WakeupInfo,
};
use super::store::{ChannelStat, EventStats, EventStore, EventStoreError};
use crate::broadcast::delivery_tracker::DeliveryTracker;
use crate::websocket::connection::OutgoingMessage;
use crate::websocket::ConnectionManager;

const DEFAULT_REPLAY_LIMIT: i64 = 100;
const MAX_REPLAY_LIMIT: i64 = 5000;

const MAX_EVENT_SIZE: usize = 1024 * 1024;
const MAX_CHANNEL_LEN: usize = 256;
const MAX_EVENT_TYPE_LEN: usize = 128;
const MAX_ACTOR_LEN: usize = 256;

/// Validate a channel id against the shared grammar (audit item 10).
///
/// Channel ids must be 1..=256 bytes using only `[A-Za-z0-9._:-]` — no `/`,
/// no whitespace, no control characters. This keeps every channel id
/// addressable as a single URL path segment on every SDK transport and
/// prevents the create-then-unreachable failure mode. Human-readable names
/// belong in metadata / `display_name`, not the id.
pub fn validate_channel_id(channel_id: &str) -> Result<(), EventError> {
    if channel_id.is_empty() {
        return Err(EventError::InvalidRequest(
            "channel_id is required".to_string(),
        ));
    }
    if channel_id.len() > MAX_CHANNEL_LEN {
        return Err(EventError::InvalidRequest(format!(
            "channel_id exceeds maximum length of {MAX_CHANNEL_LEN}"
        )));
    }
    if !channel_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "._:-".contains(c))
    {
        return Err(EventError::InvalidRequest(format!(
            "channel_id '{channel_id}' contains reserved characters — use [A-Za-z0-9._:-] only (no slashes or whitespace)"
        )));
    }
    Ok(())
}

/// Handles the record/subscribe/replay/query operations for agent events.
/// This is the primary API surface exposed to SDK clients.
pub struct EventHandler {
    store: Arc<EventStore>,
    connection_manager: Arc<ConnectionManager>,
    delivery_tracker: Arc<DeliveryTracker>,
    allow_anonymous: bool,
    /// Effective federation source policy (PR-009): per-subscriber live
    /// transformation is applied INSIDE the source instance before bytes
    /// leave it, so a gateway-side copy is never the privacy boundary.
    control_plane: Arc<crate::control_plane::ControlPlaneManager>,
}

impl EventHandler {
    async fn enforce_group_fork_channel_policy(
        &self,
        channel_id: &str,
        event_type: &str,
        payload: &sonic_rs::Value,
    ) -> Result<Option<String>, EventError> {
        let Some((fork_group_id, member_id, _source_channel, member_policy, _tool_policy)) =
            self.store.group_fork_tool_context(channel_id, "").await?
        else {
            return Ok(None);
        };
        if member_policy == "frozen" {
            let boundary = format!("member:{member_id}:event:{event_type}");
            self.store.block_group_fork(&fork_group_id, &boundary, &sonic_rs::json!({"channel_id":channel_id,"event_type":event_type,"payload":payload})).await?;
            return Err(EventError::CounterfactualBlocked {
                fork_group_id,
                boundary,
            });
        }
        Ok(Some(fork_group_id))
    }

    pub fn new(
        store: Arc<EventStore>,
        connection_manager: Arc<ConnectionManager>,
        delivery_tracker: Arc<DeliveryTracker>,
        control_plane: Arc<crate::control_plane::ControlPlaneManager>,
    ) -> Self {
        Self {
            store,
            connection_manager,
            delivery_tracker,
            allow_anonymous: false,
            control_plane,
        }
    }

    /// Enable anonymous access for dev/testing.
    pub fn with_anonymous(mut self, allow: bool) -> Self {
        self.allow_anonymous = allow;
        self
    }

    /// Record (publish) an agent event with a per-channel sequential cursor.
    ///
    /// 1. Validates the auth context
    /// 2. Validates event structure (size limits, required fields)
    /// 3. Persists the event with a gapless per-channel cursor (channel_cursors table)
    /// 4. Broadcasts to all subscribers of the channel (optionally excluding a connection)
    /// 5. Returns the fully-populated event
    ///
    /// `exclude_connection` — when set, this connection will not receive the
    /// broadcast. Used for WS-originated broadcasts to avoid echoing back to
    /// the sender. HTTP-originated events pass None.
    #[instrument(skip(self, request, auth_context), fields(channel = %request.channel_id, event_type = %request.event_type))]
    pub async fn record(
        &self,
        request: PublishEventRequest,
        auth_context: &AuthContext,
        exclude_connection: Option<uuid::Uuid>,
    ) -> Result<AgentEvent, EventError> {
        self.validate_auth(auth_context, &request.channel_id, Permission::Write)?;
        validate_event(&request)?;
        let fork_group_id = self
            .enforce_group_fork_channel_policy(
                &request.channel_id,
                &request.event_type,
                &request.payload,
            )
            .await?;

        let (event, _version) = self
            .store
            .publish_with_channel_cursor(request.clone())
            .await?;

        self.spawn_broadcast(&event, exclude_connection);
        if let Some(fork_group_id) = fork_group_id {
            let member_id = self
                .store
                .group_fork_tool_context(&request.channel_id, "")
                .await?
                .map(|(_, member, _, _, _)| member)
                .unwrap_or_default();
            self.store
                .record_group_fork_divergence(&fork_group_id, event.id, &member_id)
                .await?;
        }

        Ok(event)
    }

    /// Atomically persist an event and its resulting state snapshot
    /// (the "transition" operation), then broadcast the event.
    ///
    /// `expected_version` / `expected_cursor` optionally guard the write
    /// (see `TransitionRequest`).
    #[instrument(skip(self, request, auth_context), fields(channel = %request.channel_id, event_type = %request.event_type))]
    pub async fn record_transition(
        &self,
        request: TransitionRequest,
        auth_context: &AuthContext,
        exclude_connection: Option<uuid::Uuid>,
    ) -> Result<(AgentEvent, i64), EventError> {
        let publish_request = PublishEventRequest {
            channel_id: request.channel_id.clone(),
            event_type: request.event_type.clone(),
            payload: request.payload.clone(),
            metadata: request.metadata.clone(),
            depends_on: request.depends_on,
            dependencies: request.dependencies.clone(),
            operation_id: request.operation_id,
            step_number: None,
        };
        self.validate_auth(auth_context, &request.channel_id, Permission::Write)?;
        validate_event(&publish_request)?;
        let fork_group_id = self
            .enforce_group_fork_channel_policy(
                &request.channel_id,
                &request.event_type,
                &request.payload,
            )
            .await?;
        if sonic_rs::to_string(&request.state)
            .map(|s| s.len() > MAX_EVENT_SIZE)
            .unwrap_or(false)
        {
            return Err(EventError::InvalidRequest(format!(
                "state exceeds maximum size of {MAX_EVENT_SIZE} bytes"
            )));
        }

        let (event, version) = self.store.publish_with_state(&request).await?;

        self.spawn_broadcast(&event, exclude_connection);
        if let Some(fork_group_id) = fork_group_id {
            let member_id = self
                .store
                .group_fork_tool_context(&request.channel_id, "")
                .await?
                .map(|(_, member, _, _, _)| member)
                .unwrap_or_default();
            self.store
                .record_group_fork_divergence(&fork_group_id, event.id, &member_id)
                .await?;
        }

        Ok((event, version))
    }

    /// Broadcast a persisted event to all subscribers of its channel,
    /// tracking delivery for retry/DLQ handling.
    fn spawn_broadcast(&self, event: &AgentEvent, exclude_connection: Option<Uuid>) {
        let delivery_id = Uuid::new_v4();
        let subscriber_ids = self
            .connection_manager
            .get_topic_subscriber_ids(&event.channel_id);
        let cm = self.connection_manager.clone();
        let tracker = self.delivery_tracker.clone();
        let topic = event.channel_id.clone();
        let event_id = event.id;
        let cursor = event.cursor;
        let payload = event.payload.clone();
        let timestamp = event.timestamp;
        let event_type = event.event_type.clone();
        let actor = event.metadata.actor.clone();
        let agent_id = event.metadata.agent_id.clone();
        let user_id = event.metadata.user_id.clone();
        let metadata = event.metadata.metadata.clone();
        let depends_on = event.depends_on.map(|d| d.to_string());
        let control_plane = self.control_plane.clone();

        tokio::spawn(async move {
            let event_json = sonic_rs::json!({
                "id": event_id.to_string(),
                "channel_id": topic,
                "type": "broadcast",
                "event_type": event_type,
                "payload": payload,
                "cursor": cursor,
                "channel_cursor": cursor,
                "timestamp": timestamp.to_rfc3339(),
                "delivery_id": delivery_id.to_string(),
                "actor": actor,
                "agent_id": agent_id,
                "user_id": user_id,
                "metadata": metadata,
                "depends_on": depends_on,
            });
            let wrapper_str = match sonic_rs::to_string(&sonic_rs::json!({
                "type": "broadcast",
                "topic": topic,
                "payload": {
                    "event": event_json.clone(),
                },
            })) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to serialize broadcast wrapper");
                    return;
                }
            };
            let policy = control_plane.effective_source_policy();

            for conn_id in &subscriber_ids {
                if exclude_connection != Some(*conn_id) {
                    // PR-009: per-subscriber source-policy transform INSIDE the
                    // source instance. A federated subscriber whose policy
                    // needs per-frame redaction/filtering receives a
                    // transformed frame; local subscribers keep the shared
                    // serialized frame (fan-out is never weakened).
                    let frame = if Self::is_federated_subscriber(&cm, conn_id).await {
                        match Self::transform_for_federated(&policy, &event_json) {
                            Some(frame) => frame,
                            None => continue, // policy dropped the event (type deny)
                        }
                    } else {
                        wrapper_str.clone()
                    };
                    let send_ok = cm
                        .try_send_to_connection(
                            conn_id,
                            OutgoingMessage::Serialized(Arc::new(frame.clone())),
                        )
                        .is_ok();

                    let _ =
                        tracker.enqueue(Uuid::new_v4(), event_id, &topic, *conn_id, frame, send_ok);
                }
            }

            debug!(
                topic = %topic,
                subscriber_count = subscriber_ids.len(),
                "Broadcast dispatched to subscribers"
            );
        });
    }

    /// Whether a subscriber connection is federated (PR-009): a federated
    /// observation/command principal may need per-frame source-policy
    /// transformation before bytes leave the source instance.
    async fn is_federated_subscriber(cm: &ConnectionManager, conn_id: &Uuid) -> bool {
        match cm.get_connection(conn_id).await {
            Some(c) => c.auth_context.map(|a| a.is_federated()).unwrap_or(false),
            None => false,
        }
    }

    /// PR-009: apply the effective source policy to a live event for ONE
    /// federated subscriber and return the serialized broadcast frame (or
    /// `None` when the policy drops the event, e.g. a denied event type).
    ///
    /// The transform happens INSIDE the source instance before the bytes leave
    /// it — a gateway-side copy of policy is never a sufficient privacy
    /// boundary. Local (non-federated) subscribers always receive the shared
    /// unmodified frame, so the WS fan-out is never weakened.
    fn transform_for_federated(
        policy: &crate::auth::source_policy::FederationSourcePolicy,
        event_json: &sonic_rs::Value,
    ) -> Option<String> {
        use sonic_rs::JsonValueMutTrait;
        use sonic_rs::JsonValueTrait;
        if !policy.enabled {
            return None;
        }
        let event_type = event_json
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        // Type gate: deny wins.
        if !policy.type_allowed(event_type) {
            return None;
        }
        let mut transformed = event_json.clone();
        if let Some(payload) = transformed.get_mut("payload") {
            policy.apply_redactions_to_value(payload);
            if policy.metadata_only {
                *payload = sonic_rs::json!({});
            } else if policy.max_output_bytes > 0 {
                let size = sonic_rs::to_string(payload).map(|s| s.len()).unwrap_or(0);
                if size > policy.max_output_bytes {
                    *payload = sonic_rs::json!({});
                }
            }
        }
        let wrapper = sonic_rs::json!({
            "type": "broadcast",
            "topic": event_json.get("channel_id").and_then(|v| v.as_str()).unwrap_or(""),
            "payload": { "event": transformed },
        });
        sonic_rs::to_string(&wrapper).ok()
    }

    /// Subscribe to a channel's event stream with optional cursor resume.
    /// If from_cursor is Some, replays events from that cursor before transitioning to live.
    #[instrument(skip(self, auth_context), fields(channel = %channel_id, from_cursor = ?from_cursor))]
    pub async fn subscribe(
        &self,
        channel_id: &str,
        connection_id: Uuid,
        auth_context: &AuthContext,
        from_cursor: Option<i64>,
    ) -> Result<SubscribeResult, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;

        let (subscribed, replayed, latest_cursor) = self
            .connection_manager
            .subscribe_with_cursor(&connection_id, channel_id, from_cursor)
            .await
            .map_err(|e| EventError::Internal(format!("Subscribe failed: {e}")))?;

        Ok(SubscribeResult {
            channel: channel_id.to_string(),
            subscribed,
            cursor: Some(latest_cursor),
            replayed_events: replayed,
        })
    }

    /// Replay events from a channel starting at a given cursor.
    /// If no cursor is provided, returns events from the beginning.
    #[instrument(skip(self, request, auth_context), fields(channel = %request.channel_id, cursor = ?request.cursor))]
    pub async fn replay(
        &self,
        request: &ReplayRequest,
        auth_context: &AuthContext,
    ) -> Result<Vec<AgentEvent>, EventError> {
        self.validate_auth(auth_context, &request.channel_id, Permission::Read)?;

        let req = ReplayRequest {
            limit: Some(
                request
                    .limit
                    .unwrap_or(DEFAULT_REPLAY_LIMIT)
                    .min(MAX_REPLAY_LIMIT),
            ),
            ..request.clone()
        };

        let events = self.store.replay(&req).await?;
        Ok(events)
    }

    /// Query events across channels with filters.
    #[instrument(skip(self, request, auth_context))]
    pub async fn query(
        &self,
        request: &QueryRequest,
        auth_context: &AuthContext,
    ) -> Result<Vec<AgentEvent>, EventError> {
        if matches!(auth_context, AuthContext::Anonymous) && !self.allow_anonymous {
            return Err(EventError::Unauthenticated(
                "Authentication required".to_string(),
            ));
        }

        // Per-channel read enforcement for scoped keys.
        if let Some(ref channel_ids) = request.channel_ids {
            for ch in channel_ids {
                self.validate_auth(auth_context, ch, Permission::Read)?;
            }
        } else if !self.allow_anonymous && !auth_context.can_read("*") {
            // Unscoped channel_ids=None queries all channels — reject for
            // scoped keys to prevent privilege escalation.
            return Err(EventError::Forbidden(
                "Scoped keys must specify channel_ids".to_string(),
            ));
        }

        let events = self.store.query(request).await?;
        Ok(events)
    }

    /// Keyset-paginated cross-channel query. When `after_global_cursor` is set,
    /// continues from that bound. Returns `(events, has_more, next_cursor)`.
    #[instrument(skip(self, request, auth_context))]
    pub async fn query_page(
        &self,
        request: &QueryRequest,
        after_global_cursor: Option<i64>,
        auth_context: &AuthContext,
    ) -> Result<(Vec<AgentEvent>, bool, Option<i64>), EventError> {
        if matches!(auth_context, AuthContext::Anonymous) && !self.allow_anonymous {
            return Err(EventError::Unauthenticated(
                "Authentication required".to_string(),
            ));
        }

        // Per-channel read enforcement for scoped keys (same as query).
        if let Some(ref channel_ids) = request.channel_ids {
            for ch in channel_ids {
                self.validate_auth(auth_context, ch, Permission::Read)?;
            }
        } else if !self.allow_anonymous && !auth_context.can_read("*") {
            return Err(EventError::Forbidden(
                "Scoped keys must specify channel_ids".to_string(),
            ));
        }

        self.store
            .query_page(request, after_global_cursor)
            .await
            .map_err(EventError::from)
    }

    /// Smallest retained `global_cursor`, used to detect unsatisfiable page
    /// tokens after retention purges (typed 410).
    pub async fn min_global_cursor(&self) -> Result<Option<i64>, EventError> {
        self.store
            .min_global_cursor()
            .await
            .map_err(EventError::from)
    }

    /// Get the latest cursor for a channel.
    /// Useful for resuming subscriptions after reconnect.
    #[instrument(skip(self, auth_context), fields(channel = %channel_id))]
    pub async fn get_latest_cursor(
        &self,
        channel_id: &str,
        auth_context: &AuthContext,
    ) -> Result<Option<i64>, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        let cursor = self.store.get_latest_cursor(channel_id).await?;
        Ok(cursor)
    }

    /// Verify the tamper-evident hash chain for a channel (audit selling
    /// point 3). Read-authorized; returns the chain status.
    pub async fn verify_chain(
        &self,
        channel_id: &str,
        auth_context: &AuthContext,
    ) -> Result<crate::events::store::ChainVerification, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        self.store
            .verify_chain(channel_id)
            .await
            .map_err(EventError::from)
    }

    /// Close a channel: stamps `closed_at` on its registry row (audit item
    /// 14). Write-authorized. Does not delete data — it records the
    /// lifecycle transition so dashboards/analytics can distinguish closed
    /// channels from active ones.
    pub async fn close_channel(
        &self,
        channel_id: &str,
        auth_context: &AuthContext,
    ) -> Result<(), EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Write)?;
        self.store
            .close_channel(channel_id)
            .await
            .map_err(EventError::from)
    }

    /// Authorize a WebSocket subscribe with cursor-based catch-up replay.
    ///
    /// Reads are authorized against the connection's real principal and the
    /// target channel BEFORE any event is replayed (audit items 1 and 9).
    /// Returns `(replayed_events, latest_cursor)`. A missing/empty cursor
    /// yields an empty replay with the latest cursor so the caller still gets
    /// a `CursorSync`.
    pub async fn subscribe_with_cursor(
        &self,
        auth_context: &AuthContext,
        channel_id: &str,
        cursor: Option<i64>,
    ) -> Result<(Vec<AgentEvent>, i64), EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;

        let latest = self.store.get_latest_cursor(channel_id).await?.unwrap_or(0);
        let Some(from_cursor) = cursor else {
            return Ok((Vec::new(), latest));
        };

        let request = ReplayRequest {
            channel_id: channel_id.to_string(),
            cursor: Some(from_cursor),
            channel_cursor: None,
            limit: Some(1000),
            event_type: None,
        };
        let events = match self.store.replay(&request).await {
            Ok(events) => events,
            Err(e) => {
                tracing::warn!(
                    "subscribe replay failed for {} (cursor={:?}): {}",
                    channel_id,
                    cursor,
                    e
                );
                Vec::new()
            }
        };
        Ok((events, latest))
    }

    pub async fn list_channels(
        &self,
        auth_context: &AuthContext,
    ) -> Result<Vec<String>, EventError> {
        if self.allow_anonymous && matches!(auth_context, AuthContext::Anonymous) {
            // Anonymous is allowed — proceed
        } else if matches!(auth_context, AuthContext::Anonymous) {
            return Err(EventError::Unauthenticated(
                "Authentication required".to_string(),
            ));
        }
        // Read-scoped keys only see channels they may read (audit item 9).
        let channels = self.store.list_channels().await?;
        Ok(channels
            .into_iter()
            .filter(|ch| auth_context.can_read(ch))
            .collect())
    }

    /// Channel list enriched with per-channel event counts, latest cursor
    /// and last event timestamp (dashboard overview).
    pub async fn list_channels_with_stats(
        &self,
        auth_context: &AuthContext,
    ) -> Result<Vec<ChannelStat>, EventError> {
        if self.allow_anonymous && matches!(auth_context, AuthContext::Anonymous) {
            // Anonymous is allowed — proceed
        } else if matches!(auth_context, AuthContext::Anonymous) {
            return Err(EventError::Unauthenticated(
                "Authentication required".to_string(),
            ));
        }
        let stats = self.store.list_channels_with_stats().await?;
        Ok(stats
            .into_iter()
            .filter(|c| auth_context.can_read(&c.channel_id))
            .collect())
    }

    /// Global counters (total events, channels, state snapshots) for the
    /// dashboard overview. No auth required — used by the metrics endpoint
    /// which is already API-key gated.
    pub async fn event_stats(&self) -> Result<EventStats, EventError> {
        self.store.event_stats().await.map_err(EventError::from)
    }

    /// Unsubscribe from a channel.
    pub async fn unsubscribe(
        &self,
        channel_id: &str,
        connection_id: Uuid,
    ) -> Result<(), EventError> {
        let _ = self
            .connection_manager
            .unsubscribe_from_topic(&connection_id, channel_id)
            .await
            .map_err(|e| EventError::Internal(format!("Unsubscribe failed: {e}")))?;
        Ok(())
    }

    // ---- Auth helpers ----

    fn validate_auth(
        &self,
        auth: &AuthContext,
        channel_id: &str,
        required: Permission,
    ) -> Result<(), EventError> {
        if auth == &AuthContext::Anonymous {
            if self.allow_anonymous {
                return Ok(());
            }
            return Err(EventError::Unauthenticated(
                "Authentication required".to_string(),
            ));
        }

        // Enforce the channel-id grammar on every real channel reference.
        // Empty ids are used by channel-agnostic operations (list/query).
        if !channel_id.is_empty() {
            validate_channel_id(channel_id)?;
        }

        if channel_id.starts_with("system.") || channel_id.starts_with("admin.") {
            return Err(EventError::Forbidden(
                "System channels are not accessible".to_string(),
            ));
        }

        match required {
            Permission::Write => {
                if self.allow_anonymous && matches!(auth, AuthContext::Anonymous) {
                    return Ok(());
                }
                if !auth.can_write(channel_id) {
                    return Err(EventError::Forbidden(format!(
                        "Write permission denied on channel: {channel_id}"
                    )));
                }
            }
            Permission::Read => {
                if self.allow_anonymous && matches!(auth, AuthContext::Anonymous) {
                    return Ok(());
                }
                if !auth.can_read(channel_id) {
                    return Err(EventError::Forbidden(format!(
                        "Read permission denied on channel: {channel_id}"
                    )));
                }
            }
        }

        Ok(())
    }

    /// Public static accessor for testing auth validation logic.
    #[doc(hidden)]
    pub fn validate_auth_static(
        auth: &AuthContext,
        channel_id: &str,
        required: Permission,
    ) -> Result<(), EventError> {
        if auth == &AuthContext::Anonymous {
            return Err(EventError::Unauthenticated(
                "Authentication required".to_string(),
            ));
        }
        if channel_id.starts_with("system.") || channel_id.starts_with("admin.") {
            return Err(EventError::Forbidden(
                "System channels are not accessible".to_string(),
            ));
        }
        match required {
            Permission::Write => {
                if !auth.can_write(channel_id) {
                    return Err(EventError::Forbidden(
                        "Write permission required".to_string(),
                    ));
                }
            }
            Permission::Read => {
                if !auth.can_read(channel_id) {
                    return Err(EventError::Forbidden(
                        "Read permission required".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

pub fn validate_event(request: &PublishEventRequest) -> Result<(), EventError> {
    validate_channel_id(&request.channel_id)?;
    if request.event_type.is_empty() {
        return Err(EventError::InvalidRequest(
            "event_type is required".to_string(),
        ));
    }
    if request.event_type.len() > MAX_EVENT_TYPE_LEN {
        return Err(EventError::InvalidRequest(format!(
            "event_type exceeds maximum length of {MAX_EVENT_TYPE_LEN}"
        )));
    }
    if request.metadata.actor.is_empty() {
        return Err(EventError::InvalidRequest(
            "metadata.actor is required".to_string(),
        ));
    }
    if request.metadata.actor.len() > MAX_ACTOR_LEN {
        return Err(EventError::InvalidRequest(format!(
            "metadata.actor exceeds maximum length of {MAX_ACTOR_LEN}"
        )));
    }
    if let Ok(payload_str) = sonic_rs::to_string(&request.payload) {
        if payload_str.len() > MAX_EVENT_SIZE {
            return Err(EventError::InvalidRequest(format!(
                "payload exceeds maximum size of {MAX_EVENT_SIZE} bytes"
            )));
        }
    }
    Ok(())
}

/// Re-exported shared AuthContext type.
pub use crate::auth::types::AuthContext;

/// Validate a tool-execution body field (params / result / error) against the
/// same per-payload cap used for event payloads and state snapshots. Tool
/// bodies are persisted as JSONB and replayed into events, so they must not
/// be able to bypass the 1 MiB limit through a different route.
fn validate_tool_payload_size(
    field: &str,
    value: Option<&sonic_rs::Value>,
) -> Result<(), EventError> {
    if let Some(v) = value {
        if let Ok(s) = sonic_rs::to_string(v) {
            if s.len() > MAX_EVENT_SIZE {
                return Err(EventError::InvalidRequest(format!(
                    "{field} exceeds maximum size of {MAX_EVENT_SIZE} bytes"
                )));
            }
        }
    }
    Ok(())
}

/// Validate the combined size of a tool-failure error message + stack trace
/// against the same per-payload cap. Both are persisted as the execution's
/// `error` body and replayed into `tool.failed` events.
fn validate_tool_error_size(message: &str, stack: Option<&str>) -> Result<(), EventError> {
    let total = message.len() + stack.map(|s| s.len()).unwrap_or(0);
    if total > MAX_EVENT_SIZE {
        return Err(EventError::InvalidRequest(format!(
            "error message+stack exceeds maximum size of {MAX_EVENT_SIZE} bytes"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Read,
    Write,
}

#[derive(Debug, Clone)]
pub struct SubscribeResult {
    pub channel: String,
    pub subscribed: bool,
    pub cursor: Option<i64>,
    pub replayed_events: Vec<super::schema::AgentEvent>,
}

#[derive(Debug)]
pub enum EventError {
    Unauthenticated(String),
    Forbidden(String),
    InvalidRequest(String),
    NotFound(String),
    Internal(String),
    Store(EventStoreError),
    /// No saved snapshot exists at-or-before the requested fork cursor.
    SnapshotBoundaryNotFound {
        channel_id: String,
        at_cursor: i64,
    },
    /// Optimistic-concurrency guard rejected the write.
    VersionConflict {
        channel_id: String,
        expected_version: Option<i64>,
        actual_version: Option<i64>,
        expected_cursor: Option<i64>,
        actual_cursor: Option<i64>,
    },
    GroupNotFound(String),
    ConsumerNotFound(String),
    ExecutionGroupNotFound(String),
    ExecutionGroupMemberNotFound {
        group_id: String,
        member_id: String,
    },
    ExecutionGroupMemberClaimed {
        group_id: String,
        member_id: String,
    },
    ExecutionGroupMemberFenced {
        group_id: String,
        member_id: String,
    },
    CounterfactualBlocked {
        fork_group_id: String,
        boundary: String,
    },
    /// An ordinary fork's tool policy refused to execute a tool (`block`, or
    /// `replay` with no exact source match). The blocked boundary is persisted
    /// on the child channel as a `tool.blocked` event.
    ForkToolBlocked {
        channel_id: String,
        boundary: String,
    },
    WakeupNotFound(String),
    /// The requested idempotency key already reached a terminal state under a
    /// different request (request hash mismatch).
    ExecutionKeyMismatch {
        channel_id: String,
        key_name: String,
    },
    ExecutionNotFound(String),
    /// The operation is not permitted for the supplied claim token (a stale
    /// attempt trying to close an execution owned by another attempt).
    ExecutionNotOwned(String),
    /// The same fork operation_id was reused with a different request.
    IdempotencyConflict {
        operation_id: String,
    },
    /// The requested child channel already exists under a different fork
    /// definition (different source or boundary).
    ChannelConflict {
        channel_id: String,
    },
    /// The channel was soft-deleted; replays surface as 410 Gone.
    ChannelDeleted(String),
    /// A non-recursive delete was attempted on a channel with children.
    ChannelHasChildren {
        channel_id: String,
        count: usize,
    },
    QuotaExceeded,
    /// A completed execution's result expired under retention; the key must
    /// not be silently re-executed.
    ExecutionResultExpired {
        channel_id: String,
        key_name: String,
        execution_id: Uuid,
    },
    /// A keyset page token is malformed, tampered, or bound to different
    /// filters/caller scope.
    PageTokenInvalid(String),
    /// A keyset page token is beyond its 15-minute lifetime.
    PageTokenExpired,
    /// Retention purged the data a page token continues from; the page is
    /// unsatisfiable (typed 410).
    PageTokenUnsatisfiable {
        at_global_cursor: i64,
    },
}

impl From<EventStoreError> for EventError {
    fn from(e: EventStoreError) -> Self {
        match e {
            EventStoreError::NotFound => EventError::NotFound("Event not found".to_string()),
            EventStoreError::SnapshotBoundaryNotFound {
                channel_id,
                at_cursor,
            } => EventError::SnapshotBoundaryNotFound {
                channel_id,
                at_cursor,
            },
            EventStoreError::VersionConflict {
                channel_id,
                expected_version,
                actual_version,
                expected_cursor,
                actual_cursor,
            } => EventError::VersionConflict {
                channel_id,
                expected_version,
                actual_version,
                expected_cursor,
                actual_cursor,
            },
            EventStoreError::GroupNotFound(g) => EventError::GroupNotFound(g),
            EventStoreError::ConsumerNotFound(c) => EventError::ConsumerNotFound(c),
            EventStoreError::ExecutionGroupNotFound(g) => EventError::ExecutionGroupNotFound(g),
            EventStoreError::ExecutionGroupMemberNotFound {
                group_id,
                member_id,
            } => EventError::ExecutionGroupMemberNotFound {
                group_id,
                member_id,
            },
            EventStoreError::ExecutionGroupMemberClaimed {
                group_id,
                member_id,
            } => EventError::ExecutionGroupMemberClaimed {
                group_id,
                member_id,
            },
            EventStoreError::ExecutionGroupMemberFenced {
                group_id,
                member_id,
            } => EventError::ExecutionGroupMemberFenced {
                group_id,
                member_id,
            },
            EventStoreError::CounterfactualBlocked {
                fork_group_id,
                boundary,
            } => EventError::CounterfactualBlocked {
                fork_group_id,
                boundary,
            },
            EventStoreError::WakeupNotFound(w) => EventError::WakeupNotFound(w),
            EventStoreError::ExecutionKeyMismatch {
                channel_id,
                key_name,
            } => EventError::ExecutionKeyMismatch {
                channel_id,
                key_name,
            },
            EventStoreError::ExecutionNotFound(id) => EventError::ExecutionNotFound(id),
            EventStoreError::ExecutionNotOwned(id) => EventError::ExecutionNotOwned(id),
            EventStoreError::InvalidTimeRange(s) => {
                EventError::InvalidRequest(format!("Invalid timestamp: {s}"))
            }
            EventStoreError::IdempotencyConflict { operation_id } => {
                EventError::IdempotencyConflict { operation_id }
            }
            EventStoreError::ChannelConflict { channel_id } => {
                EventError::ChannelConflict { channel_id }
            }
            EventStoreError::ChannelDeleted(id) => EventError::ChannelDeleted(id),
            EventStoreError::ChannelHasChildren { channel_id, count } => {
                EventError::ChannelHasChildren { channel_id, count }
            }
            EventStoreError::InvalidRequest(m) => EventError::InvalidRequest(m),
            EventStoreError::QuotaExceeded => EventError::QuotaExceeded,
            EventStoreError::ExecutionResultExpired {
                channel_id,
                key_name,
                execution_id,
            } => EventError::ExecutionResultExpired {
                channel_id,
                key_name,
                execution_id,
            },
            _ => EventError::Store(e),
        }
    }
}

impl std::fmt::Display for EventError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventError::Unauthenticated(msg) => write!(f, "Unauthenticated: {msg}"),
            EventError::Forbidden(msg) => write!(f, "Forbidden: {msg}"),
            EventError::InvalidRequest(msg) => write!(f, "Invalid request: {msg}"),
            EventError::NotFound(msg) => write!(f, "Not found: {msg}"),
            EventError::Internal(msg) => write!(f, "Internal error: {msg}"),
            EventError::Store(e) => write!(f, "Store error: {e}"),
            EventError::SnapshotBoundaryNotFound { channel_id, at_cursor } => write!(
                f,
                "No saved state boundary at or before cursor {at_cursor} for channel {channel_id}"
            ),
            EventError::VersionConflict {
                channel_id,
                expected_version,
                actual_version,
                expected_cursor,
                actual_cursor,
            } => write!(
                f,
                "State version conflict for channel {channel_id}: expected version {expected_version:?} (actual {actual_version:?}), expected cursor {expected_cursor:?} (actual {actual_cursor:?})"
            ),
            EventError::GroupNotFound(g) => write!(f, "Consumer group not found: {g}"),
            EventError::ConsumerNotFound(c) => {
                write!(f, "Consumer not found or lease expired: {c}")
            }
            EventError::ExecutionGroupNotFound(g) => write!(f, "Execution group not found: {g}"),
            EventError::ExecutionGroupMemberNotFound { group_id, member_id } => write!(f, "Execution-group member not found: {group_id}/{member_id}"),
            EventError::ExecutionGroupMemberClaimed { group_id, member_id } => write!(f, "Execution-group member is held by a live owner: {group_id}/{member_id}"),
            EventError::ExecutionGroupMemberFenced { group_id, member_id } => write!(f, "Execution-group member lease is stale or fenced: {group_id}/{member_id}"),
            EventError::CounterfactualBlocked { fork_group_id, boundary } => write!(f, "Counterfactual '{fork_group_id}' blocked at {boundary}. Promote the frozen member to reactive or stop the fork."),
            EventError::ForkToolBlocked { channel_id, boundary } => write!(f, "Fork tool policy blocked channel '{channel_id}' at {boundary}. Use policy 'auto' or 'live' to allow new work, or resolve the inherited result first."),
            EventError::WakeupNotFound(w) => write!(f, "Wake-up not found: {w}"),
            EventError::ExecutionKeyMismatch { channel_id, key_name } => write!(
                f,
                "Idempotency key conflict: key '{key_name}' on channel '{channel_id}' was already used with a different request. Reuse a fresh key or return the original request parameters."
            ),
            EventError::ExecutionNotFound(id) => write!(f, "Execution not found: {id}"),
            EventError::ExecutionNotOwned(id) => write!(
                f,
                "Execution {id} is owned by another attempt; the provided claim token is no longer current."
            ),
            EventError::IdempotencyConflict { operation_id } => write!(
                f,
                "Idempotency conflict: fork operation '{operation_id}' was already used with a different request. Replay the original request or use a fresh operation_id."
            ),
            EventError::ChannelConflict { channel_id } => write!(
                f,
                "Channel conflict: '{channel_id}' already exists as a fork created from a different source or boundary. Use a fresh child channel id."
            ),
            EventError::ChannelDeleted(id) => write!(
                f,
                "Channel '{id}' was deleted and is no longer available."
            ),
            EventError::ChannelHasChildren { channel_id, count } => write!(
                f,
                "Channel '{channel_id}' has {count} child fork(es); pass recursive=true to delete the whole subtree."
            ),
            EventError::QuotaExceeded => write!(f, "Monthly event or ingress quota exceeded"),
            EventError::ExecutionResultExpired {
                channel_id,
                key_name,
                execution_id,
            } => write!(
                f,
                "Execution result expired: key '{key_name}' on channel '{channel_id}' (execution {execution_id}) completed beyond the retention window and its stored result was purged. Resolve the prior outcome downstream before retrying with a fresh key."
            ),
            EventError::PageTokenInvalid(msg) => write!(f, "Invalid page token: {msg}"),
            EventError::PageTokenExpired => write!(
                f,
                "Page token expired: re-run the query from the first page with the same filters."
            ),
            EventError::PageTokenUnsatisfiable { at_global_cursor } => write!(
                f,
                "Page token unsatisfiable: events before global_cursor {at_global_cursor} were purged by retention. Re-run the query from the first page with the same filters."
            ),
        }
    }
}

impl std::error::Error for EventError {}

// ── State Snapshot Operations ──────────────────────────────────

impl EventHandler {
    /// Save a cursor-aligned state snapshot. Returns the assigned version number.
    ///
    /// Optional optimistic-concurrency guards reject the write unless the
    /// channel's latest saved state matches (`expected_version` /
    /// `expected_cursor`; `Some(0)` as version means "no snapshot yet").
    #[instrument(skip(self, auth_context, state), fields(channel = %channel_id, cursor = %cursor))]
    pub async fn save_state(
        &self,
        channel_id: &str,
        cursor: i64,
        state: &sonic_rs::Value,
        expected_version: Option<i64>,
        expected_cursor: Option<i64>,
        auth_context: &AuthContext,
    ) -> Result<i64, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Write)?;
        if let Some((fork_group_id, member_id, _, member_policy, _)) =
            self.store.group_fork_tool_context(channel_id, "").await?
        {
            if member_policy == "frozen" {
                let boundary = format!("member:{member_id}:state");
                self.store
                    .block_group_fork(
                        &fork_group_id,
                        &boundary,
                        &sonic_rs::json!({"channel_id":channel_id}),
                    )
                    .await?;
                return Err(EventError::CounterfactualBlocked {
                    fork_group_id,
                    boundary,
                });
            }
        }
        if sonic_rs::to_string(state)
            .map(|s| s.len() > MAX_EVENT_SIZE)
            .unwrap_or(false)
        {
            return Err(EventError::InvalidRequest(format!(
                "state exceeds maximum size of {MAX_EVENT_SIZE} bytes"
            )));
        }
        self.store
            .save_state(channel_id, cursor, state, expected_version, expected_cursor)
            .await
            .map_err(EventError::from)
    }

    /// Load the latest state snapshot.
    #[instrument(skip(self, auth_context), fields(channel = %channel_id))]
    pub async fn load_state(
        &self,
        channel_id: &str,
        auth_context: &AuthContext,
    ) -> Result<Option<sonic_rs::Value>, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        match self.store.load_state(channel_id).await? {
            Some((cursor, state)) => {
                let mut result = sonic_rs::object! {};
                result.insert("cursor", cursor);
                result.insert("state", state);
                Ok(Some(sonic_rs::Value::from(result)))
            }
            None => Ok(None),
        }
    }
    /// Fork a channel into a new experiment fork.
    /// Returns (metadata, replayed) — `replayed` is true when the fork was
    /// served from an idempotency replay (same operation_id or child id).
    #[instrument(skip(self, request, auth_context))]
    pub async fn fork(
        &self,
        request: &ForkRequest,
        auth_context: &AuthContext,
    ) -> Result<(ChannelMetadata, bool), EventError> {
        self.validate_auth(auth_context, &request.source_channel_id, Permission::Read)?;
        self.validate_auth(auth_context, &request.new_channel_id, Permission::Write)?;
        self.store
            .fork_channel(request)
            .await
            .map_err(EventError::from)
    }

    /// Resolve a step number to its owning channel + cursor via the
    /// server-owned step index (walks the fork lineage for inherited steps).
    #[instrument(skip(self, auth_context))]
    pub async fn resolve_step(
        &self,
        channel_id: &str,
        step_number: i64,
        auth_context: &AuthContext,
    ) -> Result<Option<(String, i64, Option<uuid::Uuid>)>, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        self.store
            .resolve_step(channel_id, step_number)
            .await
            .map_err(EventError::from)
    }

    /// The highest step number recorded on a channel (0 = none). Used by SDK
    /// crash recovery to resume from the true progress without paging the log.
    #[instrument(skip(self, auth_context))]
    pub async fn latest_step_number(
        &self,
        channel_id: &str,
        auth_context: &AuthContext,
    ) -> Result<i64, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        self.store
            .latest_step_number(channel_id)
            .await
            .map_err(EventError::from)
    }

    /// Create an experiment group.
    pub async fn create_experiment(
        &self,
        request: &CreateExperimentRequest,
        auth_context: &AuthContext,
    ) -> Result<ExperimentGroup, EventError> {
        self.validate_auth(auth_context, "", Permission::Read)?;
        self.store
            .create_experiment(request)
            .await
            .map_err(EventError::from)
    }

    /// List experiment groups.
    pub async fn list_experiments(
        &self,
        auth_context: &AuthContext,
    ) -> Result<Vec<ExperimentGroup>, EventError> {
        self.validate_auth(auth_context, "", Permission::Read)?;
        self.store
            .list_experiments()
            .await
            .map_err(EventError::from)
    }

    /// Get an experiment group with members.
    pub async fn get_experiment(
        &self,
        group_id: &str,
        auth_context: &AuthContext,
    ) -> Result<Option<ExperimentGroup>, EventError> {
        self.validate_auth(auth_context, "", Permission::Read)?;
        self.store
            .get_experiment(group_id)
            .await
            .map_err(EventError::from)
    }

    /// Add a fork to an experiment group.
    pub async fn add_experiment_member(
        &self,
        group_id: &str,
        request: &AddExperimentMemberRequest,
        auth_context: &AuthContext,
    ) -> Result<ExperimentGroup, EventError> {
        self.validate_auth(auth_context, "", Permission::Read)?;
        self.validate_auth(auth_context, &request.channel_id, Permission::Read)?;
        self.store
            .add_experiment_member(group_id, request)
            .await
            .map_err(EventError::from)
    }

    /// Rank an experiment's members by result score.
    pub async fn rank_experiment(
        &self,
        group_id: &str,
        auth_context: &AuthContext,
    ) -> Result<(ExperimentGroup, Vec<RankedExperimentMember>), EventError> {
        self.validate_auth(auth_context, "", Permission::Read)?;
        self.store
            .rank_experiment(group_id)
            .await
            .map_err(EventError::from)
    }

    /// Record a fork outcome + score.
    pub async fn set_channel_outcome(
        &self,
        channel_id: &str,
        request: &SetOutcomeRequest,
        auth_context: &AuthContext,
    ) -> Result<ChannelMetadata, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Write)?;
        self.store
            .set_channel_outcome(channel_id, &request.outcome, request.score)
            .await
            .map_err(EventError::from)
    }

    /// Promote a winning fork into its parent (append-only merge).
    pub async fn promote_channel(
        &self,
        child_id: &str,
        auth_context: &AuthContext,
    ) -> Result<(String, i64), EventError> {
        self.validate_auth(auth_context, child_id, Permission::Write)?;
        let metadata = self
            .store
            .get_channel_metadata(child_id)
            .await?
            .ok_or_else(|| EventError::NotFound(child_id.to_string()))?;
        let parent_id = metadata.parent_channel_id.ok_or_else(|| {
            EventError::InvalidRequest("cannot promote a root channel (no parent)".to_string())
        })?;
        // Promotion mutates the parent, so child-only authorization is not
        // sufficient for scoped API keys.
        self.validate_auth(auth_context, &parent_id, Permission::Write)?;
        self.store
            .promote_channel(child_id)
            .await
            .map_err(EventError::from)
    }

    /// Soft-delete a channel (optionally the whole subtree) and purge rows.
    pub async fn delete_channel(
        &self,
        channel_id: &str,
        recursive: bool,
        auth_context: &AuthContext,
    ) -> Result<(), EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Write)?;
        self.store
            .delete_channel(channel_id, recursive)
            .await
            .map_err(EventError::from)
    }

    /// "What changed?" comparison between two channels.
    pub async fn compare_channels(
        &self,
        left: &str,
        right: &str,
        auth_context: &AuthContext,
    ) -> Result<CompareResponse, EventError> {
        self.validate_auth(auth_context, left, Permission::Read)?;
        self.validate_auth(auth_context, right, Permission::Read)?;
        self.store
            .compare_channels(left, right)
            .await
            .map_err(EventError::from)
    }

    /// Get metadata for a channel.
    #[instrument(skip(self, auth_context))]
    pub async fn get_channel_metadata(
        &self,
        channel_id: &str,
        auth_context: &AuthContext,
    ) -> Result<Option<ChannelMetadata>, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        self.store
            .get_channel_metadata(channel_id)
            .await
            .map_err(EventError::from)
    }

    /// Whether a channel has ever existed (events/state/metadata/cursors).
    #[instrument(skip(self, auth_context))]
    pub async fn channel_exists(
        &self,
        channel_id: &str,
        auth_context: &AuthContext,
    ) -> Result<bool, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        self.store
            .channel_exists(channel_id)
            .await
            .map_err(EventError::from)
    }

    /// List direct children (forks) of a channel.
    #[instrument(skip(self, auth_context))]
    pub async fn list_forks(
        &self,
        channel_id: &str,
        auth_context: &AuthContext,
    ) -> Result<Vec<ChannelMetadata>, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        self.store
            .list_forks(channel_id)
            .await
            .map_err(EventError::from)
    }

    /// Get the full execution tree starting from a root channel.
    #[instrument(skip(self, auth_context))]
    pub async fn get_fork_tree(
        &self,
        root_channel_id: &str,
        auth_context: &AuthContext,
    ) -> Result<(ForkInfo, bool), EventError> {
        self.validate_auth(auth_context, root_channel_id, Permission::Read)?;
        self.store
            .get_fork_tree(root_channel_id)
            .await
            .map_err(EventError::from)
    }

    /// Structural diff of two channels' latest saved states.
    ///
    /// Requires read access on both channels.
    #[instrument(skip(self, auth_context))]
    pub async fn diff_channels(
        &self,
        left: &str,
        right: &str,
        auth_context: &AuthContext,
    ) -> Result<StateDiffResponse, EventError> {
        self.validate_auth(auth_context, left, Permission::Read)?;
        self.validate_auth(auth_context, right, Permission::Read)?;
        self.store
            .diff_channels(left, right)
            .await
            .map_err(EventError::from)
    }

    /// Build the full decision trail (lineage + boundary state + executions)
    /// for a channel.
    #[instrument(skip(self, auth_context))]
    pub async fn get_decision_trail(
        &self,
        channel_id: &str,
        auth_context: &AuthContext,
    ) -> Result<DecisionTrail, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        self.store
            .get_decision_trail(channel_id)
            .await
            .map_err(EventError::from)
    }

    /// Update channel metadata (display_name, reason, experiment_metadata).
    #[instrument(skip(self, auth_context, request))]
    pub async fn update_metadata(
        &self,
        request: &UpdateMetadataRequest,
        auth_context: &AuthContext,
    ) -> Result<ChannelMetadata, EventError> {
        self.validate_auth(auth_context, &request.channel_id, Permission::Write)?;
        self.store
            .update_metadata(request)
            .await
            .map_err(EventError::from)
    }

    /// Load a specific version of a state snapshot.
    #[instrument(skip(self, auth_context), fields(channel = %channel_id, version = %version))]
    pub async fn load_state_version(
        &self,
        channel_id: &str,
        version: i64,
        auth_context: &AuthContext,
    ) -> Result<Option<sonic_rs::Value>, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        match self.store.load_state_version(channel_id, version).await? {
            Some((cursor, v, state)) => {
                let mut result = sonic_rs::object! {};
                result.insert("cursor", cursor);
                result.insert("version", v);
                result.insert("state", state);
                Ok(Some(sonic_rs::Value::from(result)))
            }
            None => Ok(None),
        }
    }

    /// List all state versions for a channel (metadata only, no blobs).
    #[instrument(skip(self, auth_context), fields(channel = %channel_id))]
    pub async fn list_states(
        &self,
        channel_id: &str,
        limit: Option<i64>,
        offset: Option<i64>,
        auth_context: &AuthContext,
    ) -> Result<Vec<super::schema::StateVersion>, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        self.store
            .list_states(channel_id, limit, offset)
            .await
            .map_err(EventError::from)
    }

    /// Delete a specific state version.
    #[instrument(skip(self, auth_context), fields(channel = %channel_id, version = %version))]
    pub async fn delete_state_version(
        &self,
        channel_id: &str,
        version: i64,
        auth_context: &AuthContext,
    ) -> Result<u64, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Write)?;
        self.store
            .delete_state_version(channel_id, version)
            .await
            .map_err(EventError::from)
    }

    // ── Distributed execution groups ────────────────────────────────

    pub async fn create_execution_group(
        &self,
        group_id: &str,
        metadata: Option<&sonic_rs::Value>,
        auth: &AuthContext,
    ) -> Result<ExecutionGroup, EventError> {
        // Group creation has no channel yet. Authentication is resolved by the
        // HTTP layer; anonymous callers are never allowed outside dev mode.
        if !self.allow_anonymous && matches!(auth, AuthContext::Anonymous) {
            return Err(EventError::Unauthenticated(
                "Authentication required".into(),
            ));
        }
        validate_channel_id(group_id)?;
        self.store
            .create_execution_group(group_id, metadata)
            .await
            .map_err(EventError::from)
    }

    pub async fn list_execution_groups(
        &self,
        auth: &AuthContext,
    ) -> Result<Vec<ExecutionGroup>, EventError> {
        if !self.allow_anonymous && matches!(auth, AuthContext::Anonymous) {
            return Err(EventError::Unauthenticated(
                "Authentication required".into(),
            ));
        }
        self.store
            .list_execution_groups()
            .await
            .map_err(EventError::from)
    }

    pub async fn add_execution_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        channel_id: &str,
        role: Option<&str>,
        metadata: Option<&sonic_rs::Value>,
        auth: &AuthContext,
    ) -> Result<ExecutionGroupMember, EventError> {
        self.validate_auth(auth, channel_id, Permission::Write)?;
        validate_channel_id(member_id)?;
        self.store
            .add_execution_group_member(group_id, member_id, channel_id, role, metadata)
            .await
            .map_err(EventError::from)
    }

    pub async fn list_execution_group_members(
        &self,
        group_id: &str,
        auth: &AuthContext,
    ) -> Result<Vec<ExecutionGroupMember>, EventError> {
        let members = self.store.list_execution_group_members(group_id).await?;
        for member in &members {
            self.validate_auth(auth, &member.channel_id, Permission::Read)?;
        }
        Ok(members)
    }

    pub async fn claim_execution_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        owner_id: &str,
        lease_seconds: i64,
        auth: &AuthContext,
    ) -> Result<MemberLease, EventError> {
        let member = self.member_for_auth(group_id, member_id).await?;
        self.validate_auth(auth, &member.channel_id, Permission::Write)?;
        if self
            .store
            .group_fork_member_policy(group_id, member_id)
            .await?
            .as_deref()
            == Some("frozen")
        {
            return Err(EventError::CounterfactualBlocked {
                fork_group_id: group_id.into(),
                boundary: format!("member:{member_id}:claim"),
            });
        }
        self.store
            .claim_execution_group_member(group_id, member_id, owner_id, lease_seconds)
            .await
            .map_err(EventError::from)
    }

    pub async fn heartbeat_execution_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        owner_id: &str,
        generation: i64,
        lease_seconds: i64,
        auth: &AuthContext,
    ) -> Result<MemberLease, EventError> {
        let member = self.member_for_auth(group_id, member_id).await?;
        self.validate_auth(auth, &member.channel_id, Permission::Write)?;
        self.store
            .heartbeat_execution_group_member(
                group_id,
                member_id,
                owner_id,
                generation,
                lease_seconds,
            )
            .await
            .map_err(EventError::from)
    }

    pub async fn release_execution_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        owner_id: &str,
        generation: i64,
        auth: &AuthContext,
    ) -> Result<(), EventError> {
        let member = self.member_for_auth(group_id, member_id).await?;
        self.validate_auth(auth, &member.channel_id, Permission::Write)?;
        self.store
            .release_execution_group_member(group_id, member_id, owner_id, generation)
            .await
            .map_err(EventError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_execution_group_message(
        &self,
        group_id: &str,
        from_member_id: &str,
        to_member_id: &str,
        message_type: &str,
        payload: &sonic_rs::Value,
        causal_context: Option<&sonic_rs::Value>,
        operation_id: Option<Uuid>,
        auth: &AuthContext,
    ) -> Result<GroupMessage, EventError> {
        let from = self.member_for_auth(group_id, from_member_id).await?;
        let to = self.member_for_auth(group_id, to_member_id).await?;
        self.validate_auth(auth, &from.channel_id, Permission::Write)?;
        self.validate_auth(auth, &to.channel_id, Permission::Write)?;
        if message_type.is_empty() || message_type.len() > MAX_EVENT_TYPE_LEN {
            return Err(EventError::InvalidRequest(
                "message type must be 1..=128 bytes".into(),
            ));
        }
        let from_policy = self
            .store
            .group_fork_member_policy(group_id, from_member_id)
            .await?;
        if from_policy.as_deref() == Some("frozen") {
            let boundary = format!("member:{from_member_id}:send");
            self.store
                .block_group_fork(
                    group_id,
                    &boundary,
                    &sonic_rs::json!({"to_member_id":to_member_id,"type":message_type}),
                )
                .await?;
            return Err(EventError::CounterfactualBlocked {
                fork_group_id: group_id.into(),
                boundary,
            });
        }
        if self
            .store
            .group_fork_member_policy(group_id, to_member_id)
            .await?
            .as_deref()
            == Some("frozen")
        {
            let boundary = format!("member:{to_member_id}:unexpected_message");
            self.store
                .block_group_fork(
                    group_id,
                    &boundary,
                    &sonic_rs::json!({"from_member_id":from_member_id,"type":message_type,"payload":payload}),
                )
                .await?;
            return Err(EventError::CounterfactualBlocked {
                fork_group_id: group_id.into(),
                boundary,
            });
        }
        let (message, source, delivery) = self
            .store
            .send_execution_group_message(
                group_id,
                from_member_id,
                to_member_id,
                message_type,
                payload,
                causal_context,
                operation_id,
            )
            .await?;
        self.spawn_broadcast(&source, None);
        self.spawn_broadcast(&delivery, None);
        if from_policy.is_some() {
            self.store
                .record_group_fork_divergence(group_id, source.id, from_member_id)
                .await?;
        }
        Ok(message)
    }

    pub async fn list_execution_group_messages(
        &self,
        group_id: &str,
        member_id: &str,
        after: Option<Uuid>,
        limit: i64,
        auth: &AuthContext,
    ) -> Result<Vec<GroupMessage>, EventError> {
        let member = self.member_for_auth(group_id, member_id).await?;
        self.validate_auth(auth, &member.channel_id, Permission::Read)?;
        self.store
            .list_execution_group_messages(group_id, member_id, after, limit)
            .await
            .map_err(EventError::from)
    }

    pub async fn acknowledge_execution_group_message(
        &self,
        message_id: Uuid,
        owner_id: &str,
        generation: i64,
        auth: &AuthContext,
    ) -> Result<GroupMessage, EventError> {
        // The store enforces recipient ownership; authenticate as a normal
        // writer before allowing the mutation. This is intentionally scoped
        // after the ownership check, rather than trusting client channel data.
        if !self.allow_anonymous && matches!(auth, AuthContext::Anonymous) {
            return Err(EventError::Unauthenticated(
                "Authentication required".into(),
            ));
        }
        self.store
            .acknowledge_execution_group_message(message_id, owner_id, generation)
            .await
            .map_err(EventError::from)
    }

    async fn member_for_auth(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<ExecutionGroupMember, EventError> {
        self.store
            .list_execution_group_members(group_id)
            .await?
            .into_iter()
            .find(|m| m.member_id == member_id)
            .ok_or_else(|| EventError::ExecutionGroupMemberNotFound {
                group_id: group_id.into(),
                member_id: member_id.into(),
            })
    }

    pub async fn causal_graph(
        &self,
        event_id: Uuid,
        max_nodes: i64,
        auth: &AuthContext,
    ) -> Result<CausalGraph, EventError> {
        let graph = self.store.causal_graph(event_id, max_nodes).await?;
        for event in &graph.events {
            self.validate_auth(auth, &event.channel_id, Permission::Read)?;
        }
        Ok(graph)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_group_fork(
        &self,
        fork_group_id: &str,
        source_group_id: &str,
        intervention_member_id: &str,
        intervention_cursor: i64,
        member_policies: &sonic_rs::Value,
        tool_policies: &sonic_rs::Value,
        auth: &AuthContext,
    ) -> Result<GroupForkReceipt, EventError> {
        validate_channel_id(fork_group_id)?;
        if intervention_cursor < 0 || !member_policies.is_object() || !tool_policies.is_object() {
            return Err(EventError::InvalidRequest(
                "fork policies must be objects and intervention_cursor must be non-negative".into(),
            ));
        }
        let members = self
            .store
            .list_execution_group_members(source_group_id)
            .await?;
        if !members
            .iter()
            .any(|m| m.member_id == intervention_member_id)
        {
            return Err(EventError::ExecutionGroupMemberNotFound {
                group_id: source_group_id.into(),
                member_id: intervention_member_id.into(),
            });
        }
        let policy_object = member_policies.as_object().expect("validated object");
        for member in &members {
            let mode = policy_object
                .get(&member.member_id)
                .and_then(|v| v.as_str());
            if !matches!(mode, Some("frozen" | "live" | "reactive")) {
                return Err(EventError::InvalidRequest(format!(
                    "member_policies must specify frozen, live, or reactive for '{}'",
                    member.member_id
                )));
            }
        }
        if policy_object
            .get(&intervention_member_id.to_string())
            .and_then(|v| v.as_str())
            != Some("live")
        {
            return Err(EventError::InvalidRequest(
                "the intervention member must be live".into(),
            ));
        }
        for (_, mode) in tool_policies.as_object().expect("validated object") {
            if !matches!(mode.as_str(), Some("replay" | "live" | "block")) {
                return Err(EventError::InvalidRequest(
                    "tool policy values must be replay, live, or block".into(),
                ));
            }
        }
        for member in &members {
            self.validate_auth(auth, &member.channel_id, Permission::Read)?;
        }
        self.store
            .create_group_fork(
                fork_group_id,
                source_group_id,
                intervention_member_id,
                intervention_cursor,
                member_policies,
                tool_policies,
            )
            .await
            .map_err(EventError::from)
    }

    pub async fn get_group_fork(
        &self,
        fork_group_id: &str,
        auth: &AuthContext,
    ) -> Result<GroupForkReceipt, EventError> {
        let receipt = self.store.get_group_fork(fork_group_id).await?;
        for member in self
            .store
            .list_execution_group_members(&receipt.source_group_id)
            .await?
        {
            self.validate_auth(auth, &member.channel_id, Permission::Read)?;
        }
        Ok(receipt)
    }

    pub async fn promote_group_fork_member(
        &self,
        fork_group_id: &str,
        member_id: &str,
        auth: &AuthContext,
    ) -> Result<GroupForkReceipt, EventError> {
        let member = self.member_for_auth(fork_group_id, member_id).await?;
        self.validate_auth(auth, &member.channel_id, Permission::Write)?;
        self.store
            .promote_group_fork_member(fork_group_id, member_id)
            .await
            .map_err(EventError::from)
    }

    // ── Durable Consumer Groups ────────────────────────────────────

    /// Create a durable consumer group bound to a channel.
    #[instrument(skip(self, auth_context))]
    pub async fn create_group(
        &self,
        group_id: &str,
        channel_id: &str,
        metadata: Option<&sonic_rs::Value>,
        auth_context: &AuthContext,
    ) -> Result<GroupInfo, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Write)?;
        self.store
            .create_group(group_id, channel_id, metadata)
            .await
            .map_err(EventError::from)
    }

    /// List consumer groups, optionally filtered by channel.
    #[instrument(skip(self, auth_context))]
    pub async fn list_groups(
        &self,
        channel_id: Option<&str>,
        auth_context: &AuthContext,
    ) -> Result<Vec<GroupInfo>, EventError> {
        if let Some(ch) = channel_id {
            self.validate_auth(auth_context, ch, Permission::Read)?;
        } else if !self.allow_anonymous && !auth_context.can_read("*") {
            // Scoped keys without a channel_id filter would see all groups.
            // Require channel_id to prevent information disclosure.
            return Err(EventError::Forbidden(
                "Scoped keys must specify channel_id".to_string(),
            ));
        }
        self.store
            .list_groups(channel_id)
            .await
            .map_err(EventError::from)
    }

    /// Delete a consumer group (cascades consumers and offsets).
    #[instrument(skip(self, auth_context))]
    pub async fn delete_group(
        &self,
        group_id: &str,
        auth_context: &AuthContext,
    ) -> Result<bool, EventError> {
        let group = self.store.get_group(group_id).await?;
        self.validate_auth(auth_context, &group.channel_id, Permission::Write)?;
        self.store
            .delete_group(group_id)
            .await
            .map_err(EventError::from)
    }

    /// Register (or renew) a consumer's lease and durable offset row.
    #[instrument(skip(self, auth_context))]
    pub async fn join_group(
        &self,
        group_id: &str,
        consumer_id: &str,
        lease_seconds: i64,
        auth_context: &AuthContext,
    ) -> Result<GroupOffsetInfo, EventError> {
        let group = self.store.get_group(group_id).await?;
        self.validate_auth(auth_context, &group.channel_id, Permission::Read)?;
        self.store
            .join_group(group_id, consumer_id, lease_seconds)
            .await
            .map_err(EventError::from)
    }

    /// Claim a batch of events (at-least-once, lease-based).
    #[instrument(skip(self, auth_context))]
    pub async fn claim_work(
        &self,
        group_id: &str,
        consumer_id: &str,
        limit: i64,
        auth_context: &AuthContext,
    ) -> Result<ClaimedWork, EventError> {
        let group = self.store.get_group(group_id).await?;
        self.validate_auth(auth_context, &group.channel_id, Permission::Read)?;
        self.store
            .claim_work(group_id, consumer_id, limit)
            .await
            .map_err(EventError::from)
    }

    /// Acknowledge processed work (advances the durable committed offset).
    #[instrument(skip(self, auth_context))]
    pub async fn ack_work(
        &self,
        group_id: &str,
        consumer_id: &str,
        cursor: i64,
        auth_context: &AuthContext,
    ) -> Result<(), EventError> {
        let group = self.store.get_group(group_id).await?;
        self.validate_auth(auth_context, &group.channel_id, Permission::Write)?;
        self.store
            .ack_work(group_id, consumer_id, cursor)
            .await
            .map_err(EventError::from)
    }

    /// Extend a consumer's lease.
    #[instrument(skip(self, auth_context))]
    pub async fn heartbeat(
        &self,
        group_id: &str,
        consumer_id: &str,
        lease_seconds: i64,
        auth_context: &AuthContext,
    ) -> Result<(), EventError> {
        let group = self.store.get_group(group_id).await?;
        self.validate_auth(auth_context, &group.channel_id, Permission::Read)?;
        self.store
            .heartbeat(group_id, consumer_id, lease_seconds)
            .await
            .map_err(EventError::from)
    }

    /// Per-consumer durable offsets for a group.
    #[instrument(skip(self, auth_context))]
    pub async fn group_offsets(
        &self,
        group_id: &str,
        auth_context: &AuthContext,
    ) -> Result<Vec<GroupOffsetInfo>, EventError> {
        let group = self.store.get_group(group_id).await?;
        self.validate_auth(auth_context, &group.channel_id, Permission::Read)?;
        self.store
            .get_offsets(group_id)
            .await
            .map_err(EventError::from)
    }

    // ── Persisted Wake-ups ─────────────────────────────────────────

    /// Schedule a wake-up that fires a `scheduler.wakeup` event on the channel.
    #[instrument(skip(self, auth_context))]
    pub async fn create_wakeup(
        &self,
        request: &CreateWakeupRequest,
        auth_context: &AuthContext,
    ) -> Result<WakeupInfo, EventError> {
        self.validate_auth(auth_context, &request.channel_id, Permission::Write)?;
        self.store
            .create_wakeup(request)
            .await
            .map_err(EventError::from)
    }

    /// List wake-ups, optionally filtered by channel and status.
    #[instrument(skip(self, auth_context))]
    pub async fn list_wakeups(
        &self,
        channel_id: Option<&str>,
        status: Option<&str>,
        auth_context: &AuthContext,
    ) -> Result<Vec<WakeupInfo>, EventError> {
        if let Some(ch) = channel_id {
            self.validate_auth(auth_context, ch, Permission::Read)?;
        } else if !self.allow_anonymous && !auth_context.can_read("*") {
            // Scoped keys without a channel_id filter would see all wakeups.
            // Require channel_id to prevent information disclosure.
            return Err(EventError::Forbidden(
                "Scoped keys must specify channel_id".to_string(),
            ));
        }
        self.store
            .list_wakeups(channel_id, status)
            .await
            .map_err(EventError::from)
    }

    /// Fetch a single wake-up.
    #[instrument(skip(self, auth_context))]
    pub async fn get_wakeup(
        &self,
        id: Uuid,
        auth_context: &AuthContext,
    ) -> Result<Option<WakeupInfo>, EventError> {
        let wakeup = self.store.get_wakeup(id).await?;
        if let Some(ref w) = wakeup {
            self.validate_auth(auth_context, &w.channel_id, Permission::Read)?;
        }
        Ok(wakeup)
    }

    /// Cancel a pending wake-up.
    #[instrument(skip(self, auth_context))]
    pub async fn cancel_wakeup(
        &self,
        id: Uuid,
        auth_context: &AuthContext,
    ) -> Result<bool, EventError> {
        let wakeup = self.store.get_wakeup(id).await?;
        match wakeup {
            Some(w) => {
                self.validate_auth(auth_context, &w.channel_id, Permission::Write)?;
                self.store.cancel_wakeup(id).await.map_err(EventError::from)
            }
            None => Err(EventError::WakeupNotFound(id.to_string())),
        }
    }

    // ── Idempotent Tool Executions ───────────────────────────────

    /// Parse a `claim_token` string (as carried by the HTTP request types)
    /// into a `Uuid`, rejecting malformed tokens with `InvalidRequest`.
    fn parse_claim_token(token: Option<&str>) -> Result<Option<Uuid>, EventError> {
        match token {
            Some(t) if t.trim().is_empty() => Ok(None),
            Some(t) => Uuid::parse_str(t)
                .map(Some)
                .map_err(|_| EventError::InvalidRequest(format!("invalid claim_token: {t}"))),
            None => Ok(None),
        }
    }

    /// Persist a blocked fork-tool boundary as an immutable `tool.blocked`
    /// event on the child channel. Best-effort: a failure to record must not
    /// change the block decision itself (the caller still returns the error).
    async fn record_fork_tool_block(
        &self,
        channel_id: &str,
        tool_name: &str,
        key_name: &str,
        boundary: &str,
        policy: &str,
    ) {
        let request = PublishEventRequest {
            channel_id: channel_id.to_string(),
            event_type: "tool.blocked".to_string(),
            payload: sonic_rs::json!({
                "tool_name": tool_name,
                "key_name": key_name,
                "boundary": boundary,
                "policy": policy,
            }),
            metadata: EventMetadata {
                actor: "fork-policy".to_string(),
                agent_id: None,
                user_id: None,
                metadata: None,
            },
            depends_on: None,
            dependencies: vec![],
            operation_id: None,
            step_number: None,
        };
        match self.store.publish_with_channel_cursor(request).await {
            Ok((event, _)) => self.spawn_broadcast(&event, None),
            Err(error) => warn!("failed to persist fork tool block: {error}"),
        }
    }

    /// Claim an idempotent tool execution (create-or-replay). Persists the
    /// `tool.started` lifecycle event and broadcasts it to the channel's
    /// subscribers when the caller acquires ownership.
    #[instrument(skip(self, auth_context), fields(channel = %request.channel_id, key = %request.key_name, tool = %request.tool_name))]
    pub async fn claim_execution(
        &self,
        request: &ClaimExecutionRequest,
        auth_context: &AuthContext,
    ) -> Result<ExecutionClaimResult, EventError> {
        self.validate_auth(auth_context, &request.channel_id, Permission::Write)?;
        validate_tool_payload_size("params", request.params.as_ref())?;
        if let Some((fork_group_id, member_id, source_channel_id, member_policy, policy)) = self
            .store
            .group_fork_tool_context(&request.channel_id, &request.tool_name)
            .await?
        {
            if member_policy == "frozen" {
                let boundary = format!("member:{member_id}:tool:{}", request.tool_name);
                self.store.block_group_fork(&fork_group_id, &boundary, &sonic_rs::json!({"channel_id":request.channel_id,"key_name":request.key_name})).await?;
                return Err(EventError::CounterfactualBlocked {
                    fork_group_id,
                    boundary,
                });
            }
            match policy.as_str() {
                "block" => {
                    let boundary = format!("tool:{}:blocked", request.tool_name);
                    self.store
                        .block_group_fork(
                            &fork_group_id,
                            &boundary,
                            &sonic_rs::json!({"channel_id":request.channel_id,"key_name":request.key_name}),
                        )
                        .await?;
                    return Err(EventError::CounterfactualBlocked {
                        fork_group_id,
                        boundary,
                    });
                }
                "replay" => {
                    match self
                        .store
                        .replay_group_fork_execution(request, &fork_group_id, &source_channel_id)
                        .await
                    {
                        Ok((claim, event)) => {
                            self.spawn_broadcast(&event, None);
                            return Ok(claim);
                        }
                        Err(EventStoreError::CounterfactualBlocked { boundary, .. }) => {
                            self.store
                                .block_group_fork(
                                    &fork_group_id,
                                    &boundary,
                                    &sonic_rs::json!({"channel_id":request.channel_id,"key_name":request.key_name,"tool_name":request.tool_name}),
                                )
                                .await?;
                            return Err(EventError::CounterfactualBlocked {
                                fork_group_id,
                                boundary,
                            });
                        }
                        Err(error) => return Err(EventError::from(error)),
                    }
                }
                "live" => {}
                other => {
                    return Err(EventError::Internal(format!(
                        "invalid persisted group-fork tool policy: {other}"
                    )))
                }
            }
            let (claim, event) = self.store.claim_execution(request).await?;
            if let Some(evt) = event {
                self.spawn_broadcast(&evt, None);
            }
            self.store
                .record_group_fork_live_tool(&fork_group_id, &claim.execution)
                .await?;
            return Ok(claim);
        } else if let Some((fork_sources, policy)) = self
            .store
            .fork_tool_context(&request.channel_id, &request.tool_name, &request.key_name)
            .await
            .map_err(EventError::from)?
        {
            match policy.as_str() {
                "block" => {
                    let boundary = format!("tool:{}:blocked", request.tool_name);
                    self.record_fork_tool_block(
                        &request.channel_id,
                        &request.tool_name,
                        &request.key_name,
                        &boundary,
                        "block",
                    )
                    .await;
                    return Err(EventError::ForkToolBlocked {
                        channel_id: request.channel_id.clone(),
                        boundary,
                    });
                }
                "replay" => {
                    match self
                        .store
                        .replay_fork_execution(request, &fork_sources)
                        .await
                        .map_err(EventError::from)?
                    {
                        ForkReplayResult::Replayed(claim, event) => {
                            self.spawn_broadcast(&event, None);
                            return Ok(claim);
                        }
                        ForkReplayResult::Miss => {
                            let boundary = format!("tool:{}:replay_miss", request.tool_name);
                            self.record_fork_tool_block(
                                &request.channel_id,
                                &request.tool_name,
                                &request.key_name,
                                &boundary,
                                "replay",
                            )
                            .await;
                            return Err(EventError::ForkToolBlocked {
                                channel_id: request.channel_id.clone(),
                                boundary,
                            });
                        }
                        ForkReplayResult::Mismatch => {
                            let boundary = format!("tool:{}:request_mismatch", request.tool_name);
                            self.record_fork_tool_block(
                                &request.channel_id,
                                &request.tool_name,
                                &request.key_name,
                                &boundary,
                                "replay",
                            )
                            .await;
                            return Err(EventError::ForkToolBlocked {
                                channel_id: request.channel_id.clone(),
                                boundary,
                            });
                        }
                    }
                }
                // auto: replay an exact inherited result; a miss or mismatch is
                // new work and falls through to a normal live claim.
                "auto" => {
                    if let ForkReplayResult::Replayed(claim, event) = self
                        .store
                        .replay_fork_execution(request, &fork_sources)
                        .await
                        .map_err(EventError::from)?
                    {
                        self.spawn_broadcast(&event, None);
                        return Ok(claim);
                    }
                }
                "live" => {}
                other => {
                    return Err(EventError::Internal(format!(
                        "invalid persisted fork tool policy: {other}"
                    )))
                }
            }
            let (claim, event) = self
                .store
                .claim_execution(request)
                .await
                .map_err(EventError::from)?;
            if let Some(evt) = event {
                self.spawn_broadcast(&evt, None);
            }
            return Ok(claim);
        }
        let (claim, event) = self
            .store
            .claim_execution(request)
            .await
            .map_err(EventError::from)?;
        if let Some(evt) = event {
            self.spawn_broadcast(&evt, None);
        }
        Ok(claim)
    }

    /// Complete a claimed execution with its persisted result. Emits and
    /// broadcasts the `tool.completed` lifecycle event.
    #[instrument(skip(self, request, auth_context), fields(execution = %execution_id))]
    pub async fn complete_execution(
        &self,
        execution_id: Uuid,
        request: &CompleteExecutionRequest,
        auth_context: &AuthContext,
    ) -> Result<ExecutionInfo, EventError> {
        let info = self.store.get_execution(execution_id).await?;
        match info {
            Some(e) => self.validate_auth(auth_context, &e.channel_id, Permission::Write)?,
            None => return Err(EventError::ExecutionNotFound(execution_id.to_string())),
        }
        let claim_token = Self::parse_claim_token(request.claim_token.as_deref())?;
        validate_tool_payload_size("result", request.result.as_ref())?;
        let (info, event) = self
            .store
            .complete_execution(execution_id, claim_token, request)
            .await
            .map_err(EventError::from)?;
        if let Some(evt) = event {
            self.spawn_broadcast(&evt, None);
        }
        Ok(info)
    }

    /// Mark a claimed execution failed with a structured error. Emits and
    /// broadcasts the `tool.failed` lifecycle event.
    #[instrument(skip(self, request, auth_context), fields(execution = %execution_id))]
    pub async fn fail_execution(
        &self,
        execution_id: Uuid,
        request: &FailExecutionRequest,
        auth_context: &AuthContext,
    ) -> Result<ExecutionInfo, EventError> {
        let info = self.store.get_execution(execution_id).await?;
        match info {
            Some(e) => self.validate_auth(auth_context, &e.channel_id, Permission::Write)?,
            None => return Err(EventError::ExecutionNotFound(execution_id.to_string())),
        }
        let claim_token = Self::parse_claim_token(request.claim_token.as_deref())?;
        validate_tool_error_size(&request.message, request.stack.as_deref())?;
        let (info, event) = self
            .store
            .fail_execution(execution_id, claim_token, request)
            .await
            .map_err(EventError::from)?;
        if let Some(evt) = event {
            self.spawn_broadcast(&evt, None);
        }
        Ok(info)
    }

    /// Extend a running execution's lease while its tool is still working.
    #[instrument(skip(self, request, auth_context), fields(execution = %execution_id))]
    pub async fn heartbeat_execution(
        &self,
        execution_id: Uuid,
        request: &HeartbeatExecutionRequest,
        auth_context: &AuthContext,
    ) -> Result<ExecutionInfo, EventError> {
        let info = self
            .store
            .heartbeat_execution(execution_id, request)
            .await
            .map_err(EventError::from)?;
        self.validate_auth(auth_context, &info.channel_id, Permission::Write)?;
        Ok(info)
    }

    /// Cancel a running execution without storing a result.
    #[instrument(skip(self, request, auth_context), fields(execution = %execution_id))]
    pub async fn cancel_execution(
        &self,
        execution_id: Uuid,
        request: &CancelExecutionRequest,
        auth_context: &AuthContext,
    ) -> Result<ExecutionInfo, EventError> {
        let info = self
            .store
            .cancel_execution(execution_id, request)
            .await
            .map_err(EventError::from)?;
        self.validate_auth(auth_context, &info.channel_id, Permission::Write)?;
        Ok(info)
    }

    /// Fetch a single execution by id (read permission on its channel).
    #[instrument(skip(self, auth_context), fields(execution = %execution_id))]
    pub async fn get_execution(
        &self,
        execution_id: Uuid,
        auth_context: &AuthContext,
    ) -> Result<Option<ExecutionInfo>, EventError> {
        let info = self.store.get_execution(execution_id).await?;
        if let Some(ref e) = info {
            self.validate_auth(auth_context, &e.channel_id, Permission::Read)?;
        }
        Ok(info)
    }

    /// List executions for a channel (read permission on the channel).
    #[instrument(skip(self, auth_context), fields(channel = %channel_id))]
    pub async fn list_executions(
        &self,
        channel_id: &str,
        limit: i64,
        auth_context: &AuthContext,
    ) -> Result<Vec<ExecutionInfo>, EventError> {
        self.validate_auth(auth_context, channel_id, Permission::Read)?;
        self.store
            .list_executions(channel_id, limit)
            .await
            .map_err(EventError::from)
    }

    /// Delete an execution record (write permission on its channel).
    #[instrument(skip(self, auth_context), fields(execution = %execution_id))]
    pub async fn delete_execution(
        &self,
        execution_id: Uuid,
        auth_context: &AuthContext,
    ) -> Result<bool, EventError> {
        let info = self.store.get_execution(execution_id).await?;
        match info {
            Some(e) => {
                self.validate_auth(auth_context, &e.channel_id, Permission::Write)?;
                self.store
                    .delete_execution(execution_id)
                    .await
                    .map_err(EventError::from)
            }
            None => Err(EventError::ExecutionNotFound(execution_id.to_string())),
        }
    }

    /// Dispatch due wake-ups (server-internal, invoked by the scheduler
    /// task). Persists each wake-up as a `scheduler.wakeup` event and
    /// broadcasts it live to the channel's subscribers. Returns the number
    /// dispatched.
    #[instrument(skip(self))]
    pub async fn dispatch_due_wakeups(&self, batch_limit: i64) -> Result<u64, EventError> {
        let events = self
            .store
            .dispatch_due_wakeups(batch_limit)
            .await
            .map_err(EventError::from)?;
        let count = events.len() as u64;
        for event in &events {
            self.spawn_broadcast(event, None);
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::super::schema::{EventMetadata, PublishEventRequest};
    use super::*;
    use crate::events::store::EventStoreError;

    // ── Test helpers ──────────────────────────────────────────────

    fn auth_write() -> AuthContext {
        AuthContext::Authenticated {
            user_id: "test-user".into(),
            permissions: crate::auth::parse_api_key_permissions(&sonic_rs::json!({
                "read": ["*"],
                "write": ["*"],
            })),
        }
    }

    fn auth_read_only() -> AuthContext {
        AuthContext::Authenticated {
            user_id: "test-user".into(),
            permissions: crate::auth::parse_api_key_permissions(&sonic_rs::json!({
                "read": ["*"],
            })),
        }
    }

    fn auth_admin() -> AuthContext {
        AuthContext::Authenticated {
            user_id: "admin".into(),
            permissions: crate::auth::parse_api_key_permissions(&sonic_rs::json!({
                "admin": ["*"],
            })),
        }
    }

    fn auth_no_perms() -> AuthContext {
        AuthContext::Authenticated {
            user_id: "user".into(),
            permissions: crate::auth::parse_api_key_permissions(&sonic_rs::json!({})),
        }
    }

    fn auth_apikey_write() -> AuthContext {
        AuthContext::ApiKey {
            key_id: "key-1".into(),
            key_source: "local".into(),
            expires_at: None,
            permissions: crate::auth::parse_api_key_permissions(&sonic_rs::json!({
                "read": ["*"],
                "write": ["*"],
            })),
        }
    }

    fn auth_apikey_read() -> AuthContext {
        AuthContext::ApiKey {
            key_id: "key-2".into(),
            key_source: "local".into(),
            expires_at: None,
            permissions: crate::auth::parse_api_key_permissions(&sonic_rs::json!({
                "read": ["*"],
            })),
        }
    }

    /// A key scoped to `project-a.*` — used to prove channel-level
    /// authorization is enforced (audit item 9).
    fn auth_apikey_scoped() -> AuthContext {
        AuthContext::ApiKey {
            key_id: "key-3".into(),
            key_source: "local".into(),
            expires_at: None,
            permissions: crate::auth::parse_api_key_permissions(&sonic_rs::json!({
                "read": ["project-a.*"],
            })),
        }
    }

    fn valid_request() -> PublishEventRequest {
        PublishEventRequest {
            channel_id: "test-channel".into(),
            event_type: "test.event".into(),
            payload: sonic_rs::json!({"key": "value"}),
            metadata: EventMetadata {
                actor: "test-agent".into(),
                agent_id: None,
                user_id: None,
                metadata: None,
            },
            depends_on: None,
            dependencies: Vec::new(),
            operation_id: None,
            step_number: None,
        }
    }

    // ── validate_auth tests ──────────────────────────────────────

    #[test]
    fn validate_auth_anonymous_rejected() {
        let err = EventHandler::validate_auth_static(
            &AuthContext::Anonymous,
            "normal-channel",
            Permission::Read,
        );
        assert!(err.is_err());
        assert!(matches!(err.unwrap_err(), EventError::Unauthenticated(_)));
    }

    #[test]
    fn validate_auth_system_channel_rejected() {
        let err =
            EventHandler::validate_auth_static(&auth_write(), "system.reload", Permission::Read);
        assert!(matches!(err.unwrap_err(), EventError::Forbidden(_)));
    }

    #[test]
    fn validate_auth_admin_channel_rejected() {
        let err =
            EventHandler::validate_auth_static(&auth_write(), "admin.users", Permission::Read);
        assert!(matches!(err.unwrap_err(), EventError::Forbidden(_)));
    }

    #[test]
    fn validate_auth_write_on_normal_channel_succeeds() {
        assert!(
            EventHandler::validate_auth_static(&auth_write(), "my-channel", Permission::Write,)
                .is_ok()
        );
    }

    #[test]
    fn validate_auth_read_on_normal_channel_succeeds() {
        assert!(
            EventHandler::validate_auth_static(&auth_write(), "my-channel", Permission::Read,)
                .is_ok()
        );
    }

    #[test]
    fn validate_auth_read_only_cannot_write() {
        let err =
            EventHandler::validate_auth_static(&auth_read_only(), "my-channel", Permission::Write);
        assert!(matches!(err.unwrap_err(), EventError::Forbidden(_)));
    }

    #[test]
    fn validate_auth_read_only_can_read() {
        assert!(EventHandler::validate_auth_static(
            &auth_read_only(),
            "my-channel",
            Permission::Read,
        )
        .is_ok());
    }

    #[test]
    fn validate_auth_no_perms_blocked_on_read() {
        assert!(EventHandler::validate_auth_static(
            &auth_no_perms(),
            "my-channel",
            Permission::Read,
        )
        .is_err());
    }

    #[test]
    fn validate_auth_no_perms_blocked_on_write() {
        assert!(EventHandler::validate_auth_static(
            &auth_no_perms(),
            "my-channel",
            Permission::Write,
        )
        .is_err());
    }

    #[test]
    fn validate_auth_admin_can_read_and_write() {
        assert!(
            EventHandler::validate_auth_static(&auth_admin(), "my-channel", Permission::Read,)
                .is_ok()
        );
        assert!(
            EventHandler::validate_auth_static(&auth_admin(), "my-channel", Permission::Write,)
                .is_ok()
        );
    }

    #[test]
    fn validate_auth_apikey_write_can_read_and_write() {
        assert!(EventHandler::validate_auth_static(
            &auth_apikey_write(),
            "my-channel",
            Permission::Write,
        )
        .is_ok());
        assert!(EventHandler::validate_auth_static(
            &auth_apikey_write(),
            "my-channel",
            Permission::Read,
        )
        .is_ok());
    }

    #[test]
    fn validate_auth_apikey_read_only_cannot_write() {
        assert!(EventHandler::validate_auth_static(
            &auth_apikey_read(),
            "my-channel",
            Permission::Read,
        )
        .is_ok());
        assert!(EventHandler::validate_auth_static(
            &auth_apikey_read(),
            "my-channel",
            Permission::Write,
        )
        .is_err());
    }

    #[test]
    fn validate_auth_anonymous_blocked_on_all_channels() {
        for ch in &["normal", "test.123", "deeply.nested"] {
            assert!(EventHandler::validate_auth_static(
                &AuthContext::Anonymous,
                ch,
                Permission::Read,
            )
            .is_err());
        }
    }

    #[test]
    fn validate_auth_system_channels_blocked_for_everyone() {
        for ch in &["system.restart", "system.config", "admin.keys"] {
            assert!(
                EventHandler::validate_auth_static(&auth_admin(), ch, Permission::Read,).is_err()
            );
        }
    }

    // ── validate_event tests ─────────────────────────────────────

    #[test]
    fn validate_event_valid_passes() {
        assert!(validate_event(&valid_request()).is_ok());
    }

    #[test]
    fn validate_event_missing_channel_id() {
        let mut req = valid_request();
        req.channel_id = "".into();
        assert!(matches!(
            validate_event(&req).unwrap_err(),
            EventError::InvalidRequest(_)
        ));
    }

    #[test]
    fn validate_event_channel_id_too_long() {
        let mut req = valid_request();
        req.channel_id = "x".repeat(257);
        assert!(matches!(
            validate_event(&req).unwrap_err(),
            EventError::InvalidRequest(_)
        ));
    }

    #[test]
    fn validate_event_channel_id_at_max_is_ok() {
        let mut req = valid_request();
        req.channel_id = "x".repeat(256);
        assert!(validate_event(&req).is_ok());
    }

    #[test]
    fn validate_event_missing_event_type() {
        let mut req = valid_request();
        req.event_type = "".into();
        assert!(matches!(
            validate_event(&req).unwrap_err(),
            EventError::InvalidRequest(_)
        ));
    }

    #[test]
    fn validate_event_event_type_too_long() {
        let mut req = valid_request();
        req.event_type = "x".repeat(129);
        assert!(matches!(
            validate_event(&req).unwrap_err(),
            EventError::InvalidRequest(_)
        ));
    }

    #[test]
    fn validate_event_missing_actor() {
        let mut req = valid_request();
        req.metadata.actor = "".into();
        assert!(matches!(
            validate_event(&req).unwrap_err(),
            EventError::InvalidRequest(_)
        ));
    }

    #[test]
    fn validate_event_actor_too_long() {
        let mut req = valid_request();
        req.metadata.actor = "x".repeat(257);
        assert!(matches!(
            validate_event(&req).unwrap_err(),
            EventError::InvalidRequest(_)
        ));
    }

    #[test]
    fn validate_event_payload_too_large() {
        let mut req = valid_request();
        req.payload = sonic_rs::json!({"d": "x".repeat(1024 * 1024 + 1)});
        assert!(matches!(
            validate_event(&req).unwrap_err(),
            EventError::InvalidRequest(_)
        ));
    }

    #[test]
    fn validate_event_payload_at_limit_ok() {
        let mut req = valid_request();
        req.payload = sonic_rs::json!({"d": "x".repeat(1024 * 1024 - 100)});
        assert!(validate_event(&req).is_ok());
    }

    #[test]
    fn validate_event_empty_payload_ok() {
        let mut req = valid_request();
        req.payload = sonic_rs::json!({});
        assert!(validate_event(&req).is_ok());
    }

    // ── validate_tool_payload_size tests ─────────────────────────

    #[test]
    fn tool_payload_none_and_small_ok() {
        assert!(validate_tool_payload_size("params", None).is_ok());
        assert!(validate_tool_payload_size(
            "params",
            Some(&sonic_rs::json!({"tool": "charge", "amount": 42}))
        )
        .is_ok());
    }

    #[test]
    fn tool_payload_over_cap_rejected() {
        let big = sonic_rs::json!({"blob": "x".repeat(1024 * 1024 + 1)});
        assert!(matches!(
            validate_tool_payload_size("params", Some(&big)).unwrap_err(),
            EventError::InvalidRequest(msg) if msg.contains("params exceeds maximum size")
        ));
    }

    #[test]
    fn tool_payload_at_cap_ok() {
        let at = sonic_rs::json!({"blob": "x".repeat(1024 * 1024 - 100)});
        assert!(validate_tool_payload_size("result", Some(&at)).is_ok());
    }

    #[test]
    fn tool_fail_message_stack_over_cap_rejected() {
        let stack = "x".repeat(1024 * 1024);
        assert!(matches!(
            validate_tool_error_size("boom", Some(&stack)).unwrap_err(),
            EventError::InvalidRequest(msg) if msg.contains("message+stack exceeds maximum size")
        ));
    }

    #[test]
    fn tool_fail_message_stack_at_cap_ok() {
        let stack = "x".repeat(1024 * 1024 - 5);
        assert!(validate_tool_error_size("boom", Some(&stack)).is_ok());
        assert!(validate_tool_error_size("boom", None).is_ok());
    }

    // ── AuthContext permission checks ────────────────────────────

    #[test]
    fn auth_context_can_read_write_variants() {
        let ch = "my-channel";
        assert!(auth_write().can_read(ch));
        assert!(auth_write().can_write(ch));
        assert!(auth_read_only().can_read(ch));
        assert!(!auth_read_only().can_write(ch));
        assert!(auth_admin().can_read(ch));
        assert!(auth_admin().can_write(ch));
        assert!(!auth_no_perms().can_read(ch));
        assert!(!auth_no_perms().can_write(ch));
        assert!(!AuthContext::Anonymous.can_read(ch));
        assert!(!AuthContext::Anonymous.can_write(ch));
        assert!(auth_apikey_write().can_read(ch));
        assert!(auth_apikey_write().can_write(ch));
        assert!(auth_apikey_read().can_read(ch));
        assert!(!auth_apikey_read().can_write(ch));
    }

    #[test]
    fn validate_auth_enforces_resource_scoping() {
        // A key scoped to project-a.* can read project-a but is denied
        // project-b (audit item 9 — previously only global read/write was
        // checked).
        let auth = auth_apikey_scoped();
        assert!(
            EventHandler::validate_auth_static(&auth, "project-a.ch", Permission::Read,).is_ok()
        );
        let err = EventHandler::validate_auth_static(&auth, "project-b.ch", Permission::Read);
        assert!(matches!(err.unwrap_err(), EventError::Forbidden(_)));
        // Scoped read-only key cannot write anywhere.
        let err = EventHandler::validate_auth_static(&auth, "project-a.ch", Permission::Write);
        assert!(matches!(err.unwrap_err(), EventError::Forbidden(_)));
    }

    // ── EventError tests ─────────────────────────────────────────

    #[test]
    fn event_error_display_invalid_request() {
        assert_eq!(
            EventError::InvalidRequest("msg".into()).to_string(),
            "Invalid request: msg"
        );
    }

    #[test]
    fn event_error_display_unauthenticated() {
        assert_eq!(
            EventError::Unauthenticated("nope".into()).to_string(),
            "Unauthenticated: nope"
        );
    }

    #[test]
    fn event_error_display_forbidden() {
        assert_eq!(
            EventError::Forbidden("denied".into()).to_string(),
            "Forbidden: denied"
        );
    }

    #[test]
    fn event_error_from_store_not_found() {
        let store_err = EventStoreError::NotFound;
        let handler_err = EventError::from(store_err);
        assert!(matches!(handler_err, EventError::NotFound(_)));
    }

    #[test]
    fn event_error_from_execution_errors() {
        let key = EventError::from(EventStoreError::ExecutionKeyMismatch {
            channel_id: "ch".into(),
            key_name: "k".into(),
        });
        assert!(matches!(
            key,
            EventError::ExecutionKeyMismatch { channel_id, key_name }
            if channel_id == "ch" && key_name == "k"
        ));

        let nf = EventError::from(EventStoreError::ExecutionNotFound("e1".into()));
        assert!(matches!(nf, EventError::ExecutionNotFound(id) if id == "e1"));

        let owned = EventError::from(EventStoreError::ExecutionNotOwned("e1".into()));
        assert!(matches!(owned, EventError::ExecutionNotOwned(id) if id == "e1"));
    }

    #[test]
    fn event_error_display_execution_errors() {
        assert!(EventError::ExecutionKeyMismatch {
            channel_id: "ch".into(),
            key_name: "k".into(),
        }
        .to_string()
        .contains("Idempotency key conflict"));
        assert!(EventError::ExecutionNotFound("e1".into())
            .to_string()
            .contains("not found"));
        assert!(EventError::ExecutionNotOwned("e1".into())
            .to_string()
            .contains("owned by another attempt"));
    }

    #[test]
    fn parse_claim_token_handles_all_shapes() {
        let valid = Uuid::new_v4().to_string();
        assert_eq!(
            EventHandler::parse_claim_token(Some(&valid)).unwrap(),
            Some(Uuid::parse_str(&valid).unwrap())
        );
        assert_eq!(EventHandler::parse_claim_token(None).unwrap(), None);
        assert_eq!(EventHandler::parse_claim_token(Some("")).unwrap(), None);
        assert_eq!(EventHandler::parse_claim_token(Some("  ")).unwrap(), None);
        assert!(EventHandler::parse_claim_token(Some("not-a-uuid")).is_err());
    }

    #[test]
    fn event_error_implements_std_error() {
        let e = EventError::Internal("test".into());
        assert!(<dyn std::error::Error>::is::<EventError>(&e));
    }

    #[test]
    fn permission_is_copy() {
        let p = Permission::Write;
        let q = p;
        assert_eq!(p, q);
    }

    // ── validate_channel_id charset rules ────────────────────────

    #[test]
    fn validate_channel_id_allows_supported_special_chars() {
        for ch in &[
            "a.b",
            "a_b",
            "a:b",
            "a-b",
            "project-a.channel_1:step",
            "UPPER.lower_2",
        ] {
            assert!(validate_channel_id(ch).is_ok(), "{ch} should be valid");
        }
    }

    #[test]
    fn validate_channel_id_rejects_reserved_chars() {
        for ch in &[
            "a/b",
            "a b",
            "a\tb",
            "a\nb",
            "a\\b",
            "a@b",
            "a+b",
            "é",
            "日本語",
            "a\"b",
            "a'b",
        ] {
            assert!(validate_channel_id(ch).is_err(), "{ch:?} must be rejected");
        }
    }

    #[test]
    fn validate_channel_id_length_boundaries() {
        assert!(validate_channel_id("").is_err());
        assert!(validate_channel_id(&"x".repeat(257)).is_err());
        assert!(validate_channel_id(&"x".repeat(256)).is_ok());
        assert!(validate_channel_id("x").is_ok());
    }

    // ── validate_event field boundaries ──────────────────────────

    #[test]
    fn validate_event_rejects_oversized_actor() {
        let mut req = valid_request();
        req.metadata.actor = "x".repeat(257);
        assert!(matches!(
            validate_event(&req).unwrap_err(),
            EventError::InvalidRequest(msg) if msg.contains("actor exceeds maximum")
        ));
    }

    #[test]
    fn validate_event_actor_at_max_ok() {
        let mut req = valid_request();
        req.metadata.actor = "x".repeat(256);
        assert!(validate_event(&req).is_ok());
    }

    #[test]
    fn validate_event_rejects_invalid_channel_charset() {
        let mut req = valid_request();
        req.channel_id = "bad channel/slash".into();
        assert!(matches!(
            validate_event(&req).unwrap_err(),
            EventError::InvalidRequest(_)
        ));
    }

    #[test]
    fn validate_event_accepts_non_object_payloads() {
        for payload in [
            sonic_rs::json!([]),
            sonic_rs::json!("str"),
            sonic_rs::json!(42),
            sonic_rs::json!(null),
        ] {
            let mut req = valid_request();
            req.payload = payload;
            assert!(validate_event(&req).is_ok());
        }
    }

    #[test]
    fn validate_tool_payload_size_embeds_field_name() {
        let big = sonic_rs::json!({"blob": "x".repeat(1024 * 1024 + 1)});
        let err = validate_tool_payload_size("params", Some(&big)).unwrap_err();
        assert!(matches!(err, EventError::InvalidRequest(msg) if msg.contains("params")));
        let err2 = validate_tool_payload_size("result", Some(&big)).unwrap_err();
        assert!(matches!(err2, EventError::InvalidRequest(msg) if msg.contains("result")));
    }

    // ── EventError::from(EventStoreError) mapping ────────────────

    #[test]
    fn event_error_from_store_error_maps_leaf_variants() {
        assert!(matches!(
            EventError::from(EventStoreError::NotFound),
            EventError::NotFound(_)
        ));
        assert!(matches!(
            EventError::from(EventStoreError::GroupNotFound("g".into())),
            EventError::GroupNotFound(_)
        ));
        assert!(matches!(
            EventError::from(EventStoreError::ConsumerNotFound("c".into())),
            EventError::ConsumerNotFound(_)
        ));
        assert!(matches!(
            EventError::from(EventStoreError::ExecutionGroupNotFound("g".into())),
            EventError::ExecutionGroupNotFound(_)
        ));
        assert!(matches!(
            EventError::from(EventStoreError::ExecutionNotFound("e".into())),
            EventError::ExecutionNotFound(_)
        ));
        assert!(matches!(
            EventError::from(EventStoreError::ExecutionNotOwned("e".into())),
            EventError::ExecutionNotOwned(_)
        ));
        assert!(matches!(
            EventError::from(EventStoreError::WakeupNotFound("w".into())),
            EventError::WakeupNotFound(_)
        ));
        assert!(matches!(
            EventError::from(EventStoreError::QuotaExceeded),
            EventError::QuotaExceeded
        ));
        assert!(matches!(
            EventError::from(EventStoreError::ChannelDeleted("c".into())),
            EventError::ChannelDeleted(_)
        ));
        assert!(matches!(
            EventError::from(EventStoreError::InvalidRequest("m".into())),
            EventError::InvalidRequest(_)
        ));
        assert!(matches!(
            EventError::from(EventStoreError::ExecutionInsertRace),
            EventError::Store(_)
        ));
        assert!(matches!(
            EventError::from(EventStoreError::InconsistentState("m".into())),
            EventError::Store(_)
        ));
    }

    #[test]
    fn event_error_from_store_error_maps_struct_variants() {
        let e = EventError::from(EventStoreError::ExecutionGroupMemberFenced {
            group_id: "g".into(),
            member_id: "m".into(),
        });
        assert!(
            matches!(e, EventError::ExecutionGroupMemberFenced { group_id, member_id } if group_id == "g" && member_id == "m")
        );

        let e = EventError::from(EventStoreError::ExecutionGroupMemberClaimed {
            group_id: "g".into(),
            member_id: "m".into(),
        });
        assert!(matches!(e, EventError::ExecutionGroupMemberClaimed { .. }));

        let e = EventError::from(EventStoreError::ExecutionKeyMismatch {
            channel_id: "ch".into(),
            key_name: "k".into(),
        });
        assert!(matches!(e, EventError::ExecutionKeyMismatch { .. }));

        let e = EventError::from(EventStoreError::InvalidTimeRange("bad".into()));
        assert!(matches!(e, EventError::InvalidRequest(msg) if msg.contains("Invalid timestamp")));

        let e = EventError::from(EventStoreError::CounterfactualBlocked {
            fork_group_id: "g".into(),
            boundary: "member:m:claim".into(),
        });
        assert!(
            matches!(e, EventError::CounterfactualBlocked { fork_group_id, .. } if fork_group_id == "g")
        );

        let e = EventError::from(EventStoreError::ExecutionResultExpired {
            channel_id: "ch".into(),
            key_name: "k".into(),
            execution_id: uuid::Uuid::new_v4(),
        });
        assert!(matches!(e, EventError::ExecutionResultExpired { .. }));
    }

    #[test]
    fn event_error_from_store_snapshot_and_version() {
        let e = EventError::from(EventStoreError::SnapshotBoundaryNotFound {
            channel_id: "ch".into(),
            at_cursor: 42,
        });
        assert!(
            matches!(e, EventError::SnapshotBoundaryNotFound { channel_id, at_cursor } if channel_id == "ch" && at_cursor == 42)
        );

        let e = EventError::from(EventStoreError::VersionConflict {
            channel_id: "ch".into(),
            expected_version: Some(1),
            actual_version: Some(2),
            expected_cursor: None,
            actual_cursor: None,
        });
        assert!(matches!(
            e,
            EventError::VersionConflict {
                expected_version: Some(1),
                ..
            }
        ));
    }

    // ── PR-009: source-instance per-subscriber live transform ───────────

    fn event_value(payload: sonic_rs::Value, event_type: &str) -> sonic_rs::Value {
        sonic_rs::json!({
            "id": "e1",
            "channel_id": "ch.a",
            "type": "broadcast",
            "event_type": event_type,
            "payload": payload,
            "cursor": 1,
            "timestamp": "2026-01-01T00:00:00Z",
        })
    }

    #[test]
    fn federated_transform_redacts_and_drops() {
        use crate::auth::source_policy::FederationSourcePolicy;
        // Redaction policy: strip secrets.
        let policy = FederationSourcePolicy {
            enabled: true,
            redactions: vec![crate::auth::source_policy::Redaction {
                pointer: "/secrets".into(),
                redact_with: None,
            }],
            ..Default::default()
        };
        let ev = event_value(
            sonic_rs::json!({"secrets": {"k": "sk-live"}, "ok": 1}),
            "tool.completed",
        );
        let frame = EventHandler::transform_for_federated(&policy, &ev).unwrap();
        let parsed: sonic_rs::Value = sonic_rs::from_str(&frame).unwrap();
        let inner = &parsed["payload"]["event"]["payload"];
        assert_eq!(inner["secrets"], sonic_rs::json!("[REDACTED]"));
        assert_eq!(inner["ok"], sonic_rs::json!(1));
        // The secret sentinel must NEVER appear in the emitted frame.
        assert!(!frame.contains("sk-live"));
    }

    #[test]
    fn federated_transform_metadata_only_and_type_deny() {
        use crate::auth::source_policy::FederationSourcePolicy;
        // metadata_only: payloads dropped entirely.
        let meta = FederationSourcePolicy {
            enabled: true,
            metadata_only: true,
            ..Default::default()
        };
        let ev = event_value(sonic_rs::json!({"big": "secret"} ), "tool.x");
        let frame = EventHandler::transform_for_federated(&meta, &ev).unwrap();
        assert!(!frame.contains("secret"));
        let parsed: sonic_rs::Value = sonic_rs::from_str(&frame).unwrap();
        assert_eq!(parsed["payload"]["event"]["payload"], sonic_rs::json!({}));

        // Type deny: the event is dropped entirely (None).
        let deny = FederationSourcePolicy {
            enabled: true,
            denied_types: vec!["secret.*".into()],
            ..Default::default()
        };
        let ev2 = event_value(sonic_rs::json!({"x": 1}), "secret.event");
        assert!(EventHandler::transform_for_federated(&deny, &ev2).is_none());

        // Disabled policy drops the frame too (fail closed).
        let disabled = FederationSourcePolicy::default();
        assert!(EventHandler::transform_for_federated(&disabled, &ev).is_none());
    }

    #[test]
    fn federated_transform_max_size_caps_payload() {
        use crate::auth::source_policy::FederationSourcePolicy;
        let cap = FederationSourcePolicy {
            enabled: true,
            max_output_bytes: 8,
            ..Default::default()
        };
        let ev = event_value(sonic_rs::json!({"big": "a very long payload"}), "tool.x");
        let frame = EventHandler::transform_for_federated(&cap, &ev).unwrap();
        let parsed: sonic_rs::Value = sonic_rs::from_str(&frame).unwrap();
        assert_eq!(parsed["payload"]["event"]["payload"], sonic_rs::json!({}));
    }
}
