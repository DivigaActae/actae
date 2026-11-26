// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize, Serializer};
use sonic_rs::Value;
use uuid::Uuid;

use chrono::{DateTime, Utc};

/// Well-known event type a client records to **explicitly declare that it
/// recovered after a crash**.
///
/// This is the opt-in recovery signal for clients that do not use
/// `AgentSession`'s session-metadata contract — raw `record()` calls,
/// framework adapters that own their own loop, or any HTTP client. When the
/// server records an event of this type it meters one `recovery_operations`
/// usage unit (transactionally, idempotently per `operation_id`) and bumps the
/// local `actae_recoveries_total` counter. It never affects state, step
/// resolution, or replay; it is an ordinary, auditable event on the channel.
///
/// SDKs export this string (Python `RECOVERY_MARKER_EVENT`, Go
/// `RecoveryMarkerEvent`, TS `RECOVERY_MARKER_EVENT`). See
/// `docs/MEASUREMENT.md`.
pub const RECOVERY_MARKER_EVENT: &str = "run.recovered";

fn serialize_sonic_value_opt<S>(v: &Option<Value>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match v {
        None => serializer.serialize_none(),
        Some(val) => {
            let json_str = sonic_rs::to_string(val).map_err(serde::ser::Error::custom)?;
            let serde_val: serde_json::Value =
                serde_json::from_str(&json_str).map_err(serde::ser::Error::custom)?;
            serde_val.serialize(serializer)
        }
    }
}

/// Standardized event types for AI agent execution history.
/// Every action an agent takes maps to one of these types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AgentEventType {
    /// Agent execution started
    AgentStarted,
    /// Agent execution completed (success or failure)
    AgentCompleted,
    /// Agent execution failed with error
    AgentFailed,
    /// Agent paused or yielded control
    AgentPaused,
    /// Agent resumed execution
    AgentResumed,

    /// Tool execution started
    ToolStarted,
    /// Tool execution completed with result
    ToolCompleted,
    /// Tool execution failed
    ToolFailed,
    /// A completed tool execution was replayed from its persisted result
    /// (idempotency hit — the tool function was not invoked again).
    ToolReplayed,

    /// Human handoff requested (agent waiting for human input)
    HumanHandoffRequested,
    /// Human handoff completed (input received)
    HumanHandoffCompleted,
    /// Human approval requested
    HumanApprovalRequested,
    /// Human approval granted
    HumanApprovalGranted,
    /// Human approval denied
    HumanApprovalDenied,

    /// Memory was read
    MemoryRead,
    /// Memory was written
    MemoryWritten,
    /// Memory was updated
    MemoryUpdated,
    /// Memory was deleted
    MemoryDeleted,

    /// A checkpoint was created in the execution
    CheckpointCreated,
    /// A checkpoint was restored
    CheckpointRestored,

    /// A state transition occurred
    StateTransition,

    /// A message was sent to a user
    MessageSent,
    /// A message was received from a user
    MessageReceived,

    /// A workflow was started
    WorkflowStarted,
    /// A workflow step completed
    WorkflowStepCompleted,
    /// A workflow was paused
    WorkflowPaused,
    /// A workflow was resumed
    WorkflowResumed,
    /// A workflow was completed
    WorkflowCompleted,
    /// A workflow failed
    WorkflowFailed,

    /// A run (root execution) was created
    RunCreated,
    /// A run was forked into an experiment (recorded on parent)
    ForkCreated,
    /// A new experiment fork started executing (recorded on child)
    ForkStarted,
    /// A persisted scheduled wake-up fired (recorded on the target channel)
    ScheduledWakeup,

    /// An error occurred that doesn't fit other categories
    Error,
    /// A warning was generated
    Warning,
    /// Informational event
    Info,

    /// Custom event type (for extensibility)
    #[serde(untagged)]
    Custom(String),
}

impl AgentEventType {
    pub fn as_str(&self) -> &str {
        match self {
            AgentEventType::AgentStarted => "agent.started",
            AgentEventType::AgentCompleted => "agent.completed",
            AgentEventType::AgentFailed => "agent.failed",
            AgentEventType::AgentPaused => "agent.paused",
            AgentEventType::AgentResumed => "agent.resumed",
            AgentEventType::ToolStarted => "tool.started",
            AgentEventType::ToolCompleted => "tool.completed",
            AgentEventType::ToolFailed => "tool.failed",
            AgentEventType::ToolReplayed => "tool.replayed",
            AgentEventType::HumanHandoffRequested => "human.handoff.requested",
            AgentEventType::HumanHandoffCompleted => "human.handoff.completed",
            AgentEventType::HumanApprovalRequested => "human.approval.requested",
            AgentEventType::HumanApprovalGranted => "human.approval.granted",
            AgentEventType::HumanApprovalDenied => "human.approval.denied",
            AgentEventType::MemoryRead => "memory.read",
            AgentEventType::MemoryWritten => "memory.written",
            AgentEventType::MemoryUpdated => "memory.updated",
            AgentEventType::MemoryDeleted => "memory.deleted",
            AgentEventType::CheckpointCreated => "checkpoint.created",
            AgentEventType::CheckpointRestored => "checkpoint.restored",
            AgentEventType::StateTransition => "state.transition",
            AgentEventType::MessageSent => "message.sent",
            AgentEventType::MessageReceived => "message.received",
            AgentEventType::WorkflowStarted => "workflow.started",
            AgentEventType::WorkflowStepCompleted => "workflow.step.completed",
            AgentEventType::WorkflowPaused => "workflow.paused",
            AgentEventType::WorkflowResumed => "workflow.resumed",
            AgentEventType::WorkflowCompleted => "workflow.completed",
            AgentEventType::WorkflowFailed => "workflow.failed",
            AgentEventType::RunCreated => "run.created",
            AgentEventType::ForkCreated => "fork.created",
            AgentEventType::ForkStarted => "fork.started",
            AgentEventType::ScheduledWakeup => "scheduler.wakeup",
            AgentEventType::Error => "error",
            AgentEventType::Warning => "warning",
            AgentEventType::Info => "info",
            AgentEventType::Custom(s) => s.as_str(),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "agent.started" => AgentEventType::AgentStarted,
            "agent.completed" => AgentEventType::AgentCompleted,
            "agent.failed" => AgentEventType::AgentFailed,
            "agent.paused" => AgentEventType::AgentPaused,
            "agent.resumed" => AgentEventType::AgentResumed,
            "tool.started" => AgentEventType::ToolStarted,
            "tool.completed" => AgentEventType::ToolCompleted,
            "tool.failed" => AgentEventType::ToolFailed,
            "tool.replayed" => AgentEventType::ToolReplayed,
            "human.handoff.requested" => AgentEventType::HumanHandoffRequested,
            "human.handoff.completed" => AgentEventType::HumanHandoffCompleted,
            "human.approval.requested" => AgentEventType::HumanApprovalRequested,
            "human.approval.granted" => AgentEventType::HumanApprovalGranted,
            "human.approval.denied" => AgentEventType::HumanApprovalDenied,
            "memory.read" => AgentEventType::MemoryRead,
            "memory.written" => AgentEventType::MemoryWritten,
            "memory.updated" => AgentEventType::MemoryUpdated,
            "memory.deleted" => AgentEventType::MemoryDeleted,
            "checkpoint.created" => AgentEventType::CheckpointCreated,
            "checkpoint.restored" => AgentEventType::CheckpointRestored,
            "state.transition" => AgentEventType::StateTransition,
            "message.sent" => AgentEventType::MessageSent,
            "message.received" => AgentEventType::MessageReceived,
            "workflow.started" => AgentEventType::WorkflowStarted,
            "workflow.step.completed" => AgentEventType::WorkflowStepCompleted,
            "workflow.paused" => AgentEventType::WorkflowPaused,
            "workflow.resumed" => AgentEventType::WorkflowResumed,
            "workflow.completed" => AgentEventType::WorkflowCompleted,
            "workflow.failed" => AgentEventType::WorkflowFailed,
            "run.created" => AgentEventType::RunCreated,
            // Legacy spellings remain parseable: the hash chain commits to
            // event_type, so pre-existing `run.forked` / `branch.started` rows
            // are intentionally not rewritten (see migration v30).
            "fork.created" | "run.forked" => AgentEventType::ForkCreated,
            "fork.started" | "branch.started" => AgentEventType::ForkStarted,
            "scheduler.wakeup" => AgentEventType::ScheduledWakeup,
            "error" => AgentEventType::Error,
            "warning" => AgentEventType::Warning,
            "info" => AgentEventType::Info,
            _ => AgentEventType::Custom(s.to_string()),
        }
    }
}

impl std::fmt::Display for AgentEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Metadata attached to every event.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Who or what triggered this event
    pub actor: String,
    /// The agent instance ID
    pub agent_id: Option<String>,
    /// The user ID associated with the execution
    pub user_id: Option<String>,
    /// Additional structured metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// An event record as stored and transmitted.
/// This is the core data type of the entire system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvent {
    pub id: Uuid,
    pub channel_id: String,
    /// The type of agent event
    #[serde(alias = "type")]
    pub event_type: String,
    pub payload: Value,
    pub metadata: EventMetadata,
    /// Per-channel gapless cursor (starts at 1 for each channel). This is
    /// the public cursor — replay, subscribe-resume, deep links and
    /// group offsets all speak per-channel positions.
    pub cursor: i64,
    pub timestamp: DateTime<Utc>,
    /// Optional reference to a parent event this event causally depends on.
    /// The referenced event can be in any channel, enabling cross-channel
    /// causal ordering. When replaying with `resolve_dependencies`, the
    /// referenced event is included in the result set regardless of channel.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Uuid>,
}

/// A record as stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub id: Uuid,
    pub channel_id: String,
    pub event_type: String,
    pub payload: Value,
    pub actor: String,
    pub agent_id: Option<String>,
    pub user_id: Option<String>,
    pub metadata: Option<Value>,
    pub cursor: i64,
    pub depends_on: Option<Uuid>,
    pub timestamp: DateTime<Utc>,
}

impl From<AgentEvent> for EventRecord {
    fn from(event: AgentEvent) -> Self {
        EventRecord {
            id: event.id,
            channel_id: event.channel_id,
            event_type: event.event_type,
            payload: event.payload,
            actor: event.metadata.actor,
            agent_id: event.metadata.agent_id,
            user_id: event.metadata.user_id,
            metadata: event.metadata.metadata,
            cursor: event.cursor,
            depends_on: event.depends_on,
            timestamp: event.timestamp,
        }
    }
}

impl From<EventRecord> for AgentEvent {
    fn from(record: EventRecord) -> Self {
        AgentEvent {
            id: record.id,
            channel_id: record.channel_id,
            event_type: record.event_type,
            payload: record.payload,
            metadata: EventMetadata {
                actor: record.actor,
                agent_id: record.agent_id,
                user_id: record.user_id,
                metadata: record.metadata,
            },
            cursor: record.cursor,
            depends_on: record.depends_on,
            timestamp: record.timestamp,
        }
    }
}

/// A versioned state snapshot for agent context persistence.
/// Each save creates a new immutable version, enabling time-travel
/// inspection and rollback of agent state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateVersion {
    pub version: i64,
    pub cursor: i64,
    pub timestamp: DateTime<Utc>,
}

/// A request to publish an event.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PublishEventRequest {
    pub channel_id: String,
    #[serde(alias = "type")]
    pub event_type: String,
    pub payload: Value,
    pub metadata: EventMetadata,
    /// Optional UUID of a parent event this event causally depends on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Uuid>,
    /// Additional causal parents persisted atomically with the event. The
    /// legacy `depends_on` value is also materialized into the dependency DAG.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<EventDependencyRequest>,
    /// Optional client-supplied idempotency key. When set, retrying with the
    /// same (channel_id, operation_id) returns the original event instead of
    /// inserting a duplicate — safe for client retries after lost responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<Uuid>,
    /// Optional 1-indexed step number for the server-owned step index
    /// (`channel_steps`). When set, a durable step→cursor row is recorded in
    /// the same transaction as the event, making step→cursor resolution and
    /// `fork_at_step` lineage exact and server-side.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_number: Option<i64>,
}

/// A request to replay events from a channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayRequest {
    pub channel_id: String,
    /// Per-channel cursor: replays events where `cursor >` this value.
    /// Omitted/NULL replays from the beginning.
    pub cursor: Option<i64>,
    /// Deprecated alias for `cursor` (kept for older SDK clients).
    pub channel_cursor: Option<i64>,
    pub limit: Option<i64>,
    pub event_type: Option<String>,
}

/// A request to query events across channels.
/// Use cursor-based pagination for large result sets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub channel_ids: Option<Vec<String>>,
    pub event_type: Option<String>,
    pub actor: Option<String>,
    /// Per-channel cursor range: `cursor >= cursor_start` (inclusive).
    pub cursor_start: Option<i64>,
    /// Per-channel cursor range: `cursor <= cursor_end` (inclusive).
    pub cursor_end: Option<i64>,
    /// Deprecated aliases for `cursor_start`/`cursor_end` (older SDK clients).
    pub channel_cursor_start: Option<i64>,
    pub channel_cursor_end: Option<i64>,
    /// Start timestamp (RFC 3339 format, e.g. "2024-01-01T00:00:00Z")
    pub from: Option<String>,
    /// End timestamp (RFC 3339 format)
    pub to: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// Opaque keyset continuation token returned by a prior query (additive,
    /// actae.api.v1). When present, the query continues from the previous
    /// page's `global_cursor` bound. The token is authenticated, versioned,
    /// bound to the normalized filters and caller scope, and expires in 15
    /// minutes. It exposes no SQL value and cannot be reused with changed
    /// filters. See `actae/src/events/page_token.rs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    /// Explicit pagination-mode selector (PR-007, additive). `None` or
    /// `"offset"` preserves the legacy offset-based query path when no
    /// `page_token` is supplied. `"keyset"` runs even the FIRST page (no
    /// token) through the keyset `query_page` path and returns a typed
    /// `next_page_token` when `has_more`. A token supplied with either mode
    /// continues keyset pagination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<String>,
}

/// Metadata for a run/experiment/fork.
/// Stored in the channel_metadata table, not in agent_events.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChannelMetadata {
    pub channel_id: String,
    pub parent_channel_id: Option<String>,
    pub origin_run_id: String,
    /// The RESOLVED fork boundary: the snapshot cursor the child actually
    /// inherited (equals `resolved_state_cursor`; `0` for roots). Since v17
    /// this is the resolved cursor, never the requested one.
    pub forked_at_cursor: i64,
    pub forked_at: DateTime<Utc>,
    pub display_name: Option<String>,
    pub reason: Option<String>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub experiment_metadata: Option<Value>,
    /// The cursor the caller asked for (0 = "latest"). Differs from
    /// `resolved_state_cursor` when the nearest snapshot was at an earlier
    /// cursor. Pre-v17 rows backfill this to `forked_at_cursor`.
    #[serde(default)]
    pub requested_at_cursor: i64,
    /// The snapshot cursor the fork actually inherited (0 = none / lineage-only).
    #[serde(default)]
    pub resolved_state_cursor: i64,
    /// The parent's event id the child's `fork.started` `depends_on`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_event_id: Option<Uuid>,
    /// The source snapshot version copied at fork time (0 = none).
    #[serde(default)]
    pub source_state_version: i64,
    /// SHA-256 of the copied state JSON — the immutable state fingerprint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_state_sha256: Option<String>,
    /// True when a state snapshot was copied into the child at fork time.
    #[serde(default)]
    pub restorable: bool,
    /// The immutable fork manifest (source identity, prompts, tools, model,
    /// environment fingerprint, dependencies, seed) supplied by the SDK.
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub manifest: Option<Value>,
    /// Reproducibility grade: `state_exact` | `context_exact` |
    /// `execution_replayable` | `best_effort`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reproducibility: Option<String>,
    /// Side-effect-aware fork tool policy, mirroring the group-fork model:
    /// `{"tool_name": "replay"|"block"|"live"|"auto", "*": <default>}`.
    /// Only meaningful on forked channels; `None` means the server default
    /// (`auto`) applies to the child.
    #[serde(default, serialize_with = "serialize_sonic_value_opt")]
    pub tool_policies: Option<Value>,
    /// Fork outcome: `promoted` | `rejected` | `inconclusive` | `crashed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    /// Result score recorded for ranking (higher is better).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_score: Option<f64>,
    /// Soft-delete marker (retention/GC). Replays of deleted forks return 410.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<DateTime<Utc>>,
}

/// Request to fork a channel at a specific cursor into a new experiment.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ForkRequest {
    pub source_channel_id: String,
    pub new_channel_id: String,
    /// Fork boundary: `0` forks from the latest saved state; `> 0` forks from
    /// the latest snapshot version whose `cursor <= at_cursor`.
    pub at_cursor: i64,
    /// Client-generated idempotency key. Replaying a fork with the same
    /// operation_id returns the original result without duplicating anything.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    pub display_name: Option<String>,
    pub reason: Option<String>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub experiment_metadata: Option<Value>,
    /// Optional immutable fork manifest (prompts, tools, model, provider,
    /// environment fingerprint, dependency versions, random seed, code rev).
    /// Stored on `channel_metadata.manifest`, echoed in the fork receipt.
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub manifest: Option<Value>,
    /// Optional side-effect policy for the child channel, mirroring the
    /// group-fork model: `{"tool_name": "replay"|"block"|"live"|"auto",
    /// "*": <default>}`. When omitted, the server default `auto` applies:
    /// a completed source execution with identical params is replayed, and
    /// anything new runs live. `replay`/`block` refuse to execute on a miss
    /// or mismatch (persisted as a blocked boundary).
    #[serde(default, serialize_with = "serialize_sonic_value_opt")]
    pub tool_policies: Option<Value>,
    /// Optimistic guard: reject unless the source channel's latest state
    /// version matches. `Some(0)` means "no snapshot exists yet". Lets
    /// "fork latest" mean "latest as of the version I observed".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<i64>,
    /// Optimistic guard: reject unless the source channel's latest state
    /// cursor matches.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_cursor: Option<i64>,
}

/// The immutable fork receipt returned at creation (POST /channels/fork) and
/// queryable via GET /channels/{channel_id}/receipt. This is the provenance
/// record the dashboard and audit narrative are built around.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ForkReceipt {
    /// The child channel id (the fork identity).
    pub fork_id: String,
    pub source_channel_id: String,
    pub child_channel_id: String,
    /// The cursor the caller requested (0 = "latest").
    pub requested_cursor: i64,
    /// The snapshot cursor actually inherited.
    pub resolved_cursor: i64,
    /// The parent boundary event the child's `fork.started` depends on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_event_id: Option<Uuid>,
    /// The source snapshot version copied (0 = none).
    pub source_state_version: i64,
    /// SHA-256 fingerprint of the copied state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_state_sha256: Option<String>,
    /// True when a state snapshot was copied (the fork is a restorable
    /// checkpoint). False for lineage-only/event-only forks.
    pub restorable: bool,
    /// True when this response was served from the idempotency replay path.
    pub replayed: bool,
    /// The manifest supplied at fork time.
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub manifest: Option<Value>,
    /// Reproducibility grade computed at fork time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reproducibility: Option<String>,
    /// The side-effect tool policy declared for the child (None = `auto`).
    #[serde(default, serialize_with = "serialize_sonic_value_opt")]
    pub tool_policies: Option<Value>,
}

/// The immutable fork boundary state reported on a decision trail: what the
/// child actually inherited at fork time, derived from the child's first
/// snapshot (never the parent's mutable latest state).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ForkBoundary {
    /// The channel this state was copied FROM (the parent).
    pub source_channel_id: String,
    /// The requested boundary (0 = "latest").
    pub requested_cursor: i64,
    /// The resolved boundary: snapshot cursor actually inherited.
    pub resolved_cursor: i64,
    /// The parent boundary event the child's `fork.started` depends on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_event_id: Option<Uuid>,
    /// The source snapshot version copied (0 = none).
    pub source_state_version: i64,
    /// SHA-256 fingerprint of the copied state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_state_sha256: Option<String>,
    /// True when a state snapshot was inherited (restorable checkpoint).
    pub restorable: bool,
    /// The inherited state value itself (the copied snapshot).
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub state: Option<Value>,
}

/// A first-class experiment group: a baseline run plus N variant forks
/// with declared parameter deltas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentGroup {
    pub group_id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline_channel_id: Option<String>,
    /// running | completed
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Members of the group (populated on GET /experiments/{id}).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<ExperimentMember>,
}

/// A member (fork) of an experiment group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentMember {
    pub channel_id: String,
    /// baseline | variant
    pub role: String,
    /// The declared parameter deltas vs the baseline.
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub declared_delta: Option<Value>,
    pub created_at: DateTime<Utc>,
    /// Joined from channel_metadata for ranking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_score: Option<f64>,
}

/// Request to create an experiment group.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateExperimentRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline_channel_id: Option<String>,
}

/// Request to add a fork to an experiment group.
#[derive(Debug, Clone, Deserialize)]
pub struct AddExperimentMemberRequest {
    pub channel_id: String,
    /// baseline | variant (default variant).
    #[serde(default = "default_variant_role")]
    pub role: String,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub declared_delta: Option<Value>,
}

fn default_variant_role() -> String {
    "variant".to_string()
}

/// Request to record a fork outcome + score.
#[derive(Debug, Clone, Deserialize)]
pub struct SetOutcomeRequest {
    /// promoted | rejected | inconclusive | crashed
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

/// One ranked member of an experiment (sorted by score desc, nulls last),
/// with the declared parameter delta vs the baseline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedExperimentMember {
    pub channel_id: String,
    pub role: String,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub declared_delta: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_score: Option<f64>,
}

/// The "what changed?" comparison between two channels: state diff + manifest
/// diff + tool-call diff + output diff + metrics delta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompareResponse {
    pub left_channel_id: String,
    pub right_channel_id: String,
    pub state_diff: StateDiffResponse,
    /// Structural diff of the two channels' fork manifests.
    pub manifest_diff: Vec<StateDiffEntry>,
    pub manifest_diff_truncated: bool,
    /// Tool-execution ledger diff keyed by key_name.
    pub tool_diff: Vec<StateDiffEntry>,
    pub tool_diff_truncated: bool,
    /// Final-output comparison (from experiment_metadata.result.output when
    /// present, else the last event's payload.output).
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub output_left: Option<Value>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub output_right: Option<Value>,
    pub output_changed: bool,
    /// Latency/token/cost deltas from experiment_metadata.metrics.
    pub metrics_delta: Vec<StateDiffEntry>,
}

/// Information about a fork in the execution tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkInfo {
    pub metadata: ChannelMetadata,
    pub children: Vec<ForkInfo>,
    pub event_count: i64,
    pub latest_cursor: Option<i64>,
}

/// Request to update channel metadata (display_name, reason, experiment_metadata).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateMetadataRequest {
    pub channel_id: String,
    pub display_name: Option<String>,
    pub reason: Option<String>,
    pub experiment_metadata: Option<Value>,
}

/// Atomic event+snapshot transition request.
/// Persists the event and its resulting state snapshot in a single transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionRequest {
    pub channel_id: String,
    #[serde(alias = "type")]
    pub event_type: String,
    pub payload: Value,
    pub metadata: EventMetadata,
    pub state: Value,
    /// Client-supplied idempotency key for the atomic transition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<Uuid>,
    /// Optimistic concurrency guard: reject unless the channel's latest state
    /// version matches. `Some(0)` means "no snapshot exists yet".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<i64>,
    /// Optimistic concurrency guard: reject unless the channel's latest state
    /// cursor matches.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_cursor: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<EventDependencyRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventDependencyRequest {
    pub event_id: Uuid,
    #[serde(default = "default_dependency_type")]
    pub dependency_type: String,
}

fn default_dependency_type() -> String {
    "causal".to_string()
}

/// A durable consumer group bound to a channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupInfo {
    pub group_id: String,
    pub channel_id: String,
    pub created_at: DateTime<Utc>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub metadata: Option<Value>,
}

// ── Distributed execution groups ────────────────────────────────────────
//
// These deliberately do not reuse `GroupInfo`: that type is the established
// consumer-group API.  An execution group is a logical, durable collection
// of independently running agents, each of which still owns its own channel.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionGroup {
    pub group_id: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionGroupMember {
    pub group_id: String,
    pub member_id: String,
    pub channel_id: String,
    pub role: Option<String>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub metadata: Option<Value>,
    pub owner_id: Option<String>,
    pub generation: i64,
    pub lease_until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberLease {
    pub group_id: String,
    pub member_id: String,
    pub owner_id: String,
    pub generation: i64,
    pub lease_until: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMessage {
    pub message_id: Uuid,
    pub group_id: String,
    pub from_member_id: String,
    pub to_member_id: String,
    pub message_type: String,
    pub payload: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causal_context: Option<Value>,
    pub source_event_id: Uuid,
    pub delivery_event_id: Uuid,
    pub status: String,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub acknowledged_by: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateExecutionGroupRequest {
    pub group_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddExecutionGroupMemberRequest {
    pub member_id: String,
    pub channel_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimMemberRequest {
    pub owner_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMemberRequest {
    pub owner_id: String,
    pub generation: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseMemberRequest {
    pub owner_id: String,
    pub generation: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendGroupMessageRequest {
    pub from_member_id: String,
    pub to_member_id: String,
    #[serde(rename = "type", alias = "message_type")]
    pub message_type: String,
    pub payload: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causal_context: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckGroupMessageRequest {
    pub owner_id: String,
    pub generation: i64,
}

/// A directed causal edge. `dependency_type` is intentionally explicit so
/// callers can distinguish a message delivery from a generic data/tool edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDependency {
    pub event_id: Uuid,
    pub depends_on_event_id: Uuid,
    pub dependency_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalGraph {
    pub root_event_id: Uuid,
    pub events: Vec<AgentEvent>,
    pub dependencies: Vec<EventDependency>,
    pub truncated: bool,
}

/// Immutable configuration and evolving receipt for a counterfactual group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupForkReceipt {
    pub fork_group_id: String,
    pub source_group_id: String,
    pub intervention_member_id: String,
    pub intervention_cursor: i64,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub member_policies: Option<Value>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub tool_policies: Option<Value>,
    pub status: String,
    pub first_divergence_event_id: Option<Uuid>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub receipt: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateGroupForkRequest {
    pub fork_group_id: String,
    pub source_group_id: String,
    pub intervention_member_id: String,
    pub intervention_cursor: i64,
    /// Object mapping every source member to frozen/live/reactive.
    pub member_policies: Value,
    /// Object mapping tool name to replay/live/block.
    pub tool_policies: Value,
}

/// Request to create a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateGroupRequest {
    pub group_id: String,
    pub channel_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// Request to join (or renew) a consumer's lease in a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinGroupRequest {
    pub consumer_id: String,
    /// Lease duration in seconds (default 60).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_seconds: Option<i64>,
}

/// Request to claim work batches from a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimWorkRequest {
    pub consumer_id: String,
    /// Maximum number of events to claim (default 100, max 1000).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// Request to commit a consumer's processed offset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckRequest {
    pub consumer_id: String,
    /// Committed channel_cursor (acknowledges everything up to and including this cursor).
    pub cursor: i64,
}

/// Request to extend a consumer's lease.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub consumer_id: String,
    /// Lease duration in seconds (default 60).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_seconds: Option<i64>,
}

/// A consumer's durable offset state within a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupOffsetInfo {
    pub group_id: String,
    pub consumer_id: String,
    /// Last acknowledged channel_cursor.
    pub last_cursor: i64,
    /// Highest channel_cursor currently claimed by this consumer.
    pub claimed_cursor: i64,
    pub updated_at: DateTime<Utc>,
}

/// A work batch claimed by a consumer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedWork {
    pub group_id: String,
    pub consumer_id: String,
    pub events: Vec<AgentEvent>,
    /// When this claim expires (consumer must heartbeat or re-join before this).
    pub lease_until: DateTime<Utc>,
}

/// A persisted scheduled wake-up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeupInfo {
    pub id: String,
    pub channel_id: String,
    pub run_at: DateTime<Utc>,
    /// pending | done | failed | cancelled
    pub status: String,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub payload: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub executed_at: Option<DateTime<Utc>>,
    pub attempts: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to schedule a wake-up for a channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateWakeupRequest {
    pub channel_id: String,
    /// RFC 3339 timestamp; the wake-up fires as a `scheduler.wakeup` event
    /// at or after this time.
    pub run_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

/// Request to claim (create-or-replay) an idempotent tool execution.
///
/// The idempotency unit is `(channel_id, key_name)`. A completed execution
/// with a matching request hash is replayed (the tool function never runs
/// again); a mismatch produces `IdempotencyKeyMismatch`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimExecutionRequest {
    pub channel_id: String,
    /// Client-chosen idempotency key. Must be stable across retries.
    pub key_name: String,
    /// Logical name of the tool being executed (e.g. `"charge_customer"`).
    pub tool_name: String,
    /// Full input parameters. Used to compute the request hash unless
    /// `dedup_fields` restricts identity to a subset of keys.
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub params: Option<Value>,
    /// Restrict request identity to the listed `params` keys. Prevents
    /// volatile fields (timestamps, attempt ids, nonces) from triggering a
    /// false `idempotency_key_mismatch` on legitimate retries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedup_fields: Option<Vec<String>>,
    /// Lease duration in seconds while the execution is running
    /// (default 60, max 86400).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_seconds: Option<i64>,
    /// When true, a replay of a completed execution records a one-time
    /// `tool.replayed` event on the channel (observability).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emit_replay_event: Option<bool>,
    /// Actor label stamped on tool lifecycle events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

/// A single idempotent tool execution ledger record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionInfo {
    pub id: Uuid,
    pub channel_id: String,
    pub key_name: String,
    pub tool_name: String,
    /// running | completed | failed | interrupted | cancelled | expired
    pub status: String,
    /// Number of start attempts (increments on every claim/reclaim).
    pub attempts: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_until: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_cursor: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_cursor: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_event_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_event_id: Option<Uuid>,
    pub replay_emitted: bool,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub params: Option<Value>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub result: Option<Value>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub error: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Set when the execution's replay window expires under retention. The
    /// params/result/error bodies are nulled; the tiny tombstone row keeps the
    /// key identity so a retried idempotency key fails loudly (`result_expired`)
    /// instead of silently executing again.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expired_at: Option<DateTime<Utc>>,
}

/// A structural difference between two fork states at one JSON path.
///
/// `kind` is one of:
/// - `added` — the key/path exists in `right` but not `left`
/// - `removed` — the key/path exists in `left` but not `right`
/// - `changed` — both sides have the key/path but the values differ
///
/// `left`/`right` carry the value on each side (null for added/removed);
/// `left_cursor`/`right_cursor` anchor the diff to the state snapshot each
/// side was loaded from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateDiffEntry {
    /// JSON pointer-style path (e.g. `["messages","0","role"]`). Empty for
    /// the top-level value when the whole states differ in kind.
    pub path: Vec<String>,
    pub kind: String,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub left: Option<Value>,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub right: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left_cursor: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right_cursor: Option<i64>,
}

/// A point-in-time state snapshot referenced by a diff or decision trail.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatePoint {
    pub channel_id: String,
    pub cursor: i64,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub state: Option<Value>,
}

/// The structural diff between two channels' latest saved states.
///
/// `common` is the shared ancestor state when both channels descend from the
/// same `origin_run_id` (sibling forks), else null. `left_diverged_at_cursor`
/// / `right_diverged_at_cursor` are the per-side fork boundaries where each
/// fork departed the shared lineage chain (the fork edge immediately after
/// the LCA); a side that IS the LCA has `None` (it never diverged). Siblings
/// forked at different cursors are asymmetric — both values are reported.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateDiffResponse {
    pub left_channel_id: String,
    pub right_channel_id: String,
    pub left: StatePoint,
    pub right: StatePoint,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub common: Option<StatePoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left_diverged_at_cursor: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right_diverged_at_cursor: Option<i64>,
    pub entries: Vec<StateDiffEntry>,
    /// True when `entries` is capped at `max_entries` and more differences
    /// exist (`entry_count_total > entries.len()`).
    pub truncated: bool,
    /// Total number of differing paths counted (capped internally to bound
    /// work on very divergent states).
    pub entry_count_total: usize,
    /// The cap applied to `entries`.
    pub max_entries: usize,
}

/// Request to diff two channels' latest states.
#[derive(Debug, Clone, Deserialize)]
pub struct StateDiffRequest {
    pub left: String,
    pub right: String,
}

/// One hop in a channel's lineage chain back to its `origin_run_id` root.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LineageHop {
    pub channel_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_channel_id: Option<String>,
    pub forked_at_cursor: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// The full decision trail for a channel: its metadata, the lineage chain
/// back to the root run, the immutable fork boundary it inherited, and the
/// idempotent tool-execution ledger rows that happened on this channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionTrail {
    pub channel_id: String,
    pub origin_run_id: String,
    pub ancestry: Vec<LineageHop>,
    /// The immutable boundary: the state the child actually inherited at fork
    /// time (the child's first snapshot + stored provenance). `None` for
    /// roots and lineage-only forks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boundary: Option<ForkBoundary>,
    pub executions: Vec<ExecutionInfo>,
    /// True when `ancestry` was cut off at `max_depth` (a chain deeper than
    /// the depth limit is not silently truncated).
    pub truncated: bool,
    /// The ancestry depth cap.
    pub max_depth: u32,
}

/// Outcome of claiming an execution.
///
/// `status` is one of:
/// - `claimed` — this caller now owns the running execution (must call
///   `complete` / `fail` / `heartbeat` with the returned `claim_token`).
/// - `replayed` — a completed execution with a matching request hash was
///   found; `result` carries the persisted response and the tool was not run.
/// - `reclaimed` — a previous attempt left a stale/abandoned run; ownership
///   is transferable to this caller. Callers should treat the prior outcome
///   as ambiguous and resolve before re-running a non-idempotent tool.
/// - `in_progress` — another owner currently holds a valid lease; `lease_until`
///   indicates when it lapses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionClaimResult {
    pub status: String,
    pub execution: ExecutionInfo,
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_token: Option<Uuid>,
}

/// Outcome of attempting to replay a tool call across an ordinary fork
/// boundary. The store never executes the tool: `Replayed` returns the
/// inherited result; `Miss` (no ancestor recorded the key) and `Mismatch`
/// (an ancestor recorded it with different params) are handed back to the
/// caller, which applies the fork's `tool_policies` (`block`/`replay` refuse,
/// `auto` falls through to a normal live claim).
#[derive(Debug)]
pub enum ForkReplayResult {
    /// An exact source match was materialized on the child channel.
    Replayed(ExecutionClaimResult, AgentEvent),
    /// No ancestor recorded this key.
    Miss,
    /// An ancestor recorded this key, but with a different request hash.
    Mismatch,
}

/// Request to mark an execution completed with a persisted result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteExecutionRequest {
    /// Ownership token returned by a `claimed`/`reclaimed` claim response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_token: Option<String>,
    /// The function's return value, stowed as the cached response.
    #[serde(serialize_with = "serialize_sonic_value_opt")]
    pub result: Option<Value>,
}

/// Request to mark an execution failed with a structured error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailExecutionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
}

/// Request to extend an execution's lease while its tool is still working.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatExecutionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_seconds: Option<i64>,
}

/// Request to cancel an in-flight execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelExecutionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agent_event_type_as_str() {
        assert_eq!(AgentEventType::AgentStarted.as_str(), "agent.started");
        assert_eq!(AgentEventType::AgentCompleted.as_str(), "agent.completed");
        assert_eq!(AgentEventType::ToolStarted.as_str(), "tool.started");
        assert_eq!(AgentEventType::ToolCompleted.as_str(), "tool.completed");
        assert_eq!(
            AgentEventType::HumanApprovalRequested.as_str(),
            "human.approval.requested"
        );
        assert_eq!(
            AgentEventType::HumanApprovalGranted.as_str(),
            "human.approval.granted"
        );
        assert_eq!(
            AgentEventType::CheckpointCreated.as_str(),
            "checkpoint.created"
        );
        assert_eq!(AgentEventType::MemoryWritten.as_str(), "memory.written");
        assert_eq!(
            AgentEventType::Custom("my.event".into()).as_str(),
            "my.event"
        );
    }

    #[test]
    fn test_agent_event_type_from_str() {
        assert!(matches!(
            AgentEventType::from_str("agent.started"),
            AgentEventType::AgentStarted
        ));
        assert!(matches!(
            AgentEventType::from_str("tool.completed"),
            AgentEventType::ToolCompleted
        ));
        assert!(matches!(
            AgentEventType::from_str("human.approval.denied"),
            AgentEventType::HumanApprovalDenied
        ));
        assert!(matches!(
            AgentEventType::from_str("custom.unknown"),
            AgentEventType::Custom(_)
        ));
    }

    #[test]
    fn test_agent_event_type_roundtrip() {
        for s in &[
            "agent.started",
            "tool.failed",
            "checkpoint.restored",
            "message.sent",
            "workflow.completed",
        ] {
            let et = AgentEventType::from_str(s);
            assert_eq!(et.as_str(), *s, "Round-trip failed for {s}");
        }
    }

    #[test]
    fn test_agent_event_type_all_variants_have_as_str() {
        let variants = vec![
            AgentEventType::AgentStarted,
            AgentEventType::AgentCompleted,
            AgentEventType::ToolStarted,
            AgentEventType::ToolCompleted,
            AgentEventType::HumanHandoffRequested,
            AgentEventType::HumanHandoffCompleted,
            AgentEventType::MemoryRead,
            AgentEventType::MemoryWritten,
            AgentEventType::CheckpointCreated,
            AgentEventType::CheckpointRestored,
            AgentEventType::StateTransition,
            AgentEventType::MessageSent,
            AgentEventType::WorkflowStarted,
            AgentEventType::WorkflowFailed,
            AgentEventType::RunCreated,
            AgentEventType::ForkCreated,
            AgentEventType::ForkStarted,
            AgentEventType::ScheduledWakeup,
            AgentEventType::Error,
            AgentEventType::Warning,
            AgentEventType::Info,
        ];
        for v in variants {
            let s = v.as_str();
            assert!(!s.is_empty(), "as_str() returned empty for {v:?}");
            assert_eq!(
                AgentEventType::from_str(s).as_str(),
                s,
                "Round-trip failed for {s}"
            );
        }
    }

    #[test]
    fn test_agent_event_type_display() {
        let et = AgentEventType::AgentStarted;
        assert_eq!(format!("{et}"), "agent.started");
    }

    #[test]
    fn test_event_metadata_creation() {
        let m = EventMetadata {
            actor: "bot".into(),
            agent_id: Some("agent-1".into()),
            user_id: None,
            metadata: None,
        };
        assert_eq!(m.actor, "bot");
        assert!(m.user_id.is_none());
    }

    #[test]
    fn test_agent_event_creation() {
        let e = AgentEvent {
            id: Uuid::new_v4(),
            channel_id: "ch".into(),
            event_type: "agent.started".into(),
            payload: sonic_rs::json!({"seq": 1}),
            metadata: EventMetadata {
                actor: "bot".into(),
                agent_id: None,
                user_id: None,
                metadata: None,
            },
            cursor: 1,
            depends_on: None,
            timestamp: Utc::now(),
        };
        assert_eq!(e.event_type, "agent.started");
        assert_eq!(e.cursor, 1);
    }

    #[test]
    fn test_publish_event_request() {
        let req = PublishEventRequest {
            channel_id: "ch".into(),
            event_type: "test".into(),
            payload: sonic_rs::json!({}),
            metadata: EventMetadata {
                actor: "bot".into(),
                agent_id: None,
                user_id: None,
                metadata: None,
            },
            depends_on: None,
            dependencies: Vec::new(),
            operation_id: None,
            step_number: None,
        };
        assert_eq!(req.event_type, "test");
    }

    #[test]
    fn test_replay_request() {
        let req = ReplayRequest {
            channel_id: "ch".into(),
            cursor: Some(5),
            channel_cursor: None,
            limit: Some(100),
            event_type: None,
        };
        assert_eq!(req.cursor, Some(5));
    }

    #[test]
    fn test_query_request() {
        let req = QueryRequest {
            channel_ids: Some(vec!["ch1".into()]),
            event_type: Some("test".into()),
            actor: None,
            cursor_start: None,
            cursor_end: None,
            channel_cursor_start: None,
            channel_cursor_end: None,
            from: None,
            to: None,
            limit: Some(10),
            offset: Some(0),
            page_token: None,
            pagination: None,
        };
        assert_eq!(req.limit, Some(10));
    }

    #[test]
    fn test_channel_metadata_serialization_roundtrip() {
        let metadata = ChannelMetadata {
            channel_id: "ch-1".into(),
            parent_channel_id: Some("parent-ch".into()),
            origin_run_id: "origin-run".into(),
            forked_at_cursor: 42,
            forked_at: Utc::now(),
            display_name: Some("Experiment".into()),
            reason: Some("Testing".into()),
            experiment_metadata: Some(sonic_rs::json!({"model": "gpt-5.5"})),
            requested_at_cursor: 50,
            resolved_state_cursor: 42,
            resolved_event_id: Some(Uuid::new_v4()),
            source_state_version: 3,
            source_state_sha256: Some("deadbeef".into()),
            restorable: true,
            manifest: Some(sonic_rs::json!({"model": "gpt-5.5"})),
            reproducibility: Some("context_exact".into()),
            tool_policies: None,
            outcome: Some("promoted".into()),
            result_score: Some(0.95),
            deleted_at: None,
        };
        let json = sonic_rs::to_string(&metadata).expect("serialize");
        let roundtripped: ChannelMetadata = sonic_rs::from_str(&json).expect("deserialize");
        assert_eq!(roundtripped.channel_id, metadata.channel_id);
        assert_eq!(roundtripped.parent_channel_id, metadata.parent_channel_id);
        assert_eq!(roundtripped.origin_run_id, metadata.origin_run_id);
        assert_eq!(roundtripped.forked_at_cursor, metadata.forked_at_cursor);
        assert_eq!(roundtripped.display_name, metadata.display_name);
        assert_eq!(roundtripped.reason, metadata.reason);
        assert_eq!(
            roundtripped.requested_at_cursor,
            metadata.requested_at_cursor
        );
        assert_eq!(
            roundtripped.resolved_state_cursor,
            metadata.resolved_state_cursor
        );
        assert_eq!(roundtripped.resolved_event_id, metadata.resolved_event_id);
        assert_eq!(
            roundtripped.source_state_version,
            metadata.source_state_version
        );
        assert_eq!(
            roundtripped.source_state_sha256,
            metadata.source_state_sha256
        );
        assert_eq!(roundtripped.restorable, metadata.restorable);
        assert_eq!(roundtripped.reproducibility, metadata.reproducibility);
        assert_eq!(roundtripped.outcome, metadata.outcome);
        assert_eq!(roundtripped.result_score, metadata.result_score);
    }

    #[test]
    fn test_channel_metadata_missing_provenance_defaults_to_zero() {
        // Old-style JSON without the v17 provenance fields must still parse
        // (backwards compatibility for pre-upgrade clients/dashboards).
        let json = r#"{
            "channel_id": "ch-1",
            "parent_channel_id": null,
            "origin_run_id": "ch-1",
            "forked_at_cursor": 0,
            "forked_at": "2026-01-01T00:00:00Z",
            "display_name": null,
            "reason": null,
            "experiment_metadata": null
        }"#;
        let parsed: ChannelMetadata = sonic_rs::from_str(json).expect("deserialize");
        assert_eq!(parsed.requested_at_cursor, 0);
        assert_eq!(parsed.resolved_state_cursor, 0);
        assert_eq!(parsed.source_state_version, 0);
        assert!(!parsed.restorable);
        assert!(parsed.manifest.is_none());
    }

    #[test]
    fn test_fork_request_serialization_roundtrip() {
        let req = ForkRequest {
            source_channel_id: "src".into(),
            new_channel_id: "dst".into(),
            at_cursor: 10,
            operation_id: Some("op-1".into()),
            display_name: Some("Fork".into()),
            reason: None,
            experiment_metadata: Some(sonic_rs::json!({"temp": 0.3})),
            manifest: Some(sonic_rs::json!({"seed": 42})),
            tool_policies: None,
            expected_version: Some(3),
            expected_cursor: None,
        };
        let json = sonic_rs::to_string(&req).expect("serialize");
        let roundtripped: ForkRequest = sonic_rs::from_str(&json).expect("deserialize");
        assert_eq!(roundtripped.source_channel_id, req.source_channel_id);
        assert_eq!(roundtripped.new_channel_id, req.new_channel_id);
        assert_eq!(roundtripped.at_cursor, req.at_cursor);
        assert_eq!(roundtripped.operation_id, req.operation_id);
        assert_eq!(roundtripped.display_name, req.display_name);
        assert_eq!(roundtripped.reason, req.reason);
        assert_eq!(roundtripped.manifest, req.manifest);
        assert_eq!(roundtripped.expected_version, req.expected_version);
        assert_eq!(roundtripped.expected_cursor, req.expected_cursor);
    }

    #[test]
    fn test_fork_info_serialization_roundtrip() {
        let leaf = ForkInfo {
            metadata: ChannelMetadata {
                channel_id: "leaf".into(),
                parent_channel_id: Some("parent".into()),
                origin_run_id: "root".into(),
                forked_at_cursor: 5,
                forked_at: Utc::now(),
                display_name: None,
                reason: None,
                experiment_metadata: None,
                ..Default::default()
            },
            children: vec![],
            event_count: 0,
            latest_cursor: None,
        };
        let root = ForkInfo {
            metadata: ChannelMetadata {
                channel_id: "root".into(),
                parent_channel_id: None,
                origin_run_id: "root".into(),
                forked_at_cursor: 0,
                forked_at: Utc::now(),
                display_name: None,
                reason: None,
                experiment_metadata: None,
                ..Default::default()
            },
            children: vec![leaf],
            event_count: 10,
            latest_cursor: Some(9),
        };
        let json = sonic_rs::to_string(&root).expect("serialize");
        let roundtripped: ForkInfo = sonic_rs::from_str(&json).expect("deserialize");
        assert_eq!(roundtripped.metadata.channel_id, root.metadata.channel_id);
        assert_eq!(roundtripped.children.len(), 1);
        assert_eq!(roundtripped.children[0].metadata.channel_id, "leaf");
        assert_eq!(roundtripped.event_count, 10);
        assert_eq!(roundtripped.latest_cursor, Some(9));
    }

    #[test]
    fn test_transition_request_serialization_roundtrip() {
        let req = TransitionRequest {
            channel_id: "ch".into(),
            event_type: "agent.started".into(),
            payload: sonic_rs::json!({"seq": 1}),
            metadata: EventMetadata {
                actor: "bot".into(),
                agent_id: None,
                user_id: None,
                metadata: None,
            },
            state: sonic_rs::json!({"turns": 1}),
            operation_id: None,
            expected_version: Some(3),
            expected_cursor: None,
            depends_on: None,
            dependencies: Vec::new(),
        };
        let json = sonic_rs::to_string(&req).expect("serialize");
        let roundtripped: TransitionRequest = sonic_rs::from_str(&json).expect("deserialize");
        assert_eq!(roundtripped.channel_id, req.channel_id);
        assert_eq!(roundtripped.event_type, req.event_type);
        assert_eq!(roundtripped.state, req.state);
        assert_eq!(roundtripped.expected_version, Some(3));
    }

    #[test]
    fn test_group_and_wakeup_types_serialization() {
        let group = GroupInfo {
            group_id: "g1".into(),
            channel_id: "ch".into(),
            created_at: Utc::now(),
            metadata: Some(sonic_rs::json!({"team": "a"})),
        };
        let json = sonic_rs::to_string(&group).expect("serialize");
        let roundtripped: GroupInfo = sonic_rs::from_str(&json).expect("deserialize");
        assert_eq!(roundtripped.group_id, "g1");
        assert_eq!(roundtripped.channel_id, "ch");

        let wakeup = WakeupInfo {
            id: "w-1".into(),
            channel_id: "ch".into(),
            run_at: Utc::now(),
            status: "pending".into(),
            payload: Some(sonic_rs::json!({"step": "resume"})),
            created_at: Utc::now(),
            executed_at: None,
            attempts: 0,
            error: None,
        };
        let json = sonic_rs::to_string(&wakeup).expect("serialize");
        let roundtripped: WakeupInfo = sonic_rs::from_str(&json).expect("deserialize");
        assert_eq!(roundtripped.status, "pending");
        assert_eq!(roundtripped.attempts, 0);
    }
}

#[cfg(test)]
mod wire_contract_tests {
    //! Locks the HTTP wire contract: field aliases, `skip_serializing_if`
    //! presence rules, serde defaults, and canonical sonic Value handling.
    //! These are pure serde round-trips — no database required.

    use super::*;

    fn meta(actor: &str) -> EventMetadata {
        EventMetadata {
            actor: actor.into(),
            agent_id: None,
            user_id: None,
            metadata: None,
        }
    }

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    // ── PublishEventRequest ────────────────────────────────────────────────

    #[test]
    fn publish_event_request_accepts_type_alias() {
        let req: PublishEventRequest = sonic_rs::from_str(
            r#"{"channel_id":"ch-1","type":"agent.started","payload":{"seq":1},"metadata":{"actor":"bot"}}"#,
        )
        .unwrap();
        assert_eq!(req.event_type, "agent.started");
        assert!(req.operation_id.is_none());
        assert!(req.step_number.is_none());
        assert!(req.dependencies.is_empty());
    }

    #[test]
    fn publish_event_request_accepts_event_type_field() {
        let req: PublishEventRequest = sonic_rs::from_str(
            r#"{"channel_id":"ch-1","event_type":"agent.started","payload":{},"metadata":{"actor":"bot"}}"#,
        )
        .unwrap();
        assert_eq!(req.event_type, "agent.started");
    }

    #[test]
    fn publish_event_request_skips_none_fields_on_serialize() {
        let req = PublishEventRequest {
            channel_id: "ch".into(),
            event_type: "tool.completed".into(),
            payload: sonic_rs::json!({"ok": true}),
            metadata: meta("bot"),
            depends_on: None,
            dependencies: Vec::new(),
            operation_id: None,
            step_number: None,
        };
        let json = sonic_rs::to_string(&req).unwrap();
        for skipped in ["operation_id", "step_number", "depends_on", "dependencies"] {
            assert!(
                !json.contains(skipped),
                "{skipped} must be skipped when empty"
            );
        }
        // The canonical field name `event_type` is emitted (not the alias).
        assert!(json.contains("event_type"));
    }

    #[test]
    fn publish_event_request_roundtrip_with_all_fields() {
        let req = PublishEventRequest {
            channel_id: "ch".into(),
            event_type: "step".into(),
            payload: sonic_rs::json!({"n": 1}),
            metadata: EventMetadata {
                actor: "a".into(),
                agent_id: Some("ag".into()),
                user_id: Some("u".into()),
                metadata: Some(sonic_rs::json!({"k": "v"})),
            },
            depends_on: Some(Uuid::new_v4()),
            dependencies: vec![EventDependencyRequest {
                event_id: Uuid::new_v4(),
                dependency_type: "message".into(),
            }],
            operation_id: Some(Uuid::new_v4()),
            step_number: Some(4),
        };
        let json = sonic_rs::to_string(&req).unwrap();
        let back: PublishEventRequest = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.event_type, "step");
        assert_eq!(back.step_number, Some(4));
        assert_eq!(back.dependencies.len(), 1);
        assert_eq!(back.dependencies[0].dependency_type, "message");
        assert!(back.operation_id.is_some());
        assert!(back.depends_on.is_some());
        assert_eq!(back.metadata.agent_id.as_deref(), Some("ag"));
    }

    #[test]
    fn publish_event_request_metadata_actor_required() {
        let err = sonic_rs::from_str::<PublishEventRequest>(
            r#"{"channel_id":"c","type":"step","payload":{},"metadata":{}}"#,
        );
        assert!(err.is_err(), "metadata.actor is required");
    }

    // ── AgentEvent ─────────────────────────────────────────────────────────

    #[test]
    fn agent_event_accepts_type_alias_and_skips_none() {
        let event: AgentEvent = sonic_rs::from_str(
            r#"{"id":"11111111-2222-3333-4444-555555555555","channel_id":"c","type":"step","payload":{"n":1},"metadata":{"actor":"a"},"cursor":3,"timestamp":"2026-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(event.event_type, "step");
        assert!(event.depends_on.is_none());
        let json = sonic_rs::to_string(&event).unwrap();
        assert!(!json.contains("depends_on"));
    }

    // ── ReplayRequest ──────────────────────────────────────────────────────

    #[test]
    fn replay_request_parses_both_cursor_shapes() {
        let modern: ReplayRequest =
            sonic_rs::from_str(r#"{"channel_id":"c","cursor":7,"limit":10,"event_type":"step"}"#)
                .unwrap();
        assert_eq!(modern.cursor, Some(7));
        assert_eq!(modern.limit, Some(10));

        let legacy: ReplayRequest =
            sonic_rs::from_str(r#"{"channel_id":"c","channel_cursor":9}"#).unwrap();
        assert_eq!(legacy.channel_cursor, Some(9));
        assert_eq!(legacy.cursor, None);
    }

    #[test]
    fn replay_request_emits_null_for_optional_fields() {
        // ReplayRequest has no skip_serializing_if — omitted inputs serialize
        // back as explicit nulls (the SDK tolerates both).
        let req = ReplayRequest {
            channel_id: "c".into(),
            cursor: None,
            channel_cursor: None,
            limit: None,
            event_type: None,
        };
        let json = sonic_rs::to_string(&req).unwrap();
        for field in ["cursor", "channel_cursor", "limit", "event_type"] {
            assert!(
                json.contains(&format!("\"{field}\":null")),
                "{field} should be explicit null"
            );
        }
    }

    // ── QueryRequest ───────────────────────────────────────────────────────

    #[test]
    fn query_request_parses_deprecated_cursor_aliases() {
        let req: QueryRequest = sonic_rs::from_str(
            r#"{"channel_ids":["a","b"],"channel_cursor_start":3,"channel_cursor_end":9}"#,
        )
        .unwrap();
        assert_eq!(req.channel_cursor_start, Some(3));
        assert_eq!(req.channel_cursor_end, Some(9));
        assert_eq!(req.cursor_start, None);

        let req2: QueryRequest = sonic_rs::from_str(
            r#"{"cursor_start":1,"cursor_end":5,"from":"2024-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(req2.cursor_start, Some(1));
        assert_eq!(req2.from.as_deref(), Some("2024-01-01T00:00:00Z"));
    }

    // ── TransitionRequest ──────────────────────────────────────────────────

    #[test]
    fn transition_request_accepts_type_alias() {
        let req: TransitionRequest = sonic_rs::from_str(
            r#"{"channel_id":"c","type":"agent.started","payload":{},"metadata":{"actor":"a"},"state":{"turns":1}}"#,
        )
        .unwrap();
        assert_eq!(req.event_type, "agent.started");
        assert_eq!(req.state, sonic_rs::json!({"turns": 1}));
    }

    #[test]
    fn transition_request_skips_guards_and_keys() {
        let req = TransitionRequest {
            channel_id: "c".into(),
            event_type: "step".into(),
            payload: sonic_rs::json!({}),
            metadata: meta("a"),
            state: sonic_rs::json!({}),
            operation_id: None,
            expected_version: None,
            expected_cursor: None,
            depends_on: None,
            dependencies: Vec::new(),
        };
        let json = sonic_rs::to_string(&req).unwrap();
        for absent in [
            "operation_id",
            "expected_version",
            "expected_cursor",
            "depends_on",
            "dependencies",
        ] {
            assert!(!json.contains(absent), "{absent} should be omitted");
        }
    }

    // ── ForkRequest ────────────────────────────────────────────────────────

    #[test]
    fn fork_request_roundtrip_includes_sonic_metadata() {
        let req = ForkRequest {
            source_channel_id: "parent".into(),
            new_channel_id: "child".into(),
            at_cursor: 5,
            operation_id: Some("op-1".into()),
            display_name: Some("variant".into()),
            reason: Some("counterfactual".into()),
            experiment_metadata: Some(sonic_rs::json!({"forked_at_step": 5, "name": "exp"})),
            manifest: Some(sonic_rs::json!({"model": "gpt5"})),
            tool_policies: None,
            expected_version: Some(2),
            expected_cursor: Some(5),
        };
        let json = sonic_rs::to_string(&req).unwrap();
        let back: ForkRequest = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.operation_id.as_deref(), Some("op-1"));
        assert_eq!(back.experiment_metadata.unwrap()["forked_at_step"], 5);
        assert_eq!(back.manifest.unwrap()["model"], "gpt5");
        assert_eq!(back.expected_version, Some(2));
        assert_eq!(back.expected_cursor, Some(5));
    }

    #[test]
    fn fork_request_minimal_serializes_nulls_and_skips_keys() {
        let req = ForkRequest {
            source_channel_id: "parent".into(),
            new_channel_id: "child".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            manifest: None,
            tool_policies: None,
            expected_version: None,
            expected_cursor: None,
        };
        let json = sonic_rs::to_string(&req).unwrap();
        // Idempotency keys and optimistic guards are skipped entirely...
        for absent in ["operation_id", "expected_version", "expected_cursor"] {
            assert!(!json.contains(absent), "{absent} should be omitted");
        }
        // ...while un-attributed Option fields serialize as explicit null.
        for null_field in ["display_name", "reason", "experiment_metadata", "manifest"] {
            assert!(
                json.contains(&format!("\"{null_field}\":null")),
                "{null_field} should be explicit null"
            );
        }
        let back: ForkRequest = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.at_cursor, 0);
        assert!(back.operation_id.is_none());
    }

    // ── ChannelMetadata serde defaults (back-compat) ──────────────────────

    #[test]
    fn channel_metadata_defaults_new_fields_for_old_rows() {
        // A pre-v17 row (only the original columns) must deserialize with the
        // newer i64/bool fields defaulting to zero/false instead of failing.
        let meta: ChannelMetadata = sonic_rs::from_str(
            r#"{"channel_id":"c","parent_channel_id":"p","origin_run_id":"c","forked_at_cursor":2,"forked_at":"2026-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(meta.requested_at_cursor, 0);
        assert_eq!(meta.resolved_state_cursor, 0);
        assert_eq!(meta.source_state_version, 0);
        assert!(!meta.restorable);
        assert!(meta.source_state_sha256.is_none());
        assert!(meta.reproducibility.is_none());
        assert!(meta.outcome.is_none());
        assert!(meta.deleted_at.is_none());
    }

    #[test]
    fn channel_metadata_skips_none_fields() {
        let meta = ChannelMetadata {
            channel_id: "c".into(),
            parent_channel_id: None,
            origin_run_id: "c".into(),
            forked_at_cursor: 0,
            forked_at: ts(),
            display_name: None,
            reason: None,
            experiment_metadata: None,
            requested_at_cursor: 0,
            resolved_state_cursor: 0,
            resolved_event_id: None,
            source_state_version: 0,
            source_state_sha256: None,
            restorable: false,
            manifest: None,
            reproducibility: None,
            tool_policies: None,
            outcome: None,
            result_score: None,
            deleted_at: None,
        };
        let json = sonic_rs::to_string(&meta).unwrap();
        // Fields carrying #[serde(skip_serializing_if)] are omitted...
        for absent in [
            "resolved_event_id",
            "source_state_sha256",
            "reproducibility",
            "outcome",
            "result_score",
            "deleted_at",
        ] {
            assert!(!json.contains(absent), "{absent} should be omitted");
        }
        // ...while un-attributed Option fields (incl. sonic values) are explicit nulls.
        for null_field in [
            "parent_channel_id",
            "display_name",
            "reason",
            "experiment_metadata",
            "manifest",
            "tool_policies",
        ] {
            assert!(
                json.contains(&format!("\"{null_field}\":null")),
                "{null_field} should be explicit null"
            );
        }
    }

    #[test]
    fn channel_metadata_full_roundtrip() {
        let meta = ChannelMetadata {
            channel_id: "c".into(),
            parent_channel_id: Some("p".into()),
            origin_run_id: "o".into(),
            forked_at_cursor: 3,
            forked_at: ts(),
            display_name: Some("exp".into()),
            reason: Some("why".into()),
            experiment_metadata: Some(sonic_rs::json!({"forked_at_step": 3})),
            requested_at_cursor: 3,
            resolved_state_cursor: 2,
            resolved_event_id: Some(Uuid::new_v4()),
            source_state_version: 1,
            source_state_sha256: Some("abc".into()),
            restorable: true,
            manifest: Some(sonic_rs::json!({"model": "gpt"})),
            reproducibility: Some("state_exact".into()),
            tool_policies: None,
            outcome: Some("promoted".into()),
            result_score: Some(0.9),
            deleted_at: None,
        };
        let json = sonic_rs::to_string(&meta).unwrap();
        let back: ChannelMetadata = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.forked_at_cursor, 3);
        assert_eq!(back.resolved_state_cursor, 2);
        assert!(back.restorable);
        assert_eq!(back.outcome.as_deref(), Some("promoted"));
        assert_eq!(back.experiment_metadata.unwrap()["forked_at_step"], 3);
    }

    // ── Experiment requests / defaults ─────────────────────────────────────

    #[test]
    fn add_experiment_member_role_defaults_to_variant() {
        let req: AddExperimentMemberRequest =
            sonic_rs::from_str(r#"{"channel_id":"c-v1"}"#).unwrap();
        assert_eq!(req.role, "variant");
        let req2: AddExperimentMemberRequest = sonic_rs::from_str(
            r#"{"channel_id":"c","role":"baseline","declared_delta":{"m":"x"}}"#,
        )
        .unwrap();
        assert_eq!(req2.role, "baseline");
        assert_eq!(req2.declared_delta.unwrap()["m"], "x");
    }

    #[test]
    fn set_outcome_request_roundtrip() {
        let req: SetOutcomeRequest =
            sonic_rs::from_str(r#"{"outcome":"promoted","score":0.9}"#).unwrap();
        assert_eq!(req.outcome, "promoted");
        assert_eq!(req.score, Some(0.9));
        let minimal: SetOutcomeRequest = sonic_rs::from_str(r#"{"outcome":"crashed"}"#).unwrap();
        assert_eq!(minimal.score, None);
    }

    #[test]
    fn experiment_group_skips_empty_members() {
        let group = ExperimentGroup {
            group_id: "g".into(),
            name: "exp".into(),
            description: None,
            baseline_channel_id: None,
            status: "running".into(),
            created_at: ts(),
            updated_at: ts(),
            members: Vec::new(),
        };
        let json = sonic_rs::to_string(&group).unwrap();
        assert!(!json.contains("description"));
        assert!(!json.contains("baseline_channel_id"));
        assert!(!json.contains("members"), "empty members must be skipped");
    }

    #[test]
    fn experiment_member_roundtrip_with_outcome() {
        let m = ExperimentMember {
            channel_id: "c".into(),
            role: "variant".into(),
            declared_delta: Some(sonic_rs::json!({"model": "x"})),
            created_at: ts(),
            outcome: Some("promoted".into()),
            result_score: Some(0.95),
        };
        let json = sonic_rs::to_string(&m).unwrap();
        let back: ExperimentMember = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.role, "variant");
        assert_eq!(back.outcome.as_deref(), Some("promoted"));
        assert_eq!(back.result_score, Some(0.95));
    }

    // ── SendGroupMessageRequest rename/alias ──────────────────────────────

    #[test]
    fn send_group_message_request_accepts_type_and_message_type() {
        let via_type: SendGroupMessageRequest = sonic_rs::from_str(
            r#"{"from_member_id":"a","to_member_id":"b","type":"research.result","payload":{"x":1}}"#,
        )
        .unwrap();
        assert_eq!(via_type.message_type, "research.result");

        let via_message_type: SendGroupMessageRequest = sonic_rs::from_str(
            r#"{"from_member_id":"a","to_member_id":"b","message_type":"ping","payload":{}}"#,
        )
        .unwrap();
        assert_eq!(via_message_type.message_type, "ping");
    }

    #[test]
    fn send_group_message_request_roundtrip() {
        let req = SendGroupMessageRequest {
            from_member_id: "a".into(),
            to_member_id: "b".into(),
            message_type: "x".into(),
            payload: sonic_rs::json!({"n": 1}),
            causal_context: Some(sonic_rs::json!({"depth": 2})),
            operation_id: Some(Uuid::new_v4()),
        };
        let json = sonic_rs::to_string(&req).unwrap();
        assert!(
            json.contains("\"type\":\"x\""),
            "must serialize the renamed `type` key"
        );
        let back: SendGroupMessageRequest = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.message_type, "x");
        assert!(back.operation_id.is_some());
    }

    // ── EventDependencyRequest default ─────────────────────────────────────

    #[test]
    fn event_dependency_request_defaults_to_causal() {
        let d: EventDependencyRequest =
            sonic_rs::from_str(r#"{"event_id":"11111111-2222-3333-4444-555555555555"}"#).unwrap();
        assert_eq!(d.dependency_type, "causal");
        let explicit: EventDependencyRequest = sonic_rs::from_str(
            r#"{"event_id":"11111111-2222-3333-4444-555555555555","dependency_type":"message"}"#,
        )
        .unwrap();
        assert_eq!(explicit.dependency_type, "message");
    }

    // ── Execution-group request skips ──────────────────────────────────────

    #[test]
    fn execution_group_requests_skip_optional_fields() {
        let create: CreateExecutionGroupRequest =
            sonic_rs::from_str(r#"{"group_id":"g"}"#).unwrap();
        assert!(create.metadata.is_none());
        let json = sonic_rs::to_string(&create).unwrap();
        assert!(!json.contains("metadata"));

        let claim: ClaimMemberRequest = sonic_rs::from_str(r#"{"owner_id":"o"}"#).unwrap();
        assert!(claim.lease_seconds.is_none());

        let member: AddExecutionGroupMemberRequest =
            sonic_rs::from_str(r#"{"member_id":"m","channel_id":"c"}"#).unwrap();
        assert!(member.role.is_none());
        assert!(member.metadata.is_none());
    }

    #[test]
    fn group_message_roundtrip() {
        let msg = GroupMessage {
            message_id: Uuid::new_v4(),
            group_id: "g".into(),
            from_member_id: "a".into(),
            to_member_id: "b".into(),
            message_type: "x".into(),
            payload: sonic_rs::json!({"answer": 42}),
            causal_context: None,
            source_event_id: Uuid::new_v4(),
            delivery_event_id: Uuid::new_v4(),
            status: "delivered".into(),
            acknowledged_at: None,
            acknowledged_by: None,
            created_at: ts(),
        };
        let json = sonic_rs::to_string(&msg).unwrap();
        assert!(
            !json.contains("causal_context"),
            "causal_context is skipped when None"
        );
        // acknowledged_at/by have no skip attribute → explicit nulls.
        assert!(json.contains("\"acknowledged_at\":null"));
        assert!(json.contains("\"acknowledged_by\":null"));
        let back: GroupMessage = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.message_type, "x");
        assert_eq!(back.status, "delivered");
    }

    #[test]
    fn member_lease_roundtrip() {
        let lease = MemberLease {
            group_id: "g".into(),
            member_id: "m".into(),
            owner_id: "o".into(),
            generation: 2,
            lease_until: ts(),
        };
        let json = sonic_rs::to_string(&lease).unwrap();
        let back: MemberLease = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.generation, 2);
        assert_eq!(back.owner_id, "o");
    }

    #[test]
    fn group_fork_receipt_roundtrip() {
        let receipt = GroupForkReceipt {
            fork_group_id: "fork".into(),
            source_group_id: "src".into(),
            intervention_member_id: "router".into(),
            intervention_cursor: 7,
            member_policies: Some(sonic_rs::json!({"router": "live"})),
            tool_policies: Some(sonic_rs::json!({})),
            status: "active".into(),
            first_divergence_event_id: None,
            receipt: Some(sonic_rs::json!({"replayed_events": ["a"], "promoted_members": []})),
            created_at: ts(),
            updated_at: ts(),
        };
        let json = sonic_rs::to_string(&receipt).unwrap();
        let back: GroupForkReceipt = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.intervention_cursor, 7);
        assert_eq!(back.member_policies.unwrap()["router"], "live");
        assert!(back.first_divergence_event_id.is_none());
    }

    // ── Tool-execution requests ────────────────────────────────────────────

    #[test]
    fn claim_execution_request_roundtrip() {
        let req = ClaimExecutionRequest {
            channel_id: "c".into(),
            key_name: "web.search:q".into(),
            tool_name: "web.search".into(),
            params: Some(sonic_rs::json!({"q": "actae"})),
            dedup_fields: Some(vec!["q".into()]),
            lease_seconds: Some(30),
            emit_replay_event: Some(true),
            actor: Some("bot".into()),
        };
        let json = sonic_rs::to_string(&req).unwrap();
        let back: ClaimExecutionRequest = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.dedup_fields, Some(vec!["q".into()]));
        assert_eq!(back.emit_replay_event, Some(true));
        assert_eq!(back.params.unwrap()["q"], "actae");
    }

    #[test]
    fn execution_mutation_requests_roundtrip() {
        let complete: CompleteExecutionRequest =
            sonic_rs::from_str(r#"{"claim_token":"tok","result":{"ok":true}}"#).unwrap();
        assert_eq!(complete.claim_token.as_deref(), Some("tok"));
        assert_eq!(complete.result.unwrap()["ok"], true);

        let fail: FailExecutionRequest = sonic_rs::from_str(
            r#"{"message":"boom","error_type":"timeout","stack":"...","claim_token":"tok"}"#,
        )
        .unwrap();
        assert_eq!(fail.message, "boom");
        assert_eq!(fail.error_type.as_deref(), Some("timeout"));

        let hb: HeartbeatExecutionRequest =
            sonic_rs::from_str(r#"{"claim_token":"tok","lease_seconds":120}"#).unwrap();
        assert_eq!(hb.lease_seconds, Some(120));

        let cancel: CancelExecutionRequest =
            sonic_rs::from_str(r#"{"claim_token":"tok"}"#).unwrap();
        assert_eq!(cancel.claim_token.as_deref(), Some("tok"));
        let minimal: CancelExecutionRequest = sonic_rs::from_str(r#"{}"#).unwrap();
        assert!(minimal.claim_token.is_none());
    }

    #[test]
    fn execution_info_skips_expired_and_optional() {
        let exec = ExecutionInfo {
            id: Uuid::new_v4(),
            channel_id: "c".into(),
            key_name: "k".into(),
            tool_name: "t".into(),
            status: "running".into(),
            attempts: 1,
            lease_until: None,
            started_cursor: None,
            completed_cursor: None,
            started_event_id: None,
            completed_event_id: None,
            replay_emitted: false,
            params: None,
            result: None,
            error: None,
            created_at: ts(),
            updated_at: ts(),
            completed_at: None,
            expired_at: None,
        };
        let json = sonic_rs::to_string(&exec).unwrap();
        // Optional identity/lease/timestamp fields are skipped entirely...
        for absent in [
            "lease_until",
            "started_cursor",
            "completed_cursor",
            "started_event_id",
            "completed_event_id",
            "completed_at",
            "expired_at",
        ] {
            assert!(!json.contains(absent), "{absent} should be omitted");
        }
        // ...while the sonic payload fields emit explicit nulls.
        for null_field in ["params", "result", "error"] {
            assert!(
                json.contains(&format!("\"{null_field}\":null")),
                "{null_field} should be explicit null"
            );
        }
    }

    #[test]
    fn execution_claim_result_roundtrip() {
        let result = ExecutionClaimResult {
            status: "claimed".into(),
            execution: ExecutionInfo {
                id: Uuid::new_v4(),
                channel_id: "c".into(),
                key_name: "k".into(),
                tool_name: "t".into(),
                status: "running".into(),
                attempts: 1,
                lease_until: Some(ts()),
                started_cursor: Some(1),
                completed_cursor: None,
                started_event_id: None,
                completed_event_id: None,
                replay_emitted: false,
                params: Some(sonic_rs::json!({"q": 1})),
                result: None,
                error: None,
                created_at: ts(),
                updated_at: ts(),
                completed_at: None,
                expired_at: None,
            },
            result: None,
            claim_token: Some(Uuid::new_v4()),
        };
        let json = sonic_rs::to_string(&result).unwrap();
        let back: ExecutionClaimResult = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.status, "claimed");
        assert!(back.claim_token.is_some());
        assert_eq!(back.execution.attempts, 1);
        assert_eq!(back.execution.params.unwrap()["q"], 1);
    }

    // ── Consumer-group / wakeup requests ───────────────────────────────────

    #[test]
    fn consumer_group_requests_roundtrip() {
        let create: CreateGroupRequest =
            sonic_rs::from_str(r#"{"group_id":"g","channel_id":"c"}"#).unwrap();
        assert_eq!(create.channel_id, "c");
        let json = sonic_rs::to_string(&create).unwrap();
        assert!(!json.contains("metadata"));

        let join: JoinGroupRequest = sonic_rs::from_str(r#"{"consumer_id":"c1"}"#).unwrap();
        assert!(join.lease_seconds.is_none());

        let claim: ClaimWorkRequest =
            sonic_rs::from_str(r#"{"consumer_id":"c1","limit":50}"#).unwrap();
        assert_eq!(claim.limit, Some(50));

        let ack: AckRequest = sonic_rs::from_str(r#"{"consumer_id":"c1","cursor":9}"#).unwrap();
        assert_eq!(ack.cursor, 9);

        let hb: HeartbeatRequest = sonic_rs::from_str(r#"{"consumer_id":"c1"}"#).unwrap();
        assert!(hb.lease_seconds.is_none());
    }

    #[test]
    fn wakeup_request_roundtrip() {
        let req: CreateWakeupRequest = sonic_rs::from_str(
            r#"{"channel_id":"c","run_at":"2026-01-01T00:00:00Z","payload":{"step":1}}"#,
        )
        .unwrap();
        assert_eq!(req.run_at, ts());
        assert_eq!(req.payload.unwrap()["step"], 1);
        let minimal: CreateWakeupRequest =
            sonic_rs::from_str(r#"{"channel_id":"c","run_at":"2026-01-01T00:00:00Z"}"#).unwrap();
        assert!(minimal.payload.is_none());
        let json = sonic_rs::to_string(&minimal).unwrap();
        assert!(!json.contains("payload"));
    }

    #[test]
    fn group_offset_roundtrip() {
        let off = GroupOffsetInfo {
            group_id: "g".into(),
            consumer_id: "c".into(),
            last_cursor: 4,
            claimed_cursor: 7,
            updated_at: ts(),
        };
        let json = sonic_rs::to_string(&off).unwrap();
        let back: GroupOffsetInfo = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.last_cursor, 4);
        assert_eq!(back.claimed_cursor, 7);
    }

    // ── Canonical sonic Value handling ────────────────────────────────────

    #[test]
    fn sonic_values_preserve_big_integers() {
        let v = sonic_rs::json!({"i64": 9223372036854775807i64, "u64": 18446744073709551615u64, "f": 3.5});
        let json = sonic_rs::to_string(&v).unwrap();
        let back: sonic_rs::Value = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back["i64"], v["i64"]);
        assert_eq!(back["u64"], v["u64"]);
        assert_eq!(back["f"], v["f"]);
    }

    #[test]
    fn sonic_values_roundtrip_through_serde_json() {
        // The wire layer serializes sonic Values through serde_json to avoid
        // arbitrary-precision leaks — the round trip must preserve numbers.
        let v = sonic_rs::json!({"nested": {"deep": [1, 2, 3]}, "ok": true, "n": null});
        let serde_val: serde_json::Value =
            serde_json::from_str(&sonic_rs::to_string(&v).unwrap()).unwrap();
        let sonic_back: sonic_rs::Value = sonic_rs::from_str(&serde_val.to_string()).unwrap();
        assert_eq!(sonic_back, v);
    }

    #[test]
    fn state_version_roundtrip() {
        let sv = StateVersion {
            version: 2,
            cursor: 5,
            timestamp: ts(),
        };
        let json = sonic_rs::to_string(&sv).unwrap();
        let back: StateVersion = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.version, 2);
        assert_eq!(back.cursor, 5);
        assert_eq!(back.timestamp, ts());
    }

    #[test]
    fn event_dependency_struct_roundtrip() {
        let d = EventDependency {
            event_id: Uuid::new_v4(),
            depends_on_event_id: Uuid::new_v4(),
            dependency_type: "message".into(),
        };
        let json = sonic_rs::to_string(&d).unwrap();
        let back: EventDependency = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.dependency_type, "message");
        assert_eq!(back.event_id, d.event_id);
    }

    #[test]
    fn causal_graph_roundtrip() {
        let graph = CausalGraph {
            root_event_id: Uuid::new_v4(),
            events: Vec::new(),
            dependencies: vec![EventDependency {
                event_id: Uuid::new_v4(),
                depends_on_event_id: Uuid::new_v4(),
                dependency_type: "causal".into(),
            }],
            truncated: false,
        };
        let json = sonic_rs::to_string(&graph).unwrap();
        let back: CausalGraph = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.dependencies.len(), 1);
        assert!(!back.truncated);
        assert_eq!(back.root_event_id, graph.root_event_id);
    }

    #[test]
    fn fork_boundary_roundtrip() {
        let b = ForkBoundary {
            source_channel_id: "p".into(),
            requested_cursor: 4,
            resolved_cursor: 3,
            resolved_event_id: Some(Uuid::new_v4()),
            source_state_version: 1,
            source_state_sha256: Some("deadbeef".into()),
            restorable: true,
            state: Some(sonic_rs::json!({"turns": 3})),
        };
        let json = sonic_rs::to_string(&b).unwrap();
        let back: ForkBoundary = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.resolved_cursor, 3);
        assert!(back.restorable);
        assert_eq!(back.state.unwrap()["turns"], 3);
        assert_eq!(back.source_state_sha256.as_deref(), Some("deadbeef"));
    }

    #[test]
    fn event_metadata_roundtrip() {
        let m = EventMetadata {
            actor: "bot".into(),
            agent_id: Some("ag".into()),
            user_id: None,
            metadata: Some(sonic_rs::json!({"group_id": "g"})),
        };
        let json = sonic_rs::to_string(&m).unwrap();
        // user_id has no skip attribute → explicit null; agent_id is present.
        assert!(json.contains("\"user_id\":null"));
        assert!(json.contains("\"agent_id\":\"ag\""));
        let back: EventMetadata = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.actor, "bot");
        assert_eq!(back.metadata.unwrap()["group_id"], "g");
    }
}
