// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Federation source policy (Phase 4).
//!
//! Remote (federation) access is opt-in per instance and always narrower than
//! local authority. A `FederationSourcePolicy` describes what a federated
//! principal may read:
//!
//! * **enabled** — remote access is on or off for this instance;
//! * **mode** — `hosted` (server-owned routes) or `relay` (self-hosted outbound);
//! * **channel / event-type gates** — allow/deny glob patterns (deny wins);
//! * **JSON Pointer redactions** — replace matched payload fields before
//!   egress ("redact before source-instance egress");
//! * **metadata-only** — drop payloads entirely, keep identity/type/cursor;
//! * **max output size** — payloads larger than this are reduced to metadata;
//! * **version** — monotonically increasing policy version for audit.
//!
//! Merge rule (D-008 / section 4.2): **local operator policy may only narrow
//! centralized policy.** [`FederationSourcePolicy::narrowed_by`] intersects
//! allow-lists, unions deny-lists and redactions, and takes the stricter of
//! `metadata_only` and the smaller `max_output_bytes`.

use serde::{Deserialize, Serialize};

/// Sentinel injected into a merged allow-list when the intersection of two
/// non-empty allow-lists is empty (PR-004). It can never match a real channel
/// or event type (channel ids and types follow `[A-Za-z0-9._:-]`), so the
/// merged policy denies everything instead of silently reverting to "empty =
/// allow all".
pub const DENY_ALL_SENTINEL: &str = "\u{0}__actae_deny_all__";

/// Whether a glob pattern is the deny-all sentinel.
fn is_deny_all(pattern: &str) -> bool {
    pattern == DENY_ALL_SENTINEL
}

/// A JSON Pointer redaction rule applied to event payloads before egress.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redaction {
    /// JSON Pointer, e.g. `/tools/0/input` or `/secrets`. The root `/` is the
    /// whole payload.
    pub pointer: String,
    /// Replacement value; default `"[REDACTED]"`.
    #[serde(default)]
    pub redact_with: Option<String>,
}

/// The source policy applied to federated reads/streams.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FederationSourcePolicy {
    /// Monotonic policy version (higher wins; used for audit + sync).
    #[serde(default)]
    pub version: u64,
    /// `hosted` | `relay`.
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub enabled: bool,
    /// Channel glob patterns the federated principal may read. Empty = all
    /// channels allowed (subject to the other gates).
    #[serde(default)]
    pub allowed_channels: Vec<String>,
    /// Channel glob patterns that are never readable. Deny wins.
    #[serde(default)]
    pub denied_channels: Vec<String>,
    /// Event-type glob patterns allowed. Empty = all types allowed.
    #[serde(default)]
    pub allowed_types: Vec<String>,
    /// Event-type glob patterns that are never delivered. Deny wins.
    #[serde(default)]
    pub denied_types: Vec<String>,
    /// Payload field redactions applied before egress.
    #[serde(default)]
    pub redactions: Vec<Redaction>,
    /// Drop payloads entirely (keep id/type/actor/cursor/timestamp).
    #[serde(default)]
    pub metadata_only: bool,
    /// Payloads whose serialized size exceeds this are reduced to metadata.
    /// `0` = no limit.
    #[serde(default)]
    pub max_output_bytes: usize,
}

impl FederationSourcePolicy {
    /// The "no local opinion" policy: remote access allowed exactly as the
    /// centralized policy decides, no additional narrowing. This is the
    /// operator default when `ACTAE_FEDERATION_SOURCE_POLICY` is unset — the
    /// local operator must EXPLICITLY restrict (or disable) remote access.
    pub fn no_narrow_default() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }

    /// Load the local operator source policy from `ACTAE_FEDERATION_SOURCE_POLICY`
    /// (a JSON document matching `FederationSourcePolicy`). This is the LOCAL
    /// narrow — it can only make the centralized policy stricter (see
    /// [`FederationSourcePolicy::narrowed_by`]). Unset = no narrowing.
    ///
    /// **Fail closed (PR-004):** invalid configured JSON produces a *disabled*
    /// policy (remote access off), never an allow-all. A typo must not widen
    /// access. Operators can distinguish "unset" from "explicitly disabled"
    /// via the returned policy's `enabled` flag.
    pub fn from_env() -> Self {
        match std::env::var("ACTAE_FEDERATION_SOURCE_POLICY") {
            Ok(json) => serde_json::from_str(&json).unwrap_or_else(|_| Self::disabled()),
            Err(_) => Self::no_narrow_default(),
        }
    }

    /// An explicitly disabled policy: remote access off, no reads allowed.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// Channel glob match (prefix-`*` semantics, mirroring API-key resource
    /// patterns: `*` = all, trailing `*` = prefix, otherwise exact).
    pub fn channel_allowed(&self, channel: &str) -> bool {
        if self.denied_channels.iter().any(|p| glob_match(p, channel)) {
            return false;
        }
        if self.allowed_channels.is_empty() {
            return true;
        }
        if self.allowed_channels.iter().any(|p| is_deny_all(p)) {
            return false;
        }
        self.allowed_channels.iter().any(|p| glob_match(p, channel))
    }

    /// Event-type glob match (same semantics).
    pub fn type_allowed(&self, event_type: &str) -> bool {
        if self.denied_types.iter().any(|p| glob_match(p, event_type)) {
            return false;
        }
        if self.allowed_types.is_empty() {
            return true;
        }
        if self.allowed_types.iter().any(|p| is_deny_all(p)) {
            return false;
        }
        self.allowed_types.iter().any(|p| glob_match(p, event_type))
    }

    /// Whether this policy requires per-frame transformation. The canonical
    /// server's WS fan-out shares one serialized frame per topic, so a
    /// federation subscriber whose policy needs per-frame redaction/filtering
    /// cannot be served live frames on the shared path — it must use the fleet
    /// stream (policy applied at connector egress, Phase 5). Catch-up replay
    /// and HTTP reads always apply the policy per event.
    pub fn requires_per_frame_transform(&self) -> bool {
        self.metadata_only
            || self.max_output_bytes > 0
            || !self.redactions.is_empty()
            || !self.allowed_types.is_empty()
            || !self.denied_types.is_empty()
    }

    /// Apply the policy to a single event in place. Returns `false` when the
    /// event must be dropped (type denied). Payload redaction / metadata-only /
    /// size cap are applied to the returned event.
    pub fn apply_to_event(&self, event: &mut crate::events::schema::AgentEvent) -> bool {
        if !self.type_allowed(&event.event_type) {
            return false;
        }
        if self.metadata_only {
            event.payload = sonic_rs::json!({});
            return true;
        }
        if self.max_output_bytes > 0 {
            let size = sonic_rs::to_string(&event.payload)
                .map(|s| s.len())
                .unwrap_or(0);
            if size > self.max_output_bytes {
                event.payload = sonic_rs::json!({});
                return true;
            }
        }
        if !self.redactions.is_empty() {
            for r in &self.redactions {
                apply_pointer_redaction(&mut event.payload, &r.pointer, r.redact_with.as_deref());
            }
        }
        true
    }

    /// Apply a redaction rule to an entire payload object (used by tests and
    /// by the fleet connector before NATS egress).
    pub fn apply_redactions_to_value(&self, payload: &mut sonic_rs::Value) {
        if self.metadata_only {
            *payload = sonic_rs::json!({});
            return;
        }
        for r in &self.redactions {
            apply_pointer_redaction(payload, &r.pointer, r.redact_with.as_deref());
        }
    }

    /// **Local operator policy may only narrow centralized policy.**
    /// Intersects allow-lists, unions deny-lists/redactions, and keeps the
    /// stricter of metadata-only and the smaller size cap. The local policy's
    /// `enabled` flag can only turn remote access off, never on.
    ///
    /// PR-004 guarantees the merged allow-lists are the REAL intersection of
    /// the two languages:
    ///
    /// * central allow-all (empty) + local restriction → the local restriction;
    /// * `public.*` ∩ `public.team.*` → `public.team.*` (the narrower prefix);
    /// * disjoint allow sets → a deny-all sentinel, so the merged policy
    ///   DENIES everything instead of reverting to "empty = allow all".
    ///
    /// Routing `mode` is deliberately NOT part of the local authorization
    /// narrowing merge — the merged policy always keeps the centralized mode.
    pub fn narrowed_by(&self, local: &FederationSourcePolicy) -> FederationSourcePolicy {
        let enabled = self.enabled && local.enabled;
        let allowed_channels = intersect_globs(&self.allowed_channels, &local.allowed_channels);
        let allowed_types = intersect_globs(&self.allowed_types, &local.allowed_types);
        let mut denied_channels = self.denied_channels.clone();
        for p in &local.denied_channels {
            if !denied_channels.contains(p) {
                denied_channels.push(p.clone());
            }
        }
        let mut denied_types = self.denied_types.clone();
        for p in &local.denied_types {
            if !denied_types.contains(p) {
                denied_types.push(p.clone());
            }
        }
        let mut redactions = self.redactions.clone();
        for r in &local.redactions {
            if !redactions.contains(r) {
                redactions.push(r.clone());
            }
        }
        FederationSourcePolicy {
            version: self.version.max(local.version),
            mode: self.mode.clone(),
            enabled,
            allowed_channels,
            allowed_types,
            denied_channels,
            denied_types,
            redactions,
            metadata_only: self.metadata_only || local.metadata_only,
            max_output_bytes: if self.max_output_bytes == 0 {
                local.max_output_bytes
            } else if local.max_output_bytes == 0 {
                self.max_output_bytes
            } else {
                self.max_output_bytes.min(local.max_output_bytes)
            },
        }
    }
}

/// Glob match with the repo's canonical semantics: `*` matches everything, a
/// trailing `*` is a prefix match, anything else is exact.
fn glob_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    pattern == value
}

/// The intersection of two glob languages (for narrowing, PR-004). Empty
/// list = allow all. When BOTH lists are empty, the intersection is empty
/// (still allow all — true allow-all). When exactly one is empty, the
/// intersection is the other list. When both are non-empty, the intersection
/// is the pairwise language intersection; if that is empty (disjoint sets),
/// a deny-all sentinel is returned so the merged policy DENIES everything
/// instead of silently reverting to "empty = allow all".
fn intersect_globs(a: &[String], b: &[String]) -> Vec<String> {
    if a.is_empty() {
        return b.to_vec();
    }
    if b.is_empty() {
        return a.to_vec();
    }
    let mut out: Vec<String> = Vec::new();
    for x in a {
        for y in b {
            if let Some(merged) = intersect_single_glob(x, y) {
                if !out.contains(&merged) {
                    out.push(merged);
                }
            }
        }
    }
    if out.is_empty() {
        out.push(DENY_ALL_SENTINEL.to_string());
    }
    out
}

/// The intersection of two single glob patterns under the repo semantics
/// (`*` = all, trailing `*` = prefix, otherwise exact). Returns `None` when
/// no value can match both patterns.
fn intersect_single_glob(a: &str, b: &str) -> Option<String> {
    if is_deny_all(a) || is_deny_all(b) {
        return None;
    }
    if a == "*" {
        return Some(b.to_string());
    }
    if b == "*" {
        return Some(a.to_string());
    }
    let a_pref = a.strip_suffix('*');
    let b_pref = b.strip_suffix('*');
    match (a_pref, b_pref) {
        // Both are prefix patterns `P1*`, `P2*`.
        (Some(pa), Some(pb)) => {
            if pa.starts_with(pb) {
                Some(a.to_string())
            } else if pb.starts_with(pa) {
                Some(b.to_string())
            } else {
                None
            }
        }
        // `P*` ∩ exact `b`.
        (Some(pa), None) => {
            if b.starts_with(pa) {
                Some(b.to_string())
            } else {
                None
            }
        }
        // exact `a` ∩ `P*`.
        (None, Some(pb)) => {
            if a.starts_with(pb) {
                Some(a.to_string())
            } else {
                None
            }
        }
        // exact ∩ exact.
        (None, None) => {
            if a == b {
                Some(a.to_string())
            } else {
                None
            }
        }
    }
}

/// Apply a JSON Pointer redaction to a value in place.
fn apply_pointer_redaction(root: &mut sonic_rs::Value, pointer: &str, redact_with: Option<&str>) {
    let replacement = redact_with.unwrap_or("[REDACTED]");
    let tokens = parse_pointer(pointer);
    if tokens.is_empty() {
        // Root redaction: replace the whole payload.
        *root = sonic_rs::json!(replacement);
        return;
    }
    apply_at(root, &tokens, replacement);
}

fn apply_at(value: &mut sonic_rs::Value, tokens: &[String], replacement: &str) {
    use sonic_rs::JsonValueMutTrait;
    if tokens.is_empty() {
        *value = sonic_rs::json!(replacement);
        return;
    }
    let head = &tokens[0];
    let rest = &tokens[1..];
    if let Some(obj) = value.as_object_mut() {
        if rest.is_empty() {
            if let Some(v) = obj.get_mut(head) {
                *v = sonic_rs::json!(replacement);
            }
        } else if let Some(v) = obj.get_mut(head) {
            apply_at(v, rest, replacement);
        }
    } else if let Some(arr) = value.as_array_mut() {
        if let Ok(idx) = head.parse::<usize>() {
            if let Some(v) = arr.get_mut(idx) {
                if rest.is_empty() {
                    *v = sonic_rs::json!(replacement);
                } else {
                    apply_at(v, rest, replacement);
                }
            }
        }
    }
}

/// Parse a JSON Pointer (`/a/b/0`) into its unescaped tokens.
fn parse_pointer(pointer: &str) -> Vec<String> {
    if pointer.is_empty() || pointer == "/" {
        return Vec::new();
    }
    pointer
        .strip_prefix('/')
        .unwrap_or(pointer)
        .split('/')
        .map(|tok| tok.replace("~1", "/").replace("~0", "~"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::schema::AgentEvent;
    use chrono::Utc;
    use uuid::Uuid;

    fn event(payload: sonic_rs::Value) -> AgentEvent {
        AgentEvent {
            id: Uuid::new_v4(),
            channel_id: "ch.a".into(),
            event_type: "tool.completed".into(),
            payload,
            metadata: crate::events::schema::EventMetadata {
                actor: "agent".into(),
                agent_id: None,
                user_id: None,
                metadata: None,
            },
            cursor: 1,
            timestamp: Utc::now(),
            depends_on: None,
        }
    }

    #[test]
    fn channel_allow_deny() {
        let p = FederationSourcePolicy {
            allowed_channels: vec!["public.*".into()],
            denied_channels: vec!["public.secret".into()],
            ..Default::default()
        };
        assert!(p.channel_allowed("public.news"));
        assert!(!p.channel_allowed("public.secret"));
        assert!(!p.channel_allowed("internal.x"));
    }

    #[test]
    fn empty_allowed_channel_is_allow_all() {
        let p = FederationSourcePolicy::default();
        assert!(p.channel_allowed("anything"));
    }

    #[test]
    fn type_deny_wins() {
        let p = FederationSourcePolicy {
            allowed_types: vec!["tool.*".into(), "agent.*".into()],
            denied_types: vec!["agent.thought".into()],
            ..Default::default()
        };
        assert!(p.type_allowed("tool.completed"));
        assert!(!p.type_allowed("agent.thought"));
        assert!(!p.type_allowed("user.message"));
    }

    #[test]
    fn pointer_redaction_replaces_nested() {
        let p = FederationSourcePolicy {
            redactions: vec![Redaction {
                pointer: "/secrets/api_key".into(),
                redact_with: None,
            }],
            ..Default::default()
        };
        let mut e = event(sonic_rs::json!({"secrets": {"api_key": "sk-live"}, "ok": 1}));
        assert!(p.apply_to_event(&mut e));
        assert_eq!(
            e.payload,
            sonic_rs::json!({"secrets": {"api_key": "[REDACTED]"}, "ok": 1})
        );
    }

    #[test]
    fn metadata_only_drops_payload() {
        let p = FederationSourcePolicy {
            metadata_only: true,
            ..Default::default()
        };
        let mut e = event(sonic_rs::json!({"big": "payload"}));
        assert!(p.apply_to_event(&mut e));
        assert_eq!(e.payload, sonic_rs::json!({}));
    }

    #[test]
    fn size_cap_reduces_to_metadata() {
        let p = FederationSourcePolicy {
            max_output_bytes: 8,
            ..Default::default()
        };
        let mut e = event(sonic_rs::json!({"big": "a very long payload"}));
        assert!(p.apply_to_event(&mut e));
        assert_eq!(e.payload, sonic_rs::json!({}));
    }

    #[test]
    fn denied_type_drops_event() {
        let p = FederationSourcePolicy {
            denied_types: vec!["secret.event".into()],
            ..Default::default()
        };
        let mut e = event(sonic_rs::json!({"x": 1}));
        e.event_type = "secret.event".into();
        assert!(!p.apply_to_event(&mut e));
    }

    #[test]
    fn local_policy_only_narrows() {
        let central = FederationSourcePolicy {
            allowed_channels: vec!["public.*".into(), "team.*".into()],
            denied_types: vec!["secret.*".into()],
            metadata_only: false,
            ..Default::default()
        };
        let local = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["public.*".into()], // narrower
            denied_types: vec!["secret.*".into(), "extra.denied".into()], // adds
            metadata_only: true,                       // stricter
            ..Default::default()
        };
        let merged = central.narrowed_by(&local);
        assert!(merged.channel_allowed("public.x"));
        assert!(!merged.channel_allowed("team.y"));
        assert!(!merged.type_allowed("extra.denied"));
        assert!(merged.metadata_only);
    }

    #[test]
    fn local_disabled_disables_remote() {
        let central = FederationSourcePolicy {
            enabled: true,
            ..Default::default()
        };
        let local = FederationSourcePolicy {
            enabled: false,
            ..Default::default()
        };
        assert!(!central.narrowed_by(&local).enabled);
    }

    #[test]
    fn no_narrow_default_passes_central_policy_through() {
        let central = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["public.*".into()],
            metadata_only: true,
            ..Default::default()
        };
        let local = FederationSourcePolicy::no_narrow_default();
        let merged = central.narrowed_by(&local);
        assert!(merged.enabled);
        assert!(merged.metadata_only);
        assert!(merged.channel_allowed("public.x"));
        assert!(!merged.channel_allowed("secret.x"));
    }

    #[test]
    fn no_narrow_default_enabled() {
        assert!(FederationSourcePolicy::no_narrow_default().enabled);
        assert!(!FederationSourcePolicy::default().enabled);
    }

    #[test]
    fn root_pointer_redacts_whole_payload() {
        let p = FederationSourcePolicy {
            redactions: vec![Redaction {
                pointer: "/".into(),
                redact_with: Some("[hidden]".into()),
            }],
            ..Default::default()
        };
        let mut e = event(sonic_rs::json!({"a": 1}));
        assert!(p.apply_to_event(&mut e));
        assert_eq!(e.payload, sonic_rs::json!("[hidden]"));
    }

    // ------------------------------------------------------------------ //
    // PR-004: fail-closed source-policy merge
    // ------------------------------------------------------------------ //

    #[test]
    fn central_all_local_restricts_permits_only_local() {
        // Central allows all channels; the local operator narrows to
        // `private.team`. The merged policy must permit ONLY `private.team`.
        let central = FederationSourcePolicy {
            enabled: true,
            ..Default::default() // empty allowed_channels = allow all
        };
        let local = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["private.team".into()],
            ..Default::default()
        };
        let merged = central.narrowed_by(&local);
        assert!(merged.channel_allowed("private.team"));
        assert!(!merged.channel_allowed("private.other"));
        assert!(!merged.channel_allowed("public.x"));
        assert!(!merged.channel_allowed("anything"));
    }

    #[test]
    fn prefix_intersection_yields_narrower_prefix() {
        let central = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["public.*".into()],
            ..Default::default()
        };
        let local = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["public.team.*".into()],
            ..Default::default()
        };
        let merged = central.narrowed_by(&local);
        assert!(merged.channel_allowed("public.team.a"));
        assert!(!merged.channel_allowed("public.other"));
        assert!(!merged.channel_allowed("internal.x"));
        // The narrower prefix is preserved exactly.
        assert!(merged
            .allowed_channels
            .contains(&"public.team.*".to_string()));
    }

    #[test]
    fn disjoint_allow_sets_deny_everything() {
        let central = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["public.*".into()],
            ..Default::default()
        };
        let local = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["internal.*".into()],
            ..Default::default()
        };
        let merged = central.narrowed_by(&local);
        // Disjoint — must deny everything, NOT allow everything.
        assert!(!merged.channel_allowed("public.a"));
        assert!(!merged.channel_allowed("internal.a"));
        assert!(!merged.channel_allowed("anything"));
        assert!(merged
            .allowed_channels
            .contains(&DENY_ALL_SENTINEL.to_string()));
    }

    #[test]
    fn exact_intersection_and_all_combinations() {
        let central = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["public.*".into()],
            ..Default::default()
        };
        // Exact inside the prefix survives.
        let local = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["public.news".into()],
            ..Default::default()
        };
        let merged = central.narrowed_by(&local);
        assert!(merged.channel_allowed("public.news"));
        assert!(!merged.channel_allowed("public.other"));
        assert!(merged.allowed_channels.contains(&"public.news".to_string()));
    }

    #[test]
    fn local_restriction_and_central_restriction_both_narrow() {
        // Both sides restrict; the intersection is the narrowest language.
        let central = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["team.*".into(), "public.*".into()],
            ..Default::default()
        };
        let local = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["public.*".into()],
            ..Default::default()
        };
        let merged = central.narrowed_by(&local);
        assert!(merged.channel_allowed("public.a"));
        assert!(!merged.channel_allowed("team.a"));
    }

    #[test]
    fn deny_all_sentinel_never_matches_real_channels() {
        // Defensive: the sentinel must not be usable as an allow rule.
        let p = FederationSourcePolicy {
            allowed_channels: vec![DENY_ALL_SENTINEL.to_string()],
            ..Default::default()
        };
        assert!(!p.channel_allowed("public.a"));
        assert!(!p.channel_allowed(""));
        assert!(!p.channel_allowed(DENY_ALL_SENTINEL));
        let t = FederationSourcePolicy {
            allowed_types: vec![DENY_ALL_SENTINEL.to_string()],
            ..Default::default()
        };
        assert!(!t.type_allowed("tool.x"));
    }

    #[test]
    fn routing_mode_kept_from_central_in_merge() {
        // PR-004: the local operator's routing `mode` must NOT be part of the
        // authorization-narrowing merge; the merged policy keeps the central
        // mode.
        let central = FederationSourcePolicy {
            enabled: true,
            mode: "hosted".into(),
            ..Default::default()
        };
        let local = FederationSourcePolicy {
            enabled: true,
            mode: "relay".into(),
            ..Default::default()
        };
        let merged = central.narrowed_by(&local);
        assert_eq!(merged.mode, "hosted");
    }

    #[test]
    fn invalid_local_policy_json_fails_closed() {
        // PR-004: a typo'd ACTAE_FEDERATION_SOURCE_POLICY must disable remote
        // access (fail closed), never widen it to allow-all.
        std::env::set_var("ACTAE_FEDERATION_SOURCE_POLICY", "{not valid json");
        let p = FederationSourcePolicy::from_env();
        std::env::remove_var("ACTAE_FEDERATION_SOURCE_POLICY");
        assert!(!p.enabled, "invalid JSON must disable remote access");
        // A disabled local policy must disable the merged policy even when
        // the central policy is enabled (remote access off, never allow-all).
        let central = FederationSourcePolicy {
            enabled: true,
            ..Default::default()
        };
        assert!(!central.narrowed_by(&p).enabled);
    }

    #[test]
    fn deny_always_wins_after_merge() {
        // Even when the allowed intersection permits a channel, a denied
        // entry from either side wins.
        let central = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["public.*".into()],
            denied_channels: vec!["public.secret".into()],
            ..Default::default()
        };
        let local = FederationSourcePolicy {
            enabled: true,
            allowed_channels: vec!["public.team.*".into()],
            denied_channels: vec!["public.team.hidden".into()],
            ..Default::default()
        };
        let merged = central.narrowed_by(&local);
        assert!(merged.channel_allowed("public.team.a"));
        assert!(!merged.channel_allowed("public.team.hidden"));
        assert!(!merged.channel_allowed("public.secret"));
        assert!(!merged.channel_allowed("public.other"));
    }
}
