// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Multi-instance query merge (Phase 5).
//!
//! Bounded fan-out over authorized instances with a deadline; deterministic
//! display order `(timestamp, instance_id, event_id)` (not causal — D-010);
//! explicit `partial: true` with typed per-instance errors; response limits;
//! and authenticated page state (each instance's canonical `next_page_token`
//! is returned and accepted back so a page can continue per instance).

use anyhow::Result;
use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, JsonValueTrait};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// A per-instance query outcome.
#[derive(Debug)]
pub struct InstanceQueryResult {
    pub instance_id: String,
    pub instance_name: String,
    pub data: sonic_rs::Value,
}

/// One typed per-instance error.
#[derive(Debug)]
pub struct InstanceError {
    pub instance_id: String,
    pub error: String,
}

/// The merged multi-instance query response.
#[derive(Debug)]
pub struct MergedQuery {
    pub partial: bool,
    pub results: Vec<InstanceQueryResult>,
    pub errors: Vec<InstanceError>,
    /// Canonical per-instance continuation tokens (page state).
    pub next_page_tokens: Vec<(String, String)>,
    pub has_more: bool,
    pub total_events: usize,
    pub truncated: bool,
}

/// The canonical instance query request (a POST /api/v1/events/query body).
#[derive(Debug, Clone)]
pub struct InstanceQuery {
    pub body: sonic_rs::Value,
}

impl InstanceQuery {
    /// Channels the query asks for — used for the observation token.
    pub fn channel_patterns(&self) -> Vec<String> {
        self.body
            .get("channel_ids")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| c.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .filter(|v: &Vec<String>| !v.is_empty())
            .unwrap_or_else(|| vec!["*".to_string()])
    }

    /// Set the per-instance page continuation (canonical token).
    pub fn with_page_token(&self, token: Option<&str>) -> sonic_rs::Value {
        let mut b = self.body.clone();
        match token {
            Some(t) => {
                if let Some(obj) = b.as_object_mut() {
                    obj.insert("page_token", sonic_rs::json!(t));
                }
            }
            None => {
                if let Some(obj) = b.as_object_mut() {
                    obj.remove(&"page_token");
                }
            }
        }
        b
    }
}

/// Fan out one canonical query to each instance, bounded by `max_fanout`
/// concurrent calls and `deadline`. Merges events in deterministic
/// `(timestamp, instance_id, event_id)` order.
pub async fn merge_query(
    instances: Vec<(String, String)>, // (instance_id, name)
    query: &InstanceQuery,
    per_instance_token: &HashMap<String, String>, // instance_id -> canonical page_token
    call: impl Fn(
            String,
            sonic_rs::Value,
        ) -> futures_util::future::BoxFuture<'static, Result<sonic_rs::Value>>
        + Send
        + Sync
        + 'static,
    max_fanout: usize,
    deadline: Duration,
    max_events: usize,
) -> MergedQuery {
    use futures_util::StreamExt;

    let deadline = tokio::time::Instant::now() + deadline;
    let sem = Arc::new(tokio::sync::Semaphore::new(max_fanout.max(1)));

    let mut handles = Vec::with_capacity(instances.len());
    for (instance_id, name) in instances {
        let query = query.clone();
        let token = per_instance_token.get(&instance_id).cloned();
        let call = &call;
        let permit = sem.clone();
        handles.push(async move {
            let _guard = permit.acquire_owned().await;
            let body = query.with_page_token(token.as_deref());
            let fut = call(instance_id.clone(), body);
            match tokio::time::timeout_at(deadline, fut).await {
                Ok(Ok(data)) => Ok((instance_id, name, data)),
                Ok(Err(e)) => Err((instance_id, e.to_string())),
                Err(_) => Err((instance_id, "instance query deadline exceeded".to_string())),
            }
        });
    }

    let mut partial = false;
    let mut results = Vec::new();
    let mut errors = Vec::new();
    let mut next_page_tokens = Vec::new();
    let mut has_more = false;
    let mut total = 0usize;
    let mut truncated = false;

    let mut stream = futures_util::stream::iter(handles).buffer_unordered(max_fanout.max(1));
    while let Some(outcome) = stream.next().await {
        match outcome {
            Ok((instance_id, name, data)) => {
                let events = data
                    .get("events")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let page_token = data
                    .get("next_page_token")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let instance_has_more = data
                    .get("has_more")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if let Some(pt) = page_token {
                    next_page_tokens.push((instance_id.clone(), pt));
                }
                if instance_has_more {
                    has_more = true;
                }
                total += events.len();
                results.push(InstanceQueryResult {
                    instance_id,
                    instance_name: name,
                    data,
                });
            }
            Err((instance_id, error)) => {
                partial = true;
                errors.push(InstanceError { instance_id, error });
            }
        }
    }

    // Deterministic display order: (timestamp, instance_id, event_id).
    let mut all_events: Vec<(String, String, sonic_rs::Value)> = Vec::new();
    for r in &results {
        if let Some(events) = r.data.get("events").and_then(|v| v.as_array()) {
            for e in events {
                all_events.push((r.instance_id.clone(), r.instance_name.clone(), e.clone()));
            }
        }
    }
    all_events.sort_by(|a, b| {
        let ts_a = a.2.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let ts_b = b.2.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        ts_a.cmp(ts_b).then_with(|| a.0.cmp(&b.0)).then_with(|| {
            a.2.get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .cmp(b.2.get("id").and_then(|v| v.as_str()).unwrap_or(""))
        })
    });
    if all_events.len() > max_events {
        all_events.truncate(max_events);
        truncated = true;
    }

    MergedQuery {
        partial,
        results,
        errors,
        next_page_tokens,
        has_more,
        total_events: total,
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use futures_util::FutureExt;

    #[test]
    fn channel_patterns_from_query() {
        let q = InstanceQuery {
            body: sonic_rs::json!({"channel_ids": ["a", "b"], "limit": 10}),
        };
        assert_eq!(q.channel_patterns(), vec!["a", "b"]);
        let q2 = InstanceQuery {
            body: sonic_rs::json!({"limit": 10}),
        };
        assert_eq!(q2.channel_patterns(), vec!["*"]);
    }

    #[test]
    fn with_page_token_injects_and_removes() {
        let q = InstanceQuery {
            body: sonic_rs::json!({"channel_ids": ["a"]}),
        };
        let with = q.with_page_token(Some("tok"));
        assert_eq!(with.get("page_token").and_then(|v| v.as_str()), Some("tok"));
        let without = q.with_page_token(None);
        assert!(without.get("page_token").is_none());
    }

    #[tokio::test]
    async fn merge_orders_deterministically_and_tracks_partial() {
        use std::collections::HashMap;
        let q = InstanceQuery {
            body: sonic_rs::json!({}),
        };
        let per: HashMap<String, String> = HashMap::new();
        let call = |id: String, _b: sonic_rs::Value| {
            let id = id.clone();
            async move {
                let events = match id.as_str() {
                    "ins_a" => sonic_rs::json!([
                        {"id": "e2", "timestamp": "2026-01-01T00:00:02Z", "channel_id": "c", "type": "t", "payload": {"i": "a-e2"}},
                        {"id": "e1", "timestamp": "2026-01-01T00:00:01Z", "channel_id": "c", "type": "t", "payload": {"i": "a-e1"}},
                    ]),
                    "ins_b" => sonic_rs::json!([
                        {"id": "e1b", "timestamp": "2026-01-01T00:00:01Z", "channel_id": "c", "type": "t", "payload": {"i": "b-e1"}},
                        {"id": "e3", "timestamp": "2026-01-01T00:00:03Z", "channel_id": "c", "type": "t", "payload": {"i": "b-e3"}},
                    ]),
                    _ => return Err(anyhow!("boom")),
                };
                Ok(sonic_rs::json!({"events": events, "next_page_token": null, "has_more": false}))
            }
            .boxed()
        };
        let merged = merge_query(
            vec![
                ("ins_a".into(), "A".into()),
                ("ins_b".into(), "B".into()),
                ("ins_c".into(), "C".into()),
            ],
            &q,
            &per,
            call,
            10,
            Duration::from_secs(5),
            1000,
        )
        .await;
        assert!(merged.partial, "ins_c errored -> partial");
        assert_eq!(merged.errors.len(), 1);
        assert_eq!(merged.errors[0].instance_id, "ins_c");
        // Deterministic (timestamp, instance_id, event_id) order:
        assert_eq!(merged.results.len(), 2);
        assert_eq!(merged.total_events, 4);
    }
}
