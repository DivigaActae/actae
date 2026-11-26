// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Server checkpoint — persisted state for crash recovery.
//!
//! Periodically writes a checkpoint record to PostgreSQL so the server
//! can detect whether it crashed vs. shut down cleanly, and can
//! approximate how many events might need recovery after restart.

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use std::collections::HashMap;
use tokio::sync::OnceCell;
use tracing::{debug, error, info, warn};

use crate::database::json_compat::JsonValue;

/// The checkpoint task runs every 30 seconds. DDL belongs to initialization,
/// not the hot path: apart from needless work, PostgreSQL emits a NOTICE for
/// every `CREATE TABLE IF NOT EXISTS` after the first one.
static CHECKPOINT_TABLE_READY: OnceCell<()> = OnceCell::const_new();

async fn ensure_checkpoint_table(pool: &Pool) -> Result<(), String> {
    CHECKPOINT_TABLE_READY
        .get_or_try_init(|| async {
            let client = pool.get().await.map_err(|e| format!("Pool error: {e}"))?;
            client
                .batch_execute(
                    "CREATE TABLE IF NOT EXISTS server_checkpoints (
                        id BIGSERIAL PRIMARY KEY,
                        schema_version INT NOT NULL DEFAULT 1,
                        last_event_cursor BIGINT,
                        task_last_run JSONB NOT NULL DEFAULT '{}',
                        server_started_at TIMESTAMPTZ NOT NULL,
                        checkpoint_written_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        clean_shutdown BOOLEAN NOT NULL DEFAULT false
                    )",
                )
                .await
                .map_err(|e| format!("DDL error: {e}"))?;
            Ok(())
        })
        .await
        .map(|_| ())
}

/// Snapshot of server state persisted to PostgreSQL at a configurable interval.
#[derive(Debug, Clone)]
pub struct ServerCheckpoint {
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// Last known event cursor (for approximating recovery scope).
    pub last_event_cursor: Option<i64>,
    /// Timestamp of each task's last successful run.
    pub task_last_run: HashMap<String, DateTime<Utc>>,
    /// When this server instance started.
    pub server_started_at: DateTime<Utc>,
    /// When this checkpoint was written.
    pub checkpoint_written_at: DateTime<Utc>,
    /// Whether the server shut down cleanly after this checkpoint.
    pub clean_shutdown: bool,
}

impl ServerCheckpoint {
    /// Create a new checkpoint with current server state.
    pub fn new(
        last_event_cursor: Option<i64>,
        task_last_run: HashMap<String, DateTime<Utc>>,
        clean_shutdown: bool,
    ) -> Self {
        Self {
            schema_version: 1,
            last_event_cursor,
            task_last_run,
            server_started_at: Utc::now(),
            checkpoint_written_at: Utc::now(),
            clean_shutdown,
        }
    }

    /// Persist this checkpoint to the database.
    /// The table is created once per server process if it does not exist.
    pub async fn save(&self, pool: &Pool) -> Result<(), String> {
        if let Err(e) = ensure_checkpoint_table(pool).await {
            warn!("Failed to ensure server_checkpoints table: {e}");
            return Err(e);
        }
        let client = pool.get().await.map_err(|e| format!("Pool error: {e}"))?;

        let task_run_json = {
            let mut obj = sonic_rs::object! {};
            for (key, dt) in &self.task_last_run {
                obj.insert(key.as_str(), dt.to_rfc3339().as_str());
            }
            JsonValue(sonic_rs::json!(obj))
        };

        let result = client
            .execute(
                "INSERT INTO server_checkpoints
                 (schema_version, last_event_cursor, task_last_run, server_started_at, checkpoint_written_at, clean_shutdown)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &(self.schema_version as i32),
                    &self.last_event_cursor,
                    &task_run_json,
                    &self.server_started_at,
                    &self.checkpoint_written_at,
                    &self.clean_shutdown,
                ],
            )
            .await;

        match result {
            Ok(_) => debug!("Checkpoint saved (cursor={:?})", self.last_event_cursor),
            Err(e) => {
                error!("Failed to save checkpoint: {}", e);
                return Err(format!("Insert error: {e}"));
            }
        }

        Ok(())
    }

    /// Load the most recent checkpoint from the database.
    /// Returns `None` if no checkpoint exists yet.
    pub async fn load_latest(pool: &Pool) -> Result<Option<ServerCheckpoint>, String> {
        let client = pool.get().await.map_err(|e| format!("Pool error: {e}"))?;

        let row = client
            .query_opt(
                "SELECT schema_version, last_event_cursor, task_last_run,
                        server_started_at, checkpoint_written_at, clean_shutdown
                 FROM server_checkpoints
                 ORDER BY checkpoint_written_at DESC
                 LIMIT 1",
                &[],
            )
            .await
            .map_err(|e| format!("Query error: {e}"))?;

        let Some(row) = row else {
            return Ok(None);
        };

        let task_run_json: JsonValue = row.get("task_last_run");
        let task_last_run = if let Some(obj) = task_run_json.0.as_object() {
            let mut map = HashMap::new();
            for (key, val) in obj.iter() {
                if let Some(ts_str) = val.as_str() {
                    if let Ok(dt) = ts_str.parse::<DateTime<Utc>>() {
                        map.insert(key.to_string(), dt);
                    }
                }
            }
            map
        } else {
            HashMap::new()
        };

        Ok(Some(ServerCheckpoint {
            schema_version: row.get::<_, i32>("schema_version") as u32,
            last_event_cursor: row.get("last_event_cursor"),
            task_last_run,
            server_started_at: row.get("server_started_at"),
            checkpoint_written_at: row.get("checkpoint_written_at"),
            clean_shutdown: row.get("clean_shutdown"),
        }))
    }

    /// Mark the latest checkpoint as a clean shutdown.
    /// Called during graceful shutdown.
    pub async fn mark_clean_shutdown(pool: &Pool) -> Result<(), String> {
        let client = pool.get().await.map_err(|e| format!("Pool error: {e}"))?;

        client
            .execute(
                "UPDATE server_checkpoints
                 SET clean_shutdown = true
                 WHERE checkpoint_written_at = (
                     SELECT MAX(checkpoint_written_at) FROM server_checkpoints
                 )",
                &[],
            )
            .await
            .map_err(|e| format!("Update error: {e}"))?;

        info!("Checkpoint marked as clean shutdown");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_new_basic_defaults() {
        let cp = ServerCheckpoint::new(None, HashMap::new(), false);
        assert_eq!(cp.schema_version, 1);
        assert_eq!(cp.last_event_cursor, None);
        assert!(cp.task_last_run.is_empty());
        assert!(!cp.clean_shutdown);
        let now = Utc::now();
        let diff_start = (now - cp.server_started_at).num_milliseconds().abs();
        let diff_written = (now - cp.checkpoint_written_at).num_milliseconds().abs();
        assert!(
            diff_start < 2000,
            "server_started_at within 2s of now, got {diff_start}ms"
        );
        assert!(
            diff_written < 2000,
            "checkpoint_written_at within 2s of now, got {diff_written}ms"
        );
    }

    #[test]
    fn test_new_with_cursor_and_tasks() {
        let mut task_runs = HashMap::new();
        let ts = Utc::now();
        task_runs.insert("cleanup".to_string(), ts);
        task_runs.insert("vacuum".to_string(), ts);
        let cp = ServerCheckpoint::new(Some(42), task_runs, true);
        assert_eq!(cp.schema_version, 1);
        assert_eq!(cp.last_event_cursor, Some(42));
        assert_eq!(cp.task_last_run.len(), 2);
        assert!(cp.clean_shutdown);
        assert!(cp.task_last_run.contains_key("cleanup"));
        assert!(cp.task_last_run.contains_key("vacuum"));
    }

    #[test]
    fn test_new_clean_shutdown_variants() {
        let cp_clean = ServerCheckpoint::new(Some(1), HashMap::new(), true);
        assert!(cp_clean.clean_shutdown);
        let cp_dirty = ServerCheckpoint::new(Some(1), HashMap::new(), false);
        assert!(!cp_dirty.clean_shutdown);
    }

    #[test]
    fn test_clone_preserves_fields() {
        let mut task_runs = HashMap::new();
        task_runs.insert("task_a".to_string(), Utc::now());
        let cp = ServerCheckpoint::new(Some(100), task_runs, false);
        let cloned = cp.clone();
        assert_eq!(cloned.schema_version, cp.schema_version);
        assert_eq!(cloned.last_event_cursor, cp.last_event_cursor);
        assert_eq!(cloned.clean_shutdown, cp.clean_shutdown);
        assert_eq!(cloned.server_started_at, cp.server_started_at);
        assert_eq!(cloned.checkpoint_written_at, cp.checkpoint_written_at);
        assert_eq!(cloned.task_last_run.len(), cp.task_last_run.len());
    }

    #[test]
    fn test_clone_is_distinct() {
        let mut task_runs = HashMap::new();
        task_runs.insert("a".to_string(), Utc::now());
        let cp = ServerCheckpoint::new(Some(1), task_runs, false);
        let mut cloned = cp.clone();
        cloned.task_last_run.insert("b".to_string(), Utc::now());
        assert!(cloned.task_last_run.contains_key("b"));
        assert!(!cp.task_last_run.contains_key("b"));
    }

    #[test]
    fn test_null_cursor() {
        let cp = ServerCheckpoint::new(None, HashMap::new(), false);
        assert_eq!(cp.last_event_cursor, None);
    }

    #[test]
    fn test_large_cursor() {
        let cp = ServerCheckpoint::new(Some(i64::MAX), HashMap::new(), true);
        assert_eq!(cp.last_event_cursor, Some(i64::MAX));
    }

    #[test]
    fn test_schema_version_always_one() {
        let cp = ServerCheckpoint::new(Some(999), HashMap::new(), true);
        assert_eq!(cp.schema_version, 1);
    }

    #[test]
    fn test_with_many_tasks() {
        let mut task_runs = HashMap::new();
        for i in 0..50 {
            task_runs.insert(format!("task_{i}"), Utc::now());
        }
        let cp = ServerCheckpoint::new(Some(1), task_runs, false);
        assert_eq!(cp.task_last_run.len(), 50);
        for i in 0..50 {
            assert!(cp.task_last_run.contains_key(&format!("task_{i}")));
        }
    }

    #[test]
    fn test_timestamps_are_utc() {
        let cp = ServerCheckpoint::new(None, HashMap::new(), false);
        assert_eq!(cp.server_started_at.timezone(), chrono::Utc);
        assert_eq!(cp.checkpoint_written_at.timezone(), chrono::Utc);
    }

    #[test]
    fn test_fields_publicly_accessible() {
        let cp = ServerCheckpoint::new(Some(7), HashMap::new(), true);
        let _v: u32 = cp.schema_version;
        let _c: Option<i64> = cp.last_event_cursor;
        let _m: &HashMap<String, DateTime<Utc>> = &cp.task_last_run;
        let _s: DateTime<Utc> = cp.server_started_at;
        let _w: DateTime<Utc> = cp.checkpoint_written_at;
        let _d: bool = cp.clean_shutdown;
    }
}
