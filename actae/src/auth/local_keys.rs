// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Local API-key provisioning (keys that do not come from the control
//! plane). These are `source = 'local'`: created by `actae keys create`,
//! by dev mode (the fixed dev key), and by the first-boot initial-key
//! provisioning. They are hashed with `api_key_hash_secret` (HMAC-SHA256)
//! before storage and are subject to subscription enforcement exactly like
//! control-plane keys — there is no operator-managed exemption.

use crate::config::AppConfig;
use anyhow::{Context, Result};
use deadpool_postgres::Pool;

/// Insert a new local API key (HMAC-hashed, full read/write access).
/// Returns the key prefix (for the "store it now" hint). The caller prints
/// the raw value exactly once.
pub async fn insert_api_key(
    pool: &Pool,
    config: &AppConfig,
    raw: &str,
    user_id: &str,
    name: &str,
) -> Result<String> {
    let client = pool
        .get()
        .await
        .context("failed to get client database connection to create API key")?;
    let key_hash = crate::auth::hmac_hash_api_key(raw, config.api_key_hash_secret.as_bytes());
    let key_prefix = raw[..12.min(raw.len())].to_string();
    let id = uuid::Uuid::new_v4().to_string();
    let permissions = crate::control_plane::client::full_access_permissions();
    client
        .execute(
            "INSERT INTO public.api_keys
                (id, user_id, name, key_hash, key_prefix, permissions, last_used_at,
                 expires_at, status, created_at, updated_at, source)
             VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, 'active', NOW(), NOW(), 'local')",
            &[
                &id,
                &user_id.to_string(),
                &name.to_string(),
                &key_hash,
                &key_prefix,
                &crate::database::json_compat::to_sql_json(&permissions),
            ],
        )
        .await
        .with_context(|| format!("failed to insert API key '{name}'"))?;
    Ok(key_prefix)
}
