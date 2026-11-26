// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use crate::database::error::DatabaseError;
use crate::database::models::{ApiKey, RealtimeMessage, Session, User};
use crate::database::DatabaseManager;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;

use tracing::instrument;

type Result<T, E = DatabaseError> = std::result::Result<T, E>;

#[async_trait]
pub trait Repository<T> {
    type Id;

    async fn create(&self, entity: &T) -> Result<T>;
    async fn get_by_id(&self, id: &Self::Id) -> Result<Option<T>>;
    async fn update(&self, entity: &T) -> Result<T>;
    async fn delete(&self, id: &Self::Id) -> Result<bool>;
    async fn list(&self, limit: Option<i64>, offset: Option<i64>) -> Result<Vec<T>>;
}

pub struct ApiKeyRepository {
    db_manager: Arc<DatabaseManager>,
}

impl ApiKeyRepository {
    pub fn new(db_manager: Arc<DatabaseManager>) -> Self {
        Self { db_manager }
    }

    pub async fn find_by_key_hash(&self, key_hash: &str) -> Result<Option<ApiKey>> {
        let client = self.db_manager.get_client_connection().await?;
        let row = client
            .query_opt(
                "SELECT * FROM public.api_keys WHERE key_hash = $1 AND status = 'active' AND (expires_at IS NULL OR expires_at > NOW())",
                &[&key_hash],
            )
            .await?;
        Ok(row.map(ApiKey::from))
    }

    #[instrument(skip(self))]
    pub async fn update_last_used(&self, id: &str) -> Result<()> {
        let client = self.db_manager.get_client_connection().await?;
        client
            .execute(
                "UPDATE public.api_keys SET last_used_at = NOW() WHERE id = $1",
                &[&id],
            )
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to update last used: {e}")))?;
        Ok(())
    }

    /// List API keys for a specific user
    #[instrument(skip(self))]
    pub async fn list_by_user(
        &self,
        user_id: &str,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<ApiKey>> {
        let client = self.db_manager.get_client_connection().await?;

        let limit = limit.unwrap_or(100).min(1000);
        let offset = offset.unwrap_or(0);

        let rows = client
            .query(
                "SELECT * FROM public.api_keys WHERE user_id = $1 AND status != 'deleted' ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                &[&user_id, &limit, &offset],
            )
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to list user API keys: {e}")))?;

        Ok(rows.into_iter().map(ApiKey::from).collect())
    }

    /// Deactivate (soft disable) an API key
    #[instrument(skip(self))]
    pub async fn deactivate(&self, id: &str) -> Result<bool> {
        let client = self.db_manager.get_client_connection().await?;

        let rows_affected = client
            .execute(
                "UPDATE public.api_keys SET status = 'inactive', updated_at = NOW() WHERE id = $1 AND status = 'active'",
                &[&id],
            )
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to deactivate API key: {e}")))?;

        Ok(rows_affected > 0)
    }

    /// Reactivate an API key
    #[instrument(skip(self))]
    pub async fn reactivate(&self, id: &str) -> Result<bool> {
        let client = self.db_manager.get_client_connection().await?;

        let rows_affected = client
            .execute(
                "UPDATE public.api_keys SET status = 'active', updated_at = NOW() WHERE id = $1 AND status = 'inactive'",
                &[&id],
            )
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to reactivate API key: {e}")))?;

        Ok(rows_affected > 0)
    }
}

#[async_trait]
impl Repository<ApiKey> for ApiKeyRepository {
    type Id = String;

    #[instrument(skip(self, entity))]
    async fn create(&self, entity: &ApiKey) -> Result<ApiKey> {
        let client = self.db_manager.get_client_connection().await?;

        let row = client
            .query_one(
                r#"
                INSERT INTO public.api_keys (
                    id, user_id, name, key_hash, permissions, 
                    expires_at, status, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING *
                "#,
                &[
                    &uuid::Uuid::parse_str(&entity.id).unwrap_or(uuid::Uuid::nil()),
                    &entity.user_id,
                    &entity.name,
                    &entity.key_hash,
                    &crate::database::json_compat::to_sql_json(&entity.permissions),
                    &entity.expires_at,
                    &entity.status,
                    &entity.created_at,
                    &entity.updated_at,
                ],
            )
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to create API key: {e}")))?;

        Ok(ApiKey::from(row))
    }

    async fn get_by_id(&self, id: &Self::Id) -> Result<Option<ApiKey>> {
        let client = self.db_manager.get_client_connection().await?;
        let row = client
            .query_opt("SELECT * FROM public.api_keys WHERE id = $1", &[&id])
            .await?;
        Ok(row.map(ApiKey::from))
    }

    #[instrument(skip(self, entity))]
    async fn update(&self, entity: &ApiKey) -> Result<ApiKey> {
        let client = self.db_manager.get_client_connection().await?;

        let row = client
            .query_one(
                r#"
                UPDATE public.api_keys 
                SET name = $2, permissions = $3, expires_at = $4, status = $5, 
                    updated_at = $6, last_used_at = $7
                WHERE id = $1
                RETURNING *
                "#,
                &[
                    &entity.id,
                    &entity.name,
                    &crate::database::json_compat::to_sql_json(&entity.permissions),
                    &entity.expires_at,
                    &entity.status,
                    &entity.updated_at,
                    &entity.last_used_at,
                ],
            )
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to update API key: {e}")))?;

        Ok(ApiKey::from(row))
    }

    #[instrument(skip(self))]
    async fn delete(&self, id: &Self::Id) -> Result<bool> {
        let client = self.db_manager.get_client_connection().await?;

        // Soft delete by updating status to 'deleted'
        let rows_affected = client
            .execute(
                "UPDATE public.api_keys SET status = 'deleted', updated_at = NOW() WHERE id = $1 AND status != 'deleted'",
                &[&id],
            )
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to delete API key: {e}")))?;

        Ok(rows_affected > 0)
    }

    #[instrument(skip(self))]
    async fn list(&self, limit: Option<i64>, offset: Option<i64>) -> Result<Vec<ApiKey>> {
        let client = self.db_manager.get_client_connection().await?;

        let limit = limit.unwrap_or(100).min(1000); // Cap at 1000 for performance
        let offset = offset.unwrap_or(0);

        let rows = client
            .query(
                "SELECT * FROM public.api_keys WHERE status != 'deleted' ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                &[&limit, &offset],
            )
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to list API keys: {e}")))?;

        Ok(rows.into_iter().map(ApiKey::from).collect())
    }
}

pub struct RealtimeMessageRepository {
    db_manager: Arc<DatabaseManager>,
}

impl RealtimeMessageRepository {
    pub fn new(db_manager: Arc<DatabaseManager>) -> Self {
        Self { db_manager }
    }

    #[instrument(skip(self, payload))]
    pub async fn create(&self, topic: &str, payload: &sonic_rs::Value) -> Result<RealtimeMessage> {
        let client = self.db_manager.get_main_connection().await?;

        let _payload_str =
            sonic_rs::to_string(payload).map_err(|e| DatabaseError::JsonError(e.to_string()))?;

        let row = client
            .query_one(
                "INSERT INTO public.realtime_messages (channel, payload)
                 VALUES ($1, $2)
                 RETURNING *",
                &[&topic, &crate::database::json_compat::to_sql_json(payload)],
            )
            .await?;

        // The RealtimeMessage::from(row) will work because the model has serde_json::Value
        Ok(RealtimeMessage::from(row))
    }
}

pub struct SessionRepository {
    db_manager: Arc<DatabaseManager>,
}

impl SessionRepository {
    pub fn new(db_manager: Arc<DatabaseManager>) -> Self {
        Self { db_manager }
    }

    pub async fn find_by_token(&self, token: &str) -> Result<Option<(User, Session)>> {
        let client = self.db_manager.get_client_connection().await?;
        let row = client
            .query_opt(
                "SELECT u.*, s.* FROM public.user u 
                 JOIN public.session s ON u.id = s.userId 
                 WHERE s.token = $1 AND s.expiresAt > NOW()",
                &[&token],
            )
            .await?;
        Ok(row.map(|r| (User::from(r.clone()), Session::from(r))))
    }
}

pub struct UserRepository {
    db_manager: Arc<DatabaseManager>,
}

impl UserRepository {
    pub fn new(db_manager: Arc<DatabaseManager>) -> Self {
        Self { db_manager }
    }

    pub async fn find_by_email(&self, email: &str) -> Result<Option<User>> {
        let client = self.db_manager.get_client_connection().await?;
        let row = client
            .query_opt("SELECT * FROM public.user WHERE email = $1", &[&email])
            .await?;
        Ok(row.map(User::from))
    }

    /// The local dashboard identity used for loopback auto-login: the earliest
    /// admin if one exists, otherwise the earliest user. Returns `None` only
    /// when the `public.user` table is empty (never on a provisioned instance,
    /// which always creates a first-boot admin).
    pub async fn first_admin(&self) -> Result<Option<User>> {
        let client = self.db_manager.get_client_connection().await?;
        let row = client
            .query_opt(
                "SELECT * FROM public.user
                 ORDER BY (role = 'admin') DESC, \"createdAt\" ASC
                 LIMIT 1",
                &[],
            )
            .await?;
        Ok(row.map(User::from))
    }

    /// Create-or-update a dashboard user from a verified control-plane
    /// identity (SSO). The portal customer id is the primary key, so repeat
    /// logins refresh the display fields without creating duplicates. These
    /// users are passwordless — `password_hash` is deliberately left NULL so
    /// a local password login can never masquerade as a portal identity.
    pub async fn upsert_portal_user(&self, id: &str, email: &str, name: &str) -> Result<User> {
        let client = self.db_manager.get_client_connection().await?;
        let row = client
            .query_one(
                "INSERT INTO public.user (id, email, \"emailVerified\", name, image, password_hash, \"createdAt\", \"updatedAt\")
                 VALUES ($1, $2, true, $3, NULL, NULL, NOW(), NOW())
                 ON CONFLICT (id) DO UPDATE
                 SET email = EXCLUDED.email,
                     name = EXCLUDED.name,
                     \"emailVerified\" = true,
                     \"updatedAt\" = NOW()
                 RETURNING *",
                &[&id, &email, &name],
            )
            .await?;
        Ok(User::from(row))
    }
}

/// Single-use nonce ledger for dashboard SSO handoff tokens (see
/// `client_migrations.rs` v10). `consume` atomically claims a `jti`, so a
/// replayed handoff token is rejected even while it is still inside its
/// short validity window.
pub struct SsoTokenRepository {
    db_manager: Arc<DatabaseManager>,
}

impl SsoTokenRepository {
    pub fn new(db_manager: Arc<DatabaseManager>) -> Self {
        Self { db_manager }
    }

    /// Record `jti` until `expires_at`. Returns `true` on first use, `false`
    /// when the nonce was already consumed (replay). Expired rows are pruned
    /// opportunistically on each call.
    pub async fn consume(&self, jti: &str, expires_at: DateTime<Utc>) -> Result<bool> {
        let client = self.db_manager.get_client_connection().await?;
        let _ = client
            .execute(
                "DELETE FROM public.dashboard_sso_tokens WHERE expires_at < NOW()",
                &[],
            )
            .await;
        let inserted = client
            .execute(
                "INSERT INTO public.dashboard_sso_tokens (jti, expires_at)
                 VALUES ($1, $2)
                 ON CONFLICT (jti) DO NOTHING",
                &[&jti, &expires_at],
            )
            .await?;
        Ok(inserted > 0)
    }
}

#[async_trait]
impl Repository<User> for UserRepository {
    type Id = String;

    #[instrument(skip(self, entity))]
    async fn create(&self, entity: &User) -> Result<User> {
        let client = self.db_manager.get_client_connection().await?;
        let row = client
            .query_one(
                "INSERT INTO public.user (id, email, \"emailVerified\", name, image, password_hash, role, \"createdAt\", \"updatedAt\")
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 RETURNING *",
                &[
                    &entity.id,
                    &entity.email,
                    &entity.email_verified,
                    &entity.name,
                    &entity.image,
                    &entity.password_hash,
                    &entity.role,
                    &entity.created_at,
                    &entity.updated_at,
                ],
            )
            .await?;
        Ok(User::from(row))
    }

    #[instrument(skip(self, id))]
    async fn get_by_id(&self, id: &Self::Id) -> Result<Option<User>> {
        let client = self.db_manager.get_client_connection().await?;
        let row = client
            .query_opt("SELECT * FROM public.user WHERE id = $1", &[&id])
            .await?;
        Ok(row.map(User::from))
    }

    #[instrument(skip(self, entity))]
    async fn update(&self, entity: &User) -> Result<User> {
        let client = self.db_manager.get_client_connection().await?;
        let row = client
            .query_one(
                "UPDATE public.user
                 SET email = $2, \"emailVerified\" = $3, name = $4, image = $5, password_hash = $6, role = $7, \"updatedAt\" = $8
                 WHERE id = $1
                 RETURNING *",
                &[
                    &entity.id,
                    &entity.email,
                    &entity.email_verified,
                    &entity.name,
                    &entity.image,
                    &entity.password_hash,
                    &entity.role,
                    &entity.updated_at,
                ],
            )
            .await?;
        Ok(User::from(row))
    }

    #[instrument(skip(self, id))]
    async fn delete(&self, id: &Self::Id) -> Result<bool> {
        let client = self.db_manager.get_client_connection().await?;
        let result = client
            .execute("DELETE FROM public.user WHERE id = $1", &[&id])
            .await?;
        Ok(result > 0)
    }

    #[instrument(skip(self))]
    async fn list(&self, limit: Option<i64>, offset: Option<i64>) -> Result<Vec<User>> {
        let client = self.db_manager.get_client_connection().await?;
        let rows = client
            .query(
                "SELECT * FROM public.user ORDER BY \"createdAt\" DESC LIMIT $1 OFFSET $2",
                &[&limit.unwrap_or(10), &offset.unwrap_or(0)],
            )
            .await?;
        Ok(rows.into_iter().map(User::from).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repository_types_compile() {
        // Verify all repository structs exist and have new() constructors
        let _ = std::any::type_name::<ApiKeyRepository>();
        let _ = std::any::type_name::<SessionRepository>();
        let _ = std::any::type_name::<UserRepository>();
        let _ = std::any::type_name::<RealtimeMessageRepository>();
    }

    #[test]
    fn test_repository_trait_compliance() {
        fn _api_key_implements_repository() {
            fn _check<T: Repository<ApiKey, Id = String>>() {}
            _check::<ApiKeyRepository>();
        }
        fn _user_implements_repository() {
            fn _check<T: Repository<User, Id = String>>() {}
            _check::<UserRepository>();
        }
    }

    #[test]
    fn test_api_key_repo_find_by_key_hash_signature() {
        fn _check(repo: &ApiKeyRepository) {
            let _ = repo.find_by_key_hash("");
        }
    }

    #[test]
    fn test_api_key_repo_list_by_user_signature() {
        fn _check(repo: &ApiKeyRepository) {
            let _ = repo.list_by_user("", Some(10), Some(0));
        }
    }

    #[test]
    fn test_session_repo_find_by_token_signature() {
        fn _check(repo: &SessionRepository) {
            let _ = repo.find_by_token("");
        }
    }

    #[test]
    fn test_user_repo_find_by_email_signature() {
        fn _check(repo: &UserRepository) {
            let _ = repo.find_by_email("");
        }
    }
}
