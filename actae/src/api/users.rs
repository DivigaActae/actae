// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Admin user management for standalone instances.
//!
//! `GET/POST /api/v1/users`, `PATCH/DELETE /api/v1/users/{id}` and
//! `POST /api/v1/users/{id}/password` let an **admin** dashboard account
//! create teammates, change roles, reset passwords and remove accounts.
//!
//! Security model:
//! - Authentication is a dashboard session JWT only (Bearer). API keys are
//!   the SDK surface and never grant user administration.
//! - The session must still be active in `public.dashboard_sessions`
//!   (revoked sessions are rejected), the caller's `role` must be `admin`,
//!   and the per-IP auth rate limit applies. Success/failure feed the
//!   security manager (lockout + threat monitor) like every other surface.
//! - Guards prevent locking the instance out: the last admin cannot be
//!   demoted or deleted, and you cannot delete your own account. The guard
//!   and the write run in one transaction that locks the admin set, so
//!   concurrent demotions cannot race to zero admins.
//! - Every write is parameterized; passwords go through the same
//!   `SecurePasswordHasher` policy as signup; resetting a password revokes
//!   that user's live sessions. DB errors are logged server-side (with a
//!   request id) and never leaked to the client.

use crate::api::global_state;
use crate::clean_json;
use crate::database::json_compat::CleanJson;
use axum::{
    extract::Path,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use deadpool_postgres::Pool;
use sonic_rs::JsonValueTrait;
use std::net::IpAddr;
use uuid::Uuid;

pub fn routes() -> Router<()> {
    Router::new()
        .route("/", get(list_users_handler))
        .route("/", post(create_user_handler))
        .route("/{id}", axum::routing::patch(update_user_handler))
        .route("/{id}", axum::routing::delete(delete_user_handler))
        .route("/{id}/password", post(reset_password_handler))
}

type ApiError = (StatusCode, Json<CleanJson>);

fn err(status: StatusCode, message: &str) -> ApiError {
    (status, Json(clean_json!({ "error": message })))
}

/// Log the internal detail server-side and return a generic error with a
/// correlation id (never leak DB detail to the client).
fn backend_error(message: &str, detail: std::fmt::Arguments<'_>) -> ApiError {
    let request_id = Uuid::new_v4().to_string();
    tracing::error!(request_id = %request_id, "{}: {}", message, detail);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(clean_json!({ "error": "internal error", "request_id": request_id })),
    )
}

fn user_json(row: &tokio_postgres::Row) -> CleanJson {
    clean_json!({
        "id": row.get::<_, String>("id"),
        "email": row.get::<_, String>("email"),
        "name": row.get::<_, String>("name"),
        "role": row.get::<_, String>("role"),
        "email_verified": row.get::<_, bool>("emailVerified"),
        "created_at": row.get::<_, chrono::DateTime<chrono::Utc>>("createdAt").to_rfc3339(),
    })
}

/// Resolve an active **admin** dashboard session. Returns the caller's user id,
/// the client pool, and the peer IP (for audit). Applies the per-IP auth rate
/// limit and reports the outcome to the security manager.
async fn require_admin(headers: &HeaderMap) -> Result<(String, Pool, Option<IpAddr>), ApiError> {
    let state = global_state();
    let app = &state.app_state;
    let security = &state.security_manager;
    let pool = app.db_manager.client_pool.clone();
    let ip = crate::auth::authorization::client_ip(headers);
    use crate::auth::websocket_auth::AuthMethod;

    if let Some(ip) = ip {
        use crate::security::rate_limit::RateLimitResult;
        if let RateLimitResult::Limited(info) = security.rate_limiter().check_auth_limit(ip) {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(clean_json!({
                    "error": "Too many attempts. Please try again later.",
                    "retry_after_seconds": info.retry_after_seconds,
                })),
            ));
        }
    }

    let token = match headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        Some(t) => t,
        None => {
            security.record_auth_failure(
                ip,
                None,
                AuthMethod::JWT,
                "missing bearer token on /api/v1/users".to_string(),
            );
            return Err(err(StatusCode::UNAUTHORIZED, "Authentication required"));
        }
    };

    let claims = match crate::auth::jwt::validate_token(token, &app.config) {
        Ok(c) => c,
        Err(_) => {
            security.record_auth_failure(
                ip,
                None,
                AuthMethod::JWT,
                "invalid or expired token on /api/v1/users".to_string(),
            );
            return Err(err(StatusCode::UNAUTHORIZED, "Invalid or expired token"));
        }
    };

    let jti = claims.jti.as_deref().unwrap_or("");
    if !crate::auth::session_store::is_active(&pool, jti).await {
        security.record_auth_failure(
            ip,
            Some(&claims.sub),
            AuthMethod::JWT,
            "revoked session on /api/v1/users".to_string(),
        );
        return Err(err(
            StatusCode::UNAUTHORIZED,
            "Session revoked or expired — sign in again",
        ));
    }

    let client = pool.get().await.map_err(|e| {
        backend_error("users: db connection failed", format_args!("{e:#}"))
    })?;
    let role = client
        .query_opt("SELECT role FROM public.user WHERE id = $1", &[&claims.sub])
        .await
        .map_err(|e| backend_error("users: role lookup failed", format_args!("{e:#}")))?
        .map(|r| r.get::<_, String>("role"));
    match role.as_deref() {
        Some("admin") => {
            security.record_auth_success(ip, claims.sub.clone(), AuthMethod::JWT);
            Ok((claims.sub, pool, ip))
        }
        Some(_) => {
            security.record_auth_failure(
                ip,
                Some(&claims.sub),
                AuthMethod::JWT,
                "non-admin on /api/v1/users".to_string(),
            );
            Err(err(StatusCode::FORBIDDEN, "Administrator role required"))
        }
        None => {
            security.record_auth_failure(
                ip,
                Some(&claims.sub),
                AuthMethod::JWT,
                "unknown account on /api/v1/users".to_string(),
            );
            Err(err(StatusCode::UNAUTHORIZED, "Unknown account"))
        }
    }
}

fn valid_role(role: &str) -> bool {
    role == "admin" || role == "member"
}

/// Lock the whole admin set in a stable order and return its size. Serializes
/// concurrent role/delete operations so the last-admin guard cannot race.
async fn lock_admin_count(tx: &deadpool_postgres::Transaction<'_>) -> Result<i64, ApiError> {
    let rows = tx
        .query(
            "SELECT id FROM public.user WHERE role = 'admin' ORDER BY id FOR UPDATE",
            &[],
        )
        .await
        .map_err(|e| backend_error("users: admin lock failed", format_args!("{e:#}")))?;
    Ok(rows.len() as i64)
}

async fn list_users_handler(headers: HeaderMap) -> ApiError {
    let (_, pool, _) = match require_admin(&headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => return backend_error("users: db connection failed", format_args!("{e:#}")),
    };
    let rows = match client
        .query(
            "SELECT id, email, name, role, \"emailVerified\", \"createdAt\"
             FROM public.user ORDER BY \"createdAt\" ASC",
            &[],
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return backend_error("users: list failed", format_args!("{e:#}")),
    };
    let users: Vec<CleanJson> = rows.iter().map(user_json).collect();
    (
        StatusCode::OK,
        Json(clean_json!({ "users": users })),
    )
}

async fn create_user_handler(headers: HeaderMap, body: axum::body::Bytes) -> ApiError {
    let (caller, pool, ip) = match require_admin(&headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let app = &global_state().app_state;

    let parsed: sonic_rs::Value = match sonic_rs::from_slice(body.as_ref()) {
        Ok(v) => v,
        Err(_) => return err(StatusCode::BAD_REQUEST, "Invalid request body"),
    };
    let email = parsed
        .get("email")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_lowercase())
        .unwrap_or_default();
    let password = parsed
        .get("password")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let name = parsed
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| email.split('@').next().unwrap_or("member").to_string());
    let role = parsed
        .get("role")
        .and_then(|v| v.as_str())
        .unwrap_or("member")
        .to_string();

    if email.is_empty() || !email.contains('@') {
        return err(StatusCode::BAD_REQUEST, "A valid email is required");
    }
    if !valid_role(&role) {
        return err(StatusCode::BAD_REQUEST, "role must be 'admin' or 'member'");
    }

    let user_id = format!("user_{}", Uuid::new_v4().simple());
    let hash = match app.password_hasher.hash_password(&password, &user_id).await {
        Ok(h) => h,
        Err(e) => return err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => return backend_error("users: db connection failed", format_args!("{e:#}")),
    };
    let row = match client
        .query_opt(
            "INSERT INTO public.user
                (id, email, \"emailVerified\", name, image, password_hash, role, \"createdAt\", \"updatedAt\")
             VALUES ($1, $2, true, $3, NULL, $4, $5, NOW(), NOW())
             ON CONFLICT (email) DO NOTHING
             RETURNING id, email, name, role, \"emailVerified\", \"createdAt\"",
            &[&user_id, &email, &name, &hash, &role],
        )
        .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return err(StatusCode::CONFLICT, "An account with that email already exists")
        }
        Err(e) => return backend_error("users: create failed", format_args!("{e:#}")),
    };
    tracing::info!(
        admin = %caller, target = %email, role = %role, ip = ?ip,
        "user management: created user"
    );
    (
        StatusCode::CREATED,
        Json(clean_json!({ "user": user_json(&row) })),
    )
}

async fn update_user_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> ApiError {
    let (caller, pool, ip) = match require_admin(&headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let parsed: sonic_rs::Value = match sonic_rs::from_slice(body.as_ref()) {
        Ok(v) => v,
        Err(_) => return err(StatusCode::BAD_REQUEST, "Invalid request body"),
    };

    let mut client = match pool.get().await {
        Ok(c) => c,
        Err(e) => return backend_error("users: db connection failed", format_args!("{e:#}")),
    };
    let tx = match client.transaction().await {
        Ok(t) => t,
        Err(e) => return backend_error("users: begin tx failed", format_args!("{e:#}")),
    };
    let admin_count = match lock_admin_count(&tx).await {
        Ok(n) => n,
        Err(e) => return e,
    };
    let current = match tx
        .query_opt("SELECT name, role FROM public.user WHERE id = $1", &[&id])
        .await
    {
        Ok(Some(r)) => r,
        Ok(None) => return err(StatusCode::NOT_FOUND, "No such user"),
        Err(e) => return backend_error("users: update lookup failed", format_args!("{e:#}")),
    };
    let current_role: String = current.get("role");

    let name = parsed
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| current.get::<_, String>("name"));
    let role = parsed
        .get("role")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| current_role.clone());

    if !valid_role(&role) {
        return err(StatusCode::BAD_REQUEST, "role must be 'admin' or 'member'");
    }
    if current_role == "admin" && role != "admin" && admin_count <= 1 {
        return err(StatusCode::CONFLICT, "Cannot demote the last administrator");
    }

    let row = match tx
        .query_one(
            "UPDATE public.user SET name = $2, role = $3, \"updatedAt\" = NOW()
             WHERE id = $1
             RETURNING id, email, name, role, \"emailVerified\", \"createdAt\"",
            &[&id, &name, &role],
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return backend_error("users: update failed", format_args!("{e:#}")),
    };
    if let Err(e) = tx.commit().await {
        return backend_error("users: commit failed", format_args!("{e:#}"));
    }
    tracing::info!(
        admin = %caller, target = %id, role = %role, ip = ?ip,
        "user management: updated user"
    );
    (StatusCode::OK, Json(clean_json!({ "user": user_json(&row) })))
}

async fn delete_user_handler(headers: HeaderMap, Path(id): Path<String>) -> ApiError {
    let (caller, pool, ip) = match require_admin(&headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    if id == caller {
        return err(StatusCode::CONFLICT, "You cannot delete your own account");
    }
    let mut client = match pool.get().await {
        Ok(c) => c,
        Err(e) => return backend_error("users: db connection failed", format_args!("{e:#}")),
    };
    let tx = match client.transaction().await {
        Ok(t) => t,
        Err(e) => return backend_error("users: begin tx failed", format_args!("{e:#}")),
    };
    let admin_count = match lock_admin_count(&tx).await {
        Ok(n) => n,
        Err(e) => return e,
    };
    let target = match tx
        .query_opt("SELECT role FROM public.user WHERE id = $1", &[&id])
        .await
    {
        Ok(Some(r)) => r.get::<_, String>("role"),
        Ok(None) => return err(StatusCode::NOT_FOUND, "No such user"),
        Err(e) => return backend_error("users: delete lookup failed", format_args!("{e:#}")),
    };
    if target == "admin" && admin_count <= 1 {
        return err(StatusCode::CONFLICT, "Cannot delete the last administrator");
    }
    let deleted = match tx
        .execute("DELETE FROM public.user WHERE id = $1", &[&id])
        .await
    {
        Ok(n) => n,
        Err(e) => return backend_error("users: delete failed", format_args!("{e:#}")),
    };
    if let Err(e) = tx.commit().await {
        return backend_error("users: commit failed", format_args!("{e:#}"));
    }
    if deleted == 0 {
        return err(StatusCode::NOT_FOUND, "No such user");
    }
    let _ = crate::auth::session_store::revoke_user(&pool, &id).await;
    tracing::info!(admin = %caller, target = %id, ip = ?ip, "user management: deleted user");
    (StatusCode::OK, Json(clean_json!({ "deleted": true })))
}

async fn reset_password_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> ApiError {
    let (caller, pool, ip) = match require_admin(&headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let app = &global_state().app_state;
    let parsed: sonic_rs::Value = match sonic_rs::from_slice(body.as_ref()) {
        Ok(v) => v,
        Err(_) => return err(StatusCode::BAD_REQUEST, "Invalid request body"),
    };
    let password = parsed
        .get("password")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let hash = match app.password_hasher.hash_password(password, &id).await {
        Ok(h) => h,
        Err(e) => return err(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => return backend_error("users: db connection failed", format_args!("{e:#}")),
    };
    let updated = match client
        .execute(
            "UPDATE public.user SET password_hash = $2, \"updatedAt\" = NOW() WHERE id = $1",
            &[&id, &hash],
        )
        .await
    {
        Ok(n) => n,
        Err(e) => return backend_error("users: reset failed", format_args!("{e:#}")),
    };
    if updated == 0 {
        return err(StatusCode::NOT_FOUND, "No such user");
    }
    let _ = crate::auth::session_store::revoke_user(&pool, &id).await;
    tracing::info!(admin = %caller, target = %id, ip = ?ip, "user management: reset password");
    (StatusCode::OK, Json(clean_json!({ "reset": true })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_validation() {
        assert!(valid_role("admin"));
        assert!(valid_role("member"));
        assert!(!valid_role("owner"));
        assert!(!valid_role(""));
    }
}
