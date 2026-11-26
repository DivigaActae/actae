// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use crate::clean_json;
use crate::database::json_compat::CleanJson;
use axum::{
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use uuid::Uuid;

use crate::api::global_state;
use crate::auth::jwt::Claims;
use crate::database::repositories::{Repository, SsoTokenRepository};
use crate::state::AppState;
use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, JsonValueTrait};

pub fn routes() -> Router<()> {
    Router::new()
        .route("/signup", post(signup_handler))
        .route("/login", post(login_handler))
        .route("/sso", post(sso_handler))
        .route("/setup", post(setup_handler))
        .route("/api-key-login", post(api_key_login_handler))
        .route("/local-session", post(local_session_handler))
        .route("/config", get(auth_config_handler))
        .route("/me", get(me_handler))
        .route("/logout", post(logout_handler))
        .route("/logout-all", post(logout_all_handler))
        .route("/capabilities", get(capabilities_handler))
}

/// Canonical manifest operation ids, embedded at build time so the capabilities
/// response and the published contract can never drift apart (RUST-001).
const MANIFEST: &str = include_str!("../../../spec/api-parity/v1/manifest.json");

/// Derive the named action scopes granted by a principal's permission set
/// (D-007). Read grants yield read/stream scopes; write grants yield mutation
/// scopes; delete grants yield destructive scopes; admin yields everything.
fn scopes_for_permissions(
    perms: &crate::auth::authorization::ApiKeyPermissions,
) -> Vec<&'static str> {
    let mut scopes: Vec<&'static str> = Vec::new();
    if !perms.read.is_empty() {
        scopes.extend([
            "events:read",
            "state:read",
            "channels:read",
            "streams:read",
            "payloads:read",
        ]);
    }
    if !perms.write.is_empty() {
        scopes.extend([
            "events:write",
            "state:write",
            "channels:write",
            "forks:create",
            "forks:promote",
            "groups:manage",
            "execution_groups:manage",
            "experiments:manage",
            "scheduler:manage",
            "executions:manage",
        ]);
    }
    if !perms.delete.is_empty() {
        scopes.extend(["state:delete", "channels:delete"]);
    }
    if !perms.admin.is_empty() {
        scopes.extend([
            "events:read",
            "events:write",
            "state:read",
            "state:write",
            "state:delete",
            "channels:read",
            "channels:write",
            "channels:delete",
            "forks:create",
            "forks:promote",
            "groups:manage",
            "execution_groups:manage",
            "experiments:manage",
            "scheduler:manage",
            "executions:manage",
            "streams:read",
            "payloads:read",
        ]);
    }
    scopes.sort_unstable();
    scopes.dedup();
    scopes
}

fn manifest_operation_ids() -> Vec<String> {
    sonic_rs::from_str::<sonic_rs::Value>(MANIFEST)
        .ok()
        .and_then(|v| v.as_object().and_then(|o| o.get(&"operations")).cloned())
        .and_then(|ops| ops.as_array().map(|arr| arr.to_vec()))
        .map(|ops| {
            ops.iter()
                .filter_map(|o| {
                    o.as_object()
                        .and_then(|obj| obj.get(&"id"))
                        .and_then(|i| i.as_str())
                        .map(|s| s.to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Additive capability metadata shared by both auth paths below.
fn capabilities_payload(
    perms: &crate::auth::authorization::ApiKeyPermissions,
    auth_method: &str,
) -> sonic_rs::Value {
    let scopes: Vec<String> = scopes_for_permissions(perms)
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    let operations = manifest_operation_ids();
    let perms_value = sonic_rs::to_value(perms).unwrap_or_else(|_| sonic_rs::json!({}));
    sonic_rs::json!({
        "api_version": "actae.api.v1",
        "engine_version": env!("CARGO_PKG_VERSION"),
        "auth_method": auth_method,
        "permissions": perms_value,
        "scopes": scopes,
        "operations": operations,
        "limits": {
            "max_query_limit": 1000,
            "max_payload_bytes": 1024 * 1024,
            "page_token_ttl_secs": 900,
            "ws_max_message_size": 1024 * 1024,
        },
    })
}

/// Merge additive auth-path extras (user_id/key_id/source/expiry) into the
/// shared capabilities payload and wrap it for HTTP output.
fn capabilities_response(
    perms: &crate::auth::authorization::ApiKeyPermissions,
    auth_method: &str,
    extras: sonic_rs::Object,
) -> (StatusCode, Json<CleanJson>) {
    let mut value = capabilities_payload(perms, auth_method);
    if let Some(obj) = value.as_object_mut() {
        for (key, val) in &extras {
            obj.insert(key, val.clone());
        }
    }
    (StatusCode::OK, Json(CleanJson(value)))
}

/// GET /api/v1/auth/capabilities — return the caller's parsed key scopes
/// (plus expiry and key source) so SDKs can expose can_read/can_write/
/// can_publish/can_fork before attempting an operation (selling point 5).
/// Additive since 1.1.0: api_version, engine_version, named scopes, manifest
/// operation ids, and limits (Phase 1 — formalize and complete direct API).
/// Uses the same resolution as every other authenticated surface: a Bearer
/// session JWT (full access) or a live X-API-Key row (its parsed scopes).
async fn capabilities_handler(headers: HeaderMap) -> (StatusCode, Json<CleanJson>) {
    let state = global_state();
    let app = &state.app_state;

    if let Some(auth_header) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(token) = auth_header
            .strip_prefix("Bearer ")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            if let Ok(claims) = crate::auth::jwt::validate_token(token, &app.config) {
                let perms = crate::auth::authorization::ApiKeyPermissions::full();
                let mut extras = sonic_rs::Object::new();
                extras.insert("user_id", sonic_rs::json!(claims.sub));
                extras.insert("expires_at", sonic_rs::Value::default());
                return capabilities_response(&perms, "jwt", extras);
            }
            // Purpose-bound federation token (Phase 4): observation is
            // read-only; command is a single action. Never full access.
            let fed_config = app.control_plane.federation_config();
            let instance_id = app.config.install_id.clone().unwrap_or_default();
            if let Ok(fed) = crate::auth::federation::verify(&fed_config, token, &instance_id) {
                let perms = crate::auth::authorization::ApiKeyPermissions {
                    read: match fed.purpose {
                        crate::auth::federation::FederationPurpose::Observation => {
                            fed.channels.clone()
                        }
                        crate::auth::federation::FederationPurpose::Command
                            if fed.action.as_deref() == Some("read") =>
                        {
                            fed.resources.clone()
                        }
                        _ => Vec::new(),
                    },
                    write: match fed.action.as_deref() {
                        Some("write") => fed.resources.clone(),
                        _ => Vec::new(),
                    },
                    delete: match fed.action.as_deref() {
                        Some("delete") => fed.resources.clone(),
                        _ => Vec::new(),
                    },
                    admin: Vec::new(),
                };
                let mut extras = sonic_rs::Object::new();
                extras.insert("principal", sonic_rs::json!(fed.principal));
                extras.insert("federation_purpose", sonic_rs::json!(fed.purpose.as_str()));
                if let Some(op) = &fed.operation_id {
                    extras.insert("operation_id", sonic_rs::json!(op));
                }
                return capabilities_response(&perms, "federation", extras);
            }
        }
    }

    if let Some(key_val) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return match crate::auth::api_key::validate_api_key(key_val, app).await {
            Ok(Some(key)) => {
                let perms = crate::auth::parse_api_key_permissions(&key.permissions);
                let mut extras = sonic_rs::Object::new();
                extras.insert("key_id", sonic_rs::json!(key.id));
                extras.insert("key_source", sonic_rs::json!(key.source));
                extras.insert(
                    "expires_at",
                    key.expires_at
                        .map(|t| sonic_rs::json!(t.to_rfc3339()))
                        .unwrap_or_else(sonic_rs::Value::default),
                );
                capabilities_response(&perms, "api_key", extras)
            }
            Ok(None) => (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Invalid API key"})),
            ),
            Err(e) => {
                let request_id = Uuid::new_v4().to_string();
                tracing::error!(
                    request_id = %request_id,
                    "capabilities auth backend error: {e:#}"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(clean_json!({
                        "error": "authentication backend error",
                        "request_id": request_id,
                    })),
                )
            }
        };
    }

    (
        StatusCode::UNAUTHORIZED,
        Json(clean_json!({"error": "Authentication required"})),
    )
}

#[derive(sonic_rs::Deserialize)]
struct SignupRequest {
    email: String,
    password: String,
    name: Option<String>,
}

#[derive(sonic_rs::Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
}

#[derive(sonic_rs::Deserialize)]
struct ApiKeyLoginRequest {
    api_key: String,
}

#[derive(sonic_rs::Deserialize)]
struct SsoRequest {
    token: String,
}

#[derive(sonic_rs::Deserialize)]
struct SetupRequest {
    token: String,
    password: String,
    name: Option<String>,
}

fn password_auth_disabled(state: &AppState) -> bool {
    !state.config.enable_signup
}

fn make_token_response(user: &crate::database::models::User, token: &str) -> CleanJson {
    clean_json!({
        "user": {
            "id": user.id,
            "email": user.email,
            "email_verified": user.email_verified,
            "name": user.name,
            "image": user.image,
            "role": user.role,
        },
        "token": token,
    })
}

fn extract_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

fn generate_jwt(
    user_id: &str,
    state: &AppState,
    auth_method: Option<&str>,
    key_name: Option<&str>,
) -> Result<(String, Claims), CleanJson> {
    let now = Utc::now();
    let exp = (now + chrono::Duration::hours(24)).timestamp();
    let iat = now.timestamp();
    let nbf = now.timestamp();
    let iss = state
        .config
        .jwt_issuer
        .clone()
        .unwrap_or_else(|| "actae".to_string());
    let aud = state
        .config
        .jwt_audience
        .clone()
        .unwrap_or_else(|| "actae-clients".to_string());

    let claims = Claims {
        sub: user_id.to_string(),
        exp,
        iat,
        nbf: Some(nbf),
        iss: Some(iss),
        aud: Some(aud),
        jti: Some(Uuid::new_v4().to_string()),
        permissions: None,
        session_id: None,
        client_ip: None,
        device_id: None,
        metadata: key_name.map(|name| serde_json::json!({ "key_name": name })),
        auth_method: auth_method.map(|m| m.to_string()),
    };

    let header = Header::new(Algorithm::HS256);
    let token = encode(
        &header,
        &claims,
        &EncodingKey::from_secret(state.config.jwt_secret.as_bytes()),
    )
    .map_err(|e| {
        // Never leak the JWT backend detail to the client (audit item 15).
        tracing::error!("failed to mint session token: {e}");
        clean_json!({"error": "failed to issue session token"})
    })?;
    Ok((token, claims))
}

/// Record a minted session in the server-side ledger so it is revocable.
async fn record_session(
    pool: &deadpool_postgres::Pool,
    jti: Option<&str>,
    exp: i64,
    user_id: &str,
    auth_method: &str,
) {
    if let Some(jti) = jti {
        let expires_at = chrono::DateTime::from_timestamp(exp, 0).unwrap_or_else(Utc::now);
        crate::auth::session_store::record(pool, jti, user_id, auth_method, expires_at).await;
    }
}

fn validate_email(email: &str) -> bool {
    email.len() <= 254 && email.contains('@') && email.split('@').all(|p| !p.is_empty())
}

async fn signup_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = global_state();
    let app = &state.app_state;

    // Email/password auth is opt-in (`ENABLE_SIGNUP=true`); the SaaS-issued
    // API key is the primary credential. Refuse early to avoid leaking the
    // existence of the endpoint.
    if password_auth_disabled(app) {
        return (
            StatusCode::FORBIDDEN,
            Json(clean_json!({
                "error": "Password signup is disabled on this instance. Log in with your Actae API key.",
            })),
        );
    }

    // Trusted client IP only (peer-IP middleware) — a client-supplied
    // X-Forwarded-For is never read for auth rate limiting / lockout.
    if let Some(ip) = crate::auth::authorization::client_ip(&headers) {
        use crate::security::rate_limit::RateLimitResult;
        if let RateLimitResult::Limited(info) =
            state.security_manager.rate_limiter().check_auth_limit(ip)
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(clean_json!({
                    "error": "Too many authentication attempts. Please try again later.",
                    "retry_after_seconds": info.retry_after_seconds,
                })),
            );
        }
    }
    let request: SignupRequest = match sonic_rs::from_slice(body.as_ref()) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid request body"})),
            )
        }
    };

    let email = request.email.trim().to_lowercase();
    if !validate_email(&email) {
        return (
            StatusCode::BAD_REQUEST,
            Json(clean_json!({"error": "Invalid email address"})),
        );
    }
    if request.password.len() < 8 || request.password.len() > 128 {
        return (
            StatusCode::BAD_REQUEST,
            Json(clean_json!({"error": "Password must be between 8 and 128 characters"})),
        );
    }

    if let Ok(Some(_)) = app.user_repo.find_by_email(&email).await {
        return (
            StatusCode::CONFLICT,
            Json(clean_json!({"error": "Email already registered"})),
        );
    }

    let user_id = Uuid::new_v4().to_string();
    let name = request
        .name
        .filter(|n| !n.trim().is_empty())
        .unwrap_or_else(|| email.split('@').next().unwrap_or("User").to_string());

    let password_hash = match app
        .password_hasher
        .hash_password(&request.password, &user_id)
        .await
    {
        Ok(h) => h,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": e.to_string()})),
            )
        }
    };

    let user = crate::database::models::User {
        id: user_id.clone(),
        email: email.clone(),
        email_verified: false,
        name,
        image: None,
        password_hash: Some(password_hash),
        role: "member".into(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    match app.user_repo.create(&user).await {
        Ok(created) => match generate_jwt(&created.id, app, Some("password"), None) {
            Ok((token, claims)) => {
                record_session(&app.db_manager.client_pool, claims.jti.as_deref(), claims.exp, &created.id, "password").await;
                (
                    StatusCode::CREATED,
                    Json(make_token_response(&created, &token)),
                )
            }
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(e)),
        },
        Err(e) => {
            tracing::error!("failed to create user: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(clean_json!({"error": "failed to create user"})),
            )
        }
    }
}

async fn login_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = global_state();
    let app = &state.app_state;

    // Trusted client IP only (peer-IP middleware) — a client-supplied
    // X-Forwarded-For is never read for auth rate limiting / lockout.
    if let Some(ip) = crate::auth::authorization::client_ip(&headers) {
        use crate::security::rate_limit::RateLimitResult;
        if let RateLimitResult::Limited(info) =
            state.security_manager.rate_limiter().check_auth_limit(ip)
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(clean_json!({
                    "error": "Too many authentication attempts. Please try again later.",
                    "retry_after_seconds": info.retry_after_seconds,
                })),
            );
        }
    }
    let request: LoginRequest = match sonic_rs::from_slice(body.as_ref()) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid request body"})),
            )
        }
    };

    let email = request.email.trim().to_lowercase();

    // Enrolled instances delegate dashboard login to the control plane: the
    // portal account is the single identity. The instance verifies the
    // returned Ed25519 claims and mints its own session — it never sees or
    // stores a password hash.
    if app.control_plane.is_enrolled() {
        return match app
            .control_plane
            .authenticate_dashboard(&email, &request.password)
            .await
        {
            Ok(claims) => match mint_portal_session(app, &claims).await {
                Ok((user, token)) => {
                    (StatusCode::OK, Json(make_token_response(&user, &token)))
                }
                Err((status, body)) => (status, Json(body)),
            },
            Err(e) if e.to_string().contains("invalid credentials") => (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Invalid email or password"})),
            ),
            Err(e) => {
                tracing::warn!("dashboard login could not reach the control plane: {e:#}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(clean_json!({
                        "error": "Cannot reach the Actae portal to verify your credentials. Try again shortly."
                    })),
                )
            }
        };
    }

    // Standalone instances authenticate against the local user table. Logging
    // in is always allowed (only *signup* is gated by ENABLE_SIGNUP), so a
    // first-boot admin account remains reachable.
    let user = match app.user_repo.find_by_email(&email).await {
        Ok(Some(u)) => u,
        Ok(None) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Invalid email or password"})),
            )
        }
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(clean_json!({"error": "Database error"})),
            )
        }
    };

    let hash = match &user.password_hash {
        Some(h) => h,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Invalid email or password"})),
            )
        }
    };

    match app
        .password_hasher
        .verify_password(&request.password, hash, &user.id)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Invalid email or password"})),
            )
        }
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(clean_json!({"error": "Verification error"})),
            )
        }
    }

    match generate_jwt(&user.id, app, Some("password"), None) {
        Ok((token, claims)) => {
            record_session(&app.db_manager.client_pool, claims.jti.as_deref(), claims.exp, &user.id, "password").await;
            (StatusCode::OK, Json(make_token_response(&user, &token)))
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(e)),
    }
}

/// Consume a verified control-plane identity and mint a dashboard session.
///
/// Shared by `sso_handler` (a handoff token from the portal) and
/// `login_handler` (credentials verified by the control plane). The `jti` is
/// claimed in the single-use ledger first, so a replayed handoff token is
/// rejected before any session is issued.
async fn mint_portal_session(
    app: &AppState,
    claims: &crate::control_plane::license::DashboardClaims,
) -> Result<(crate::database::models::User, String), (StatusCode, CleanJson)> {
    let expires = chrono::DateTime::from_timestamp(claims.exp, 0)
        .unwrap_or_else(|| Utc::now() + chrono::Duration::minutes(15));
    let repo = SsoTokenRepository::new(app.db_manager.clone());
    match repo.consume(&claims.jti, expires).await {
        Ok(true) => {}
        Ok(false) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                clean_json!({
                    "error": "This sign-in link has already been used. Return to the Actae portal and click Open dashboard again."
                }),
            ))
        }
        Err(e) => {
            tracing::error!("dashboard SSO nonce store error: {e:#}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                clean_json!({"error": "Database error"}),
            ));
        }
    }

    let name = if claims.name.trim().is_empty() {
        claims.email.clone()
    } else {
        claims.name.clone()
    };
    let user = match app
        .user_repo
        .upsert_portal_user(&claims.sub, &claims.email, &name)
        .await
    {
        Ok(u) => u,
        Err(e) => {
            tracing::error!("failed to upsert portal user: {e:#}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                clean_json!({"error": "failed to create session user"}),
            ));
        }
    };

    match generate_jwt(&user.id, app, Some("sso"), None) {
        Ok((token, claims)) => {
            record_session(&app.db_manager.client_pool, claims.jti.as_deref(), claims.exp, &user.id, "sso").await;
            Ok((user, token))
        }
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

/// Exchange a control-plane-issued SSO handoff token for a dashboard
/// session. The token is signed with the key whose public half is embedded
/// in the binary (`ACTAE_LICENSE_PUBKEY`), bound to this installation, and
/// single-use; the browser never sees any credential beyond the short-lived
/// token in the URL fragment.
async fn sso_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = global_state();
    let app = &state.app_state;

    if let Some(ip) = crate::auth::authorization::client_ip(&headers) {
        use crate::security::rate_limit::RateLimitResult;
        if let RateLimitResult::Limited(info) =
            state.security_manager.rate_limiter().check_auth_limit(ip)
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(clean_json!({
                    "error": "Too many authentication attempts. Please try again later.",
                    "retry_after_seconds": info.retry_after_seconds,
                })),
            );
        }
    }
    let request: SsoRequest = match sonic_rs::from_slice(body.as_ref()) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid request body"})),
            )
        }
    };

    let claims = match crate::control_plane::license::verify_dashboard_token(&request.token) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("rejected dashboard SSO token: {e:#}");
            return (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Invalid or expired sign-in link"})),
            );
        }
    };

    // The token must be bound to *this* installation. A standalone instance
    // has no install id and therefore cannot accept an SSO token.
    let matches_install = app
        .control_plane
        .enrollment()
        .map(|e| e.install_id == claims.install_id)
        .unwrap_or(false);
    if !matches_install {
        tracing::warn!(
            "rejected dashboard SSO token for install '{}' on a different instance",
            claims.install_id
        );
        return (
            StatusCode::UNAUTHORIZED,
            Json(clean_json!({"error": "This sign-in link is not valid for this instance"})),
        );
    }

    match mint_portal_session(app, &claims).await {
        Ok((user, token)) => (StatusCode::OK, Json(make_token_response(&user, &token))),
        Err((status, body)) => (status, Json(body)),
    }
}

/// Log in with an API key (programmatic key → session exchange; no longer the
/// dashboard's default credential). Mints a session JWT whose subject is the
/// key's `user_id` and whose `auth_method` claim marks it as API-key-issued,
/// so `me` and the WebSocket path skip the local users-table lookup for it.
async fn api_key_login_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = global_state();
    let app = &state.app_state;

    // Trusted client IP only (peer-IP middleware) — a client-supplied
    // X-Forwarded-For is never read for auth rate limiting / lockout.
    if let Some(ip) = crate::auth::authorization::client_ip(&headers) {
        use crate::security::rate_limit::RateLimitResult;
        if let RateLimitResult::Limited(info) =
            state.security_manager.rate_limiter().check_auth_limit(ip)
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(clean_json!({
                    "error": "Too many authentication attempts. Please try again later.",
                    "retry_after_seconds": info.retry_after_seconds,
                })),
            );
        }
    }
    let request: ApiKeyLoginRequest = match sonic_rs::from_slice(body.as_ref()) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid request body"})),
            )
        }
    };

    let key = match crate::auth::api_key::validate_api_key(&request.api_key, app).await {
        Ok(Some(k)) => k,
        Ok(None) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Invalid API key"})),
            )
        }
        Err(e) => {
            let request_id = uuid::Uuid::new_v4().to_string();
            tracing::error!(
                request_id = %request_id,
                "api-key auth backend error on api-key-login: {e:#}"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(clean_json!({
                    "error": "authentication backend error",
                    "request_id": request_id,
                })),
            );
        }
    };

    // Consistency with the HTTP API: a key of a lapsed subscription must
    // not mint an exempt dashboard session that bypasses the 402 lock.
    if let Some(reason) = app.control_plane.blocks_key() {
        let (status, code) = if reason.http_status == 503 {
            (StatusCode::SERVICE_UNAVAILABLE, "subscription_unverifiable")
        } else {
            (StatusCode::PAYMENT_REQUIRED, "subscription_required")
        };
        return (
            status,
            Json(clean_json!({
                "status": code,
                "error": reason.message,
            })),
        );
    }

    match generate_jwt(&key.user_id, app, Some("api_key"), Some(&key.name)) {
        Ok((token, claims)) => {
            record_session(&app.db_manager.client_pool, claims.jti.as_deref(), claims.exp, &key.user_id, "api_key").await;
            (
                StatusCode::OK,
                Json(clean_json!({
                    "user": {
                        "id": key.user_id,
                        "email": "",
                        "email_verified": false,
                        "name": key.name,
                        "image": null,
                    },
                    "token": token,
                })),
            )
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(e)),
    }
}

/// `POST /api/v1/auth/local-session` — loopback dashboard auto-login.
///
/// A standalone instance served to a same-machine browser mints an admin session
/// with **no credential**, so the local self-hosted dashboard never shows a
/// login or setup form (the "no account required" promise). Programmatic access
/// still uses API keys — only the co-located browser is trusted.
///
/// Refused (403) when the peer/`Host`/`Origin` are not loopback (see
/// [`crate::auth::authorization::is_loopback_dashboard_request`]), the instance
/// is enrolled (portal SSO is the identity), or `ACTAE_LOCAL_AUTO_LOGIN=0`.
async fn local_session_handler(headers: HeaderMap) -> (StatusCode, Json<CleanJson>) {
    let Some(state) = crate::api::try_global_state() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(clean_json!({"error": "Server not initialized"})),
        );
    };
    let app = &state.app_state;

    if !crate::auth::local_dashboard_auto_login_enabled() || app.control_plane.is_enrolled() {
        return (
            StatusCode::FORBIDDEN,
            Json(clean_json!({
                "error": "Local auto-login is not available — sign in with your account."
            })),
        );
    }
    // Same-machine browser only, and never via a DNS-rebound public host.
    if !crate::auth::authorization::is_loopback_dashboard_request(&headers) {
        return (
            StatusCode::FORBIDDEN,
            Json(clean_json!({
                "error": "Local auto-login is only available from the same machine."
            })),
        );
    }

    let user = match app.user_repo.first_admin().await {
        Ok(Some(u)) => u,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(clean_json!({"error": "No local dashboard user exists"})),
            )
        }
        Err(e) => {
            tracing::error!("local auto-login user lookup failed: {e:#}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(clean_json!({"error": "Database error"})),
            );
        }
    };

    // The local operator is already in: retire any pending first-run setup token
    // so the dashboard never redirects to the setup form. A password can still be
    // set later with `actae passwd` (needed only for remote access).
    let _ = crate::auth::setup::mark_complete(&app.db_manager.client_pool).await;

    match generate_jwt(&user.id, app, Some("local"), None) {
        Ok((token, claims)) => {
            record_session(
                &app.db_manager.client_pool,
                claims.jti.as_deref(),
                claims.exp,
                &user.id,
                "local",
            )
            .await;
            tracing::info!(user = %user.email, "dashboard session auto-issued (loopback)");
            (StatusCode::OK, Json(make_token_response(&user, &token)))
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(e)),
    }
}

/// Public endpoint the dashboard uses to decide which login modes to show.
async fn auth_config_handler() -> (StatusCode, Json<CleanJson>) {
    let Some(state) = crate::api::try_global_state() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(clean_json!({"error": "Server not initialized"})),
        );
    };
    let app = &state.app_state;
    // `sso` means "log in with your Actae portal account": the instance is
    // enrolled and verifies credentials through the control plane. Local
    // password login is the standalone path (signup itself is opt-in).
    let sso = app.control_plane.is_enrolled();
    // First-run setup: a standalone instance whose initial admin has no
    // password yet shows the "set your admin password" page.
    let setup_required =
        !sso && crate::auth::setup::is_required(&app.db_manager.client_pool).await;
    // The dashboard attempts a loopback auto-login when this is true, so a local
    // self-hosted instance never shows a login/setup form.
    let local_auto_login = !sso && crate::auth::local_dashboard_auto_login_enabled();
    (
        StatusCode::OK,
        Json(clean_json!({
            "enable_signup": app.config.enable_signup,
            "sso": sso,
            "setup_required": setup_required,
            "local_auto_login": local_auto_login,
        })),
    )
}

/// `POST /api/v1/auth/setup` — first-run: set the initial admin password using
/// the one-time setup token printed at first boot. Available only while the
/// instance is standalone and setup is incomplete.
async fn setup_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let Some(state) = crate::api::try_global_state() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(clean_json!({"error": "Server not initialized"})),
        );
    };
    let app = &state.app_state;

    if let Some(ip) = crate::auth::authorization::client_ip(&headers) {
        use crate::security::rate_limit::RateLimitResult;
        if let RateLimitResult::Limited(info) =
            state.security_manager.rate_limiter().check_auth_limit(ip)
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(clean_json!({
                    "error": "Too many attempts. Please try again later.",
                    "retry_after_seconds": info.retry_after_seconds,
                })),
            );
        }
    }
    if app.control_plane.is_enrolled() {
        return (
            StatusCode::FORBIDDEN,
            Json(clean_json!({
                "error": "This instance uses the Actae portal account; first-run setup does not apply."
            })),
        );
    }
    let request: SetupRequest = match sonic_rs::from_slice(body.as_ref()) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid request body"})),
            )
        }
    };
    if request.password.len() < 8 || request.password.len() > 128 {
        return (
            StatusCode::BAD_REQUEST,
            Json(clean_json!({"error": "Password must be between 8 and 128 characters"})),
        );
    }

    let pool = &app.db_manager.client_pool;

    // Fast, non-authoritative pre-checks for accurate error messages; the
    // transaction inside `setup::complete` is the authoritative guard.
    let Some(setup_state) = crate::auth::setup::load(pool).await else {
        return (
            StatusCode::CONFLICT,
            Json(clean_json!({"error": "First-run setup is not available on this instance"})),
        );
    };
    if setup_state.setup_complete {
        return (
            StatusCode::CONFLICT,
            Json(clean_json!({"error": "Setup has already been completed — sign in instead"})),
        );
    }
    if !crate::auth::setup::token_matches(&setup_state, request.token.trim()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(clean_json!({"error": "Invalid or expired setup token"})),
        );
    }

    // Locate the passwordless first-boot admin (re-validated under lock inside
    // the transaction, so this read need not be).
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("setup: no db connection: {e:#}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(clean_json!({"error": "internal error"})),
            );
        }
    };
    let row = match client
        .query_opt(
            "SELECT id FROM public.user
             WHERE password_hash IS NULL ORDER BY \"createdAt\" ASC LIMIT 1",
            &[],
        )
        .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return (
                StatusCode::CONFLICT,
                Json(clean_json!({"error": "No admin account is awaiting setup"})),
            )
        }
        Err(e) => {
            tracing::error!("setup: admin lookup failed: {e:#}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(clean_json!({"error": "internal error"})),
            );
        }
    };
    let user_id: String = row.get("id");

    // Hash before taking the transaction so no slow work happens under locks.
    let hash = match app.password_hasher.hash_password(&request.password, &user_id).await {
        Ok(h) => h,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": e.to_string()})),
            )
        }
    };

    use crate::auth::setup::SetupOutcome;
    match crate::auth::setup::complete(
        pool,
        request.token.trim(),
        &user_id,
        &hash,
        request.name.as_deref(),
    )
    .await
    {
        Ok(SetupOutcome::Completed {
            user_id,
            email,
            name,
        }) => match generate_jwt(&user_id, app, Some("password"), None) {
            Ok((token, claims)) => {
                record_session(pool, claims.jti.as_deref(), claims.exp, &user_id, "password").await;
                let user = crate::database::models::User {
                    id: user_id,
                    email,
                    email_verified: true,
                    name,
                    image: None,
                    password_hash: None,
                    role: "admin".into(),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                };
                (StatusCode::OK, Json(make_token_response(&user, &token)))
            }
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(e)),
        },
        Ok(SetupOutcome::AlreadyComplete) => (
            StatusCode::CONFLICT,
            Json(clean_json!({"error": "Setup has already been completed — sign in instead"})),
        ),
        Ok(SetupOutcome::NoAdmin) => (
            StatusCode::CONFLICT,
            Json(clean_json!({"error": "No admin account is awaiting setup"})),
        ),
        Ok(SetupOutcome::InvalidToken) => (
            StatusCode::UNAUTHORIZED,
            Json(clean_json!({"error": "Invalid or expired setup token"})),
        ),
        Ok(SetupOutcome::Unavailable) => (
            StatusCode::CONFLICT,
            Json(clean_json!({"error": "First-run setup is not available on this instance"})),
        ),
        Err(e) => {
            tracing::error!("setup failed: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(clean_json!({"error": "internal error"})),
            )
        }
    }
}

async fn me_handler(headers: HeaderMap) -> (StatusCode, Json<CleanJson>) {    let state = global_state();
    let app = &state.app_state;
    let token = match extract_token(&headers) {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Missing authorization header"})),
            )
        }
    };

    let claims = match crate::auth::jwt::validate_token(&token, &app.config) {
        Ok(c) => c,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Invalid or expired token"})),
            )
        }
    };

    // Enforce server-side revocation: the session must have a live ledger row.
    let session_ok = match claims.jti.as_deref() {
        Some(jti) => {
            crate::auth::session_store::is_active(&app.db_manager.client_pool, jti).await
        }
        None => false,
    };
    if !session_ok {
        return (
            StatusCode::UNAUTHORIZED,
            Json(clean_json!({"error": "Session expired or signed out — sign in again"})),
        );
    }

    // API-key sessions (dashboard "log in with your key") carry a subject
    // that may be a SaaS identity — there is no local users-table row. The
    // key name (when minted) is carried in the claims metadata.
    if claims.auth_method.as_deref() == Some("api_key") {
        let name = claims
            .metadata
            .as_ref()
            .and_then(|m| m.get("key_name"))
            .and_then(|v| v.as_str())
            .unwrap_or("API Key");
        return (
            StatusCode::OK,
            Json(clean_json!({
                "user": {
                    "id": claims.sub,
                    "email": "",
                    "email_verified": false,
                    "name": name,
                    "image": null,
                    "role": "member",
                }
            })),
        );
    }

    match app.user_repo.get_by_id(&claims.sub).await {
        Ok(Some(user)) => (
            StatusCode::OK,
            Json(clean_json!({
                "user": {
                    "id": user.id,
                    "email": user.email,
                    "email_verified": user.email_verified,
                    "name": user.name,
                    "image": user.image,
                    "role": user.role,
                }
            })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(clean_json!({"error": "User not found"})),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(clean_json!({"error": "Database error"})),
        ),
    }
}

async fn logout_handler(headers: HeaderMap) -> (StatusCode, Json<CleanJson>) {
    // Revoke this session server-side so a captured token cannot be reused.
    // Uses the non-panicking accessor so logout is safe before/without a
    // running server state (e.g. unit tests).
    if let Some(state) = crate::api::try_global_state() {
        let app = &state.app_state;
        if let Some(token) = extract_token(&headers) {
            if let Ok(claims) = crate::auth::jwt::validate_token(&token, &app.config) {
                if let Some(jti) = claims.jti.as_deref() {
                    crate::auth::session_store::revoke(&app.db_manager.client_pool, jti).await;
                }
            }
        }
    }
    (
        StatusCode::OK,
        Json(clean_json!({"status": "ok", "message": "Logged out"})),
    )
}

/// `POST /api/v1/auth/logout-all` — revoke every session for the caller
/// ("sign out everywhere"). Requires a valid session token.
async fn logout_all_handler(headers: HeaderMap) -> (StatusCode, Json<CleanJson>) {
    let Some(state) = crate::api::try_global_state() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(clean_json!({"error": "Server not initialized"})),
        );
    };
    let app = &state.app_state;
    let token = match extract_token(&headers) {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Missing authorization header"})),
            )
        }
    };
    let claims = match crate::auth::jwt::validate_token(&token, &app.config) {
        Ok(c) => c,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({"error": "Invalid or expired session token"})),
            )
        }
    };
    let revoked =
        crate::auth::session_store::revoke_user(&app.db_manager.client_pool, &claims.sub).await;
    (
        StatusCode::OK,
        Json(clean_json!({"status": "ok", "revoked": revoked})),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::models::User;

    fn test_user() -> User {
        User {
            id: "user-1".into(),
            email: "test@example.com".into(),
            email_verified: true,
            name: "Test".into(),
            image: None,
            password_hash: Some("hash".into()),
            role: "admin".into(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_make_token_response() {
        let user = test_user();
        let resp = make_token_response(&user, "token-123");
        let resp_str = sonic_rs::to_string(&resp).unwrap();
        assert!(resp_str.contains("test@example.com"));
        assert!(resp_str.contains("token-123"));
    }

    #[test]
    fn test_make_token_response_no_image() {
        let user = User {
            image: None,
            ..test_user()
        };
        let resp = make_token_response(&user, "tok");
        let resp_str = sonic_rs::to_string(&resp).unwrap();
        assert!(
            resp_str.contains("null"),
            "expected null image, got: {resp_str}"
        );
    }

    #[test]
    fn test_extract_token_none() {
        let headers = HeaderMap::new();
        assert_eq!(extract_token(&headers), None);
    }

    #[test]
    fn test_extract_token_valid() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer mytoken".parse().unwrap());
        assert_eq!(extract_token(&headers), Some("mytoken".to_string()));
    }

    #[test]
    fn test_extract_token_wrong_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic abc".parse().unwrap());
        assert_eq!(extract_token(&headers), None);
    }

    #[tokio::test]
    async fn test_logout_handler_response() {
        let headers = HeaderMap::new();
        let (status, json) = logout_handler(headers).await;
        assert_eq!(status, StatusCode::OK);
        let json_str = sonic_rs::to_string(&json.0).unwrap();
        assert!(json_str.contains(r#""status":"ok""#));
    }
}
