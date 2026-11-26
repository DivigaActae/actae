// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! User-scoped endpoints (preferences persistence).
//!
//! `GET/PUT /api/v1/user/preferences` store an opaque JSON object per
//! resolved identity. Identity resolution mirrors the rest of the API:
//! a Bearer session JWT (minted by `/api/v1/auth/api-key-login` or
//! `/login`) or a raw `X-API-Key` header. Both resolve to the same
//! `user_id` namespace, so the dashboard (JWT) and HTTP SDK clients
//! (X-API-Key) share the same preference store.
//!
//! Security model:
//! - Credentials are never ambient: a Bearer token (HMAC-signed, 24h
//!   expiry) or a live API-key row is required for every request; there
//!   is no cookie fallback, so the wide-open CORS layer cannot be used
//!   cross-site (no credential headers are readable cross-origin).
//! - The API-key path revalidates the key row and applies the
//!   subscription lock exactly like `/api/v1/auth/api-key-login`.
//!   The JWT path trusts the already-gated
//!   session (login enforces the lock at mint time).
//! - Every failed/successful credential resolution feeds
//!   `security_manager.record_auth_failure/success`, so IP lockout,
//!   user lockout and the threat monitor treat this endpoint like any
//!   other authenticated surface.
//! - Input is capped at 64 KB and must be a JSON object; all queries
//!   are parameterized; stored values are round-tripped through their
//!   JSON string form on output (`clean_sonic_value`) so the sonic_rs
//!   arbitrary-precision representation can never leak into responses.
//! - No per-IP rate limit on purpose: a 401 precedes any work, the
//!   payload cap bounds write amplification, and the operation is a
//!   single-row indexed upsert — per-IP throttling here would only
//!   hand an unauthenticated attacker a cheap way to exhaust a victim
//!   IP's bucket. (DoS surface equals what a valid credential already
//!   has via `/api/v1/events/record`.)

use crate::api::global_state;
use crate::clean_json;
use crate::database::json_compat::{clean_sonic_value, CleanJson, JsonValue};
use axum::{
    http::{HeaderMap, StatusCode},
    routing::{get, put},
    Json, Router,
};
use sonic_rs::JsonValueTrait;
use sonic_rs::Value as SonicValue;

/// Build a stable internal-error response: the detail is logged server-side
/// and never sent to the client (audit item 15).
fn backend_error(message: &str, detail: std::fmt::Arguments<'_>) -> (StatusCode, Json<CleanJson>) {
    let request_id = uuid::Uuid::new_v4().to_string();
    tracing::error!(request_id = %request_id, "{}: {}", message, detail);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(clean_json!({
            "error": "internal error",
            "request_id": request_id,
        })),
    )
}

/// Hard cap on the serialized preferences object (abuse guard).
const MAX_PREFS_BYTES: usize = 64 * 1024;

pub fn routes() -> Router<()> {
    Router::new()
        .route("/preferences", get(get_preferences_handler))
        .route("/preferences", put(put_preferences_handler))
}

/// Client IP for audit/lockout purposes — the trusted peer-IP value recorded
/// by the middleware. A client-supplied `X-Forwarded-For` is never read here.
fn client_ip(headers: &HeaderMap) -> Option<std::net::IpAddr> {
    crate::auth::authorization::client_ip(headers)
}

/// Resolve the preference owner from the request headers.
///
/// Order: Bearer JWT (dashboard sessions) → `X-API-Key` (SDK clients).
/// The JWT subject is trusted as-is (signature checked); the API-key
/// path revalidates the key row and applies the subscription lock the
/// same way `/api/v1/auth/api-key-login` does. Every outcome is
/// reported to the security manager so failed credentials accumulate
/// toward IP/user lockout and the threat monitor sees the traffic.
async fn resolve_identity(headers: &HeaderMap) -> Result<String, (StatusCode, Json<CleanJson>)> {
    let state = global_state();
    let app = &state.app_state;
    let security = &state.security_manager;
    let ip = client_ip(headers);

    if let Some(auth_header) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(token) = auth_header
            .strip_prefix("Bearer ")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            return match crate::auth::jwt::validate_token(token, &app.config) {
                Ok(claims) => {
                    security.record_auth_success(
                        ip,
                        claims.sub.clone(),
                        crate::auth::websocket_auth::AuthMethod::JWT,
                    );
                    Ok(claims.sub)
                }
                Err(_) => {
                    security.record_auth_failure(
                        ip,
                        None,
                        crate::auth::websocket_auth::AuthMethod::JWT,
                        "Invalid or expired session token on /user/preferences".to_string(),
                    );
                    Err((
                        StatusCode::UNAUTHORIZED,
                        Json(clean_json!({"error": "Invalid or expired token"})),
                    ))
                }
            };
        }
    }

    if let Some(key_val) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return match crate::auth::api_key::validate_api_key(key_val, app).await {
            Ok(Some(key)) => {
                if let Some(reason) = app.control_plane.blocks_key() {
                    let (status, code) = if reason.http_status == 503 {
                        (StatusCode::SERVICE_UNAVAILABLE, "subscription_unverifiable")
                    } else {
                        (StatusCode::PAYMENT_REQUIRED, "subscription_required")
                    };
                    Err((
                        status,
                        Json(clean_json!({
                            "status": code,
                            "error": reason.message,
                        })),
                    ))
                } else {
                    security.record_auth_success(
                        ip,
                        key.user_id.clone(),
                        crate::auth::websocket_auth::AuthMethod::ApiKey,
                    );
                    Ok(key.user_id)
                }
            }
            Ok(None) => {
                security.record_auth_failure(
                    ip,
                    None,
                    crate::auth::websocket_auth::AuthMethod::ApiKey,
                    "Invalid API key on /user/preferences".to_string(),
                );
                Err((
                    StatusCode::UNAUTHORIZED,
                    Json(clean_json!({"error": "Invalid API key"})),
                ))
            }
            Err(e) => {
                security.record_auth_failure(
                    ip,
                    None,
                    crate::auth::websocket_auth::AuthMethod::ApiKey,
                    format!("Auth error on /user/preferences: {e}"),
                );
                let request_id = uuid::Uuid::new_v4().to_string();
                tracing::error!(
                    request_id = %request_id,
                    "auth backend error on /user/preferences: {e:#}"
                );
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(clean_json!({
                        "error": "authentication backend error",
                        "request_id": request_id,
                    })),
                ))
            }
        };
    }

    security.record_auth_failure(
        ip,
        None,
        crate::auth::websocket_auth::AuthMethod::ApiKey,
        "Missing credentials on /user/preferences".to_string(),
    );
    Err((
        StatusCode::UNAUTHORIZED,
        Json(clean_json!({
            "error": "Authentication required — provide a Bearer session token or X-API-Key header",
        })),
    ))
}

/// GET /api/v1/user/preferences
///
/// Returns `{"prefs": {...}}` — an empty object when nothing is stored.
async fn get_preferences_handler(headers: HeaderMap) -> (StatusCode, Json<CleanJson>) {
    let app = &global_state().app_state;

    let user_id = match resolve_identity(&headers).await {
        Ok(id) => id,
        Err(e) => return e,
    };

    let client = match app.db_manager.get_client_connection().await {
        Ok(c) => c,
        Err(e) => {
            return backend_error("preferences DB lookup failed", format_args!("{e:#}"));
        }
    };

    let prefs = match client
        .query_opt(
            "SELECT prefs FROM public.preferences WHERE user_id = $1",
            &[&user_id],
        )
        .await
    {
        Ok(Some(row)) => row.get::<_, JsonValue>("prefs").0,
        Ok(None) => sonic_rs::json!({}),
        Err(e) => {
            return backend_error("preferences DB lookup failed", format_args!("{e:#}"));
        }
    };

    (
        StatusCode::OK,
        Json(clean_json!({"prefs": clean_sonic_value(&prefs)})),
    )
}

/// PUT /api/v1/user/preferences
///
/// Body: `{"prefs": {...}}` — replaces the whole preferences object
/// (upsert). The envelope must be a JSON object and its `prefs` value must
/// itself be a JSON object, at most 64 KB serialized.
async fn put_preferences_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let app = &global_state().app_state;

    let user_id = match resolve_identity(&headers).await {
        Ok(id) => id,
        Err(e) => return e,
    };

    if body.len() > MAX_PREFS_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(
                clean_json!({"error": format!("Preferences object too large (max {MAX_PREFS_BYTES} bytes)")}),
            ),
        );
    }

    let envelope: SonicValue = match sonic_rs::from_slice(body.as_ref()) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid request body"})),
            )
        }
    };

    if !envelope.is_object() {
        return (
            StatusCode::BAD_REQUEST,
            Json(clean_json!({"error": "Body must be a JSON object like {\"prefs\": {...}}"})),
        );
    }

    let prefs = match envelope.get("prefs") {
        Some(v) if v.is_object() => v.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "prefs must be a JSON object"})),
            )
        }
    };

    let client = match app.db_manager.get_client_connection().await {
        Ok(c) => c,
        Err(e) => {
            return backend_error("preferences DB lookup failed", format_args!("{e:#}"));
        }
    };

    if let Err(e) = client
        .execute(
            "INSERT INTO public.preferences (user_id, prefs) VALUES ($1, $2)
             ON CONFLICT (user_id) DO UPDATE SET prefs = EXCLUDED.prefs, updated_at = NOW()",
            &[&user_id, &JsonValue::new(prefs.clone())],
        )
        .await
    {
        return backend_error("preferences upsert failed", format_args!("{e:#}"));
    }

    (
        StatusCode::OK,
        Json(clean_json!({"prefs": clean_sonic_value(&prefs)})),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routes_register_preferences() {
        // Route registration is exercised by the Python integration suite;
        // this only pins the module path.
        let _ = routes;
    }

    #[test]
    fn test_rejects_non_object_prefs() {
        for raw in [
            b"[1,2,3]".as_slice(),
            b"\"string\"".as_slice(),
            b"42".as_slice(),
            b"null".as_slice(),
        ] {
            let parsed: SonicValue = sonic_rs::from_slice(raw).unwrap();
            assert!(
                !parsed.is_object(),
                "expected {:?} to be rejected as non-object",
                String::from_utf8_lossy(raw)
            );
        }
    }

    #[test]
    fn test_accepts_object_prefs() {
        let parsed: SonicValue = sonic_rs::from_slice(br#"{"flow":{"fields":["uuid"]}}"#).unwrap();
        assert!(parsed.is_object());
    }

    #[test]
    fn test_extracts_inner_prefs_object() {
        // The handler stores only the inner `prefs` value, never the envelope.
        let envelope: SonicValue =
            sonic_rs::from_slice(br#"{"prefs":{"flow":{"layout":"dagre"}}}"#).unwrap();
        assert!(envelope.is_object());
        let inner = envelope.get("prefs").expect("prefs key present");
        assert!(inner.is_object());
        assert_eq!(
            inner
                .get("flow")
                .and_then(|f| f.get("layout"))
                .and_then(|l| l.as_str()),
            Some("dagre")
        );
    }

    #[test]
    fn test_rejects_non_object_inner_prefs() {
        for inner in [
            b"[1,2,3]".as_slice(),
            b"\"string\"".as_slice(),
            b"42".as_slice(),
        ] {
            let envelope: SonicValue =
                sonic_rs::from_slice(&[b"{\"prefs\":", inner, b"}".as_slice()].concat()).unwrap();
            let prefs = envelope.get("prefs").unwrap();
            assert!(
                !prefs.is_object(),
                "expected inner prefs {inner:?} to be rejected"
            );
        }
    }

    #[test]
    fn test_size_cap_matches_handler() {
        assert_eq!(MAX_PREFS_BYTES, 64 * 1024);
    }
}
