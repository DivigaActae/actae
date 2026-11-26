// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Shared authorization service.
//!
//! Single source of truth for API-key permission parsing, resource pattern
//! matching, and principal construction. Both HTTP (`resolve_auth_context` in
//! `main.rs`) and WebSocket (`WebSocketAuthenticator`) build their
//! `AuthContext` through this module, so the two transports can never drift
//! apart again (see audit items 2, 3, 4, 9, 12).
//!
//! Fail-closed contract: an empty, malformed, or unrecognized permission
//! payload yields an `ApiKeyPermissions` with no grant lists, which denies
//! every action. A full-access key (dashboard JWT, locally provisioned
//! keys) is expressed as `read`/`write`/`admin` all `["*"]`.

use crate::auth::types::AuthContext;
use crate::database::models::ApiKey as ApiKeyRow;
use crate::state::AppState;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

/// Resource-scoped permission grants for an API key.
///
/// Stored shape (canonical): `{"read": ["public.*"], "write": [], "delete": [],
/// "admin": []}`. Each list holds resource patterns; a missing key deserializes
/// as an empty list. A legacy capability-array shape (`["read", "write",
/// "read:project-a.*"]`) is also accepted for backward compatibility.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ApiKeyPermissions {
    #[serde(default)]
    pub read: Vec<String>,
    #[serde(default)]
    pub write: Vec<String>,
    #[serde(default)]
    pub delete: Vec<String>,
    #[serde(default)]
    pub admin: Vec<String>,
}

impl ApiKeyPermissions {
    /// Full access on every action/resource (dashboard session JWTs and
    /// locally provisioned keys created with `full_access_permissions()`).
    pub fn full() -> Self {
        Self {
            read: vec!["*".to_string()],
            write: vec!["*".to_string()],
            delete: Vec::new(),
            admin: vec!["*".to_string()],
        }
    }

    /// Authorize `action` ("read"/"write"/"delete"/"admin") against
    /// `resource` (a channel id). Admin patterns grant every action; a
    /// non-matching or empty list denies.
    pub fn check(&self, action: &str, resource: &str) -> bool {
        let list: &[String] = match action {
            "read" => &self.read,
            "write" => &self.write,
            "delete" => &self.delete,
            "admin" => &self.admin,
            _ => return false,
        };
        if list.iter().any(|p| Self::matches(p, resource)) {
            return true;
        }
        self.admin.iter().any(|p| Self::matches(p, resource))
    }

    /// Pattern matching: `*` matches every resource; a trailing `*` is a
    /// prefix match; anything else is an exact match.
    pub fn matches(pattern: &str, resource: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return resource.starts_with(prefix);
        }
        pattern == resource
    }
}

/// Parse the stored `permissions` JSONB into an `ApiKeyPermissions`.
///
/// Accepts the canonical object shape and the legacy capability array.
/// Anything unrecognized (malformed, empty, wrong type) fails closed to an
/// all-empty permission set so the key authenticates but authorizes nothing.
pub fn parse_api_key_permissions(v: &sonic_rs::Value) -> ApiKeyPermissions {
    let mut out = ApiKeyPermissions::default();

    if let Some(obj) = v.as_object() {
        for (key, list) in [
            ("read", &mut out.read),
            ("write", &mut out.write),
            ("delete", &mut out.delete),
            ("admin", &mut out.admin),
        ] {
            if let Some(arr) = obj.get(&key).and_then(|x| x.as_array()) {
                for item in arr.iter() {
                    if let Some(s) = item.as_str() {
                        list.push(s.to_string());
                    }
                }
            }
        }
        return out;
    }

    if let Some(arr) = v.as_array() {
        for item in arr.iter() {
            let Some(cap) = item.as_str() else { continue };
            match cap {
                "read" => out.read.push("*".to_string()),
                "write" => out.write.push("*".to_string()),
                "delete" => out.delete.push("*".to_string()),
                "admin" => out.admin.push("*".to_string()),
                _ => {
                    if let Some(rest) = cap.strip_prefix("read:") {
                        out.read.push(rest.to_string());
                    } else if let Some(rest) = cap.strip_prefix("write:") {
                        out.write.push(rest.to_string());
                    } else if let Some(rest) = cap.strip_prefix("delete:") {
                        out.delete.push(rest.to_string());
                    } else if let Some(rest) = cap.strip_prefix("admin:") {
                        out.admin.push(rest.to_string());
                    }
                    // Unknown capability contributes nothing (fail closed).
                }
            }
        }
        return out;
    }

    // Null / scalar / unexpected shape: deny all.
    out
}

/// An authenticated identity with its full authorization grants.
/// This is the single "principal" used by both transports.
#[derive(Debug, Clone)]
pub struct Principal {
    pub user_id: String,
    /// `"api_key"`, `"jwt"`, `"session"`, or `"anonymous"`.
    pub auth_method: String,
    pub key_id: Option<String>,
    pub key_source: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub permissions: ApiKeyPermissions,
}

impl Principal {
    pub fn authorize(&self, action: &str, resource: &str) -> bool {
        self.permissions.check(action, resource)
    }
}

/// Build the `AuthContext` (the authorization object consumed by handlers)
/// from a resolved principal.
pub fn build_auth_context(p: &Principal) -> AuthContext {
    match p.auth_method.as_str() {
        "api_key" => AuthContext::ApiKey {
            key_id: p.key_id.clone().unwrap_or_default(),
            key_source: p
                .key_source
                .clone()
                .unwrap_or_else(|| "control_plane".to_string()),
            expires_at: p.expires_at,
            permissions: p.permissions.clone(),
        },
        "anonymous" => AuthContext::Anonymous,
        _ => AuthContext::Authenticated {
            user_id: p.user_id.clone(),
            permissions: p.permissions.clone(),
        },
    }
}

/// Resolve a raw API key to its database row, shared by HTTP and WebSocket.
///
/// Hashes with `api_key_hash_secret` (HMAC-SHA256, the current standard used
/// by `actae keys create` and control-plane synced keys) and falls back to
/// raw SHA-256 for legacy keys. Only `status = 'active'` keys that are not
/// expired resolve. There must be no other API-key hash/fallback logic
/// anywhere in the codebase (audit item 4).
pub async fn resolve_api_key(key: &str, app_state: &AppState) -> Result<Option<ApiKeyRow>> {
    let client = app_state.db_manager.get_client_connection().await?;

    let hmac_hash =
        crate::auth::hmac_hash_api_key(key, app_state.config.api_key_hash_secret.as_bytes());
    let row = client
        .query_opt(
            "SELECT * FROM public.api_keys
             WHERE key_hash = $1 AND status = 'active' AND (expires_at IS NULL OR expires_at > NOW())",
            &[&hmac_hash],
        )
        .await?;
    if let Some(r) = row {
        return Ok(Some(ApiKeyRow::from(r)));
    }

    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    let sha256_hash = format!("{:x}", hasher.finalize());
    let row = client
        .query_opt(
            "SELECT * FROM public.api_keys
             WHERE key_hash = $1 AND status = 'active' AND (expires_at IS NULL OR expires_at > NOW())",
            &[&sha256_hash],
        )
        .await?;
    Ok(row.map(ApiKeyRow::from))
}

/// The trusted client IP recorded by the peer-IP middleware
/// (`x-actae-client-ip`).
///
/// This is the ONLY header-derived IP that auth, audit, lockout and
/// rate-limit paths may trust. A client-supplied `X-Forwarded-For` is never
/// read directly here — the middleware derives the value from the actual
/// socket peer, honoring forwarding headers only when the immediate peer is
/// a configured trusted proxy (audit: rate limiting trusts raw attacker IP
/// headers).
///
/// Returns `None` when the middleware did not run (e.g. a handler invoked
/// directly in tests or an embedded server path that omits ConnectInfo) —
/// callers then have *no* IP, never a spoofed one.
pub fn client_ip(headers: &axum::http::HeaderMap) -> Option<std::net::IpAddr> {
    headers
        .get("x-actae-client-ip")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<std::net::IpAddr>().ok())
}

/// True when a request is a browser/agent hitting the dashboard on the same
/// machine, for the loopback-only dashboard auto-login.
///
/// Three independent conditions must all hold:
/// 1. the trusted peer IP (set by the peer-IP middleware, never a spoofable
///    header) is loopback;
/// 2. the `Host` header names a loopback host; and
/// 3. any `Origin` header is loopback as well.
///
/// Conditions 2–3 defeat DNS rebinding: a malicious page on a public domain
/// that re-resolves to `127.0.0.1` still sends its real `Host`/`Origin`, so it
/// is refused. Non-browser clients (no `Origin`) are allowed when the other two
/// hold.
pub fn is_loopback_dashboard_request(headers: &axum::http::HeaderMap) -> bool {
    let peer_loopback = client_ip(headers)
        .map(|ip| ip.is_loopback())
        .unwrap_or(false);
    if !peer_loopback {
        return false;
    }
    let host_ok = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(host_is_loopback)
        .unwrap_or(false);
    if !host_ok {
        return false;
    }
    match headers
        .get(axum::http::header::ORIGIN)
        .and_then(|v| v.to_str().ok())
    {
        None => true,
        Some(origin) => origin_is_loopback(origin),
    }
}

/// True when a `Host` header value (host or host:port, IPv4/IPv6/name) is a
/// loopback name.
fn host_is_loopback(host: &str) -> bool {
    let host = host.trim();
    // Strip an optional port, handling the bracketed IPv6 form `[::1]:8002`.
    // A bare IPv6 (more than one colon, unbracketed) has no port to strip.
    let without_port = if let Some(rest) = host.strip_prefix('[') {
        match rest.find(']') {
            Some(end) => &host[..end + 2],
            None => host,
        }
    } else if host.matches(':').count() == 1 {
        match host.rsplit_once(':') {
            Some((name, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => {
                name
            }
            _ => host,
        }
    } else {
        host
    };
    let name = without_port.trim_matches(|c| c == '[' || c == ']');
    if name.eq_ignore_ascii_case("localhost") {
        return true;
    }
    name.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

/// True when an `Origin` header value points at a loopback host.
fn origin_is_loopback(origin: &str) -> bool {
    match url::Url::parse(origin) {
        Ok(u) => match u.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
            None => false,
        },
        Err(_) => false,
    }
}

/// Resolve the caller IP, never trusting `X-Forwarded-For` unless the
/// immediate peer is a configured trusted proxy (audit item 6).
///
/// Trusted-proxy semantics: when the direct peer belongs to
/// `ACTAE_TRUSTED_PROXIES`, walk the forwarded chain right-to-left skipping
/// trusted entries and return the first untrusted address; if every hop is
/// trusted, fall back to the leftmost forwarded value. Without trusted
/// proxies the socket peer address is authoritative.
pub fn resolve_client_addr(
    socket_peer: Option<std::net::IpAddr>,
    forwarded_for: Option<&str>,
    trusted_proxies: &[ipnet::IpNet],
) -> Option<std::net::IpAddr> {
    let peer_ip = socket_peer;

    let trusted_peer = match (peer_ip, trusted_proxies.is_empty()) {
        (_, true) => false, // no trusted proxies configured: peer is never trusted for forwarding
        (Some(ip), false) => trusted_proxies.iter().any(|n| n.contains(&ip)),
        (None, _) => false,
    };

    if !trusted_peer {
        return peer_ip;
    }

    let chain: Vec<std::net::IpAddr> = forwarded_for
        .unwrap_or_default()
        .split(',')
        .filter_map(|s| s.trim().parse::<std::net::IpAddr>().ok())
        .collect();
    if chain.is_empty() {
        return peer_ip;
    }

    // Walk right-to-left (the order proxies appended them) returning the
    // first address that is NOT a trusted proxy. If every hop is trusted,
    // the leftmost (original client) wins.
    for ip in chain.iter().rev() {
        if !trusted_proxies.iter().any(|n| n.contains(ip)) {
            return Some(*ip);
        }
    }
    chain.first().copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::json;

    #[test]
    fn test_default_denies_everything() {
        let perms = ApiKeyPermissions::default();
        assert!(!perms.check("read", "any.channel"));
        assert!(!perms.check("write", "any.channel"));
        assert!(!perms.check("admin", "any.channel"));
    }

    #[test]
    fn test_full_grants_everything() {
        let perms = ApiKeyPermissions::full();
        assert!(perms.check("read", "any.channel"));
        assert!(perms.check("write", "any.channel"));
        assert!(perms.check("admin", "any.channel"));
    }

    #[test]
    fn test_match_exact_prefix_and_wildcard() {
        assert!(ApiKeyPermissions::matches("*", "x"));
        assert!(ApiKeyPermissions::matches("public.*", "public.a"));
        assert!(ApiKeyPermissions::matches("public.a", "public.a"));
        assert!(!ApiKeyPermissions::matches("public.a", "public.b"));
        assert!(!ApiKeyPermissions::matches("public.*", "private.a"));
    }

    #[test]
    fn test_parse_canonical_object() {
        let v = json!({"read": ["project-a.*"], "write": [], "admin": []});
        let p = parse_api_key_permissions(&v);
        assert!(p.check("read", "project-a.ch"));
        assert!(!p.check("read", "project-b.ch"));
        assert!(!p.check("write", "project-a.ch"));
    }

    #[test]
    fn test_parse_missing_keys_fail_closed() {
        // Control-plane keys may omit delete/admin.
        let v = json!({"read": ["public.*"], "write": ["*"]});
        let p = parse_api_key_permissions(&v);
        assert!(p.check("read", "public.x"));
        assert!(p.check("write", "public.x"));
        assert!(!p.check("delete", "public.x"));
    }

    #[test]
    fn test_parse_empty_object_denies() {
        let p = parse_api_key_permissions(&json!({}));
        assert!(!p.check("read", "anything"));
    }

    #[test]
    fn test_parse_malformed_denies() {
        for v in [json!(null), json!(42), json!("read"), json!([7, true])] {
            let p = parse_api_key_permissions(&v);
            assert!(!p.check("read", "anything"), "should deny for {v:?}");
            assert!(!p.check("write", "anything"));
        }
    }

    #[test]
    fn test_parse_legacy_capability_array() {
        let v = json!(["read", "write", "read:public.*"]);
        let p = parse_api_key_permissions(&v);
        assert!(p.check("read", "x"));
        assert!(p.check("write", "x"));
        assert!(!p.check("delete", "x"));
    }

    #[test]
    fn test_parse_legacy_connect_only_denies_channels() {
        let v = json!(["connect"]);
        let p = parse_api_key_permissions(&v);
        assert!(!p.check("read", "x"));
        assert!(!p.check("write", "x"));
    }

    #[test]
    fn test_admin_grants_all_actions() {
        let v = json!({"read": [], "admin": ["ops.*"]});
        let p = parse_api_key_permissions(&v);
        assert!(p.check("write", "ops.ch"));
        assert!(p.check("read", "ops.ch"));
        assert!(p.check("delete", "ops.ch"));
        assert!(!p.check("read", "other.ch"));
    }

    #[test]
    fn test_build_auth_context_variants() {
        let api_key = Principal {
            user_id: "u".into(),
            auth_method: "api_key".into(),
            key_id: Some("k".into()),
            key_source: Some("control_plane".into()),
            expires_at: None,
            permissions: parse_api_key_permissions(&json!({"read": ["a.*"]})),
        };
        match build_auth_context(&api_key) {
            AuthContext::ApiKey {
                key_id,
                permissions,
                ..
            } => {
                assert_eq!(key_id, "k");
                assert!(permissions.check("read", "a.1"));
            }
            other => panic!("expected ApiKey, got {other:?}"),
        }

        let anon = Principal {
            user_id: "anon_1".into(),
            auth_method: "anonymous".into(),
            key_id: None,
            key_source: None,
            expires_at: None,
            permissions: ApiKeyPermissions::default(),
        };
        assert!(matches!(build_auth_context(&anon), AuthContext::Anonymous));

        let jwt = Principal {
            user_id: "u".into(),
            auth_method: "jwt".into(),
            key_id: None,
            key_source: None,
            expires_at: None,
            permissions: ApiKeyPermissions::full(),
        };
        assert!(matches!(
            build_auth_context(&jwt),
            AuthContext::Authenticated { .. }
        ));
    }

    #[test]
    fn test_client_ip_consumes_only_trusted_header() {
        use axum::http::{HeaderMap, HeaderValue};

        // Middleware-derived value is trusted.
        let mut h = HeaderMap::new();
        h.insert("x-actae-client-ip", HeaderValue::from_static("203.0.113.9"));
        assert_eq!(client_ip(&h), Some("203.0.113.9".parse().unwrap()));

        // A raw X-Forwarded-For is IGNORED — even when x-actae-client-ip is
        // absent (no fallback to a client-controlled value).
        let mut h2 = HeaderMap::new();
        h2.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.7, 10.0.0.1"),
        );
        assert_eq!(client_ip(&h2), None, "raw XFF must never be trusted");

        // The trusted header wins over any XFF the client also sent.
        let mut h3 = HeaderMap::new();
        h3.insert("x-actae-client-ip", HeaderValue::from_static("203.0.113.9"));
        h3.insert("x-forwarded-for", HeaderValue::from_static("198.51.100.7"));
        assert_eq!(client_ip(&h3), Some("203.0.113.9".parse().unwrap()));
    }

    #[test]
    fn host_is_loopback_handles_names_ports_and_ipv6() {
        assert!(host_is_loopback("localhost"));
        assert!(host_is_loopback("localhost:8002"));
        assert!(host_is_loopback("127.0.0.1"));
        assert!(host_is_loopback("127.0.0.1:8002"));
        assert!(host_is_loopback("[::1]:8002"));
        assert!(host_is_loopback("::1"));
        assert!(!host_is_loopback("evil.example"));
        assert!(!host_is_loopback("evil.example:8002"));
        assert!(!host_is_loopback("10.0.0.5"));
        assert!(!host_is_loopback(""));
    }

    #[test]
    fn origin_is_loopback_only_for_loopback_origins() {
        assert!(origin_is_loopback("http://localhost:3000"));
        assert!(origin_is_loopback("http://127.0.0.1:8002"));
        assert!(origin_is_loopback("https://localhost"));
        assert!(!origin_is_loopback("http://evil.example"));
        assert!(!origin_is_loopback("http://10.0.0.5:8002"));
        assert!(!origin_is_loopback("not a url"));
    }

    #[test]
    fn loopback_dashboard_request_requires_peer_host_and_origin() {
        use axum::http::{HeaderMap, HeaderValue};

        let local = HeaderValue::from_static("127.0.0.1");
        let host = |v: &'static str| HeaderValue::from_static(v);

        // Loopback peer + loopback host, no Origin (curl/non-browser): allowed.
        let mut ok = HeaderMap::new();
        ok.insert("x-actae-client-ip", local.clone());
        ok.insert(axum::http::header::HOST, host("127.0.0.1:8002"));
        assert!(is_loopback_dashboard_request(&ok));

        // Browser same-origin: loopback Origin: allowed.
        let mut ok_origin = ok.clone();
        ok_origin.insert(
            axum::http::header::ORIGIN,
            host("http://localhost:3000"),
        );
        assert!(is_loopback_dashboard_request(&ok_origin));

        // Non-loopback peer: refused.
        let mut remote = HeaderMap::new();
        remote.insert("x-actae-client-ip", host("203.0.113.9"));
        remote.insert(axum::http::header::HOST, host("127.0.0.1:8002"));
        assert!(!is_loopback_dashboard_request(&remote));

        // DNS rebinding: loopback peer but a public Host: refused.
        let mut rebound = HeaderMap::new();
        rebound.insert("x-actae-client-ip", local.clone());
        rebound.insert(axum::http::header::HOST, host("evil.example:8002"));
        assert!(!is_loopback_dashboard_request(&rebound));

        // Public Origin: refused.
        let mut bad_origin = ok.clone();
        bad_origin.insert(axum::http::header::ORIGIN, host("http://evil.example"));
        assert!(!is_loopback_dashboard_request(&bad_origin));

        // Missing Host: refused.
        let mut no_host = HeaderMap::new();
        no_host.insert("x-actae-client-ip", local.clone());
        assert!(!is_loopback_dashboard_request(&no_host));
    }

    #[test]
    fn test_resolve_client_addr_ignores_spoofed_xff_without_proxies() {
        let ip = "203.0.113.9".parse().unwrap();
        let xff = Some("1.2.3.4, 5.6.7.8");
        let out = resolve_client_addr(Some(ip), xff, &[]);
        assert_eq!(
            out,
            Some(ip),
            "must use socket peer when no trusted proxies"
        );
    }

    #[test]
    fn test_resolve_client_addr_trusted_chain_rightmost_untrusted() {
        let peer: std::net::IpAddr = "10.0.0.1".parse().unwrap();
        let trusted: Vec<ipnet::IpNet> = ["10.0.0.0/8".parse().unwrap()].to_vec();
        // Proxy appended: original client 203.0.113.9 first, then proxy 10.0.0.5.
        let xff = Some("203.0.113.9, 10.0.0.5");
        assert_eq!(
            resolve_client_addr(Some(peer), xff, &trusted),
            Some("203.0.113.9".parse().unwrap())
        );
    }

    #[test]
    fn test_resolve_client_addr_all_trusted_uses_leftmost() {
        let peer: std::net::IpAddr = "10.0.0.2".parse().unwrap();
        let trusted: Vec<ipnet::IpNet> = ["10.0.0.0/8".parse().unwrap()].to_vec();
        let xff = Some("10.0.0.3, 10.0.0.4");
        assert_eq!(
            resolve_client_addr(Some(peer), xff, &trusted),
            Some("10.0.0.3".parse().unwrap())
        );
    }

    #[test]
    fn test_resolve_client_addr_garbage_xff_with_trusted_peer_uses_peer() {
        let peer: std::net::IpAddr = "10.0.0.2".parse().unwrap();
        let trusted: Vec<ipnet::IpNet> = ["10.0.0.0/8".parse().unwrap()].to_vec();
        let out = resolve_client_addr(Some(peer), Some("not-an-ip"), &trusted);
        assert_eq!(out, Some("10.0.0.2".parse().unwrap()));
    }
}
