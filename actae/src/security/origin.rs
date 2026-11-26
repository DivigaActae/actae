// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! WebSocket origin policy (audit item 5).
//!
//! The previous implementation defaulted to `"*"` when no origins were
//! configured and matched allowed origins with `contains()` substring
//! checks — so allowing `https://example.com` also allowed
//! `https://example.com.attacker.test`.
//!
//! New semantics:
//! - Configured origins are parsed as URLs and compared exactly on
//!   (scheme, lowercase host, effective port). No substring/`ends_with`.
//! - With no configured origins, browser connections are limited to
//!   same-origin: the Origin must match the request `Host` header on
//!   host + effective port, with the scheme derived from TLS on/off.
//! - `"*"` in `WEBSOCKET_ALLOWED_ORIGINS` is an explicit opt-in wildcard.
//! - A missing `Origin` header (CLI/server SDK clients, curl) is allowed —
//!   origin checks only apply when a browser sends the header.

use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedOrigin {
    scheme: String,
    host: String,
    port: u16,
}

impl NormalizedOrigin {
    /// Parse and normalize a browser `Origin` header value
    /// (e.g. `https://example.com:8443`).
    fn from_origin(origin: &str) -> Option<Self> {
        let url = Url::parse(origin).ok()?;
        let host = url.host_str()?.to_lowercase();
        let port = url.port_or_known_default()?;
        Some(Self {
            scheme: url.scheme().to_lowercase(),
            host,
            port,
        })
    }

    /// Parse a `Host` header (`example.com` or `example.com:8443`). The
    /// scheme is derived from whether the connection is TLS.
    fn from_host_header(host: &str, tls: bool) -> Option<Self> {
        let host = host.trim();
        if host.is_empty() {
            return None;
        }
        let (hostname, port) = if let Some((h, p)) = host.rsplit_once(':') {
            let p: u16 = p.parse().ok()?;
            if h.is_empty() {
                return None;
            }
            (h, p)
        } else {
            (host, if tls { 443 } else { 80 })
        };
        Some(Self {
            scheme: if tls {
                "https".to_string()
            } else {
                "http".to_string()
            },
            host: hostname.to_lowercase(),
            port,
        })
    }
}

#[derive(Debug, Clone)]
pub struct OriginPolicy {
    allowed: Vec<NormalizedOrigin>,
    wildcard: bool,
}

impl OriginPolicy {
    /// Build the policy from `WEBSOCKET_ALLOWED_ORIGINS`.
    ///
    /// `None` (or empty) config means same-origin only — a browser may only
    /// connect when its Origin matches the request's Host header.
    pub fn from_config(allowed_origins: Option<&[String]>) -> Self {
        match allowed_origins {
            None => Self {
                allowed: Vec::new(),
                wildcard: false,
            },
            Some(list) => {
                let wildcard = list.iter().any(|s| s.trim() == "*");
                let allowed = list
                    .iter()
                    .filter(|s| s.trim() != "*")
                    .filter_map(|s| NormalizedOrigin::from_origin(s.trim()))
                    .collect();
                Self { allowed, wildcard }
            }
        }
    }

    /// Decide whether a connection with the given `Origin` header may be
    /// upgraded. `host_header` is the request's `Host`; `tls` indicates the
    /// connection is HTTPS/wss (used for the same-origin scheme derivation).
    pub fn allows(&self, origin: &str, host_header: Option<&str>, tls: bool) -> bool {
        let Some(origin) = NormalizedOrigin::from_origin(origin) else {
            return false;
        };

        if self.wildcard {
            return true;
        }

        if self.allowed.iter().any(|a| a == &origin) {
            return true;
        }

        // No configured origins → same-origin only.
        if self.allowed.is_empty() {
            if let Some(host) = host_header.and_then(|h| NormalizedOrigin::from_host_header(h, tls))
            {
                return host == origin;
            }
        }

        false
    }

    /// A missing `Origin` header (CLI/SDK clients) is always allowed.
    pub fn allows_missing_origin(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(list: Option<&[&str]>) -> OriginPolicy {
        let v: Option<Vec<String>> = list.map(|l| l.iter().map(|s| s.to_string()).collect());
        OriginPolicy::from_config(v.as_deref())
    }

    #[test]
    fn test_no_config_allows_same_origin() {
        let p = policy(None);
        assert!(p.allows("https://example.com", Some("example.com"), true));
        assert!(p.allows("http://localhost:8002", Some("localhost:8002"), false));
        assert!(!p.allows("https://example.com", Some("example.com:8002"), true));
        assert!(!p.allows("https://example.com", Some("attacker.test"), true));
        // Scheme mismatch: http origin on a TLS connection host.
        assert!(!p.allows("http://example.com", Some("example.com"), true));
        assert!(p.allows_missing_origin());
    }

    #[test]
    fn test_explicit_origin_exact_match() {
        let p = policy(Some(&["https://example.com"]));
        assert!(p.allows("https://example.com", None, true));
        // Substring lookalikes must be rejected — the old `contains()` bug.
        assert!(!p.allows("https://example.com.attacker.test", None, true));
        assert!(!p.allows("https://attacker-example.com", None, true));
        assert!(!p.allows("https://sub.example.com", None, true));
        // Port mismatch.
        assert!(!p.allows("https://example.com:8443", None, true));
        // Scheme mismatch.
        assert!(!p.allows("http://example.com", None, true));
    }

    #[test]
    fn test_wildcard_opt_in_allows_everything() {
        let p = policy(Some(&["*"]));
        assert!(p.allows("https://anything.test", None, true));
        assert!(p.allows("http://localhost", None, false));
    }

    #[test]
    fn test_malformed_origin_denied() {
        let p = policy(None);
        assert!(!p.allows("", None, true));
        assert!(!p.allows("not-a-url", None, true));
        assert!(!p.allows("javascript:alert(1)", None, true));
    }

    #[test]
    fn test_effective_port_normalization() {
        let p = policy(Some(&["https://example.com"]));
        // https on default port 443 normalizes equal to an explicit :443.
        assert!(p.allows("https://example.com:443", None, true));
        // Explicitly configured https://example.com:8443 must differ from :443.
        let p2 = policy(Some(&["https://example.com:8443"]));
        assert!(!p2.allows("https://example.com", None, true));
        assert!(p2.allows("https://example.com:8443", None, true));
    }

    #[test]
    fn test_empty_list_is_same_origin_like_none() {
        let empty = policy(Some(&[]));
        let none = policy(None);
        for (origin, host, tls) in [
            ("https://example.com", Some("example.com"), true),
            ("http://localhost:8002", Some("localhost:8002"), false),
            ("https://evil.test", Some("example.com"), true),
        ] {
            assert_eq!(
                empty.allows(origin, host, tls),
                none.allows(origin, host, tls)
            );
        }
    }

    #[test]
    fn test_wildcard_plus_specific_list_still_wildcards() {
        let p = policy(Some(&["*", "https://example.com"]));
        assert!(p.allows("https://anything.test", None, true));
        assert!(p.allows("ftp://whatever", None, false));
    }

    #[test]
    fn test_host_header_normalization() {
        let p = policy(None);
        // Uppercase host header matches lowercase origin.
        assert!(p.allows("https://EXAMPLE.com", Some("example.com"), true));
        assert!(p.allows("https://example.com", Some("EXAMPLE.com"), true));
        // Explicit port on the Host header.
        assert!(p.allows("https://example.com:8443", Some("example.com:8443"), true));
        assert!(!p.allows("https://example.com:8443", Some("example.com:443"), true));
        // Scheme derived from TLS: http origin on a plain-HTTP host.
        assert!(p.allows("http://example.com", Some("example.com"), false));
        assert!(p.allows("http://example.com:8080", Some("example.com:8080"), false));
    }

    #[test]
    fn test_garbage_origins_and_hosts_denied() {
        let p = policy(None);
        // Origin present but unparseable → deny (missing origin is the only
        // allowed non-browser case).
        assert!(!p.allows("not a url", Some("example.com"), true));
        assert!(!p.allows("null", Some("example.com"), true));
        // Malformed Host header → same-origin check has nothing to match.
        assert!(!p.allows("https://example.com", Some("example.com:notaport"), true));
        assert!(!p.allows("https://example.com", Some(""), true));
    }

    #[test]
    fn test_origin_normalization_ignores_path_and_query() {
        let p = policy(Some(&["https://example.com"]));
        // Browsers send bare origins, but a path/query must not break parsing.
        assert!(p.allows("https://example.com/", None, true));
        assert!(p.allows("https://example.com/?a=1", None, true));
    }
}
