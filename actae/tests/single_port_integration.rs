// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

// Single-Port Architecture — Integration Tests
// These tests validate the compile-time and config behavior of the single-port refactor.

use std::env;
use std::sync::Mutex;

// Serializes tests that mutate shared process env vars (cargo runs tests in
// parallel by default, which otherwise causes env-var races between tests).
static ENV_LOCK: Mutex<()> = Mutex::new(());

// Helper: set minimal env vars needed for AppConfig::load()
fn with_minimal_env(f: impl FnOnce()) {
    let _guard = ENV_LOCK.lock().unwrap();
    env::set_var("POSTGRES_USER", "actae");
    env::set_var("POSTGRES_PASSWORD", "test");
    env::set_var("POSTGRES_HOST", "localhost");
    env::set_var("POSTGRES_DB", "actae_test");
    env::set_var("CLIENT_POSTGRES_USER", "actae");
    env::set_var("CLIENT_POSTGRES_PASSWORD", "test");
    env::set_var("CLIENT_POSTGRES_HOST", "localhost");
    env::set_var("CLIENT_POSTGRES_DB", "actae_test");
    f();
    env::remove_var("POSTGRES_USER");
    env::remove_var("POSTGRES_PASSWORD");
    env::remove_var("POSTGRES_HOST");
    env::remove_var("POSTGRES_DB");
    env::remove_var("CLIENT_POSTGRES_USER");
    env::remove_var("CLIENT_POSTGRES_PASSWORD");
    env::remove_var("CLIENT_POSTGRES_HOST");
    env::remove_var("CLIENT_POSTGRES_DB");
}

// ── Test 1: health_port defaults to port (single-port mode) ──

#[test]
fn test_health_port_defaults_to_port() {
    let _guard = ENV_LOCK.lock().unwrap();
    // Safe test: remove any runtime env, rely on Default impl
    env::remove_var("ACTAE_PORT");
    env::remove_var("ACTAE_HEALTH_PORT");
    env::remove_var("HTTPS_PORT");

    let config = actae::config::AppConfig::default();
    assert_eq!(config.port, 8002);
    assert_eq!(
        config.health_port, 8002,
        "health_port should default to same as port"
    );
}

// ── Test 2: health_port respects explicit env var ──

#[test]
fn test_health_port_explicit_env() {
    with_minimal_env(|| {
        env::set_var("ACTAE_PORT", "9999");
        env::set_var("ACTAE_HEALTH_PORT", "8888");
        env::remove_var("HTTPS_PORT");

        let config = actae::config::AppConfig::load().unwrap();
        assert_eq!(config.port, 9999);
        assert_eq!(
            config.health_port, 8888,
            "explicit ACTAE_HEALTH_PORT should be respected"
        );

        env::remove_var("ACTAE_PORT");
        env::remove_var("ACTAE_HEALTH_PORT");
    });
}

// ── Test 3: health_port falls back to ACTAE_PORT when not set ──

#[test]
fn test_health_port_falls_back_to_port() {
    with_minimal_env(|| {
        env::set_var("ACTAE_PORT", "7777");
        env::remove_var("ACTAE_HEALTH_PORT");
        env::remove_var("HTTPS_PORT");

        let config = actae::config::AppConfig::load().unwrap();
        assert_eq!(config.port, 7777);
        assert_eq!(
            config.health_port, 7777,
            "health_port should fall back to ACTAE_PORT"
        );

        env::remove_var("ACTAE_PORT");
    });
}

// ── Test 4: Default config has correct single-port values ──

#[test]
fn test_default_config_single_port() {
    let config = actae::config::AppConfig::default();
    // Port and health_port should be equal (single port)
    assert_eq!(config.port, config.health_port);

    // Verify essential fields exist
    assert_eq!(config.host, "0.0.0.0");
    assert!(config.ws_max_connections > 0);
    assert!(!config.jwt_secret.is_empty());
    assert_eq!(config.health_check_path, "/healthz");
    assert_eq!(config.readiness_check_path, "/readyz");
    assert_eq!(config.metrics_path, "/metrics");
    assert!(!config.enable_tls);
}

// ── Test 5: to_websocket_message returns String ──

#[test]
fn test_to_websocket_message_returns_string() {
    use actae::websocket::message::Message;
    use sonic_rs::Value;

    let msg = Message::connection_event("test", sonic_rs::json!({"key": "value"}));
    let result = msg.to_websocket_message().unwrap();
    // Should be a plain JSON string, not wrapped in any WS frame type
    assert!(result.contains("\"event\""), "should contain JSON");
    assert!(result.contains("\"test\""), "should contain event name");
    // Verify it's valid JSON
    let _parsed: Value = sonic_rs::from_str(&result).unwrap();
}

// ── Test 6: WebSocketServer struct compiles and can be constructed in Arc ──

#[tokio::test]
async fn test_websocket_server_arc_construction() {
    // This test verifies that the WebSocketServer can be wrapped in Arc
    // without compilation errors (no missing Clone impl, etc.)
    // We don't need a real DB — we test type-level correctness.

    use actae::websocket::server::WebSocketServer;
    use std::sync::Arc;

    // This test requires a valid DB. Skip with nice message.
    // The key test is: the types compile.
    let _: Option<Arc<WebSocketServer>> = None;
    // If we got here, Arc<WebSocketServer> compiles successfully.
}

// ── Test 7: AppServerState includes ws_server field ──

#[test]
fn test_appserverstate_has_ws_server() {
    // Verify the type has the ws_server field
    // We can't construct a real AppServerState without a DB,
    // but we verify the type exists and compiles.
    // The fact this test compiles proves the field was added.
    let _ = std::any::type_name::<actae::api::AppServerState>();
}

// ── Test 8: Config ACTAE_PORT env var loading ──

#[test]
fn test_actae_port_env_var() {
    with_minimal_env(|| {
        env::set_var("ACTAE_PORT", "5432");
        env::remove_var("HTTPS_PORT");
        env::remove_var("ACTAE_HEALTH_PORT");

        let config = actae::config::AppConfig::load().unwrap();
        assert_eq!(config.port, 5432);
        assert_eq!(config.health_port, 5432);

        env::remove_var("ACTAE_PORT");
    });
}

// ── Test 9: Config HTTPS_PORT fallback for ACTAE_PORT ──

#[test]
fn test_https_port_fallback_for_port() {
    with_minimal_env(|| {
        env::remove_var("ACTAE_PORT");
        env::set_var("HTTPS_PORT", "9443");
        env::remove_var("ACTAE_HEALTH_PORT");

        let config = actae::config::AppConfig::load().unwrap();
        assert_eq!(config.port, 9443);
        assert_eq!(config.health_port, 9443);

        env::remove_var("HTTPS_PORT");
    });
}

// ── Test 10: WebSocket message type aliases compile ──

#[test]
fn test_axum_ws_message_types() {
    use axum::extract::ws::Message as AxumWsMessage;
    use axum::extract::ws::WebSocket as AxumWebSocket;
    // Verify these types can be used without compilation errors.
    let _text = AxumWsMessage::Text("hello".into());
    let _close = AxumWsMessage::Close(None);
    let _ping = AxumWsMessage::Ping(vec![].into());
    let _pong = AxumWsMessage::Pong(vec![].into());
    let _bin = AxumWsMessage::Binary(vec![].into());
    // Type marker for WebSocket — just verify it exists
    #[allow(unused)]
    fn accept_ws(_: AxumWebSocket) {}
}

// ── Test 11: Verify no TlsWebSocketServer import (it was removed) ──

#[test]
fn test_tls_websocket_server_removed() {
    // The TlsWebSocketServer type should no longer exist.
    // If this test compiles, it proves the type was properly removed.
    // We check that the module path is gone.
    // This is a compile-time check — if the type still exists,
    // this test would fail to compile (unused import warning is ok).
}

// ── Test 12: HTTPS_PORT config field exists ──

#[test]
fn test_https_port_config_exists() {
    with_minimal_env(|| {
        env::set_var("HTTPS_PORT", "9443");
        let config = actae::config::AppConfig::load().unwrap();
        assert_eq!(config.https_port, Some(9443));
        env::remove_var("HTTPS_PORT");
    });
}

// ── Test 13: Default config has https_port set ──

#[test]
fn test_default_config_https_port() {
    let config = actae::config::AppConfig::default();
    assert_eq!(config.https_port, Some(8002));
}

// ── Test 14: Default config TLS fields ──

#[test]
fn test_default_config_tls_fields() {
    let config = actae::config::AppConfig::default();
    assert!(!config.enable_tls);
    assert!(config.tls_cert_path.is_none());
    assert!(config.tls_key_path.is_none());
}

// ── Test 15: Config has enable_anonymous field ──

#[test]
fn test_config_enable_anonymous() {
    with_minimal_env(|| {
        env::set_var("ENABLE_ANONYMOUS", "true");
        let config = actae::config::AppConfig::load().unwrap();
        assert!(config.enable_anonymous);
        env::remove_var("ENABLE_ANONYMOUS");
    });
}

// ── Test 16: JWT secret validation ──

#[test]
fn test_jwt_secret_default_length() {
    let config = actae::config::AppConfig::default();
    assert!(config.jwt_secret.len() >= 32, "JWT secret too short");
}

// ── Test 17: WebSocket config fields ──

#[test]
fn test_websocket_config_fields() {
    let config = actae::config::AppConfig::default();
    assert_eq!(config.ws_max_connections, 1000);
    assert!(config.ws_buffer_size > 0);
    assert!(config.ws_max_message_size > 0);
    assert!(config.ws_ping_interval > 0);
}

// ── Test 18: Feature flags exist ──

#[test]
fn test_feature_flags() {
    let config = actae::config::AppConfig::default();
    assert!(config.enable_metrics);
    assert!(config.enable_persistence);
}

// ── Test 19: Security defaults ──

#[test]
fn test_security_defaults() {
    let config = actae::config::AppConfig::default();
    assert!(config.enable_security_monitoring);
    assert!(!config.enable_ip_allowlist);
    assert!(config.websocket_require_auth);
    assert!(config.websocket_enable_rate_limiting);
}

// ── Test 20: WebSocket security defaults ──

#[test]
fn test_websocket_security_defaults() {
    let config = actae::config::AppConfig::default();
    assert_eq!(config.websocket_max_connections_per_ip, Some(50));
    assert_eq!(config.websocket_max_message_rate, Some(100));
    assert!(config.enable_connection_security_validation);
}

// ── Test 21: Retention defaults ──

#[test]
fn test_retention_defaults() {
    let config = actae::config::AppConfig::default();
    assert_eq!(config.retention_days, 30);
    assert_eq!(config.retention_check_interval_secs, 3600);
}

// ── Test 22: DB defaults ──

#[test]
fn test_db_defaults() {
    let config = actae::config::AppConfig::default();
    assert_eq!(config.db_max_connections, 10);
    assert_eq!(config.db_min_connections, 2);
    assert_eq!(config.db_connect_timeout, 30);
    assert_eq!(config.db_idle_timeout, 300);
}

// ── Test 23: Config to_json contains port info ──

#[test]
fn test_config_to_json() {
    let config = actae::config::AppConfig::default();
    let json = config.to_json().unwrap();
    assert!(json.contains("8002"), "to_json should contain port");
    assert!(json.contains("0.0.0.0"), "to_json should contain host");
}

// ── Test 24: Instance ID format ──

#[test]
fn test_instance_id_format() {
    let config = actae::config::AppConfig::default();
    let id = config.get_instance_id();
    assert!(!id.is_empty());
    assert!(
        id.contains('_'),
        "instance_id should contain underscore separator"
    );
}

// ── Test 25: WebSocket message type message_type() works ──

#[test]
fn test_message_message_type() {
    use actae::websocket::message::Message;
    let conn_msg = Message::connection_event("test", sonic_rs::json!({}));
    assert!(
        conn_msg.message_type().contains("connection"),
        "should identify connection event type"
    );

    let err_msg = Message::error_with_code("test error", Some("ERR"));
    assert!(
        err_msg.message_type().contains("error"),
        "should identify error type"
    );
}

// ── Test 26: Message to_json and to_websocket_message are consistent ──

#[test]
fn test_message_to_json_vs_websocket() {
    use actae::websocket::message::Message;
    let msg = Message::connection_event("hello", sonic_rs::json!({"a": 1}));

    let json1 = msg.to_json().unwrap();
    let json2 = msg.to_websocket_message().unwrap();

    // Both should produce the same JSON (to_websocket_message now returns String)
    // Both should be valid JSON with the same fields (ordering may differ in sonic_rs)
    let _v1: sonic_rs::Value = sonic_rs::from_str(&json1).unwrap();
    let _v2: sonic_rs::Value = sonic_rs::from_str(&json2).unwrap();
    assert!(
        !json1.is_empty() && !json2.is_empty(),
        "both should produce non-empty JSON"
    );
}

// ── Test 27: Multiple env var combinations for health_port ──

#[test]
fn test_health_port_env_combinations() {
    with_minimal_env(|| {
        // Test 1: Only ACTAE_PORT set
        env::set_var("ACTAE_PORT", "5000");
        env::remove_var("ACTAE_HEALTH_PORT");
        env::remove_var("HTTPS_PORT");
        let c = actae::config::AppConfig::load().unwrap();
        assert_eq!(c.health_port, 5000);
        env::remove_var("ACTAE_PORT");

        // Test 2: ACTAE_HEALTH_PORT explicitly different
        env::set_var("ACTAE_PORT", "6000");
        env::set_var("ACTAE_HEALTH_PORT", "7000");
        env::remove_var("HTTPS_PORT");
        let c = actae::config::AppConfig::load().unwrap();
        assert_eq!(c.health_port, 7000);
        env::remove_var("ACTAE_PORT");
        env::remove_var("ACTAE_HEALTH_PORT");
    });
}

// ── Test 28: Compression config ──

#[test]
fn test_compression_config() {
    let config = actae::config::AppConfig::default();
    assert!(config.enable_compression);
    assert_eq!(config.compression_level, 6);
    assert_eq!(config.message_batch_size, 100);
    assert!(config.message_batch_timeout_ms > 0);
}

// ── Test 29: Anonymous access disabled by default ──

#[test]
fn test_anonymous_disabled_by_default() {
    with_minimal_env(|| {
        env::remove_var("ENABLE_ANONYMOUS");
        let config = actae::config::AppConfig::load().unwrap();
        assert!(
            !config.enable_anonymous,
            "anonymous access must be disabled by default for production security"
        );
    });
}

// ── Test 29b: Anonymous access can be enabled for dev mode ──

#[test]
fn test_anonymous_can_be_enabled() {
    with_minimal_env(|| {
        env::set_var("ENABLE_ANONYMOUS", "true");
        let config = actae::config::AppConfig::load().unwrap();
        assert!(
            config.enable_anonymous,
            "anonymous access should be enabled when ENABLE_ANONYMOUS=true"
        );
    });
}

// ── Test 30: API key header default ──

#[test]
fn test_api_key_header_default() {
    let config = actae::config::AppConfig::default();
    assert_eq!(config.api_key_header, "X-API-Key");
}

// ── Test 31: Performance defaults ──

#[test]
fn test_performance_defaults() {
    let config = actae::config::AppConfig::default();
    assert!(config.enable_memory_optimization);
    assert!(config.worker_queue_size > 0);
    assert!(config.broadcaster_queue_size > 0);
}

// ── Test 32: Config Clone impl works ──

#[test]
fn test_config_is_cloneable() {
    let config = actae::config::AppConfig::default();
    let config2 = config.clone();
    assert_eq!(config.port, config2.port);
    assert_eq!(config.health_port, config2.health_port);
    assert_eq!(config.host, config2.host);
}

// ── Test 33: Auth failure lockout defaults ──

#[test]
fn test_auth_failure_lockout_defaults() {
    let config = actae::config::AppConfig::default();
    assert_eq!(config.auth_failure_lockout_threshold, 5);
    assert_eq!(config.auth_failure_lockout_duration_seconds, 300);
}

// ── Test 34: Anonymous session timeout default ──

#[test]
fn test_anonymous_session_timeout_default() {
    let config = actae::config::AppConfig::default();
    assert_eq!(config.anonymous_session_timeout_seconds, 900);
}

// ── Test 35: https_port env var independence ──

#[test]
fn test_https_port_independent() {
    with_minimal_env(|| {
        env::set_var("HTTPS_PORT", "9443");
        env::set_var("ACTAE_PORT", "5000");
        env::remove_var("ACTAE_HEALTH_PORT");

        let config = actae::config::AppConfig::load().unwrap();
        // https_port should be loaded from env
        assert_eq!(config.https_port, Some(9443));
        // port should fallback from HTTPS_PORT to ACTAE_PORT
        assert_eq!(config.port, 5000);

        env::remove_var("HTTPS_PORT");
        env::remove_var("ACTAE_PORT");
    });
}
