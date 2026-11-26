// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for open-socket reauthorization (audit: scope reduction).
//!
//! Requires a running PostgreSQL (DATABASE_URL, e.g. the isolated dev PG).
//! A key's permissions are changed in the DB while a socket is open and
//! authenticated; `ConnectionManager::reauthorize_all` must refresh the
//! socket's principal and drop subscriptions the narrower scope no longer
//! covers — without a reconnect.

use std::sync::Arc;
use std::time::Duration;

use actae::auth::types::AuthContext;
use actae::config::AppConfig;
use actae::database::{
    repositories::{ApiKeyRepository, SessionRepository, UserRepository},
    DatabaseManager,
};
use actae::state::AppState;
use actae::websocket::batch_broadcaster::{BatchBroadcastConfig, BatchBroadcaster};
use actae::websocket::connection::Connection;
use actae::websocket::connection_manager::ConnectionManager;
use uuid::Uuid;

fn get_db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://actae:actae_dev@localhost:5432/actae".to_string())
}

fn pool_from_url(url: &str) -> deadpool_postgres::Pool {
    use deadpool_postgres::Runtime;
    let pg: tokio_postgres::Config = url.parse().expect("invalid DATABASE_URL");
    let mut cfg = deadpool_postgres::Config::new();
    cfg.host = pg.get_hosts().first().map(|h| match h {
        tokio_postgres::config::Host::Tcp(s) => s.clone(),
        tokio_postgres::config::Host::Unix(p) => format!("/{}", p.display()),
    });
    cfg.port = pg.get_ports().first().copied();
    cfg.user = pg.get_user().map(|u| u.to_owned());
    cfg.password = pg
        .get_password()
        .and_then(|b| std::str::from_utf8(b).ok().map(|s| s.to_string()));
    cfg.dbname = pg.get_dbname().map(|s| s.to_owned());
    cfg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .expect("failed to create pool")
}

async fn build_app_state() -> AppState {
    let url = get_db_url();
    let main_pool = pool_from_url(&url);
    let client_pool = pool_from_url(&url);

    // Self-sufficient schema setup: integration test binaries run against the
    // shared DATABASE_URL database in unspecified order, so this file cannot
    // assume another binary (or a dev server) already applied the client
    // migrations. run_client_migrations is idempotent (_client_migrations).
    actae::database::client_migrations::run_client_migrations(&main_pool)
        .await
        .expect("client migrations");

    let db_manager = Arc::new(DatabaseManager {
        main_pool: main_pool.clone(),
        client_pool: client_pool.clone(),
        pool_config: Arc::new(actae::database::PoolConfig {
            max_connections: 5,
            min_connections: 1,
            connection_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300),
            enable_connection_warmup: false,
        }),
        circuit_breakers: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        db_semaphore: Arc::new(tokio::sync::Semaphore::new(100)),
    });

    let batch_broadcaster = Arc::new(
        BatchBroadcaster::new(BatchBroadcastConfig::default())
            .await
            .expect("batch broadcaster"),
    );

    AppState {
        config: Arc::new(AppConfig::default()),
        db_manager: db_manager.clone(),
        user_repo: Arc::new(UserRepository::new(db_manager.clone())),
        api_key_repo: Arc::new(ApiKeyRepository::new(db_manager.clone())),
        session_repo: Arc::new(SessionRepository::new(db_manager.clone())),
        batch_broadcaster,
        password_hasher: Arc::new(
            actae::auth::password_hash::SecurePasswordHasher::with_secure_defaults(),
        ),
        auth_manager: Arc::new(actae::auth::AuthManager::new(db_manager.clone(), vec![])),
        control_plane: Arc::new(
            actae::control_plane::ControlPlaneManager::new(
                Arc::new(AppConfig::default()),
                client_pool,
            )
            .expect("control plane"),
        ),
    }
}

async fn insert_key(app: &AppState, id: &str, perms: &str) {
    let client = app.db_manager.get_client_connection().await.unwrap();
    client
        .execute(
            "INSERT INTO public.api_keys
                (id, user_id, name, key_hash, key_prefix, permissions, last_used_at,
                 expires_at, status, created_at, updated_at, source)
             VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, 'active', NOW(), NOW(), 'local')
             ON CONFLICT (id) DO UPDATE SET permissions = EXCLUDED.permissions, status = 'active'",
            &[
                &id,
                &"scope-test-owner".to_string(),
                &"scope-test".to_string(),
                &"not-used".to_string(),
                &"scope".to_string(),
                &actae::database::json_compat::to_sql_json(
                    &sonic_rs::from_str::<sonic_rs::Value>(perms).unwrap(),
                ),
            ],
        )
        .await
        .unwrap();
}

fn auth_from_perms(key_id: &str, perms: &str) -> AuthContext {
    AuthContext::ApiKey {
        key_id: key_id.into(),
        key_source: "local".into(),
        expires_at: None,
        permissions: actae::auth::parse_api_key_permissions(
            &sonic_rs::from_str::<sonic_rs::Value>(perms).unwrap(),
        ),
    }
}

#[tokio::test]
async fn scope_reduction_refreshes_open_socket_and_drops_subs() {
    let app = build_app_state().await;
    let cm = Arc::new(ConnectionManager::new(10, 100));
    let key_id = Uuid::new_v4().to_string();

    // Broad scope initially: read+write on ch-a and ch-b.
    insert_key(
        &app,
        &key_id,
        r#"{"read": ["ch-a", "ch-b"], "write": ["ch-a", "ch-b"]}"#,
    )
    .await;

    let conn_id = Uuid::new_v4();
    let conn = Connection::anonymous(None, None);
    let _rx = cm.add_connection(conn_id, conn).await.unwrap();
    cm.authenticate_connection(
        &conn_id,
        key_id.clone(),
        Some("api_key".into()),
        auth_from_perms(
            &key_id,
            r#"{"read": ["ch-a", "ch-b"], "write": ["ch-a", "ch-b"]}"#,
        ),
    )
    .await
    .unwrap();
    cm.subscribe_to_topic(&conn_id, "ch-a").await.unwrap();
    cm.subscribe_to_topic(&conn_id, "ch-b").await.unwrap();

    // Reduce the key's scope: read-only, ch-a only.
    insert_key(&app, &key_id, r#"{"read": ["ch-a"], "write": []}"#).await;

    let changed = cm.reauthorize_all(&app).await;
    assert_eq!(changed, 1, "the socket's principal must be refreshed");

    // The open socket now carries the narrowed principal.
    let conn = cm.get_connection(&conn_id).await.expect("connection gone");
    let ac = conn.auth_context.clone().expect("principal cleared");
    assert!(ac.can_read("ch-a"), "ch-a stays readable");
    assert!(!ac.can_read("ch-b"), "ch-b no longer readable");
    assert!(!ac.can_write("ch-a"), "write revoked on open socket");

    // The out-of-scope subscription is dropped; the in-scope one survives.
    assert!(
        !conn.is_subscribed("ch-b").await,
        "ch-b sub must be dropped"
    );
    assert!(conn.is_subscribed("ch-a").await, "ch-a sub must survive");
    assert_eq!(cm.topic_subscriber_count("ch-b").await, 0);
    assert_eq!(cm.topic_subscriber_count("ch-a").await, 1);
}

#[tokio::test]
async fn revoked_key_deauthenticates_open_socket() {
    let app = build_app_state().await;
    let cm = Arc::new(ConnectionManager::new(10, 100));
    let key_id = Uuid::new_v4().to_string();
    insert_key(&app, &key_id, r#"{"read": ["*"], "write": ["*"]}"#).await;

    let conn_id = Uuid::new_v4();
    let conn = Connection::anonymous(None, None);
    let _rx = cm.add_connection(conn_id, conn).await.unwrap();
    cm.authenticate_connection(
        &conn_id,
        key_id.clone(),
        Some("api_key".into()),
        auth_from_perms(&key_id, r#"{"read": ["*"], "write": ["*"]}"#),
    )
    .await
    .unwrap();
    cm.subscribe_to_topic(&conn_id, "ch-a").await.unwrap();

    // Revoke the key.
    let client = app.db_manager.get_client_connection().await.unwrap();
    client
        .execute(
            "UPDATE public.api_keys SET status = 'revoked' WHERE id = $1",
            &[&key_id],
        )
        .await
        .unwrap();

    let changed = cm.reauthorize_all(&app).await;
    assert_eq!(changed, 1);
    let conn = cm.get_connection(&conn_id).await.expect("connection gone");
    assert!(
        conn.auth_context.is_none(),
        "revoked socket must be de-authorized"
    );
    assert!(!conn.is_subscribed("ch-a").await);
    assert_eq!(cm.topic_subscriber_count("ch-a").await, 0);
}

#[tokio::test]
async fn unchanged_scope_still_refreshes_principal() {
    let app = build_app_state().await;
    let cm = Arc::new(ConnectionManager::new(10, 100));
    let key_id = Uuid::new_v4().to_string();
    insert_key(&app, &key_id, r#"{"read": ["*"], "write": ["*"]}"#).await;

    let conn_id = Uuid::new_v4();
    let conn = Connection::anonymous(None, None);
    let _rx = cm.add_connection(conn_id, conn).await.unwrap();
    cm.authenticate_connection(
        &conn_id,
        key_id.clone(),
        Some("api_key".into()),
        auth_from_perms(&key_id, r#"{"read": ["*"], "write": ["*"]}"#),
    )
    .await
    .unwrap();
    cm.subscribe_to_topic(&conn_id, "ch-a").await.unwrap();

    // Same scope, but the DB row is re-touched (e.g. last_used). The sweep
    // still replaces the principal object with a freshly-parsed one.
    insert_key(&app, &key_id, r#"{"read": ["*"], "write": ["*"]}"#).await;

    let changed = cm.reauthorize_all(&app).await;
    assert_eq!(changed, 1);
    let conn = cm.get_connection(&conn_id).await.expect("connection gone");
    let ac = conn.auth_context.clone().expect("principal must stay");
    assert!(ac.can_read("ch-a"));
    assert!(conn.is_subscribed("ch-a").await);
}
