// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! `actae-federation-gateway` — the fleet gateway (Phase 5).
//!
//! A second, deliberately narrow binary: authentication, routing, typed
//! aggregation, bounded live fan-out and typed dispatch to the canonical
//! instance API (RUST-007). It holds no data-plane DB credential and no
//! generic proxy endpoint (D-005).
//!
//! Usage:
//!   GATEWAY_EXPERIMENTAL_ENABLE=1 \
//!   GATEWAY_CONTROL_PLANE_URL=http://saas:8004 \
//!   GATEWAY_INTERNAL_TOKEN=<shared with the SaaS internal endpoints> \
//!   GATEWAY_FEDERATION_VERIFICATION_KEYS=kid:base64url(ed25519-public-key) \
//!   cargo run --bin actae-federation-gateway --features federation-gateway

use std::sync::Arc;

use actae::federation_gateway::relay_mtls;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = actae::federation_gateway::config::GatewayConfig::from_env()?;
    let state = actae::federation_gateway::GatewayState::new(config).await?;

    tracing::info!(
        bind = %state.config.bind,
        control_plane = %state.config.control_plane_url,
        fleet_enabled = state.config.enable_commands || state.config.enable_ops_dispatch || state.config.enable_relay,
        "actae-federation-gateway starting"
    );

    let app = actae::federation_gateway::create_gateway_router(state.clone());

    // Deep-002: when relay mTLS is fully configured, start a SEPARATE mTLS
    // listener on the dedicated relay port (see relay_mtls::serve_relay_mtls):
    // client certs REQUIRED, cert SAN bound to the installation, relay-only
    // routes, rotation-overlap CA list. The credential-only relay route is NOT
    // registered on the main listener in this mode (create_gateway_router
    // enforces it), so the main port cannot bypass mTLS.
    if state.config.enable_relay && state.config.relay_mtls_enabled() {
        let relay_port = state
            .config
            .relay_port
            .ok_or_else(|| anyhow::anyhow!("GATEWAY_RELAY_PORT required for relay mTLS"))?;
        let ca_paths = state
            .config
            .relay_ca_bundle_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("GATEWAY_RELAY_CA_BUNDLE required for relay mTLS"))?;
        let cert_path = state
            .config
            .relay_tls_cert_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("GATEWAY_RELAY_TLS_CERT required for relay mTLS"))?;
        let key_path = state
            .config
            .relay_tls_key_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("GATEWAY_RELAY_TLS_KEY required for relay mTLS"))?;

        // Rotation overlap: GATEWAY_RELAY_CA_BUNDLE is a comma-separated list
        // of PEM bundle paths (old + new CA during a rotation window).
        let roots = relay_mtls::load_relay_roots(ca_paths)?;
        let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build relay client verifier: {e}"))?;
        let certs = relay_mtls::load_cert_chain(cert_path)?;
        use rustls::pki_types::pem::PemObject;
        let key = rustls::pki_types::PrivateKeyDer::from_pem_file(key_path)
            .map_err(|e| anyhow::anyhow!("failed to read GATEWAY_RELAY_TLS_KEY: {e}"))?;
        let tls_config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .map_err(|e| anyhow::anyhow!("failed to configure relay mTLS: {e}"))?;

        // Bind the configured relay address (no hard-coded 0.0.0.0).
        let relay_bind = format!("{}:{relay_port}", state.config.bind_host());
        let relay_listener = tokio::net::TcpListener::bind(&relay_bind).await?;
        tracing::info!(
            relay_bind = %relay_bind,
            ca_bundle = %ca_paths,
            "relay mTLS listener starting (client certs required)"
        );

        let relay_app = actae::federation_gateway::create_relay_router(state.clone());
        tokio::spawn(async move {
            relay_mtls::serve_relay_mtls(relay_listener, Arc::new(tls_config), relay_app).await;
        });
    }

    let listener = tokio::net::TcpListener::bind(state.config.bind).await?;
    tracing::info!(
        "actae-federation-gateway listening on {}",
        state.config.bind
    );

    // Optional listener TLS (required when binding a non-loopback address).
    if let (Some(cert), Some(key)) = (
        state.config.tls_cert_path.as_deref(),
        state.config.tls_key_path.as_deref(),
    ) {
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
            .await
            .map_err(|e| anyhow::anyhow!("failed to load gateway TLS: {e}"))?;
        let std_listener = listener
            .into_std()
            .map_err(|e| anyhow::anyhow!("failed to convert listener for TLS: {e}"))?;
        std_listener
            .set_nonblocking(true)
            .map_err(|e| anyhow::anyhow!("failed to set listener nonblocking: {e}"))?;
        axum_server::from_tcp_rustls(std_listener, tls)
            .serve(app.into_make_service())
            .await
            .map_err(|e| anyhow::anyhow!("gateway TLS server failed: {e}"))?;
    } else {
        axum::serve(listener, app.into_make_service())
            .await
            .map_err(|e| anyhow::anyhow!("gateway server failed: {e}"))?;
    }
    Ok(())
}
