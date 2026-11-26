// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Relay mTLS listener (Deep-002).
//!
//! The production relay endpoint runs on a dedicated mTLS port that:
//! * REQUIRES a client certificate signed by the configured relay CA(s) —
//!   missing / wrong-CA / expired / revoked certificates fail the handshake
//!   (no bearer-only fallback on this port);
//! * binds the client certificate's `actae-relay://<installation_id>` URI SAN
//!   to the connection and injects it per-request so the relay handler can
//!   refuse a valid cert bound to a different installation;
//! * mounts ONLY the relay router (fleet query/dispatch/stream are never
//!   reachable on this port);
//! * supports rotation overlap via a comma-separated `GATEWAY_RELAY_CA_BUNDLE`
//!   list (old + new CA files during a rotation).
//!
//! The credential-only relay path on the main listener is dev-only; when
//! `relay_mtls_enabled()` is true, `create_gateway_router` does not register
//! the relay route at all, so the main port cannot bypass mTLS.

use std::sync::Arc;

use axum::Router;
use tower::Service;

use super::relay::RelayInstallId;

/// Load every CA certificate from a comma-separated list of PEM bundle paths
/// into a RootCertStore (rotation overlap: list old + new CAs during a
/// rotation window).
pub fn load_relay_roots(ca_paths: &str) -> anyhow::Result<rustls::RootCertStore> {
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    let mut any_cert = false;
    for path in ca_paths
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let certs = rustls::pki_types::CertificateDer::pem_file_iter(path)
            .map_err(|e| anyhow::anyhow!("failed to read relay CA bundle {path}: {e}"))?;
        let mut found_in_file = false;
        for cert in certs {
            let cert =
                cert.map_err(|e| anyhow::anyhow!("relay CA bundle {path} parse error: {e}"))?;
            roots
                .add(cert)
                .map_err(|e| anyhow::anyhow!("failed to add relay CA root from {path}: {e}"))?;
            found_in_file = true;
            any_cert = true;
        }
        // Fail closed (Deep-002): a listed CA file that contains NO certificates
        // is malformed/rotated-out material — do not silently accept it.
        if !found_in_file {
            return Err(anyhow::anyhow!(
                "relay CA bundle {path} contains no certificates"
            ));
        }
    }
    if !any_cert {
        return Err(anyhow::anyhow!(
            "GATEWAY_RELAY_CA_BUNDLE contains no certificates"
        ));
    }
    Ok(roots)
}

/// Load the relay listener certificate chain (server identity).
pub fn load_cert_chain(
    cert_path: &str,
) -> anyhow::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use rustls::pki_types::pem::PemObject;
    rustls::pki_types::CertificateDer::pem_file_iter(cert_path)
        .map_err(|e| anyhow::anyhow!("failed to read GATEWAY_RELAY_TLS_CERT: {e}"))?
        .map(|c| c.map_err(|e| anyhow::anyhow!("GATEWAY_RELAY_TLS_CERT parse error: {e}")))
        .collect()
}

/// Extract the installation id bound to a verified relay client certificate:
/// the `actae-relay://<installation_id>` URI SAN. Returns `None` when the
/// certificate has no such SAN — the connection is rejected at the edge.
pub fn peer_installation_id(
    tls: &tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
) -> Option<String> {
    let (_, conn) = tls.get_ref();
    let certs = conn.peer_certificates()?;
    let der = certs.first()?;
    let (_, cert) = x509_parser::parse_x509_certificate(der.as_ref()).ok()?;
    let san = cert.subject_alternative_name().ok()??;
    use x509_parser::extensions::GeneralName;
    for gn in &san.value.general_names {
        if let GeneralName::URI(uri) = gn {
            if let Some(id) = uri.strip_prefix("actae-relay://") {
                if !id.is_empty() {
                    return Some(id.to_string());
                }
            }
        }
    }
    None
}

/// Serve the relay router over an mTLS accept loop. Every accepted connection
/// must complete the mTLS handshake AND carry a client certificate whose
/// `actae-relay://<install_id>` URI SAN maps to an installation; the identity
/// is injected per-request so the relay handler can refuse a certificate bound
/// to a different installation.
pub async fn serve_relay_mtls(
    listener: tokio::net::TcpListener,
    tls_config: Arc<rustls::ServerConfig>,
    app: Router,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls_config);
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(%e, "relay mTLS accept failed");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let tls = match acceptor.accept(stream).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::debug!(
                        %e,
                        "relay mTLS handshake failed (missing/wrong-CA/expired/revoked client cert)"
                    );
                    return;
                }
            };
            let Some(install) = peer_installation_id(&tls) else {
                tracing::debug!("relay client certificate has no actae-relay:// SAN; rejecting");
                return;
            };
            let svc = tower::ServiceBuilder::new()
                .map_request(move |req: axum::http::Request<hyper::body::Incoming>| {
                    let install = install.clone();
                    let (parts, body) = req.into_parts();
                    let mut req =
                        axum::http::Request::from_parts(parts, axum::body::Body::new(body));
                    req.extensions_mut().insert(RelayInstallId(install));
                    req
                })
                .service(app.clone());
            // Bridge the tower service to hyper's Service trait.
            let hyper_svc = hyper::service::service_fn(move |req| {
                let mut svc = svc.clone();
                async move { svc.call(req).await }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(tls), hyper_svc)
                .with_upgrades()
                .await
            {
                tracing::debug!(%e, "relay connection error");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, SanType, PKCS_ECDSA_P256_SHA256,
    };
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use std::str::FromStr;
    use std::time::Duration;

    /// Self-signed CA (returns the rcgen Certificate so it can sign leaves).
    fn make_ca() -> (rcgen::Certificate, KeyPair) {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "actae-relay-test-ca");
        let kp = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let cert = params.self_signed(&kp).unwrap();
        (cert, kp)
    }

    fn ca_der(ca: &rcgen::Certificate) -> CertificateDer<'static> {
        CertificateDer::from(ca.der().to_vec())
    }

    fn issue_leaf(
        ca: &rcgen::Certificate,
        ca_key: &KeyPair,
        san_uri: &str,
        expired: bool,
        not_yet_valid: bool,
    ) -> (CertificateDer<'static>, KeyPair) {
        let leaf_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let mut params = CertificateParams::default();
        params.subject_alt_names = vec![SanType::URI(rcgen::Ia5String::from_str(san_uri).unwrap())];
        params
            .distinguished_name
            .push(DnType::CommonName, "relay-client");
        if expired {
            use rcgen::date_time_ymd;
            params.not_before = date_time_ymd(2020, 1, 1);
            params.not_after = date_time_ymd(2020, 1, 2);
        }
        if not_yet_valid {
            use rcgen::date_time_ymd;
            params.not_before = date_time_ymd(2999, 1, 1);
            params.not_after = date_time_ymd(2999, 1, 2);
        }
        let cert = params.signed_by(&leaf_key, ca, ca_key).unwrap();
        (CertificateDer::from(cert.der().to_vec()), leaf_key)
    }

    /// Server leaf with a DNS SAN for `localhost` (so the mTLS client can
    /// verify the server hostname) plus the install URI SAN.
    fn issue_server_leaf(
        ca: &rcgen::Certificate,
        ca_key: &KeyPair,
    ) -> (CertificateDer<'static>, KeyPair) {
        let leaf_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let mut params = CertificateParams::default();
        params.subject_alt_names = vec![
            SanType::DnsName(rcgen::Ia5String::from_str("localhost").unwrap()),
            SanType::URI(rcgen::Ia5String::from_str("actae-relay://gateway").unwrap()),
        ];
        params
            .distinguished_name
            .push(DnType::CommonName, "relay-server");
        let cert = params.signed_by(&leaf_key, ca, ca_key).unwrap();
        (CertificateDer::from(cert.der().to_vec()), leaf_key)
    }

    fn key_der(kp: &KeyPair) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(kp.serialize_der()))
    }

    /// Server mTLS config: trust the CA, require client certs, serve with a
    /// leaf cert signed by the same CA (so the client, trusting the CA, can
    /// verify the server).
    fn server_config(ca: &rcgen::Certificate, ca_key: &KeyPair) -> Arc<rustls::ServerConfig> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca_der(ca)).unwrap();
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let (srv_cert, srv_key) = issue_server_leaf(ca, ca_key);
        Arc::new(
            rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(vec![srv_cert], key_der(&srv_key))
                .unwrap(),
        )
    }

    #[allow(dead_code)]
    fn client_config(
        ca: &rcgen::Certificate,
        leaf: &CertificateDer<'static>,
        key: &KeyPair,
    ) -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca_der(ca)).unwrap();
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(vec![leaf.clone()], key_der(key))
                .unwrap(),
        )
    }

    /// Complete an mTLS handshake + a real WS-upgrade GET to the relay connect
    /// path and return the HTTP status (0 when the handshake fails; 101 =
    /// upgraded, meaning the TLS + cert identity were accepted and the relay
    /// session loop began).
    async fn relay_handshake(
        ca: &rcgen::Certificate,
        leaf: &CertificateDer<'static>,
        key: &KeyPair,
        addr: std::net::SocketAddr,
    ) -> u16 {
        self::tests::relay_handshake_cas(&[ca], leaf, key, addr).await
    }

    async fn relay_handshake_cas(
        cas: &[&rcgen::Certificate],
        leaf: &CertificateDer<'static>,
        key: &KeyPair,
        addr: std::net::SocketAddr,
    ) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio_rustls::client::TlsConnector;
        let mut roots = rustls::RootCertStore::empty();
        for ca in cas {
            roots.add(ca_der(ca)).unwrap();
        }
        let config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(vec![leaf.clone()], key_der(key))
                .unwrap(),
        );
        let connector = TlsConnector::from(config);
        let stream = match tokio::net::TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(_) => return 0,
        };
        let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = match connector.connect(server_name, stream).await {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let key_b64 = "c3RhdGljLXRlc3Qta2V5";
        let req = format!(
            "GET /v1/relay/instances/ins_1/connect HTTP/1.1\r\n\
             Host: localhost\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {key_b64}\r\n\
             Sec-WebSocket-Version: 13\r\n\r\n"
        );
        if tls.write_all(req.as_bytes()).await.is_err() {
            return 0;
        }
        let mut buf = Vec::new();
        // Bound the read: the relay session loop times out and closes after
        // ~1s, so this returns quickly; a deadline guards against hangs.
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            let _ = tls.read_to_end(&mut buf).await;
        })
        .await;
        let text = String::from_utf8_lossy(&buf).to_string();
        text.lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn relay_mtls_accepts_valid_cert_and_rejects_mismatched_install() {
        let (ca, ca_key) = make_ca();
        let (good_leaf, good_key) = issue_leaf(&ca, &ca_key, "actae-relay://ins_1", false, false);
        let (other_leaf, other_key) = issue_leaf(&ca, &ca_key, "actae-relay://ins_2", false, false);
        let cfg = server_config(&ca, &ca_key);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = super::super::create_relay_router(test_state().await);
        tokio::spawn(serve_relay_mtls(listener, cfg, app));

        // Valid cert bound to ins_1 -> reaches the handler (404 = "relay not
        // configured" because no relay credential is set — the point is it is
        // a normal app response, not a TLS failure and not a 403).
        let status = relay_handshake(&ca, &good_leaf, &good_key, addr).await;
        // A valid install-bound cert completes the mTLS handshake and upgrades
        // (101) — TLS + cert identity accepted and the relay session began.
        assert_eq!(
            status, 101,
            "valid install-bound cert should be accepted (101), not a TLS failure (0) or 403"
        );

        // Valid cert bound to a DIFFERENT installation -> app rejects with 403.
        let status = relay_handshake(&ca, &other_leaf, &other_key, addr).await;
        assert_eq!(
            status, 403,
            "certificate bound to another installation must be rejected"
        );
    }

    #[tokio::test]
    async fn relay_mtls_rejects_wrong_ca_and_expired_certs() {
        let (ca, ca_key) = make_ca();
        let (wrong_ca, wrong_ca_key) = make_ca();
        let (wrong_leaf, wrong_key) = issue_leaf(
            &wrong_ca,
            &wrong_ca_key,
            "actae-relay://ins_1",
            false,
            false,
        );
        let (expired_leaf, expired_key) =
            issue_leaf(&ca, &ca_key, "actae-relay://ins_1", true, false);
        let cfg = server_config(&ca, &ca_key);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = super::super::create_relay_router(test_state().await);
        tokio::spawn(serve_relay_mtls(listener, cfg, app));

        // Wrong-CA cert -> TLS handshake fails (status 0 = no HTTP response).
        let status = relay_handshake(&ca, &wrong_leaf, &wrong_key, addr).await;
        assert_eq!(
            status, 0,
            "wrong-CA client cert must fail the mTLS handshake"
        );

        // Expired cert -> TLS handshake fails.
        let status = relay_handshake(&ca, &expired_leaf, &expired_key, addr).await;
        assert_eq!(
            status, 0,
            "expired client cert must fail the mTLS handshake"
        );
    }

    async fn test_state() -> Arc<super::super::GatewayState> {
        use super::super::config::GatewayConfig;
        use std::collections::HashMap;
        let cfg = GatewayConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            tls_cert_path: None,
            tls_key_path: None,
            control_plane_url: "http://saas:8004".into(),
            internal_token: "a-very-long-internal-token".into(),
            gateway_id: "gw-test".into(),
            saas_ca_bundle_path: None,
            saas_client_cert_path: None,
            saas_client_key_path: None,
            allow_insecure_saas: true,
            relay_ca_bundle_path: None,
            relay_port: None,
            relay_tls_cert_path: None,
            relay_tls_key_path: None,
            nats_servers: None,
            nats_subject_prefix: "actae.fleet.live".into(),
            nats_ca_bundle_path: None,
            nats_client_cert_path: None,
            nats_client_key_path: None,
            nats_max_frame_bytes: 1024 * 1024,
            federation: crate::auth::federation::FederationConfig::default(),
            instance_base_urls: HashMap::new(),
            max_fanout: 20,
            query_deadline: Duration::from_secs(10),
            max_response_events: 1000,
            body_limit_bytes: 2 * 1024 * 1024,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(30),
            routing_cache_ttl: Duration::from_secs(30),
            rate_limit_per_sec: 100,
            max_stream_connections: 100,
            breaker_threshold: 3,
            breaker_open_for: Duration::from_secs(60),
            breaker_half_open_probes: 1,
            relay_credential: Some("relay-shared".into()),
            relay_heartbeat_interval: Duration::from_secs(20),
            relay_heartbeat_timeout: Duration::from_secs(45),
            org_quota_per_min: 0,
            enable_relay: true,
            enable_commands: true,
            enable_ops_dispatch: true,
            allow_root: true,
        };
        super::super::GatewayState::new(cfg).await.unwrap()
    }

    #[test]
    fn load_relay_roots_rejects_empty_and_parses_pem() {
        assert!(load_relay_roots("").is_err());
        assert!(load_relay_roots("  , , ").is_err());
        let (ca, _) = make_ca();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(ca.der());
        std::fs::write(
            &path,
            format!("-----BEGIN CERTIFICATE-----\n{b64}\n-----END CERTIFICATE-----\n"),
        )
        .unwrap();
        let roots = load_relay_roots(path.to_str().unwrap()).unwrap();
        assert!(!roots.is_empty());
        // A non-PEM file among the list fails loudly (fail closed on bad CA
        // material) rather than silently accepting only the good bundle.
        let bad = dir.path().join("bad.pem");
        std::fs::write(&bad, "not a certificate").unwrap();
        assert!(load_relay_roots(&format!("{},{}", path.display(), bad.display())).is_err());
    }

    #[test]
    fn config_validates_relay_mtls_atomicity() {
        use super::super::config::GatewayConfig;
        use std::collections::HashMap;
        let base = GatewayConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            tls_cert_path: None,
            tls_key_path: None,
            control_plane_url: "http://saas:8004".into(),
            internal_token: "a-very-long-internal-token".into(),
            gateway_id: "gw-test".into(),
            saas_ca_bundle_path: None,
            saas_client_cert_path: None,
            saas_client_key_path: None,
            allow_insecure_saas: true,
            relay_ca_bundle_path: None,
            relay_port: None,
            relay_tls_cert_path: None,
            relay_tls_key_path: None,
            nats_servers: None,
            nats_subject_prefix: "actae.fleet.live".into(),
            nats_ca_bundle_path: None,
            nats_client_cert_path: None,
            nats_client_key_path: None,
            nats_max_frame_bytes: 1024 * 1024,
            federation: crate::auth::federation::FederationConfig::default(),
            instance_base_urls: HashMap::new(),
            max_fanout: 20,
            query_deadline: Duration::from_secs(10),
            max_response_events: 1000,
            body_limit_bytes: 2 * 1024 * 1024,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(30),
            routing_cache_ttl: Duration::from_secs(30),
            rate_limit_per_sec: 100,
            max_stream_connections: 100,
            breaker_threshold: 3,
            breaker_open_for: Duration::from_secs(60),
            breaker_half_open_probes: 1,
            relay_credential: Some("relay-shared".into()),
            relay_heartbeat_interval: Duration::from_secs(20),
            relay_heartbeat_timeout: Duration::from_secs(45),
            org_quota_per_min: 0,
            enable_relay: true,
            enable_commands: true,
            enable_ops_dispatch: true,
            allow_root: true,
        };
        // Partial: port + CA but no cert/key -> rejected.
        let partial = GatewayConfig {
            relay_port: Some(8811),
            relay_ca_bundle_path: Some("/tmp/ca.pem".into()),
            ..base.clone()
        };
        assert!(partial.validate().is_err());
        // Complete: all four parts -> valid.
        let complete = GatewayConfig {
            relay_port: Some(8811),
            relay_ca_bundle_path: Some("/tmp/ca.pem".into()),
            relay_tls_cert_path: Some("/tmp/cert.pem".into()),
            relay_tls_key_path: Some("/tmp/key.pem".into()),
            ..base
        };
        assert!(complete.validate().is_ok());
        assert!(complete.relay_mtls_enabled());
    }

    #[tokio::test]
    async fn main_router_does_not_serve_relay_when_mtls_enabled() {
        // Deep-002: with relay mTLS active the relay endpoint is served ONLY on
        // the dedicated mTLS port — the credential-only main listener must not
        // expose the relay route (otherwise a client could bypass mTLS).
        use super::super::config::GatewayConfig;
        use std::collections::HashMap;
        let cfg = GatewayConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            tls_cert_path: None,
            tls_key_path: None,
            control_plane_url: "http://saas:8004".into(),
            internal_token: "a-very-long-internal-token".into(),
            gateway_id: "gw-test".into(),
            saas_ca_bundle_path: None,
            saas_client_cert_path: None,
            saas_client_key_path: None,
            allow_insecure_saas: true,
            relay_ca_bundle_path: Some("/tmp/ca.pem".into()),
            relay_port: Some(8811),
            relay_tls_cert_path: Some("/tmp/cert.pem".into()),
            relay_tls_key_path: Some("/tmp/key.pem".into()),
            nats_servers: None,
            nats_subject_prefix: "actae.fleet.live".into(),
            nats_ca_bundle_path: None,
            nats_client_cert_path: None,
            nats_client_key_path: None,
            nats_max_frame_bytes: 1024 * 1024,
            federation: crate::auth::federation::FederationConfig::default(),
            instance_base_urls: HashMap::new(),
            max_fanout: 20,
            query_deadline: Duration::from_secs(10),
            max_response_events: 1000,
            body_limit_bytes: 2 * 1024 * 1024,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(30),
            routing_cache_ttl: Duration::from_secs(30),
            rate_limit_per_sec: 100,
            max_stream_connections: 100,
            breaker_threshold: 3,
            breaker_open_for: Duration::from_secs(60),
            breaker_half_open_probes: 1,
            relay_credential: Some("relay-shared".into()),
            relay_heartbeat_interval: Duration::from_secs(20),
            relay_heartbeat_timeout: Duration::from_secs(45),
            org_quota_per_min: 0,
            enable_relay: true,
            enable_commands: true,
            enable_ops_dispatch: true,
            allow_root: true,
        };
        assert!(cfg.relay_mtls_enabled());
        let state = super::super::GatewayState::new(cfg).await.unwrap();
        let router = super::super::create_gateway_router(state);
        use tower::ServiceExt;
        let resp = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/relay/instances/ins_1/connect")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::NOT_FOUND,
            "main listener must not serve the relay route in mTLS mode"
        );
    }

    #[tokio::test]
    async fn relay_mtls_rotation_overlap_accepts_old_and_new_ca() {
        // Rotation (GA gate): during an overlap window the listener trusts
        // BOTH the old and new CAs, so certs signed by either still work.
        let (old_ca, old_ca_key) = make_ca();
        let (new_ca, new_ca_key) = make_ca();
        // Server leaf signed by the NEW CA (both are in the trust store).
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca_der(&old_ca)).unwrap();
        roots.add(ca_der(&new_ca)).unwrap();
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let (srv_cert, srv_key) = issue_server_leaf(&new_ca, &new_ca_key);
        let cfg = Arc::new(
            rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(vec![srv_cert], key_der(&srv_key))
                .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = super::super::create_relay_router(test_state().await);
        tokio::spawn(serve_relay_mtls(listener, cfg, app));

        // Old-CA-signed client cert is still accepted during the overlap.
        let (old_leaf, old_key) =
            issue_leaf(&old_ca, &old_ca_key, "actae-relay://ins_1", false, false);
        let status = relay_handshake_cas(&[&old_ca, &new_ca], &old_leaf, &old_key, addr).await;
        assert_eq!(
            status, 101,
            "old-CA cert must be accepted during rotation overlap"
        );
        // New-CA-signed client cert is accepted too.
        let (new_leaf, new_key) =
            issue_leaf(&new_ca, &new_ca_key, "actae-relay://ins_1", false, false);
        let status = relay_handshake_cas(&[&old_ca, &new_ca], &new_leaf, &new_key, addr).await;
        assert_eq!(status, 101, "new-CA cert must be accepted after rotation");
    }

    #[tokio::test]
    async fn relay_mtls_rejects_not_yet_valid_and_revoked_certs() {
        // Clock-skew / revocation negatives: a cert whose validity window has
        // not started, and a cert whose CA has been removed from the trust
        // store (effective revocation), both fail the handshake.
        let (ca, ca_key) = make_ca();
        let (good, good_key) = issue_leaf(&ca, &ca_key, "actae-relay://ins_1", false, false);
        let (future, future_key) = issue_leaf(&ca, &ca_key, "actae-relay://ins_1", false, true);
        let cfg = server_config(&ca, &ca_key);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = super::super::create_relay_router(test_state().await);
        tokio::spawn(serve_relay_mtls(listener, cfg, app));

        // Sanity: a valid cert is accepted.
        let status = relay_handshake(&ca, &good, &good_key, addr).await;
        assert_eq!(status, 101, "valid cert must be accepted");
        // Not-yet-valid (clock skew forward) -> handshake fails.
        let status = relay_handshake(&ca, &future, &future_key, addr).await;
        assert_eq!(
            status, 0,
            "not-yet-valid client cert must fail the handshake"
        );

        // Revocation: the instance's CA is no longer in the trust store.
        let (other_ca, _) = make_ca();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca_der(&other_ca)).unwrap();
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let (srv_cert, srv_key) = issue_server_leaf(&ca, &ca_key);
        let revoked_cfg = Arc::new(
            rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(vec![srv_cert], key_der(&srv_key))
                .unwrap(),
        );
        let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap();
        let app2 = super::super::create_relay_router(test_state().await);
        tokio::spawn(serve_relay_mtls(listener2, revoked_cfg, app2));
        // The client trusts `ca`, but the server now trusts `other_ca` only —
        // the server's client-cert verification rejects the ca-signed cert.
        let status = relay_handshake(&ca, &good, &good_key, addr2).await;
        assert_eq!(status, 0, "a cert whose CA was removed (revoked) must fail");
    }
}
