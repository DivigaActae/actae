// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use axum::Router;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::fs;
use tracing::{error, info};

use crate::config::AppConfig;
use crate::mtls_config::MTLSConfig;

/// HTTPS server for serving metrics, health checks, and internal API endpoints
pub struct HttpsServer {
    config: Arc<AppConfig>,
    router: Router,
    /// An optional listener already bound to the HTTPS port (reserved by the
    /// managed-storage startup so another process cannot win the port race while
    /// PostgreSQL is being initialized). When absent the server binds itself.
    prebound: Option<std::net::TcpListener>,
}

impl HttpsServer {
    /// Create a new HTTPS server instance
    pub fn new(config: Arc<AppConfig>, router: Router) -> Self {
        Self {
            config,
            router,
            prebound: None,
        }
    }

    /// Create an HTTPS server that serves from an already-bound listener
    /// (the managed-storage reserved HTTP socket).
    pub fn new_with_listener(
        config: Arc<AppConfig>,
        router: Router,
        listener: std::net::TcpListener,
    ) -> Self {
        Self {
            config,
            router,
            prebound: Some(listener),
        }
    }

    /// Start the HTTPS server with TLS configuration
    pub async fn start(&self) -> Result<()> {
        // mTLS is an opt-in hardening tier (default off). Configuration is
        // validated at load time: ENABLE_MTLS requires ENABLE_TLS and a CA.
        if self.config.enable_mtls {
            info!(" mTLS enabled, starting with client certificate validation");
            self.start_with_mtls().await
        } else {
            info!(" mTLS disabled, starting with standard TLS");
            self.start_with_tls().await
        }
    }

    /// Serve from the pre-bound listener (or bind `https_addr` when none was
    /// reserved). Shared tail of the TLS and mTLS startup paths.
    async fn serve_with_tls(
        &self,
        https_addr: SocketAddr,
        tls_config: axum_server::tls_rustls::RustlsConfig,
    ) -> Result<()> {
        let server = match &self.prebound {
            Some(listener) => axum_server::tls_rustls::from_tcp_rustls(
                listener
                    .try_clone()
                    .context("failed to clone reserved HTTP listener")?,
                tls_config,
            )
            .serve(
                self.router
                    .clone()
                    .into_make_service_with_connect_info::<std::net::SocketAddr>(),
            ),
            None => axum_server::bind_rustls(https_addr, tls_config).serve(
                self.router
                    .clone()
                    .into_make_service_with_connect_info::<std::net::SocketAddr>(),
            ),
        };

        if let Err(e) = server.await {
            error!(" HTTPS server error: {}", e);
            return Err(e.into());
        }

        Ok(())
    }

    /// Start the HTTPS server with mTLS (mutual TLS) configuration
    pub async fn start_with_mtls(&self) -> Result<()> {
        // Validate TLS certificate and key paths
        let cert_path = self
            .config
            .tls_cert_path
            .as_ref()
            .context("TLS certificate path is required for HTTPS")?;
        let key_path = self
            .config
            .tls_key_path
            .as_ref()
            .context("TLS private key path is required for HTTPS")?;

        // Determine CA certificate path (mTLS only): explicit env config or
        // fall back to `ca.crt` next to the server certificate.
        let ca_cert_path = self.config.ca_cert_path.clone().unwrap_or_else(|| {
            let cert_dir = std::path::Path::new(cert_path)
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."));
            cert_dir.join("ca.crt").to_string_lossy().to_string()
        });

        // Get HTTPS port
        let https_port = self.config.https_port.unwrap_or(8002);

        // Create socket address
        let https_addr = SocketAddr::from((
            self.config.host.parse::<std::net::IpAddr>().map_err(|e| {
                error!(" Invalid host address '{}': {}", self.config.host, e);
                e
            })?,
            https_port,
        ));

        info!(" Start HTTPS server with mTLS on {}", https_addr);
        info!(" Using TLS certificate: {}", cert_path);
        info!(" Using TLS private key: {}", key_path);
        info!("Using CA certificate: {}", ca_cert_path);

        // Create mTLS configuration with client certificate validation required
        let mtls_config = MTLSConfig::new(cert_path.clone(), key_path.clone(), ca_cert_path)
            .require_client_cert(true);

        // Load mTLS configuration
        let tls_config = mtls_config
            .load_server_config()
            .await
            .context("Failed to load mTLS configuration")?;

        // Create HTTPS server with mTLS
        info!(" HTTPS server with mTLS starting on {}", https_addr);
        info!(
            " Metrics available at https://{}{} (requires client certificate)",
            https_addr, self.config.metrics_path
        );
        info!(
            " Health check available at https://{}{} (requires client certificate)",
            https_addr, self.config.health_check_path
        );

        // Start the server (from the reserved listener when present)
        self.serve_with_tls(https_addr, tls_config).await
    }

    /// Start the HTTPS server with standard TLS configuration (no client certificate)
    pub async fn start_with_tls(&self) -> Result<()> {
        // Validate TLS certificate and key paths
        let cert_path = self
            .config
            .tls_cert_path
            .as_ref()
            .context("TLS certificate path is required for HTTPS")?;
        let key_path = self
            .config
            .tls_key_path
            .as_ref()
            .context("TLS private key path is required for HTTPS")?;

        // Get HTTPS port
        let https_port = self.config.https_port.unwrap_or(8002);

        // Create socket address
        let https_addr = SocketAddr::from((
            self.config.host.parse::<std::net::IpAddr>().map_err(|e| {
                error!(" Invalid host address '{}': {}", self.config.host, e);
                e
            })?,
            https_port,
        ));

        info!(" Starting HTTPS server on {}", https_addr);
        info!(" Using TLS certificate: {}", cert_path);
        info!(" Using TLS private key: {}", key_path);

        // Create TLS configuration without client certificate validation
        let ca_cert_path = ""; // Not used for basic TLS
        let mtls_config = MTLSConfig::new_tls_only(
            cert_path.clone(),
            key_path.clone(),
            ca_cert_path.to_string(),
        );

        // Load TLS configuration
        let tls_config = mtls_config
            .load_server_config()
            .await
            .context("Failed to load TLS configuration")?;

        // Create HTTPS server
        info!(" HTTPS server starting on {}", https_addr);
        info!(
            " Metrics available at https://{}{}",
            https_addr, self.config.metrics_path
        );
        info!(
            " Health check available at https://{}{}",
            https_addr, self.config.health_check_path
        );

        // Start the server (from the reserved listener when present)
        self.serve_with_tls(https_addr, tls_config).await
    }

    /// Validate TLS certificate and key files
    pub async fn validate_tls_files(&self) -> Result<()> {
        let cert_path = self
            .config
            .tls_cert_path
            .as_ref()
            .context("TLS certificate path is required for HTTPS")?;
        let key_path = self
            .config
            .tls_key_path
            .as_ref()
            .context("TLS private key path is required for HTTPS")?;

        // Check if files exist
        if !tokio::fs::try_exists(cert_path).await? {
            return Err(anyhow::anyhow!(
                "TLS certificate file not found: {cert_path}"
            ));
        }

        if !tokio::fs::try_exists(key_path).await? {
            return Err(anyhow::anyhow!(
                "TLS private key file not found: {key_path}"
            ));
        }

        // Try to read the files to ensure they're accessible
        let cert_content = fs::read_to_string(cert_path)
            .await
            .context("Failed to read TLS certificate file")?;
        let key_content = fs::read_to_string(key_path)
            .await
            .context("Failed to read TLS private key file")?;

        // Basic validation
        if cert_content.is_empty() {
            return Err(anyhow::anyhow!("TLS certificate file is empty"));
        }

        if key_content.is_empty() {
            return Err(anyhow::anyhow!("TLS private key file is empty"));
        }

        if !cert_content.contains("BEGIN CERTIFICATE") {
            return Err(anyhow::anyhow!(
                "TLS certificate file does not appear to be a valid PEM certificate"
            ));
        }

        if !key_content.contains("BEGIN PRIVATE KEY")
            && !key_content.contains("BEGIN RSA PRIVATE KEY")
        {
            return Err(anyhow::anyhow!(
                "TLS private key file does not appear to be a valid PEM private key"
            ));
        }

        info!(" TLS certificate and key files validated successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;

    #[test]
    fn test_https_server_new() {
        let config = Arc::new(AppConfig::default());
        let router = Router::new();
        let server = HttpsServer::new(config.clone(), router);
        assert!(Arc::ptr_eq(&server.config, &config));
    }

    #[test]
    fn test_https_server_port_default() {
        let config = Arc::new(AppConfig::default());
        let router = Router::new();
        let _server = HttpsServer::new(config, router);
        // HttpsServer doesn't implement Debug, just verify construction
    }
}
