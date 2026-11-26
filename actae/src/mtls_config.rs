// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use rustls::{pki_types::CertificateDer, server::WebPkiClientVerifier, ServerConfig};
use rustls_pemfile::{certs, private_key};
use std::{io::BufReader, path::Path, sync::Arc};
use tokio::fs;
use tracing::{debug, info, warn};
use x509_parser::prelude::*;

/// mTLS configuration for mutual TLS authentication
pub struct MTLSConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: String,
    pub require_client_cert: bool,
}

impl MTLSConfig {
    /// Create a new mTLS configuration with required client certificates
    pub fn new(cert_path: String, key_path: String, ca_cert_path: String) -> Self {
        Self {
            cert_path,
            key_path,
            ca_cert_path,
            require_client_cert: true,
        }
    }

    /// Create a new TLS configuration without client certificate requirement
    pub fn new_tls_only(cert_path: String, key_path: String, _ca_cert_path: String) -> Self {
        let ca = cert_path.clone();
        Self {
            cert_path,
            key_path,
            ca_cert_path: ca,
            require_client_cert: false,
        }
    }

    /// Set whether client certificates are required
    pub fn require_client_cert(mut self, require: bool) -> Self {
        self.require_client_cert = require;
        self
    }

    /// Load server configuration with full mTLS client certificate validation
    pub async fn load_server_config(&self) -> Result<RustlsConfig> {
        info!(" Loading production-grade mTLS configuration");
        info!(" Server certificate: {}", self.cert_path);
        info!(" Server private key: {}", self.key_path);
        info!("CA certificate: {}", self.ca_cert_path);

        // Validate all files exist
        self.validate_certificate_files().await?;

        if self.require_client_cert {
            info!(" Client certificate validation: REQUIRED");
            self.load_mtls_config().await
        } else {
            info!(" Client certificate validation: OPTIONAL");
            self.load_tls_config().await
        }
    }

    /// Load full mTLS configuration with client certificate validation
    async fn load_mtls_config(&self) -> Result<RustlsConfig> {
        // Load server certificate and key
        let cert_file = fs::File::open(&self.cert_path)
            .await
            .context("Failed to open server certificate file")?;
        let key_file = fs::File::open(&self.key_path)
            .await
            .context("Failed to open server private key file")?;
        let ca_file = fs::File::open(&self.ca_cert_path)
            .await
            .context("Failed to open CA certificate file")?;

        // Parse server certificate chain
        let cert_chain = certs(&mut BufReader::new(cert_file.into_std().await))
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to parse server certificate")?;

        if cert_chain.is_empty() {
            return Err(anyhow::anyhow!(
                "No certificates found in server certificate file"
            ));
        }

        // Parse private key
        let private_key = private_key(&mut BufReader::new(key_file.into_std().await))
            .context("Failed to parse server private key")?
            .ok_or_else(|| anyhow::anyhow!("No private key found in key file"))?;

        // Load CA certificate for client validation
        let ca_certs = certs(&mut BufReader::new(ca_file.into_std().await))
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to parse CA certificate")?;

        if ca_certs.is_empty() {
            return Err(anyhow::anyhow!(
                "No CA certificates found in CA certificate file"
            ));
        }

        let mut root_store = rustls::RootCertStore::empty();
        let ca_der: Vec<CertificateDer> = ca_certs
            .iter()
            .map(|c| CertificateDer::from(c.to_vec()))
            .collect();
        let (added, _ignored) = root_store.add_parsable_certificates(ca_der);
        if added == 0 {
            return Err(anyhow::anyhow!(
                "No valid CA certificates could be added to the root store"
            ));
        }

        // Create client certificate verifier
        let client_cert_verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .context("Failed to build client certificate verifier")?;

        // Create server configuration with mTLS
        let config = ServerConfig::builder()
            .with_client_cert_verifier(client_cert_verifier)
            .with_single_cert(cert_chain, private_key)
            .context("Failed to create server config with client certificate validation")?;

        info!(" mTLS server configuration created successfully");
        info!(" Client certificate validation: ENABLED");
        info!("Trusted CA certificates: {}", ca_certs.len());

        Ok(RustlsConfig::from_config(Arc::new(config)))
    }

    /// Load basic TLS configuration without client certificate validation
    async fn load_tls_config(&self) -> Result<RustlsConfig> {
        let config = RustlsConfig::from_pem_file(&self.cert_path, &self.key_path)
            .await
            .context("Failed to load TLS certificate and key")?;

        info!(" Basic TLS server configuration created successfully");
        info!("Client certificate validation: DISABLED");

        Ok(config)
    }

    /// Validate that all required certificate files exist and are readable
    async fn validate_certificate_files(&self) -> Result<()> {
        let files = [
            (&self.cert_path, "Server certificate"),
            (&self.key_path, "Server private key"),
            (&self.ca_cert_path, "CA certificate"),
        ];

        for (path, description) in &files {
            if !Path::new(path).exists() {
                return Err(anyhow::anyhow!("{description} file not found: {path}"));
            }

            // Check if file is readable
            match fs::read_to_string(path).await {
                Ok(content) => {
                    if content.is_empty() {
                        return Err(anyhow::anyhow!("{description} file is empty: {path}"));
                    }
                    debug!(" {} file validated: {}", description, path);
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Cannot read {description} file {path}: {e}"
                    ));
                }
            }
        }

        info!(" All mTLS certificate files validated");
        Ok(())
    }
}

/// Extract client certificate information from a DER-encoded X.509 certificate.
/// Uses the x509-parser crate for proper ASN.1 parsing and field extraction.
pub fn extract_client_info_from_cert(cert_der: &[u8]) -> Option<ClientCertInfo> {
    match parse_x509_certificate(cert_der) {
        Ok((_, cert)) => {
            let subject = cert.subject().to_string();
            // Extract the CN (Common Name) from the subject.
            let common_name = cert
                .subject()
                .iter_common_name()
                .next()
                .and_then(|cn| cn.as_str().ok())
                .unwrap_or("")
                .to_string();
            let serial_number = cert.raw_serial_as_string();
            Some(ClientCertInfo {
                common_name,
                subject,
                serial_number,
                is_valid: true,
            })
        }
        Err(e) => {
            warn!("Failed to parse client certificate: {e}");
            None
        }
    }
}

/// Client certificate information extracted from the connection
#[derive(Debug, Clone)]
pub struct ClientCertInfo {
    pub common_name: String,
    pub subject: String,
    pub serial_number: String,
    pub is_valid: bool,
}

impl ClientCertInfo {
    pub fn unknown() -> Self {
        Self {
            common_name: "unknown".to_string(),
            subject: "unknown".to_string(),
            serial_number: "unknown".to_string(),
            is_valid: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_mtls_new_requires_client_cert() {
        let config = MTLSConfig::new(
            "/path/to/cert.pem".into(),
            "/path/to/key.pem".into(),
            "/path/to/ca.pem".into(),
        );
        assert_eq!(config.cert_path, "/path/to/cert.pem");
        assert_eq!(config.key_path, "/path/to/key.pem");
        assert_eq!(config.ca_cert_path, "/path/to/ca.pem");
        assert!(config.require_client_cert);
    }

    #[test]
    fn test_mtls_new_tls_only_config() {
        let config = MTLSConfig::new_tls_only(
            "/path/to/cert.pem".into(),
            "/path/to/key.pem".into(),
            "/path/to/ca.pem".into(),
        );
        assert_eq!(config.cert_path, "/path/to/cert.pem");
        assert_eq!(config.key_path, "/path/to/key.pem");
        assert_eq!(config.ca_cert_path, config.cert_path);
        assert!(!config.require_client_cert);
    }

    #[test]
    fn test_require_client_cert_builder_chaining() {
        let config = MTLSConfig::new("/cert.pem".into(), "/key.pem".into(), "/ca.pem".into())
            .require_client_cert(false);
        assert!(!config.require_client_cert);

        let config = config.require_client_cert(true);
        assert!(config.require_client_cert);
    }

    #[test]
    fn test_client_cert_info_unknown() {
        let info = ClientCertInfo::unknown();
        assert_eq!(info.common_name, "unknown");
        assert_eq!(info.subject, "unknown");
        assert_eq!(info.serial_number, "unknown");
        assert!(!info.is_valid);
    }

    #[test]
    fn test_extract_client_info_invalid_der() {
        let result = extract_client_info_from_cert(b"not-a-valid-der-certificate");
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_client_info_empty_bytes() {
        let result = extract_client_info_from_cert(b"");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_validate_certificate_files_nonexistent() {
        let config = MTLSConfig::new(
            "/nonexistent/cert.pem".into(),
            "/nonexistent/key.pem".into(),
            "/nonexistent/ca.pem".into(),
        );
        let result = config.validate_certificate_files().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("file not found"),
            "Error should mention file not found, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_validate_certificate_files_empty() {
        let cert_file = NamedTempFile::new().unwrap();
        let key_file = NamedTempFile::new().unwrap();
        let ca_file = NamedTempFile::new().unwrap();

        let config = MTLSConfig::new(
            cert_file.path().to_string_lossy().into(),
            key_file.path().to_string_lossy().into(),
            ca_file.path().to_string_lossy().into(),
        );
        let result = config.validate_certificate_files().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("file is empty"),
            "Error should mention file is empty, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_validate_certificate_files_valid() {
        let mut cert_file = NamedTempFile::new().unwrap();
        writeln!(
            cert_file,
            "-----BEGIN CERTIFICATE-----\ndummy\n-----END CERTIFICATE-----"
        )
        .unwrap();
        let mut key_file = NamedTempFile::new().unwrap();
        writeln!(
            key_file,
            "-----BEGIN PRIVATE KEY-----\ndummy\n-----END PRIVATE KEY-----"
        )
        .unwrap();
        let mut ca_file = NamedTempFile::new().unwrap();
        writeln!(
            ca_file,
            "-----BEGIN CERTIFICATE-----\ndummy\n-----END CERTIFICATE-----"
        )
        .unwrap();

        let config = MTLSConfig::new(
            cert_file.path().to_string_lossy().into(),
            key_file.path().to_string_lossy().into(),
            ca_file.path().to_string_lossy().into(),
        );
        let result = config.validate_certificate_files().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_certificate_files_one_missing() {
        let mut cert_file = NamedTempFile::new().unwrap();
        writeln!(cert_file, "content").unwrap();
        let mut key_file = NamedTempFile::new().unwrap();
        writeln!(key_file, "content").unwrap();

        let config = MTLSConfig::new(
            cert_file.path().to_string_lossy().into(),
            key_file.path().to_string_lossy().into(),
            "/nonexistent/ca.pem".into(),
        );
        let result = config.validate_certificate_files().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("CA certificate"),
            "Error should mention CA certificate, got: {err}"
        );
    }
}
