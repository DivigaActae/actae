// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Gateway → SaaS internal client (Phase 5).
//!
//! The gateway talks to the SaaS's `/v1/internal/*` endpoints over HTTP with
//! a shared bearer token (edge mTLS in production). It never receives a SaaS
//! DB credential. This is the ONLY source of instance routing (base URLs,
//! remote-access policy, tags) and of purpose-bound instance tokens.

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::time::Duration;

fn str_slice_is_empty(v: &[String]) -> bool {
    v.is_empty()
}

/// One routed instance from the SaaS routing manifest.
#[derive(Debug, Clone, Deserialize)]
pub struct RoutedInstance {
    pub instance_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub remote_access: RemoteAccessWire,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RemoteAccessWire {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub policy: serde_json::Value,
    #[serde(default)]
    pub policy_version: u64,
}

/// The gateway's routing view of an organization.
#[derive(Debug, Clone, Deserialize)]
pub struct OrgRouting {
    pub organization_id: String,
    #[serde(default)]
    pub instances: Vec<RoutedInstance>,
}

/// A purpose-bound instance token minted by the SaaS for the gateway.
#[derive(Debug, Clone)]
pub struct InstanceToken {
    pub token: String,
    pub expires_in: u64,
}

#[derive(Clone)]
pub struct SaasInternalClient {
    base_url: String,
    internal_token: String,
    http: reqwest::Client,
}

impl std::fmt::Debug for SaasInternalClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaasInternalClient")
            .field("base_url", &self.base_url)
            .field("internal_token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl SaasInternalClient {
    pub fn new(
        base_url: &str,
        internal_token: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
        ca_bundle_path: Option<&str>,
        client_cert_path: Option<&str>,
        client_key_path: Option<&str>,
    ) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .timeout(request_timeout)
            .connect_timeout(connect_timeout)
            .user_agent(format!(
                "actae-federation-gateway/{}",
                env!("CARGO_PKG_VERSION")
            ));
        // PR-012: pin the SaaS CA and present the gateway's own client
        // identity (mTLS workload identity). A wrong CA / hostname / expired
        // client cert fails the handshake — no fallback to bearer-only.
        if let Some(ca) = ca_bundle_path {
            let pem = std::fs::read(ca).context("failed to read GATEWAY_SAAS_CA_BUNDLE")?;
            let cert = reqwest::Certificate::from_pem(&pem)
                .context("GATEWAY_SAAS_CA_BUNDLE is not a valid PEM CA bundle")?;
            builder = builder.add_root_certificate(cert);
        }
        if let (Some(cert), Some(key)) = (client_cert_path, client_key_path) {
            // reqwest's from_pem takes a single PEM; concatenate cert + key.
            let mut pem = std::fs::read(cert).context("failed to read GATEWAY_SAAS_CLIENT_CERT")?;
            pem.extend_from_slice(
                &std::fs::read(key).context("failed to read GATEWAY_SAAS_CLIENT_KEY")?,
            );
            let identity = reqwest::Identity::from_pem(&pem)
                .context("GATEWAY_SAAS_CLIENT_CERT/KEY is not a valid PEM identity")?;
            builder = builder.identity(identity);
        }
        let http = builder
            .build()
            .context("failed to build SaaS internal HTTP client")?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            internal_token: internal_token.to_string(),
            http,
        })
    }

    fn auth(&self) -> String {
        format!("Bearer {}", self.internal_token)
    }

    /// Fetch the org routing manifest (instances + remote access + tags).
    pub async fn org_routing(&self, org: &str) -> Result<OrgRouting> {
        let url = format!(
            "{}/v1/internal/organizations/{}/instances",
            self.base_url, org
        );
        let resp = self
            .http
            .get(&url)
            .header("authorization", self.auth())
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        if !resp.status().is_success() {
            return Err(anyhow!("SaaS routing returned {}", resp.status()));
        }
        resp.json()
            .await
            .with_context(|| format!("invalid routing manifest from {url}"))
    }

    /// Find a routable instance by id. Requires remote access enabled AND a
    /// base URL (hosted) or an operator-provided override.
    pub fn routable<'a>(
        &'a self,
        routing: &'a OrgRouting,
        instance_id: &str,
    ) -> Option<(&'a RoutedInstance, String)> {
        let inst = routing
            .instances
            .iter()
            .find(|i| i.instance_id == instance_id)?;
        if !inst.remote_access.enabled {
            return None;
        }
        let base = inst
            .base_url
            .clone()
            .or_else(|| self.instance_override(instance_id));
        base.map(|b| (inst, b))
    }

    fn instance_override(&self, _instance_id: &str) -> Option<String> {
        // Set at construction from the operator config (dev/test); production
        // hosted instances come from the routing manifest. Never from a
        // customer request.
        None
    }

    /// Mint a short-lived purpose-bound instance token via the SaaS internal
    /// exchange (the gateway's only token source).
    #[allow(clippy::too_many_arguments)]
    pub async fn mint_instance_token(
        &self,
        org: &str,
        instance_id: &str,
        purpose: &str,
        operation_id: Option<&str>,
        action: Option<&str>,
        resources: &[String],
        channels: &[String],
        request_digest: Option<&str>,
    ) -> Result<InstanceToken> {
        let url = format!("{}/v1/internal/fleet/token", self.base_url);
        #[derive(serde::Serialize)]
        struct Body<'a> {
            organization_id: &'a str,
            instance_id: &'a str,
            purpose: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            operation_id: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            action: Option<&'a str>,
            #[serde(skip_serializing_if = "str_slice_is_empty")]
            resources: &'a [String],
            #[serde(skip_serializing_if = "str_slice_is_empty")]
            channels: &'a [String],
            #[serde(skip_serializing_if = "Option::is_none")]
            request_digest: Option<&'a str>,
        }
        let body = Body {
            organization_id: org,
            instance_id,
            purpose,
            operation_id,
            action,
            resources,
            channels,
            request_digest,
        };
        let resp = self
            .http
            .post(&url)
            .header("authorization", self.auth())
            .json(&body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        if !resp.status().is_success() {
            return Err(anyhow!("SaaS token exchange returned {}", resp.status()));
        }
        #[derive(Deserialize)]
        struct Wire {
            token: String,
            #[serde(default)]
            expires_in: u64,
        }
        let wire: Wire = resp
            .json()
            .await
            .context("invalid token exchange response")?;
        Ok(InstanceToken {
            token: wire.token,
            expires_in: wire.expires_in,
        })
    }

    /// Atomically consume a one-use stream ticket (Origin-bound).
    pub async fn consume_stream_ticket(
        &self,
        ticket: &str,
        origin: Option<&str>,
    ) -> Result<ConsumedTicket> {
        let url = format!("{}/v1/internal/stream-tickets/consume", self.base_url);
        #[derive(serde::Serialize)]
        struct Body<'a> {
            ticket: &'a str,
        }
        let mut req = self
            .http
            .post(&url)
            .header("authorization", self.auth())
            .json(&Body { ticket });
        if let Some(origin) = origin {
            req = req.header("origin", origin);
        }
        let resp = req.send().await.with_context(|| format!("POST {url}"))?;
        if !resp.status().is_success() {
            return Err(anyhow!("ticket consume returned {}", resp.status()));
        }
        resp.json().await.context("invalid ticket consume response")
    }

    /// Acquire (or renew) the fenced connector lease for an instance
    /// (PR-010). Returns the fencing generation; a bumped generation means
    /// this gateway was fenced by a takeover and MUST stop publishing.
    pub async fn acquire_lease(
        &self,
        instance_id: &str,
        gateway_id: &str,
        ttl_secs: u64,
    ) -> Result<LeaseInfo> {
        let url = format!(
            "{}/v1/internal/instances/{instance_id}/leases",
            self.base_url
        );
        #[derive(serde::Serialize)]
        struct Body<'a> {
            gateway_id: &'a str,
            ttl_secs: u64,
        }
        let resp = self
            .http
            .post(&url)
            .header("authorization", self.auth())
            .json(&Body {
                gateway_id,
                ttl_secs,
            })
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        if !resp.status().is_success() {
            return Err(anyhow!("lease acquire returned {}", resp.status()));
        }
        resp.json().await.context("invalid lease response")
    }

    /// Release the connector lease (graceful shutdown) — but ONLY when this
    /// gateway + fencing generation still holds it (Deep-003). A stale owner
    /// releasing after a takeover must NOT erase the replacement's lease; the
    /// SaaS returns 409 in that case and the release is a no-op here.
    pub async fn release_lease(
        &self,
        instance_id: &str,
        gateway_id: &str,
        fencing_generation: u64,
    ) -> Result<()> {
        let url = format!(
            "{}/v1/internal/instances/{instance_id}/leases/release",
            self.base_url
        );
        #[derive(serde::Serialize)]
        struct Body<'a> {
            gateway_id: &'a str,
            fencing_generation: u64,
        }
        let resp = self
            .http
            .post(&url)
            .header("authorization", self.auth())
            .json(&Body {
                gateway_id,
                fencing_generation,
            })
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        if resp.status() == reqwest::StatusCode::CONFLICT {
            // Stale release: someone else holds the lease now — nothing to do.
            tracing::debug!(
                instance_id,
                "connector lease release skipped: no longer the holder"
            );
            return Ok(());
        }
        if !resp.status().is_success() {
            return Err(anyhow!("lease release returned {}", resp.status()));
        }
        Ok(())
    }

    /// Renew the fenced connector lease (PR-010). A 409 means this gateway was
    /// fenced by a takeover and MUST stop publishing.
    pub async fn renew_lease(
        &self,
        instance_id: &str,
        gateway_id: &str,
        ttl_secs: u64,
    ) -> Result<LeaseInfo> {
        let url = format!(
            "{}/v1/internal/instances/{instance_id}/leases/renew",
            self.base_url
        );
        #[derive(serde::Serialize)]
        struct Body<'a> {
            gateway_id: &'a str,
            ttl_secs: u64,
        }
        let resp = self
            .http
            .post(&url)
            .header("authorization", self.auth())
            .json(&Body {
                gateway_id,
                ttl_secs,
            })
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        if resp.status() == reqwest::StatusCode::CONFLICT {
            return Err(anyhow!("connector lease fenced by a takeover"));
        }
        if !resp.status().is_success() {
            return Err(anyhow!("lease renew returned {}", resp.status()));
        }
        resp.json().await.context("invalid lease response")
    }
}

/// A fenced connector lease response.
#[derive(Debug, Clone, Deserialize)]
pub struct LeaseInfo {
    pub installation_id: String,
    #[serde(default)]
    pub gateway_id: String,
    #[serde(default)]
    pub fencing_generation: u64,
    #[serde(default)]
    pub expires_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumedTicket {
    pub organization_id: String,
    #[serde(default)]
    pub grants: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_builds() {
        let c = SaasInternalClient::new(
            "http://saas:8004",
            "internal-secret",
            Duration::from_secs(5),
            Duration::from_secs(30),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(c.auth(), "Bearer internal-secret");
    }
}
