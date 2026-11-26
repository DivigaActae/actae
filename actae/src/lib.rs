#![allow(
// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

    clippy::type_complexity,
    clippy::field_reassign_with_default,
    clippy::uninlined_format_args,
    clippy::assertions_on_constants,
    clippy::too_many_arguments,
    clippy::incompatible_msrv,
    clippy::await_holding_lock,
    clippy::unnecessary_to_owned,
    clippy::unnecessary_literal_unwrap,
    clippy::let_underscore_future,
    clippy::if_same_then_else,
    clippy::only_used_in_recursion,
    clippy::doc_lazy_continuation
)]

pub mod api;
pub mod auth;
pub mod broadcast;
pub mod cli;
/// Local SDK client credentials (`~/.actae/client.json`): the zero-copy
/// "run it and your SDKs are configured" path. See `docs/CLIENT_CREDENTIALS.md`.
pub mod client_credentials;
pub mod config;
pub mod control_plane;
pub mod dashboard;
pub mod database;
pub mod embedded_pg;
pub mod events;
/// Fleet gateway (Phase 5) — a second, narrow binary compiled behind the
/// `federation-gateway` feature so the default server build stays lean and
/// the gateway's routes never share the instance router (RUST-007).
#[cfg(feature = "federation-gateway")]
pub mod federation_gateway;

pub mod https_server;
pub mod metrics;
pub mod mtls_config;
/// Self-hosted outbound relay client (Phase 7). Disabled by default; an
/// enrolled instance dials OUT to the gateway's relay listener.
pub mod relay;

pub mod scheduler;
pub mod secrets;
pub mod security;
pub mod state;
pub mod websocket;

/// Human-readable version string for `actae --version`:
/// `actae <semver> (<git sha>, <profile>)`.
pub fn version_string() -> String {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    format!(
        "actae {} ({}, {})",
        env!("CARGO_PKG_VERSION"),
        env!("ACTAE_GIT_SHA"),
        profile
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_library_compiles() {
        assert!(true);
    }
}
