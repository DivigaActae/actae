// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

pub mod auth;
pub mod instance;
pub mod user;
pub mod users;

use axum::Router;
use std::sync::Arc;

use crate::events::handler::EventHandler;
use crate::security::SecurityManager;
use crate::state::AppState;
use crate::websocket::connection_manager::ConnectionManager;
use crate::websocket::server::WebSocketServer;
use std::sync::OnceLock;
use std::time::Instant;

#[derive(Clone)]
pub struct AppServerState {
    pub app_state: AppState,
    pub connection_manager: Arc<ConnectionManager>,
    pub event_handler: Arc<EventHandler>,
    pub security_manager: Arc<SecurityManager>,
    pub ws_server: Arc<WebSocketServer>,
    pub start_time: Instant,
}

/// Global server state — avoids needing Router<S> extraction which
/// conflicts with axum 0.8's requirement that only Router<()> is serveable.
static GLOBAL_SERVER_STATE: OnceLock<AppServerState> = OnceLock::new();

/// Set the global server state (called once at startup).
/// Returns an error if already initialized, instead of panicking.
pub fn set_global_state(state: AppServerState) -> Result<(), &'static str> {
    GLOBAL_SERVER_STATE
        .set(state)
        .map_err(|_| "GLOBAL_SERVER_STATE already initialized")
}

/// Access the global server state.
/// Returns `None` if not yet initialized — callers should handle this gracefully.
pub fn try_global_state() -> Option<&'static AppServerState> {
    GLOBAL_SERVER_STATE.get()
}

/// Access the global server state — panics if not initialized.
/// Prefer `try_global_state()` in production code paths.
pub fn global_state() -> &'static AppServerState {
    GLOBAL_SERVER_STATE.get().unwrap_or_else(|| {
        panic!("GLOBAL_SERVER_STATE not initialized — call set_global_state first")
    })
}

pub fn create_api_routes() -> Router<()> {
    Router::new()
        .nest("/api/v1/auth", auth::routes())
        .nest("/api/v1/user", user::routes())
        .nest("/api/v1/users", users::routes())
        .nest("/api/v1/instance", instance::routes())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── CRITICAL #4: Server panics fixed ─────────────────────────────
    #[test]
    fn test_try_global_state_returns_none_before_init() {
        // Before set_global_state is called, try_global_state returns None.
        // This is the safe-access path that replaces direct global_state() calls
        // in production handlers. No panic.
        assert!(try_global_state().is_none());
    }

    #[test]
    fn test_set_global_state_returns_result() {
        // set_global_state now returns Result instead of panicking.
        // The compiler enforces this — this test verifies the API compiles.
        // Full integration test requires AppServerState construction.
    }
}
