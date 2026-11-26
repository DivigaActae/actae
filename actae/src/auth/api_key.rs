// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use crate::state::AppState;
use anyhow::Result;

/// Validate an API key and return its database row.
///
/// This is a thin wrapper over the shared resolver in
/// `auth::authorization::resolve_api_key` — there must be no duplicate
/// hash/fallback logic here (audit item 4). The resolver tries
/// HMAC-SHA256 with `api_key_hash_secret` first, then raw SHA-256 for
/// legacy keys, and only returns active, unexpired keys.
pub async fn validate_api_key(
    api_key: &str,
    app_state: &AppState,
) -> Result<Option<crate::database::models::ApiKey>> {
    crate::auth::authorization::resolve_api_key(api_key, app_state).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_module_compiles() {}
}
